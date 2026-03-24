#!/usr/bin/env python3
"""
quality/non_regression_tests.py
===============================
Tests de non-regression par snapshot pour le pipeline turf.

Charge un snapshot precedent (quality/regression_snapshot.json), le compare
avec les statistiques actuelles des fichiers data_master, et echoue si :
  - Le nombre d'enregistrements a diminue
  - Le nombre de champs a diminue
  - La plage de dates s'est retrecie
  - De nouveaux champs 100%% null sont apparus

Apres un passage reussi, sauvegarde un nouveau snapshot.
Streaming ligne par ligne, RAM < 2 Go.

Usage :
    python quality/non_regression_tests.py
    python quality/non_regression_tests.py --data-dir path/to/data_master
    python quality/non_regression_tests.py --snapshot path/to/snapshot.json
    python quality/non_regression_tests.py --update-only  # force snapshot save
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

DATA_MASTER = _PROJECT_ROOT / "data_master"
DEFAULT_SNAPSHOT = _PROJECT_ROOT / "quality" / "regression_snapshot.json"

log = logging.getLogger(__name__)

# Target JSONL files to monitor
TARGET_FILES = [
    "partants_master.jsonl",
    "courses_master.jsonl",
    "partants_master_enrichi.jsonl",
    "partants_master_enrichi_sl.jsonl",
    "partants_master_enrichi_tf.jsonl",
    "course_profiles.jsonl",
    "horse_career_stats.jsonl",
    "jockey_stats.jsonl",
]

# Date field candidates (checked in order, first found wins)
DATE_FIELD_CANDIDATES = [
    "date_reunion_iso",
    "date_course",
    "date",
    "dateReunion",
]


# ===========================================================================
# STATS COLLECTOR
# ===========================================================================


class FileStats:
    """Collects statistics for a single JSONL file via streaming."""

    def __init__(self) -> None:
        self.record_count: int = 0
        self.field_counts: dict[str, int] = defaultdict(int)
        self.null_counts: dict[str, int] = defaultdict(int)
        self.all_fields: set[str] = set()
        self.date_min: str | None = None
        self.date_max: str | None = None

    def ingest(self, rec: dict) -> None:
        self.record_count += 1
        for key, val in rec.items():
            self.all_fields.add(key)
            self.field_counts[key] += 1
            if val is None or (isinstance(val, str) and val.strip() == ""):
                self.null_counts[key] += 1

        # Track date range
        for df in DATE_FIELD_CANDIDATES:
            date_val = rec.get(df)
            if date_val and isinstance(date_val, str) and len(date_val) >= 10:
                date_str = date_val[:10]
                if self.date_min is None or date_str < self.date_min:
                    self.date_min = date_str
                if self.date_max is None or date_str > self.date_max:
                    self.date_max = date_str
                break

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dict."""
        # Fields that are 100% null
        full_null_fields = sorted(
            f
            for f in self.all_fields
            if self.null_counts.get(f, 0) == self.field_counts.get(f, 0)
            and self.field_counts.get(f, 0) > 0
        )
        return {
            "record_count": self.record_count,
            "field_count": len(self.all_fields),
            "fields": sorted(self.all_fields),
            "date_min": self.date_min,
            "date_max": self.date_max,
            "full_null_fields": full_null_fields,
        }


def collect_file_stats(filepath: Path) -> FileStats:
    """Stream a JSONL file and collect statistics."""
    stats = FileStats()
    with open(filepath, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            stats.ingest(rec)
    return stats


# ===========================================================================
# SNAPSHOT
# ===========================================================================


def build_snapshot(data_dir: Path) -> dict[str, Any]:
    """Build a snapshot of current data statistics."""
    snapshot: dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "files": {},
    }

    for fname in TARGET_FILES:
        fpath = data_dir / fname
        if not fpath.exists():
            log.info("Skipping %s (not found)", fname)
            continue
        log.info("Collecting stats for %s ...", fname)
        t0 = time.perf_counter()
        stats = collect_file_stats(fpath)
        elapsed = time.perf_counter() - t0
        log.info(
            "  %s: %d records, %d fields in %.1fs",
            fname,
            stats.record_count,
            len(stats.all_fields),
            elapsed,
        )
        snapshot["files"][fname] = stats.to_dict()

    return snapshot


def save_snapshot(snapshot: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info("Snapshot saved to %s", path)


def load_snapshot(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Failed to load snapshot: %s", exc)
        return None


# ===========================================================================
# REGRESSION CHECKS
# ===========================================================================


class RegressionFailure:
    def __init__(self, file: str, check: str, detail: str) -> None:
        self.file = file
        self.check = check
        self.detail = detail

    def __str__(self) -> str:
        return f"[{self.file}] {self.check}: {self.detail}"


def compare_snapshots(
    prev: dict[str, Any], curr: dict[str, Any]
) -> list[RegressionFailure]:
    """Compare two snapshots and return a list of failures."""
    failures: list[RegressionFailure] = []

    prev_files = prev.get("files", {})
    curr_files = curr.get("files", {})

    for fname, prev_stats in prev_files.items():
        if fname not in curr_files:
            failures.append(
                RegressionFailure(
                    fname,
                    "fichier_absent",
                    "Le fichier existait dans le snapshot precedent mais est "
                    "absent maintenant",
                )
            )
            continue

        curr_stats = curr_files[fname]

        # 1. Record count must not decrease
        prev_rc = prev_stats.get("record_count", 0)
        curr_rc = curr_stats.get("record_count", 0)
        if curr_rc < prev_rc:
            failures.append(
                RegressionFailure(
                    fname,
                    "record_count_decrease",
                    f"Avant: {prev_rc:,} -> Maintenant: {curr_rc:,} "
                    f"(perte de {prev_rc - curr_rc:,} enregistrements)",
                )
            )

        # 2. Field count must not decrease
        prev_fc = prev_stats.get("field_count", 0)
        curr_fc = curr_stats.get("field_count", 0)
        if curr_fc < prev_fc:
            # Find which fields were lost
            prev_fields = set(prev_stats.get("fields", []))
            curr_fields = set(curr_stats.get("fields", []))
            lost = sorted(prev_fields - curr_fields)
            lost_str = ", ".join(lost[:10])
            failures.append(
                RegressionFailure(
                    fname,
                    "field_count_decrease",
                    f"Avant: {prev_fc} -> Maintenant: {curr_fc} "
                    f"(champs perdus: {lost_str})",
                )
            )

        # 3. Date range must not shrink
        prev_dmin = prev_stats.get("date_min")
        prev_dmax = prev_stats.get("date_max")
        curr_dmin = curr_stats.get("date_min")
        curr_dmax = curr_stats.get("date_max")

        if prev_dmin and curr_dmin and curr_dmin > prev_dmin:
            failures.append(
                RegressionFailure(
                    fname,
                    "date_range_shrunk_start",
                    f"date_min: {prev_dmin} -> {curr_dmin} "
                    f"(perte de donnees anciennes)",
                )
            )
        if prev_dmax and curr_dmax and curr_dmax < prev_dmax:
            failures.append(
                RegressionFailure(
                    fname,
                    "date_range_shrunk_end",
                    f"date_max: {prev_dmax} -> {curr_dmax} "
                    f"(perte de donnees recentes)",
                )
            )

        # 4. New 100%-null fields must not appear
        prev_null = set(prev_stats.get("full_null_fields", []))
        curr_null = set(curr_stats.get("full_null_fields", []))
        new_nulls = sorted(curr_null - prev_null)
        if new_nulls:
            nulls_str = ", ".join(new_nulls[:10])
            failures.append(
                RegressionFailure(
                    fname,
                    "new_full_null_fields",
                    f"{len(new_nulls)} nouveau(x) champ(s) 100%% null: "
                    f"{nulls_str}",
                )
            )

    return failures


# ===========================================================================
# REPORT
# ===========================================================================


def build_report(
    failures: list[RegressionFailure],
    prev_snapshot: dict[str, Any] | None,
    curr_snapshot: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append("# Non-Regression Test Report\n")

    if prev_snapshot is None:
        lines.append("**Premier run** : aucun snapshot precedent trouve.")
        lines.append("Snapshot initial sauvegarde.\n")
        # Show current stats
        lines.append("## Statistiques actuelles\n")
        lines.append("| Fichier | Records | Champs | Date min | Date max |")
        lines.append("|---------|---------|--------|----------|----------|")
        for fname, stats in curr_snapshot.get("files", {}).items():
            lines.append(
                f"| {fname} "
                f"| {stats.get('record_count', 0):,} "
                f"| {stats.get('field_count', 0)} "
                f"| {stats.get('date_min', 'N/A')} "
                f"| {stats.get('date_max', 'N/A')} |"
            )
        return "\n".join(lines)

    prev_ts = prev_snapshot.get("timestamp", "inconnu")
    curr_ts = curr_snapshot.get("timestamp", "inconnu")
    lines.append(f"- Snapshot precedent : `{prev_ts}`")
    lines.append(f"- Snapshot actuel    : `{curr_ts}`\n")

    if not failures:
        lines.append("## Resultat : PASS\n")
        lines.append("Aucune regression detectee.\n")
    else:
        lines.append(f"## Resultat : FAIL ({len(failures)} regression(s))\n")
        lines.append("| Fichier | Controle | Detail |")
        lines.append("|---------|----------|--------|")
        for f in failures:
            lines.append(f"| {f.file} | {f.check} | {f.detail} |")

    # Comparison table
    lines.append("\n## Comparaison\n")
    lines.append(
        "| Fichier | Records (avant) | Records (apres) | "
        "Champs (avant) | Champs (apres) |"
    )
    lines.append("|---------|-----------------|-----------------|"
                 "----------------|----------------|")
    all_files = sorted(
        set(prev_snapshot.get("files", {})) | set(curr_snapshot.get("files", {}))
    )
    for fname in all_files:
        ps = prev_snapshot.get("files", {}).get(fname, {})
        cs = curr_snapshot.get("files", {}).get(fname, {})
        lines.append(
            f"| {fname} "
            f"| {ps.get('record_count', '-'):,} "
            f"| {cs.get('record_count', '-'):,} "
            f"| {ps.get('field_count', '-')} "
            f"| {cs.get('field_count', '-')} |"
        )

    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Non-regression snapshot tests for turf data pipeline"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_MASTER,
        help="Path to data_master directory",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=DEFAULT_SNAPSHOT,
        help="Path to regression snapshot JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_PROJECT_ROOT / "quality" / "non_regression_report.md",
        help="Path to write the report (Markdown)",
    )
    parser.add_argument(
        "--update-only",
        action="store_true",
        help="Force update snapshot without comparison",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    data_dir: Path = args.data_dir
    if not data_dir.is_dir():
        log.error("Data directory not found: %s", data_dir)
        return 1

    # Build current snapshot
    log.info("Building current snapshot from %s ...", data_dir)
    t0 = time.perf_counter()
    curr_snapshot = build_snapshot(data_dir)
    elapsed = time.perf_counter() - t0
    log.info("Snapshot built in %.1fs", elapsed)

    if not curr_snapshot["files"]:
        log.error("No data files found in %s", data_dir)
        return 1

    # Load previous snapshot
    snapshot_path: Path = args.snapshot
    prev_snapshot = None if args.update_only else load_snapshot(snapshot_path)

    # Compare
    failures: list[RegressionFailure] = []
    if prev_snapshot is not None:
        failures = compare_snapshots(prev_snapshot, curr_snapshot)

    # Generate report
    report = build_report(failures, prev_snapshot, curr_snapshot)
    output_path: Path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    log.info("Report written to %s", output_path)

    # Save new snapshot only if no failures (or first run / update-only)
    if not failures:
        save_snapshot(curr_snapshot, snapshot_path)
    else:
        log.warning(
            "Snapshot NOT updated due to %d regression(s). "
            "Fix the regressions or use --update-only to force.",
            len(failures),
        )

    # Print summary
    if prev_snapshot is None:
        print(
            f"\n[non_regression] INIT -- Snapshot initial cree avec "
            f"{len(curr_snapshot['files'])} fichier(s)"
        )
        return 0

    if failures:
        print(f"\n[non_regression] FAIL -- {len(failures)} regression(s) detectee(s):")
        for f in failures:
            print(f"  - {f}")
        return 2
    else:
        print(
            f"\n[non_regression] PASS -- Aucune regression "
            f"({len(curr_snapshot['files'])} fichier(s) verifies)"
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())
