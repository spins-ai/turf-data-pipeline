#!/usr/bin/env python3
"""
quality/referential_integrity_checker.py
========================================
Integrite referentielle -- Verifie la coherence inter-fichiers.

Controles :
  1. partant_uid dans labels -> doit exister dans partants_master
  2. course_uid dans features -> doit exister dans courses_master
  3. hippodrome_normalise -> doit exister dans hippodromes_db

Echantillonne 10 000 enregistrements par controle (reservoir sampling).
Streaming ligne par ligne, RAM < 2 Go.

Usage :
    python quality/referential_integrity_checker.py
    python quality/referential_integrity_checker.py --data-dir path/to/data_master
    python quality/referential_integrity_checker.py --sample-size 50000
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
from pathlib import Path

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

DATA_MASTER = _PROJECT_ROOT / "data_master"
DEFAULT_OUTPUT = _PROJECT_ROOT / "quality" / "referential_integrity_report.md"
DEFAULT_SAMPLE_SIZE = 10_000
RESERVOIR_SEED = 42

log = logging.getLogger(__name__)


# ===========================================================================
# STREAMING HELPERS
# ===========================================================================


def _stream_field_set(filepath: Path, field: str) -> set[str]:
    """Stream a JSONL file and collect unique values for *field* as strings.

    Returns the full set (needed for reference lookups).  For very large
    files this is still bounded by unique cardinality, not record count.
    """
    values: set[str] = set()
    if not filepath.exists():
        log.warning("File not found: %s", filepath)
        return values

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
            val = rec.get(field)
            if val is not None:
                values.add(str(val))
    return values


def _reservoir_field_sample(
    filepath: Path, field: str, k: int, seed: int
) -> list[str]:
    """Reservoir-sample *k* non-null values of *field* from a JSONL file."""
    rng = random.Random(seed)
    reservoir: list[str] = []
    n = 0

    if not filepath.exists():
        log.warning("File not found for sampling: %s", filepath)
        return reservoir

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
            val = rec.get(field)
            if val is None:
                continue
            n += 1
            sval = str(val)
            if n <= k:
                reservoir.append(sval)
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    reservoir[j] = sval
    return reservoir


def _load_hippodromes_db() -> set[str]:
    """Load hippodromes_db keys from the Python module."""
    try:
        from hippodromes_db import HIPPODROMES_DB
        return set(HIPPODROMES_DB.keys())
    except ImportError:
        # Try alternate location
        db_path = _PROJECT_ROOT / "hippodromes_db.py"
        if db_path.exists():
            import importlib.util
            spec = importlib.util.spec_from_file_location("hippodromes_db", db_path)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)  # type: ignore[union-attr]
                return set(getattr(mod, "HIPPODROMES_DB", {}).keys())
        log.warning("hippodromes_db module not found")
        return set()


# ===========================================================================
# CHECKS
# ===========================================================================


class CheckResult:
    """Result of a single referential integrity check."""

    def __init__(self, name: str, description: str) -> None:
        self.name = name
        self.description = description
        self.sample_size = 0
        self.reference_size = 0
        self.orphans: int = 0
        self.orphan_examples: list[str] = []
        self.elapsed: float = 0.0
        self.skipped: bool = False
        self.skip_reason: str = ""

    @property
    def passed(self) -> bool:
        return not self.skipped and self.orphans == 0


def check_partant_uid_in_labels(
    data_dir: Path, sample_size: int, seed: int
) -> CheckResult:
    """partant_uid in enriched labels files must exist in partants_master."""
    result = CheckResult(
        "partant_uid (labels -> partants_master)",
        "Chaque partant_uid dans les fichiers enrichis doit exister "
        "dans partants_master.jsonl",
    )
    t0 = time.perf_counter()

    partants_file = data_dir / "partants_master.jsonl"
    if not partants_file.exists():
        result.skipped = True
        result.skip_reason = f"{partants_file.name} introuvable"
        return result

    # Build reference set from partants_master
    log.info("Building partant_uid reference set from %s ...", partants_file.name)
    ref_uids = _stream_field_set(partants_file, "partant_uid")
    result.reference_size = len(ref_uids)

    if not ref_uids:
        result.skipped = True
        result.skip_reason = "Aucun partant_uid dans partants_master"
        return result

    # Find enriched/labels files to check
    label_files = sorted(data_dir.glob("partants_master_enrichi*.jsonl"))
    if not label_files:
        result.skipped = True
        result.skip_reason = "Aucun fichier enrichi trouve"
        return result

    # Sample from all label files combined
    all_sampled_uids: list[str] = []
    per_file = max(1, sample_size // len(label_files))
    for lf in label_files:
        log.info("Sampling %d partant_uid from %s ...", per_file, lf.name)
        sampled = _reservoir_field_sample(lf, "partant_uid", per_file, seed)
        all_sampled_uids.extend(sampled)

    result.sample_size = len(all_sampled_uids)
    orphans: list[str] = []
    for uid in all_sampled_uids:
        if uid not in ref_uids:
            orphans.append(uid)

    result.orphans = len(orphans)
    result.orphan_examples = orphans[:10]
    result.elapsed = time.perf_counter() - t0
    return result


def check_course_uid_in_courses(
    data_dir: Path, sample_size: int, seed: int
) -> CheckResult:
    """course_uid in partants_master must exist in courses_master."""
    result = CheckResult(
        "course_uid (partants -> courses_master)",
        "Chaque course_uid dans partants_master doit exister "
        "dans courses_master.jsonl",
    )
    t0 = time.perf_counter()

    courses_file = data_dir / "courses_master.jsonl"
    partants_file = data_dir / "partants_master.jsonl"

    if not courses_file.exists():
        result.skipped = True
        result.skip_reason = f"{courses_file.name} introuvable"
        return result
    if not partants_file.exists():
        result.skipped = True
        result.skip_reason = f"{partants_file.name} introuvable"
        return result

    log.info("Building course_uid reference set from %s ...", courses_file.name)
    ref_uids = _stream_field_set(courses_file, "course_uid")
    # Also try id_course / idCourse as course_uid may be stored differently
    if not ref_uids:
        for alt_field in ("id_course", "idCourse", "course_id"):
            ref_uids = _stream_field_set(courses_file, alt_field)
            if ref_uids:
                log.info("Used alternate field %s for courses reference", alt_field)
                break
    result.reference_size = len(ref_uids)

    if not ref_uids:
        result.skipped = True
        result.skip_reason = "Aucun course_uid dans courses_master"
        return result

    log.info("Sampling %d course_uid from %s ...", sample_size, partants_file.name)
    sampled = _reservoir_field_sample(partants_file, "course_uid", sample_size, seed)
    result.sample_size = len(sampled)

    orphans: list[str] = []
    for uid in sampled:
        if uid not in ref_uids:
            orphans.append(uid)

    result.orphans = len(orphans)
    result.orphan_examples = orphans[:10]
    result.elapsed = time.perf_counter() - t0
    return result


def check_hippodrome_in_db(
    data_dir: Path, sample_size: int, seed: int
) -> CheckResult:
    """hippodrome_normalise in partants_master must exist in hippodromes_db."""
    result = CheckResult(
        "hippodrome_normalise (partants -> hippodromes_db)",
        "Chaque hippodrome_normalise dans partants_master doit exister "
        "dans hippodromes_db.HIPPODROMES_DB",
    )
    t0 = time.perf_counter()

    partants_file = data_dir / "partants_master.jsonl"
    if not partants_file.exists():
        result.skipped = True
        result.skip_reason = f"{partants_file.name} introuvable"
        return result

    hippo_db = _load_hippodromes_db()
    result.reference_size = len(hippo_db)

    if not hippo_db:
        result.skipped = True
        result.skip_reason = "hippodromes_db vide ou introuvable"
        return result

    log.info(
        "Sampling %d hippodrome_normalise from %s ...",
        sample_size,
        partants_file.name,
    )
    # Try hippodrome_normalise first, fallback to hippodrome
    sampled = _reservoir_field_sample(
        partants_file, "hippodrome_normalise", sample_size, seed
    )
    if not sampled:
        sampled = _reservoir_field_sample(
            partants_file, "hippodrome", sample_size, seed
        )
    result.sample_size = len(sampled)

    orphans: list[str] = []
    seen_orphans: set[str] = set()
    for val in sampled:
        normalized = val.strip().lower()
        if normalized not in hippo_db and normalized not in seen_orphans:
            orphans.append(val)
            seen_orphans.add(normalized)

    result.orphans = len(orphans)
    result.orphan_examples = orphans[:10]
    result.elapsed = time.perf_counter() - t0
    return result


# ===========================================================================
# REPORT
# ===========================================================================


def build_report(results: list[CheckResult]) -> str:
    lines: list[str] = []
    lines.append("# Referential Integrity Report\n")

    for r in results:
        icon = "SKIP" if r.skipped else ("PASS" if r.passed else "FAIL")
        lines.append(f"## [{icon}] {r.name}\n")
        lines.append(f"_{r.description}_\n")

        if r.skipped:
            lines.append(f"**Ignore** : {r.skip_reason}\n")
            continue

        lines.append(f"- Reference set : {r.reference_size:,} valeurs uniques")
        lines.append(f"- Echantillon   : {r.sample_size:,} enregistrements")
        lines.append(f"- Orphelins     : {r.orphans:,}")
        if r.orphan_examples:
            examples_str = ", ".join(f"`{e}`" for e in r.orphan_examples)
            lines.append(f"- Exemples      : {examples_str}")
        lines.append(f"- Duree         : {r.elapsed:.1f}s\n")

    # Summary
    total_checks = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.skipped and not r.passed)
    skipped = sum(1 for r in results if r.skipped)
    lines.append("## Resume\n")
    lines.append(f"- Controles : {total_checks}")
    lines.append(f"- PASS      : {passed}")
    lines.append(f"- FAIL      : {failed}")
    lines.append(f"- SKIP      : {skipped}")
    status = "PASS" if failed == 0 else "FAIL"
    lines.append(f"- Statut    : **{status}**")

    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Referential integrity checker for turf data pipeline"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_MASTER,
        help="Path to data_master directory",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to write the report (Markdown)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Records to sample per check (default: 10000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=RESERVOIR_SEED,
        help="Random seed for reservoir sampling",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    data_dir: Path = args.data_dir
    if not data_dir.is_dir():
        log.error("Data directory not found: %s", data_dir)
        return 1

    results: list[CheckResult] = []

    log.info("=== Check 1: partant_uid in labels -> partants_master ===")
    results.append(
        check_partant_uid_in_labels(data_dir, args.sample_size, args.seed)
    )

    log.info("=== Check 2: course_uid in partants -> courses_master ===")
    results.append(
        check_course_uid_in_courses(data_dir, args.sample_size, args.seed)
    )

    log.info("=== Check 3: hippodrome_normalise -> hippodromes_db ===")
    results.append(
        check_hippodrome_in_db(data_dir, args.sample_size, args.seed)
    )

    report = build_report(results)

    output_path: Path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    log.info("Report written to %s", output_path)

    failed = sum(1 for r in results if not r.skipped and not r.passed)
    passed = sum(1 for r in results if r.passed)
    skipped = sum(1 for r in results if r.skipped)
    status = "PASS" if failed == 0 else "FAIL"
    print(
        f"\n[referential_integrity] {status} -- "
        f"{passed} passed, {failed} failed, {skipped} skipped"
    )

    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
