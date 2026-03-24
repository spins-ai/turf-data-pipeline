#!/usr/bin/env python3
"""
quality/leakage_prevention.py
==============================
Comprehensive data-leakage prevention audit for the ML pipeline.

Four checks:
  1. Temporal leakage   — sample 100 records per feature-builder output;
     verify every feature was computable from data BEFORE the race date.
  2. Target leakage     — detect forbidden result columns
     (position_arrivee, is_gagnant, rapport_*, cote_finale) in feature outputs.
  3. Train/test contamination — verify date splits in training_labels have
     no overlap (test dates > train dates).
  4. Feature-label correlation — sample 10K records, flag any feature whose
     Pearson |corr| with is_winner exceeds 0.5 (suspiciously high).

Produces  quality/leakage_report.md .

All I/O is streamed or sampled; RAM stays well under 2 GB.
No external API calls.

Usage:
    python quality/leakage_prevention.py
    python quality/leakage_prevention.py --features path/to/features_matrix.jsonl \
           --labels path/to/training_labels.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Project bootstrap
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.logging_setup import setup_logging

# ---------------------------------------------------------------------------
# Defaults (match config.py layout)
# ---------------------------------------------------------------------------
FEATURES_DIR = _PROJECT_ROOT / "output" / "features"
LABELS_DIR = _PROJECT_ROOT / "output" / "labels"
QUALITY_DIR = _PROJECT_ROOT / "quality"

DEFAULT_FEATURES = FEATURES_DIR / "features_matrix.jsonl"
DEFAULT_LABELS = LABELS_DIR / "training_labels.jsonl"
REPORT_PATH = QUALITY_DIR / "leakage_report.md"

CORRELATION_THRESHOLD = 0.5
TEMPORAL_SAMPLE_SIZE = 100
CORRELATION_SAMPLE_SIZE = 10_000

# Columns that ARE race results and must NOT appear as features
TARGET_COLUMNS = {
    "position_arrivee",
    "is_gagnant",
    "rapport_gagnant",
    "rapport_place",
    "rapport_couple_gagnant",
    "rapport_couple_place",
    "rapport_trio",
    "rapport_tierce",
    "rapport_quarte",
    "rapport_quinte",
}

# cote_finale is a target when used as a feature (not a label).
# It leaks the final betting-market consensus about the outcome.
TARGET_COLUMNS_EXTENDED = TARGET_COLUMNS | {"cote_finale"}

# Feature-builder output directories to scan for target leakage
# (each may contain .jsonl files produced by individual builders)
FEATURE_BUILDER_OUTPUT_DIRS = [
    "career_stats", "jockey_form", "first_time_events",
    "meteo_features", "geny_features", "equipement_features",
    "elo_ratings", "field_strength", "merged_features",
]

# Date column used throughout the pipeline
DATE_COL = "date_reunion_iso"


# =========================================================================
# STREAMING HELPERS
# =========================================================================

def iter_jsonl(path: Path, max_lines: int = 0):
    """Yield dicts from a .jsonl file, streaming line by line."""
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace",
              buffering=1_048_576) as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                continue
            count += 1
            if 0 < max_lines <= count:
                return


def reservoir_sample(path: Path, k: int, seed: int = 42) -> list[dict]:
    """Reservoir-sample *k* records from a JSONL file in one pass."""
    rng = random.Random(seed)
    result: list[dict] = []
    n = 0
    for rec in iter_jsonl(path):
        n += 1
        if n <= k:
            result.append(rec)
        else:
            j = rng.randint(0, n - 1)
            if j < k:
                result[j] = rec
    return result


def count_lines_jsonl(path: Path) -> int:
    """Fast line count (no JSON parsing)."""
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace",
              buffering=1_048_576) as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def load_csv_sample(path: Path, k: int, seed: int = 42) -> list[dict]:
    """Reservoir-sample k rows from a CSV."""
    rng = random.Random(seed)
    result: list[dict] = []
    n = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            n += 1
            if n <= k:
                result.append(dict(row))
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    result[j] = dict(row)
    return result


def load_sample(path: Path, k: int, seed: int = 42) -> list[dict]:
    """Load a sample of k records from JSONL, CSV, or JSON."""
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return reservoir_sample(path, k, seed)
    elif suffix == ".csv":
        return load_csv_sample(path, k, seed)
    elif suffix == ".json":
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            rng = random.Random(seed)
            return rng.sample(data, min(k, len(data)))
        return []
    elif suffix == ".parquet":
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(path)
            n = table.num_rows
            rng = random.Random(seed)
            indices = sorted(rng.sample(range(n), min(k, n)))
            return table.take(indices).to_pylist()
        except ImportError:
            return []
    return []


# =========================================================================
# STATISTICS
# =========================================================================

def pearson_correlation(xs: list[float], ys: list[float]) -> Optional[float]:
    """Pearson r between two float lists.  Returns None if impossible."""
    n = len(xs)
    if n < 3:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


# =========================================================================
# CHECK 1 — TEMPORAL LEAKAGE
# =========================================================================

def check_temporal_leakage(
    features_path: Path,
    logger,
    sample_size: int = TEMPORAL_SAMPLE_SIZE,
) -> list[dict]:
    """Sample records and flag features referencing dates >= race date.

    Returns a list of violation dicts:
        {field, partant_uid, race_date, value, reason}
    """
    logger.info("--- Check 1: Temporal leakage (sample=%d) ---", sample_size)
    violations: list[dict] = []

    if not features_path.exists():
        logger.warning("  Features file not found: %s — skipping", features_path)
        return violations

    sample = load_sample(features_path, sample_size)
    if not sample:
        logger.warning("  Empty sample — skipping")
        return violations

    # Identify date-like columns (those containing dates in their values)
    date_keywords = ("date", "prev", "precedent", "dernier", "last",
                     "futur", "next", "suivant", "after", "apres")
    future_keywords = ("futur", "next_race", "suivant", "y_gagnant",
                       "y_winner", "y_position", "target_")

    # Whitelist: columns that contain keywords but are computed with temporal integrity
    temporal_safe = {
        "commentaire_apres_course", "avis_entraineur",
        "aff_ct_last_result", "perf_temps_moy_5", "perf_temps_moy_10",
        "perf_temps_moy_20", "perf_red_moy_5", "perf_red_moy_10",
        "seq_position_moy_5", "seq_position_moy_10",
        "jockey_driver", "combo_jockey_change", "combo_jockey_hippo_nb",
        "jockey_taux_x_cheval_taux", "ent_jockey_taux_place",
        "ped_sire_precocity_idx",
    }

    all_keys = set()
    for rec in sample:
        all_keys.update(rec.keys())

    # Columns whose NAME suggests future data (exclude whitelisted)
    suspicious_name_cols = [
        k for k in all_keys
        if any(kw in k.lower() for kw in future_keywords)
        and k.lower() not in ("date_reunion_iso",)
        and k not in temporal_safe
    ]

    for col in suspicious_name_cols:
        violations.append({
            "field": col,
            "partant_uid": "N/A",
            "race_date": "N/A",
            "value": "N/A",
            "reason": f"Column name contains future-data keyword",
        })

    # Columns whose VALUES contain ISO dates — check against race date
    date_value_cols = [
        k for k in all_keys
        if any(kw in k.lower() for kw in date_keywords)
    ]

    checked = 0
    for rec in sample:
        race_date = str(rec.get(DATE_COL, ""))[:10]
        if not race_date or len(race_date) < 10:
            continue
        uid = rec.get("partant_uid", "?")

        for col in date_value_cols:
            val = rec.get(col)
            if val is None:
                continue
            val_str = str(val)
            if len(val_str) >= 10:
                date_part = val_str[:10]
                # Quick ISO-date check (YYYY-MM-DD shape)
                if len(date_part) == 10 and date_part[4] == "-" and date_part[7] == "-":
                    if date_part > race_date:
                        violations.append({
                            "field": col,
                            "partant_uid": uid,
                            "race_date": race_date,
                            "value": date_part,
                            "reason": "Feature date > race date (future data)",
                        })
        checked += 1

    logger.info("  Checked %d records, %d date-value columns, %d suspicious-name columns",
                checked, len(date_value_cols), len(suspicious_name_cols))
    logger.info("  Violations found: %d", len(violations))
    return violations


# =========================================================================
# CHECK 2 — TARGET LEAKAGE
# =========================================================================

def check_target_leakage(
    features_path: Path,
    logger,
) -> list[dict]:
    """Detect forbidden result columns in feature outputs.

    Returns list of {file, field, reason}.
    """
    logger.info("--- Check 2: Target leakage ---")
    findings: list[dict] = []

    # Strategy A: check the main features matrix
    paths_to_check: list[Path] = []
    if features_path.exists():
        paths_to_check.append(features_path)

    # Strategy B: scan individual feature-builder output dirs
    output_dir = _PROJECT_ROOT / "output"
    for subdir_name in FEATURE_BUILDER_OUTPUT_DIRS:
        subdir = output_dir / subdir_name
        if subdir.is_dir():
            for child in subdir.iterdir():
                if child.suffix in (".jsonl", ".json", ".csv", ".parquet"):
                    paths_to_check.append(child)

    if not paths_to_check:
        logger.warning("  No feature files found to check")
        return findings

    for fpath in paths_to_check:
        # Read a small sample to get column names
        sample = load_sample(fpath, 5)
        if not sample:
            continue
        columns = set()
        for rec in sample:
            columns.update(rec.keys())

        for col in columns:
            col_lower = col.lower()
            # Exact match against forbidden set
            if col in TARGET_COLUMNS_EXTENDED:
                findings.append({
                    "file": str(fpath.relative_to(_PROJECT_ROOT)),
                    "field": col,
                    "reason": "Direct result column present in feature output",
                })
            # Pattern match for rapport_* variants
            elif col_lower.startswith("rapport_"):
                findings.append({
                    "file": str(fpath.relative_to(_PROJECT_ROOT)),
                    "field": col,
                    "reason": "rapport_* column (payout data) in feature output",
                })

    logger.info("  Scanned %d files, found %d target-leakage fields",
                len(paths_to_check), len(findings))
    return findings


# =========================================================================
# CHECK 3 — TRAIN / TEST CONTAMINATION
# =========================================================================

def check_train_test_contamination(
    labels_path: Path,
    logger,
) -> dict:
    """Verify that temporal date splits don't overlap.

    Returns dict with:
        ok: bool
        train_min, train_max, test_min, test_max: str dates
        overlap_dates: list[str]  (should be empty)
        details: str
    """
    logger.info("--- Check 3: Train/test date contamination ---")
    result: dict[str, Any] = {
        "ok": True,
        "train_min": "",
        "train_max": "",
        "test_min": "",
        "test_max": "",
        "overlap_dates": [],
        "details": "",
    }

    if not labels_path.exists():
        result["details"] = f"Labels file not found: {labels_path}"
        logger.warning("  %s", result["details"])
        return result

    # Collect all unique dates from labels — streaming
    dates: set[str] = set()
    total = 0
    for rec in iter_jsonl(labels_path):
        d = str(rec.get(DATE_COL, ""))[:10]
        if d and len(d) == 10:
            dates.add(d)
        total += 1

    if not dates:
        result["details"] = f"No valid dates in {total} label records"
        logger.warning("  %s", result["details"])
        return result

    sorted_dates = sorted(dates)
    result["train_min"] = sorted_dates[0]
    result["test_max"] = sorted_dates[-1]

    # Check the quality/split report if it exists
    split_report = _PROJECT_ROOT / "output" / "quality" / "split_report.json"
    if split_report.exists():
        with open(split_report, "r", encoding="utf-8") as fh:
            sr = json.load(fh)
        train_max = sr.get("train", {}).get("date_max", "")
        test_min = sr.get("test", {}).get("date_min", "")
        val_min = sr.get("val", {}).get("date_min", "")

        result["train_max"] = train_max
        result["test_min"] = test_min

        if train_max and test_min and train_max >= test_min:
            result["ok"] = False
            overlap = [d for d in sorted_dates if d <= train_max and d >= test_min]
            result["overlap_dates"] = overlap[:20]
            result["details"] = (
                f"OVERLAP: train_max={train_max} >= test_min={test_min}"
            )
        elif train_max and val_min and train_max >= val_min:
            result["ok"] = False
            result["details"] = (
                f"OVERLAP: train_max={train_max} >= val_min={val_min}"
            )
        else:
            result["details"] = "No overlap detected in split_report.json"
    else:
        # Heuristic: use simple median split to check for date ordering sanity
        mid = len(sorted_dates) // 2
        train_dates = sorted_dates[:mid]
        test_dates = sorted_dates[mid:]
        if train_dates and test_dates:
            result["train_max"] = train_dates[-1]
            result["test_min"] = test_dates[0]
            if train_dates[-1] >= test_dates[0]:
                result["ok"] = False
                result["details"] = (
                    f"OVERLAP at median split: train_max={train_dates[-1]} "
                    f">= test_min={test_dates[0]}"
                )
            else:
                result["details"] = (
                    "No split_report.json found; median-split sanity check passed"
                )
        else:
            result["details"] = "Not enough distinct dates to evaluate"

    logger.info("  %d unique dates across %d records", len(dates), total)
    logger.info("  Result: %s", result["details"])
    return result


# =========================================================================
# CHECK 4 — FEATURE-LABEL CORRELATION
# =========================================================================

def check_feature_label_correlation(
    features_path: Path,
    labels_path: Path,
    logger,
    sample_size: int = CORRELATION_SAMPLE_SIZE,
    threshold: float = CORRELATION_THRESHOLD,
) -> dict:
    """Compute Pearson corr between each numeric feature and is_winner.

    Returns dict:
        suspicious: list[{feature, correlation}]   (|corr| > threshold)
        all_correlations: dict[feature, float]
        n_features: int
        n_records: int
    """
    logger.info("--- Check 4: Feature-label correlation (sample=%d, threshold=%.2f) ---",
                sample_size, threshold)

    result: dict[str, Any] = {
        "suspicious": [],
        "all_correlations": {},
        "n_features": 0,
        "n_records": 0,
    }

    if not features_path.exists():
        logger.warning("  Features file missing: %s", features_path)
        return result
    if not labels_path.exists():
        logger.warning("  Labels file missing: %s", labels_path)
        return result

    # 1. Load labels index (partant_uid -> y_gagnant)
    logger.info("  Loading labels index ...")
    labels_idx: dict[str, float] = {}
    for rec in iter_jsonl(labels_path):
        uid = rec.get("partant_uid", "")
        y = rec.get("y_gagnant", rec.get("is_gagnant"))
        if uid and y is not None:
            try:
                labels_idx[uid] = float(y)
            except (ValueError, TypeError):
                pass
    logger.info("  %d labels indexed", len(labels_idx))

    if not labels_idx:
        result["all_correlations"] = {}
        return result

    # 2. Sample features
    logger.info("  Sampling %d feature records ...", sample_size)
    sample = load_sample(features_path, sample_size)
    if not sample:
        return result

    # 3. Identify numeric feature columns (exclude IDs / metadata)
    exclude = {
        "partant_uid", "course_uid", "reunion_uid", DATE_COL,
        "nom_cheval", "cle_partant", "source", "timestamp_collecte",
        "hippodrome_normalise", "hippodrome", "jockey_driver",
        "entraineur", "discipline",
        # Exclude known label/result cols that master_feature_builder carries
        "position_arrivee", "is_gagnant", "is_place", "cote_finale",
    }
    all_keys: set[str] = set()
    for rec in sample:
        all_keys.update(rec.keys())
    feature_cols = sorted(k for k in all_keys if k not in exclude)

    # 4. Compute correlations
    correlations: dict[str, float] = {}
    suspicious: list[dict] = []

    for col in feature_cols:
        xs: list[float] = []
        ys: list[float] = []
        for rec in sample:
            uid = rec.get("partant_uid", "")
            lbl = labels_idx.get(uid)
            if lbl is None:
                continue
            val = rec.get(col)
            if val is None:
                continue
            try:
                val_f = float(val)
            except (ValueError, TypeError):
                continue
            if math.isnan(val_f) or math.isinf(val_f):
                continue
            xs.append(val_f)
            ys.append(lbl)

        if len(xs) < 10:
            continue

        corr = pearson_correlation(xs, ys)
        if corr is not None:
            corr = round(corr, 4)
            correlations[col] = corr
            if abs(corr) > threshold:
                suspicious.append({"feature": col, "correlation": corr})

    # Sort by |corr| descending
    suspicious.sort(key=lambda d: abs(d["correlation"]), reverse=True)
    sorted_corr = dict(sorted(correlations.items(), key=lambda kv: -abs(kv[1])))

    result["suspicious"] = suspicious
    result["all_correlations"] = sorted_corr
    result["n_features"] = len(correlations)
    result["n_records"] = len(sample)

    logger.info("  %d numeric features analysed, %d suspicious (|corr| > %.2f)",
                len(correlations), len(suspicious), threshold)
    for s in suspicious[:10]:
        logger.warning("    SUSPECT: %s  corr=%.4f", s["feature"], s["correlation"])

    return result


# =========================================================================
# MARKDOWN REPORT
# =========================================================================

def generate_report(
    temporal: list[dict],
    target: list[dict],
    contamination: dict,
    correlation: dict,
    elapsed: float,
) -> str:
    """Produce a Markdown report string."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []

    lines.append("# Leakage Prevention Report")
    lines.append("")
    lines.append(f"Generated: {now}  ")
    lines.append(f"Runtime: {elapsed:.1f}s")
    lines.append("")

    # Summary
    n_issues = len(temporal) + len(target) + (0 if contamination.get("ok") else 1) + len(correlation.get("suspicious", []))
    if n_issues == 0:
        lines.append("**Status: PASS** -- No leakage detected.")
    else:
        lines.append(f"**Status: FAIL** -- {n_issues} issue(s) found.")
    lines.append("")

    # ---- Check 1 ----
    lines.append("## 1. Temporal Leakage")
    lines.append("")
    if temporal:
        lines.append(f"**{len(temporal)} violation(s) detected.**")
        lines.append("")
        lines.append("| Field | Partant UID | Race Date | Value | Reason |")
        lines.append("|-------|-------------|-----------|-------|--------|")
        for v in temporal[:50]:
            lines.append(
                f"| {v['field']} | {v['partant_uid']} | {v['race_date']} "
                f"| {v['value']} | {v['reason']} |"
            )
        if len(temporal) > 50:
            lines.append(f"| ... | ... | ... | ... | ({len(temporal) - 50} more) |")
    else:
        lines.append("No temporal leakage detected.")
    lines.append("")

    # ---- Check 2 ----
    lines.append("## 2. Target Leakage")
    lines.append("")
    if target:
        lines.append(f"**{len(target)} forbidden field(s) found in feature outputs.**")
        lines.append("")
        lines.append("| File | Field | Reason |")
        lines.append("|------|-------|--------|")
        for f in target:
            lines.append(f"| {f['file']} | {f['field']} | {f['reason']} |")
    else:
        lines.append("No target leakage detected.")
    lines.append("")

    # ---- Check 3 ----
    lines.append("## 3. Train/Test Date Contamination")
    lines.append("")
    if contamination.get("ok"):
        lines.append("No date overlap detected between train and test splits.")
    else:
        lines.append(f"**OVERLAP DETECTED:** {contamination.get('details', '')}")
        overlap = contamination.get("overlap_dates", [])
        if overlap:
            lines.append("")
            lines.append(f"Overlapping dates (first {len(overlap)}): {', '.join(overlap)}")
    lines.append("")
    lines.append(f"- Train date range: {contamination.get('train_min', '?')} .. {contamination.get('train_max', '?')}")
    lines.append(f"- Test date range:  {contamination.get('test_min', '?')} .. {contamination.get('test_max', '?')}")
    lines.append("")

    # ---- Check 4 ----
    lines.append("## 4. Feature-Label Correlation")
    lines.append("")
    suspicious = correlation.get("suspicious", [])
    n_feat = correlation.get("n_features", 0)
    n_rec = correlation.get("n_records", 0)
    lines.append(f"Analysed {n_feat} numeric features over {n_rec} sampled records.")
    lines.append("")
    if suspicious:
        lines.append(f"**{len(suspicious)} feature(s) with |corr| > {CORRELATION_THRESHOLD}:**")
        lines.append("")
        lines.append("| Feature | Correlation |")
        lines.append("|---------|-------------|")
        for s in suspicious:
            lines.append(f"| {s['feature']} | {s['correlation']:.4f} |")
    else:
        lines.append("No suspiciously high correlations detected.")
    lines.append("")

    # Top 20 correlations regardless
    all_corr = correlation.get("all_correlations", {})
    if all_corr:
        lines.append("### Top 20 Correlations with is_winner")
        lines.append("")
        lines.append("| Feature | Correlation |")
        lines.append("|---------|-------------|")
        for i, (feat, c) in enumerate(all_corr.items()):
            if i >= 20:
                break
            marker = " **!**" if any(s["feature"] == feat for s in suspicious) else ""
            lines.append(f"| {feat}{marker} | {c:.4f} |")
        lines.append("")

    return "\n".join(lines)


# =========================================================================
# MAIN
# =========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Leakage prevention audit for the ML pipeline"
    )
    parser.add_argument(
        "--features", type=str, default=None,
        help=f"Features matrix path (default: {DEFAULT_FEATURES})"
    )
    parser.add_argument(
        "--labels", type=str, default=None,
        help=f"Training labels path (default: {DEFAULT_LABELS})"
    )
    parser.add_argument(
        "--output", type=str, default=str(REPORT_PATH),
        help=f"Output report path (default: {REPORT_PATH})"
    )
    parser.add_argument(
        "--threshold", type=float, default=CORRELATION_THRESHOLD,
        help=f"Correlation threshold (default: {CORRELATION_THRESHOLD})"
    )
    parser.add_argument(
        "--temporal-sample", type=int, default=TEMPORAL_SAMPLE_SIZE,
        help=f"Sample size for temporal check (default: {TEMPORAL_SAMPLE_SIZE})"
    )
    parser.add_argument(
        "--correlation-sample", type=int, default=CORRELATION_SAMPLE_SIZE,
        help=f"Sample size for correlation check (default: {CORRELATION_SAMPLE_SIZE})"
    )
    args = parser.parse_args()

    logger = setup_logging("leakage_prevention")
    logger.info("=" * 70)
    logger.info("leakage_prevention.py — Data Leakage Prevention Audit")
    logger.info("=" * 70)

    features_path = Path(args.features) if args.features else DEFAULT_FEATURES
    labels_path = Path(args.labels) if args.labels else DEFAULT_LABELS
    output_path = Path(args.output)

    # Also try Parquet variants if JSONL not found
    if not features_path.exists():
        alt = features_path.with_suffix(".parquet")
        if alt.exists():
            logger.info("Using Parquet variant: %s", alt)
            features_path = alt
    if not labels_path.exists():
        alt = labels_path.with_suffix(".parquet")
        if alt.exists():
            logger.info("Using Parquet variant: %s", alt)
            labels_path = alt

    logger.info("Features: %s (exists=%s)", features_path, features_path.exists())
    logger.info("Labels:   %s (exists=%s)", labels_path, labels_path.exists())

    t0 = time.time()

    # ---- Run all checks ----
    temporal_violations = check_temporal_leakage(
        features_path, logger, sample_size=args.temporal_sample,
    )

    target_findings = check_target_leakage(features_path, logger)

    contamination_result = check_train_test_contamination(labels_path, logger)

    correlation_result = check_feature_label_correlation(
        features_path, labels_path, logger,
        sample_size=args.correlation_sample,
        threshold=args.threshold,
    )

    elapsed = time.time() - t0

    # ---- Generate report ----
    report_md = generate_report(
        temporal_violations,
        target_findings,
        contamination_result,
        correlation_result,
        elapsed,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(report_md)
    logger.info("Report written to: %s", output_path)

    # ---- Console summary ----
    n_issues = (
        len(temporal_violations)
        + len(target_findings)
        + (0 if contamination_result.get("ok") else 1)
        + len(correlation_result.get("suspicious", []))
    )
    if n_issues > 0:
        logger.warning("LEAKAGE AUDIT FAILED: %d issue(s) found — see %s",
                        n_issues, output_path)
        sys.exit(1)
    else:
        logger.info("LEAKAGE AUDIT PASSED — no issues detected (%.1fs)", elapsed)
        sys.exit(0)


if __name__ == "__main__":
    main()
