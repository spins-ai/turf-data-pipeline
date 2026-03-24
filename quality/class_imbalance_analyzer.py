#!/usr/bin/env python3
"""
quality/class_imbalance_analyzer.py
====================================
Analyse la distribution des labels (desequilibre de classes) et propose
des splits temporels optimaux pour l'entrainement ML.

Controles :
  1. Distribution gagnants vs perdants, places vs non-places, DNF
  2. Ratios de desequilibre
  3. Suggestion de dates de coupure train/val/test (70/15/15 par records,
     en respectant l'ordre temporel)
  4. Verification optionnelle : aucun cheval dans train ET test
  5. Poids de classes recommandes pour modeles ML
  6. Rapport markdown : quality/class_imbalance_report.md

Streaming CSV/JSONL -- RAM < 2 GB.
Aucun appel API : traitement 100% local.

Usage :
    python3 quality/class_imbalance_analyzer.py
    python3 quality/class_imbalance_analyzer.py --labels output/labels/training_labels.jsonl
    python3 quality/class_imbalance_analyzer.py --check-horse-leakage
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

LABELS_DIR = _PROJECT_ROOT / "output" / "labels"
DEFAULT_LABELS = LABELS_DIR / "training_labels.jsonl"
OUTPUT_DIR = _PROJECT_ROOT / "quality"

# Target split ratios
TRAIN_RATIO = 0.70
VAL_RATIO = 0.15
TEST_RATIO = 0.15


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# STREAMING READER
# ===========================================================================

def _parse_bool(val: str) -> bool | None:
    """Parse boolean from CSV string."""
    if val is None:
        return None
    v = str(val).strip().lower()
    if v in ("true", "1", "yes"):
        return True
    if v in ("false", "0", "no"):
        return False
    return None


def _parse_int(val: str) -> int | None:
    """Parse integer from CSV string."""
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def stream_labels(path: Path) -> tuple[list[dict], int]:
    """Stream training labels, auto-detecting CSV vs JSONL format.

    Returns (records, total_count) where each record is a lightweight dict
    with only the fields we need for analysis.
    """
    records: list[dict] = []
    total = 0
    t0 = time.time()
    last_report = t0

    with open(path, "r", encoding="utf-8") as f:
        # Sniff first line to detect format
        first_line = f.readline().strip()
        f.seek(0)

        if first_line.startswith("{"):
            # JSONL format
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1
                records.append({
                    "date": str(raw.get("date_reunion_iso", "")),
                    "is_winner": _parse_bool(raw.get("is_winner")),
                    "is_place": _parse_bool(raw.get("is_place")),
                    "position": _parse_int(raw.get("position")),
                    "is_dnf": _parse_bool(raw.get("is_dnf")),
                    "nb_partants": _parse_int(raw.get("nb_partants")),
                    "partant_uid": str(raw.get("partant_uid", "")),
                    "course_uid": str(raw.get("course_uid", "")),
                })
                now = time.time()
                if now - last_report > 60:
                    print(f"  [{int(now - t0)}s] {total:,} lignes lues")
                    last_report = now
        else:
            # CSV format (header on first line)
            reader = csv.DictReader(f)
            for raw in reader:
                total += 1
                records.append({
                    "date": str(raw.get("date_reunion_iso", "")),
                    "is_winner": _parse_bool(raw.get("is_winner")),
                    "is_place": _parse_bool(raw.get("is_place")),
                    "position": _parse_int(raw.get("position")),
                    "is_dnf": _parse_bool(raw.get("is_dnf")),
                    "nb_partants": _parse_int(raw.get("nb_partants")),
                    "partant_uid": str(raw.get("partant_uid", "")),
                    "course_uid": str(raw.get("course_uid", "")),
                })
                now = time.time()
                if now - last_report > 60:
                    print(f"  [{int(now - t0)}s] {total:,} lignes lues")
                    last_report = now

    return records, total


# ===========================================================================
# ANALYSIS
# ===========================================================================

def analyze_label_distribution(records: list[dict]) -> dict:
    """Count winners, placed, DNF, and compute imbalance ratios."""
    n = len(records)

    # Winner distribution
    winners = sum(1 for r in records if r["is_winner"] is True)
    losers = sum(1 for r in records if r["is_winner"] is False)
    winner_unknown = n - winners - losers

    # Placed distribution
    placed = sum(1 for r in records if r["is_place"] is True)
    not_placed = sum(1 for r in records if r["is_place"] is False)
    place_unknown = n - placed - not_placed

    # DNF distribution
    dnf = sum(1 for r in records if r["is_dnf"] is True)
    finished = sum(1 for r in records if r["is_dnf"] is False)
    dnf_unknown = n - dnf - finished

    # Position distribution (top-N buckets)
    positions = [r["position"] for r in records if r["position"] is not None]
    top1 = sum(1 for p in positions if p == 1)
    top3 = sum(1 for p in positions if p <= 3)
    top5 = sum(1 for p in positions if p <= 5)

    # Imbalance ratios
    win_ratio = losers / winners if winners > 0 else float("inf")
    place_ratio = not_placed / placed if placed > 0 else float("inf")
    dnf_ratio = finished / dnf if dnf > 0 else float("inf")

    # Average field size for expected win rate
    nb_partants_list = [r["nb_partants"] for r in records if r["nb_partants"] is not None]
    avg_field = sum(nb_partants_list) / len(nb_partants_list) if nb_partants_list else 0
    expected_win_rate = 1.0 / avg_field if avg_field > 0 else 0
    actual_win_rate = winners / n if n > 0 else 0

    return {
        "total": n,
        "winners": winners,
        "losers": losers,
        "winner_unknown": winner_unknown,
        "placed": placed,
        "not_placed": not_placed,
        "place_unknown": place_unknown,
        "dnf": dnf,
        "finished": finished,
        "dnf_unknown": dnf_unknown,
        "top1": top1,
        "top3": top3,
        "top5": top5,
        "n_with_position": len(positions),
        "win_imbalance_ratio": round(win_ratio, 2),
        "place_imbalance_ratio": round(place_ratio, 2),
        "dnf_imbalance_ratio": round(dnf_ratio, 2),
        "avg_field_size": round(avg_field, 1),
        "expected_win_rate": round(expected_win_rate, 4),
        "actual_win_rate": round(actual_win_rate, 4),
    }


def compute_temporal_split_dates(
    records: list[dict],
    train_ratio: float = TRAIN_RATIO,
    val_ratio: float = VAL_RATIO,
) -> dict:
    """Find optimal date cutoffs for train/val/test that approximate target ratios.

    Records must be sorted by date. Split boundaries align to date boundaries
    (no date is split across sets).
    """
    # Sort records by date
    dated = [(r["date"], i) for i, r in enumerate(records) if r["date"]]
    dated.sort(key=lambda x: x[0])
    n = len(dated)

    if n == 0:
        return {"error": "no dated records"}

    # Count records per date
    date_counts: list[tuple[str, int]] = []
    current_date = dated[0][0]
    current_count = 0
    for dt, _ in dated:
        if dt == current_date:
            current_count += 1
        else:
            date_counts.append((current_date, current_count))
            current_date = dt
            current_count = 1
    date_counts.append((current_date, current_count))

    # Find train_end date
    target_train = int(n * train_ratio)
    target_val_end = int(n * (train_ratio + val_ratio))

    cumsum = 0
    train_end_date = date_counts[0][0]
    val_end_date = date_counts[0][0]
    train_n = 0
    val_n = 0

    for dt, cnt in date_counts:
        cumsum += cnt
        if cumsum <= target_train:
            train_end_date = dt
            train_n = cumsum
        if cumsum <= target_val_end:
            val_end_date = dt
            val_n = cumsum

    # Actual split sizes
    test_n = n - val_n
    actual_val_n = val_n - train_n

    return {
        "total_dated": n,
        "train_end_date": train_end_date,
        "val_end_date": val_end_date,
        "train_n": train_n,
        "val_n": actual_val_n,
        "test_n": test_n,
        "train_pct": round(100.0 * train_n / n, 1),
        "val_pct": round(100.0 * actual_val_n / n, 1),
        "test_pct": round(100.0 * test_n / n, 1),
        "date_min": date_counts[0][0],
        "date_max": date_counts[-1][0],
        "n_unique_dates": len(date_counts),
    }


def check_horse_leakage(
    records: list[dict],
    train_end_date: str,
    val_end_date: str,
) -> dict:
    """Check if any horse (partant_uid prefix = horse_id) appears in both
    train and test sets. This is an optional, potentially strict check."""
    train_horses: set[str] = set()
    test_horses: set[str] = set()

    for r in records:
        dt = r["date"]
        uid = r["partant_uid"]
        if not dt or not uid:
            continue
        if dt <= train_end_date:
            train_horses.add(uid)
        elif dt > val_end_date:
            test_horses.add(uid)

    overlap = train_horses & test_horses
    return {
        "train_unique_horses": len(train_horses),
        "test_unique_horses": len(test_horses),
        "overlap_count": len(overlap),
        "overlap_pct_of_test": round(
            100.0 * len(overlap) / len(test_horses), 1
        ) if test_horses else 0,
        "note": (
            "Overlap is expected in horse racing (same horses race repeatedly). "
            "This is informational, not necessarily a problem."
        ),
    }


def compute_class_weights(dist: dict) -> dict:
    """Compute recommended class weights for ML models."""
    weights = {}
    n = dist["total"]

    # Binary: is_winner
    if dist["winners"] > 0 and dist["losers"] > 0:
        total_wl = dist["winners"] + dist["losers"]
        w_pos = total_wl / (2 * dist["winners"])
        w_neg = total_wl / (2 * dist["losers"])
        weights["is_winner"] = {
            "positive (1)": round(w_pos, 4),
            "negative (0)": round(w_neg, 4),
            "ratio_pos_neg": round(w_pos / w_neg, 2),
            "sklearn_class_weight": {
                "0": round(w_neg, 4),
                "1": round(w_pos, 4),
            },
        }

    # Binary: is_place
    if dist["placed"] > 0 and dist["not_placed"] > 0:
        total_pl = dist["placed"] + dist["not_placed"]
        w_pos = total_pl / (2 * dist["placed"])
        w_neg = total_pl / (2 * dist["not_placed"])
        weights["is_place"] = {
            "positive (1)": round(w_pos, 4),
            "negative (0)": round(w_neg, 4),
            "ratio_pos_neg": round(w_pos / w_neg, 2),
            "sklearn_class_weight": {
                "0": round(w_neg, 4),
                "1": round(w_pos, 4),
            },
        }

    # DNF
    if dist["dnf"] > 0 and dist["finished"] > 0:
        total_df = dist["dnf"] + dist["finished"]
        w_pos = total_df / (2 * dist["dnf"])
        w_neg = total_df / (2 * dist["finished"])
        weights["is_dnf"] = {
            "positive (1)": round(w_pos, 4),
            "negative (0)": round(w_neg, 4),
            "ratio_pos_neg": round(w_pos / w_neg, 2),
        }

    # Sampling strategy suggestions
    weights["recommendations"] = []
    if dist["win_imbalance_ratio"] > 10:
        weights["recommendations"].append(
            f"High win imbalance ({dist['win_imbalance_ratio']}:1). "
            f"Consider: focal loss, SMOTE on features, or stratified sampling."
        )
    if dist["win_imbalance_ratio"] > 5:
        weights["recommendations"].append(
            "Use class_weight='balanced' in sklearn or equivalent."
        )
    if dist["dnf"] > 0:
        pct_dnf = 100.0 * dist["dnf"] / dist["total"]
        if pct_dnf > 5:
            weights["recommendations"].append(
                f"DNF rate is {pct_dnf:.1f}%. Consider a two-stage model: "
                f"first predict DNF, then predict position among finishers."
            )

    return weights


# ===========================================================================
# REPORT GENERATION
# ===========================================================================

def generate_markdown_report(
    dist: dict,
    split: dict,
    horse_leakage: dict | None,
    class_weights: dict,
    output_path: Path,
) -> None:
    """Write class_imbalance_report.md."""
    lines: list[str] = []
    lines.append("# Class Imbalance & Split Analysis Report")
    lines.append("")
    lines.append(f"- **Total records**: {dist['total']:,}")
    lines.append(f"- **Average field size**: {dist['avg_field_size']}")
    lines.append("")

    # --- 1. Label Distribution ---
    lines.append("## 1. Label Distribution")
    lines.append("")
    lines.append("### Winners vs Losers")
    lines.append("")
    lines.append("| Category | Count | % |")
    lines.append("|---|---:|---:|")
    n = dist["total"]
    for label, key in [("Winners", "winners"), ("Losers", "losers"), ("Unknown", "winner_unknown")]:
        c = dist[key]
        pct = 100.0 * c / n if n > 0 else 0
        lines.append(f"| {label} | {c:,} | {pct:.2f}% |")
    lines.append(f"| **Imbalance ratio** | **{dist['win_imbalance_ratio']}:1** | |")
    lines.append(f"| Expected win rate (1/N) | | {dist['expected_win_rate']:.4f} |")
    lines.append(f"| Actual win rate | | {dist['actual_win_rate']:.4f} |")
    lines.append("")

    lines.append("### Placed vs Not Placed")
    lines.append("")
    lines.append("| Category | Count | % |")
    lines.append("|---|---:|---:|")
    for label, key in [("Placed", "placed"), ("Not placed", "not_placed"), ("Unknown", "place_unknown")]:
        c = dist[key]
        pct = 100.0 * c / n if n > 0 else 0
        lines.append(f"| {label} | {c:,} | {pct:.2f}% |")
    lines.append(f"| **Imbalance ratio** | **{dist['place_imbalance_ratio']}:1** | |")
    lines.append("")

    lines.append("### DNF (Did Not Finish)")
    lines.append("")
    lines.append("| Category | Count | % |")
    lines.append("|---|---:|---:|")
    for label, key in [("DNF", "dnf"), ("Finished", "finished"), ("Unknown", "dnf_unknown")]:
        c = dist[key]
        pct = 100.0 * c / n if n > 0 else 0
        lines.append(f"| {label} | {c:,} | {pct:.2f}% |")
    lines.append("")

    lines.append("### Position Buckets")
    lines.append("")
    np_ = dist["n_with_position"]
    lines.append(f"- Records with position: {np_:,}")
    if np_ > 0:
        lines.append(f"- Top 1: {dist['top1']:,} ({100.0*dist['top1']/np_:.2f}%)")
        lines.append(f"- Top 3: {dist['top3']:,} ({100.0*dist['top3']/np_:.2f}%)")
        lines.append(f"- Top 5: {dist['top5']:,} ({100.0*dist['top5']/np_:.2f}%)")
    lines.append("")

    # --- 2. Temporal Split ---
    lines.append("## 2. Recommended Temporal Split (70/15/15)")
    lines.append("")
    if "error" not in split:
        lines.append(f"- Date range: {split['date_min']} to {split['date_max']}")
        lines.append(f"- Unique dates: {split['n_unique_dates']:,}")
        lines.append("")
        lines.append("| Split | Date cutoff | Records | % |")
        lines.append("|---|---|---:|---:|")
        lines.append(f"| Train | <= {split['train_end_date']} | "
                     f"{split['train_n']:,} | {split['train_pct']}% |")
        lines.append(f"| Val | ({split['train_end_date']}, {split['val_end_date']}] | "
                     f"{split['val_n']:,} | {split['val_pct']}% |")
        lines.append(f"| Test | > {split['val_end_date']} | "
                     f"{split['test_n']:,} | {split['test_pct']}% |")
        lines.append("")
        lines.append("```python")
        lines.append("# Suggested usage in pipeline:")
        lines.append(f'TRAIN_END = "{split["train_end_date"]}"')
        lines.append(f'VAL_END   = "{split["val_end_date"]}"')
        lines.append("```")
    else:
        lines.append(f"Error: {split['error']}")
    lines.append("")

    # --- 3. Horse Leakage ---
    lines.append("## 3. Horse Overlap Between Train & Test (informational)")
    lines.append("")
    if horse_leakage is not None:
        lines.append(f"- Unique horses in train: {horse_leakage['train_unique_horses']:,}")
        lines.append(f"- Unique horses in test: {horse_leakage['test_unique_horses']:,}")
        lines.append(f"- Overlap: {horse_leakage['overlap_count']:,} "
                     f"({horse_leakage['overlap_pct_of_test']}% of test horses)")
        lines.append(f"- Note: {horse_leakage['note']}")
    else:
        lines.append("Skipped (use --check-horse-leakage to enable).")
    lines.append("")

    # --- 4. Class Weights ---
    lines.append("## 4. Recommended Class Weights")
    lines.append("")
    for target in ["is_winner", "is_place", "is_dnf"]:
        if target in class_weights:
            cw = class_weights[target]
            lines.append(f"### {target}")
            lines.append("")
            lines.append(f"- Weight positive (1): {cw['positive (1)']}")
            lines.append(f"- Weight negative (0): {cw['negative (0)']}")
            lines.append(f"- Ratio: {cw['ratio_pos_neg']}x")
            if "sklearn_class_weight" in cw:
                sk = cw["sklearn_class_weight"]
                lines.append(f"- `class_weight={{0: {sk['0']}, 1: {sk['1']}}}`")
            lines.append("")

    recs = class_weights.get("recommendations", [])
    if recs:
        lines.append("### Recommendations")
        lines.append("")
        for rec in recs:
            lines.append(f"- {rec}")
        lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Class imbalance & temporal split analyzer for training labels"
    )
    parser.add_argument(
        "--labels", type=str, default=str(DEFAULT_LABELS),
        help="Path to training_labels.jsonl (or .csv)",
    )
    parser.add_argument(
        "--output", type=str,
        default=str(OUTPUT_DIR / "class_imbalance_report.md"),
        help="Output report path",
    )
    parser.add_argument(
        "--check-horse-leakage", action="store_true",
        help="Check horse overlap between train and test (optional, slower)",
    )
    parser.add_argument(
        "--train-ratio", type=float, default=TRAIN_RATIO,
        help=f"Train split ratio (default: {TRAIN_RATIO})",
    )
    parser.add_argument(
        "--val-ratio", type=float, default=VAL_RATIO,
        help=f"Validation split ratio (default: {VAL_RATIO})",
    )
    args = parser.parse_args()

    logger = setup_logging("class_imbalance_analyzer")

    labels_path = Path(args.labels)
    if not labels_path.exists():
        logger.error("Fichier introuvable: %s", labels_path)
        sys.exit(1)

    output_path = Path(args.output)

    logger.info("=" * 70)
    logger.info("class_imbalance_analyzer.py -- Desequilibre des classes")
    logger.info("=" * 70)
    logger.info("Fichier: %s", labels_path)

    # Phase 1: Stream labels
    logger.info("Phase 1: Lecture des labels (streaming)...")
    records, total_count = stream_labels(labels_path)
    logger.info("Total: %d records lus", total_count)

    if not records:
        logger.error("Aucun enregistrement lu.")
        return 1

    # Phase 2: Label distribution
    logger.info("Phase 2: Distribution des labels...")
    dist = analyze_label_distribution(records)
    logger.info("  Winners: %d, Losers: %d (ratio %s:1)",
                dist["winners"], dist["losers"], dist["win_imbalance_ratio"])
    logger.info("  Placed: %d, Not placed: %d (ratio %s:1)",
                dist["placed"], dist["not_placed"], dist["place_imbalance_ratio"])
    logger.info("  DNF: %d, Finished: %d", dist["dnf"], dist["finished"])

    # Phase 3: Temporal split
    logger.info("Phase 3: Calcul des dates de coupure temporelle...")
    split = compute_temporal_split_dates(records, args.train_ratio, args.val_ratio)
    if "error" not in split:
        logger.info("  Train end: %s (%d records, %.1f%%)",
                    split["train_end_date"], split["train_n"], split["train_pct"])
        logger.info("  Val end: %s (%d records, %.1f%%)",
                    split["val_end_date"], split["val_n"], split["val_pct"])
        logger.info("  Test: %d records (%.1f%%)", split["test_n"], split["test_pct"])

    # Phase 4: Horse leakage (optional)
    horse_leakage = None
    if args.check_horse_leakage and "error" not in split:
        logger.info("Phase 4: Verification chevauchement chevaux train/test...")
        horse_leakage = check_horse_leakage(
            records, split["train_end_date"], split["val_end_date"]
        )
        logger.info("  Overlap: %d chevaux (%s%% du test)",
                    horse_leakage["overlap_count"],
                    horse_leakage["overlap_pct_of_test"])

    # Phase 5: Class weights
    logger.info("Phase 5: Calcul des poids de classes...")
    class_weights = compute_class_weights(dist)
    for target in ["is_winner", "is_place"]:
        if target in class_weights:
            cw = class_weights[target]
            logger.info("  %s: weight_pos=%.4f, weight_neg=%.4f",
                       target, cw["positive (1)"], cw["negative (0)"])

    # Phase 6: Report
    logger.info("Phase 6: Generation du rapport...")
    generate_markdown_report(dist, split, horse_leakage, class_weights, output_path)
    logger.info("Rapport sauve: %s", output_path)

    # Console summary
    print(f"\n{'='*70}")
    print("CLASS IMBALANCE & SPLIT ANALYSIS")
    print(f"{'='*70}")
    print(f"Total records: {total_count:,}")
    print()
    print(f"Winners:    {dist['winners']:>10,}  ({100.0*dist['winners']/dist['total']:.2f}%)")
    print(f"Losers:     {dist['losers']:>10,}  ({100.0*dist['losers']/dist['total']:.2f}%)")
    print(f"Placed:     {dist['placed']:>10,}  ({100.0*dist['placed']/dist['total']:.2f}%)")
    print(f"DNF:        {dist['dnf']:>10,}  ({100.0*dist['dnf']/dist['total']:.2f}%)")
    print(f"Win ratio:  {dist['win_imbalance_ratio']}:1")
    print()
    if "error" not in split:
        print(f"Suggested split dates:")
        print(f"  Train end: {split['train_end_date']}  ({split['train_n']:,} records)")
        print(f"  Val end:   {split['val_end_date']}  ({split['val_n']:,} records)")
        print(f"  Test:      {split['test_n']:,} records")
    print(f"\nReport: {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
