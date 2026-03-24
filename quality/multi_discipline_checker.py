#!/usr/bin/env python3
"""
quality/multi_discipline_checker.py
====================================
Verifie la coherence des disciplines dans partants_master.jsonl.

Controles :
  1. Distribution des disciplines (trot_attele, trot_monte, plat, haie,
     steeple, cross_country)
  2. Enregistrements avec discipline inconnue ou manquante
  3. Contamination croisee : features specifiques a une discipline qui
     apparaissent dans une discipline incompatible
     - oeilleres est specifique au trot
     - features obstacle ne doivent pas apparaitre en plat
  4. Rapport markdown : quality/multi_discipline_report.md

Streaming + reservoir sampling (50K) -- RAM < 2 GB.
Aucun appel API : traitement 100% local.

Usage :
    python3 quality/multi_discipline_checker.py
    python3 quality/multi_discipline_checker.py --sample-size 100000
    python3 quality/multi_discipline_checker.py --partants path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
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

DATA_MASTER = _PROJECT_ROOT / "data_master"
DEFAULT_PARTANTS = DATA_MASTER / "partants_master.jsonl"
OUTPUT_DIR = _PROJECT_ROOT / "quality"

SAMPLE_SIZE = 50_000
RESERVOIR_SEED = 42

# Known racing disciplines
KNOWN_DISCIPLINES = {
    "trot_attele",
    "trot_monte",
    "plat",
    "haie",
    "steeple",
    "cross_country",
}

TROT_DISCIPLINES = {"trot_attele", "trot_monte"}
OBSTACLE_DISCIPLINES = {"haie", "steeple", "cross_country"}
FLAT_DISCIPLINES = {"plat"}

# Features specific to trot (should NOT appear in galop races)
TROT_SPECIFIC_FEATURES = {
    "oeilleres",
    "allure",
    "deferre",
    "reduction_km_ms",
}

# Features related to obstacles (should NOT appear in plat races)
OBSTACLE_RELATED_KEYWORDS = [
    "obstacle",
    "haie",
    "steeple",
    "saut",
    "franchissement",
]

# Features specific to flat / galop (should NOT appear in trot)
GALOP_SPECIFIC_FEATURES = {
    "poids_base_kg",
    "poids_porte_kg",
    "surcharge_decharge_kg",
    "handicap_valeur",
    "handicap_distance_m",
}


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# RESERVOIR SAMPLING
# ===========================================================================

def reservoir_sample(
    path: Path,
    sample_size: int,
    seed: int = RESERVOIR_SEED,
) -> tuple[list[dict], int]:
    """Stream JSONL with reservoir sampling, returns (sample, total_count)."""
    reservoir: list[dict] = []
    rng = random.Random(seed)
    total = 0
    t0 = time.time()
    last_report = t0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1

            if total <= sample_size:
                reservoir.append(rec)
            else:
                j = rng.randint(1, total)
                if j <= sample_size:
                    reservoir[j - 1] = rec

            now = time.time()
            if now - last_report > 60:
                elapsed = now - t0
                rate = total / elapsed
                print(f"  [{int(elapsed)}s] {total:,} lignes lues, {rate:,.0f} lignes/s")
                last_report = now

    return reservoir, total


# ===========================================================================
# ANALYSIS
# ===========================================================================

def analyze_discipline_distribution(records: list[dict]) -> dict:
    """Count distribution of discipline field."""
    counter: Counter = Counter()
    missing = 0
    unknown: Counter = Counter()

    for rec in records:
        disc = rec.get("discipline")
        if disc is None or str(disc).strip() == "":
            missing += 1
            continue
        disc_lower = str(disc).strip().lower()
        if disc_lower in KNOWN_DISCIPLINES:
            counter[disc_lower] += 1
        else:
            unknown[disc_lower] += 1

    return {
        "counts": dict(counter.most_common()),
        "missing": missing,
        "unknown": dict(unknown.most_common(20)),
    }


def _has_nonempty(rec: dict, field: str) -> bool:
    """Check if a record has a non-empty, non-None value for field."""
    val = rec.get(field)
    if val is None:
        return False
    if isinstance(val, str) and val.strip() == "":
        return False
    if isinstance(val, (int, float)) and val == 0:
        return False
    return True


def _has_obstacle_feature(rec: dict) -> list[str]:
    """Return list of obstacle-related feature keys with non-empty values."""
    found = []
    for key in rec:
        key_lower = key.lower()
        for kw in OBSTACLE_RELATED_KEYWORDS:
            if kw in key_lower:
                if _has_nonempty(rec, key):
                    found.append(key)
                break
    return found


def analyze_cross_discipline_contamination(records: list[dict]) -> dict:
    """Check for features that shouldn't appear in certain disciplines."""

    contamination: dict[str, dict] = defaultdict(lambda: {
        "count": 0,
        "examples": [],
    })

    for rec in records:
        disc = rec.get("discipline")
        if disc is None or str(disc).strip() == "":
            continue
        disc_lower = str(disc).strip().lower()
        if disc_lower not in KNOWN_DISCIPLINES:
            continue

        # Check 1: Trot-specific features in galop/obstacle races
        if disc_lower not in TROT_DISCIPLINES:
            for feat in TROT_SPECIFIC_FEATURES:
                if _has_nonempty(rec, feat):
                    key = f"trot_feature_in_{disc_lower}"
                    contamination[key]["count"] += 1
                    if len(contamination[key]["examples"]) < 3:
                        contamination[key]["examples"].append(
                            f"{feat}={rec.get(feat)!r} (course={rec.get('course_uid', '?')})"
                        )

        # Check 2: Obstacle features in plat races
        if disc_lower in FLAT_DISCIPLINES:
            obs_feats = _has_obstacle_feature(rec)
            if obs_feats:
                key = "obstacle_feature_in_plat"
                contamination[key]["count"] += 1
                if len(contamination[key]["examples"]) < 3:
                    contamination[key]["examples"].append(
                        f"{obs_feats} (course={rec.get('course_uid', '?')})"
                    )

        # Check 3: Galop-specific features (weights/handicaps) in trot
        if disc_lower in TROT_DISCIPLINES:
            for feat in GALOP_SPECIFIC_FEATURES:
                if _has_nonempty(rec, feat):
                    key = f"galop_feature_in_{disc_lower}"
                    contamination[key]["count"] += 1
                    if len(contamination[key]["examples"]) < 3:
                        contamination[key]["examples"].append(
                            f"{feat}={rec.get(feat)!r} (course={rec.get('course_uid', '?')})"
                        )

    return dict(contamination)


def analyze_allure_vs_discipline(records: list[dict]) -> dict:
    """Cross-check allure field vs discipline."""
    mismatches = {"count": 0, "examples": []}
    checked = 0

    for rec in records:
        disc = rec.get("discipline")
        allure = rec.get("allure")
        if not disc or not allure:
            continue
        disc_lower = str(disc).strip().lower()
        allure_lower = str(allure).strip().lower()
        checked += 1

        # Trot discipline should have trot allure
        if disc_lower in TROT_DISCIPLINES and allure_lower not in ("trot", ""):
            mismatches["count"] += 1
            if len(mismatches["examples"]) < 5:
                mismatches["examples"].append(
                    f"discipline={disc_lower}, allure={allure_lower}"
                )

        # Galop disciplines should have galop allure
        if disc_lower in (FLAT_DISCIPLINES | OBSTACLE_DISCIPLINES) and allure_lower == "trot":
            mismatches["count"] += 1
            if len(mismatches["examples"]) < 5:
                mismatches["examples"].append(
                    f"discipline={disc_lower}, allure={allure_lower}"
                )

    return {
        "checked": checked,
        "mismatches": mismatches,
    }


# ===========================================================================
# REPORT GENERATION
# ===========================================================================

def generate_markdown_report(
    total_records: int,
    sample_size: int,
    distribution: dict,
    contamination: dict,
    allure_check: dict,
    output_path: Path,
) -> None:
    """Write multi_discipline_report.md."""
    lines: list[str] = []
    lines.append("# Multi-Discipline Consistency Report")
    lines.append("")
    lines.append(f"- **Total records scanned**: {total_records:,}")
    lines.append(f"- **Sample size (reservoir)**: {sample_size:,}")
    lines.append("")

    # --- Distribution ---
    lines.append("## 1. Discipline Distribution")
    lines.append("")
    counts = distribution["counts"]
    total_known = sum(counts.values())
    lines.append("| Discipline | Count | % of sample |")
    lines.append("|---|---:|---:|")
    for disc in KNOWN_DISCIPLINES:
        c = counts.get(disc, 0)
        pct = 100.0 * c / sample_size if sample_size > 0 else 0
        lines.append(f"| {disc} | {c:,} | {pct:.2f}% |")
    lines.append(f"| **Known total** | **{total_known:,}** | "
                 f"**{100.0 * total_known / sample_size:.2f}%** |")
    lines.append("")

    # Missing
    missing = distribution["missing"]
    pct_missing = 100.0 * missing / sample_size if sample_size > 0 else 0
    lines.append(f"### Missing discipline: {missing:,} ({pct_missing:.2f}%)")
    lines.append("")

    # Unknown
    unknown = distribution["unknown"]
    if unknown:
        lines.append("### Unknown discipline values")
        lines.append("")
        lines.append("| Value | Count |")
        lines.append("|---|---:|")
        for val, cnt in unknown.items():
            lines.append(f"| `{val}` | {cnt:,} |")
        lines.append("")
    else:
        lines.append("No unknown discipline values found.")
        lines.append("")

    # --- Contamination ---
    lines.append("## 2. Cross-Discipline Feature Contamination")
    lines.append("")
    if contamination:
        lines.append("| Issue | Count | Examples |")
        lines.append("|---|---:|---|")
        for key, info in sorted(contamination.items(), key=lambda x: -x[1]["count"]):
            examples_str = "; ".join(info["examples"][:2])
            lines.append(f"| {key} | {info['count']:,} | {examples_str} |")
        lines.append("")
    else:
        lines.append("No cross-discipline contamination detected.")
        lines.append("")

    # --- Allure check ---
    lines.append("## 3. Allure vs Discipline Consistency")
    lines.append("")
    allure_mis = allure_check.get("mismatches", {})
    n_checked = allure_check.get("checked", 0)
    n_mis = allure_mis.get("count", 0)
    lines.append(f"- Records checked: {n_checked:,}")
    lines.append(f"- Mismatches: {n_mis:,}")
    if n_mis > 0 and allure_mis.get("examples"):
        lines.append("")
        lines.append("Examples:")
        lines.append("")
        for ex in allure_mis["examples"]:
            lines.append(f"  - {ex}")
    lines.append("")

    # --- Summary ---
    lines.append("## 4. Summary")
    lines.append("")
    total_issues = (
        missing
        + sum(len(v) for v in unknown.values())
        + sum(info["count"] for info in contamination.values())
        + n_mis
    )
    if total_issues == 0:
        lines.append("All checks passed. No discipline inconsistencies found.")
    else:
        lines.append(f"**{total_issues:,} total issues detected.** Review above sections.")
    lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Multi-discipline consistency checker for partants_master"
    )
    parser.add_argument(
        "--partants", type=str, default=str(DEFAULT_PARTANTS),
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--sample-size", type=int, default=SAMPLE_SIZE,
        help=f"Reservoir sample size (default: {SAMPLE_SIZE:,})",
    )
    parser.add_argument(
        "--output", type=str, default=str(OUTPUT_DIR / "multi_discipline_report.md"),
        help="Output report path",
    )
    args = parser.parse_args()

    logger = setup_logging("multi_discipline_checker")

    partants_path = Path(args.partants)
    if not partants_path.exists():
        logger.error("Fichier introuvable: %s", partants_path)
        sys.exit(1)

    sample_size = args.sample_size
    output_path = Path(args.output)

    logger.info("=" * 70)
    logger.info("multi_discipline_checker.py -- Coherence des disciplines")
    logger.info("=" * 70)
    logger.info("Fichier: %s", partants_path)
    logger.info("Echantillon: %d", sample_size)

    # Phase 1: Reservoir sampling
    logger.info("Phase 1: Reservoir sampling (%d records)...", sample_size)
    records, total_count = reservoir_sample(partants_path, sample_size)
    actual_sample = len(records)
    logger.info("Total lignes: %d, echantillon: %d", total_count, actual_sample)

    if actual_sample == 0:
        logger.error("Aucun enregistrement lu.")
        return 1

    # Phase 2: Analysis
    logger.info("Phase 2: Analyse distribution des disciplines...")
    distribution = analyze_discipline_distribution(records)
    for disc, cnt in distribution["counts"].items():
        logger.info("  %s: %d (%.1f%%)", disc, cnt, 100.0 * cnt / actual_sample)
    logger.info("  Manquantes: %d", distribution["missing"])
    logger.info("  Inconnues: %d valeurs distinctes", len(distribution["unknown"]))

    logger.info("Phase 2b: Contamination croisee...")
    contamination = analyze_cross_discipline_contamination(records)
    for key, info in contamination.items():
        logger.info("  %s: %d occurrences", key, info["count"])

    logger.info("Phase 2c: Allure vs discipline...")
    allure_check = analyze_allure_vs_discipline(records)
    logger.info("  Verifies: %d, incoherences: %d",
                allure_check["checked"], allure_check["mismatches"]["count"])

    # Phase 3: Report
    logger.info("Phase 3: Generation du rapport...")
    generate_markdown_report(
        total_records=total_count,
        sample_size=actual_sample,
        distribution=distribution,
        contamination=contamination,
        allure_check=allure_check,
        output_path=output_path,
    )
    logger.info("Rapport sauve: %s", output_path)

    # Console summary
    print(f"\n{'='*70}")
    print("MULTI-DISCIPLINE CONSISTENCY REPORT")
    print(f"{'='*70}")
    print(f"Total records: {total_count:,} | Sample: {actual_sample:,}")
    print()
    print("Discipline distribution:")
    for disc, cnt in distribution["counts"].items():
        print(f"  {disc:<20} {cnt:>8,} ({100.0*cnt/actual_sample:.1f}%)")
    print(f"  {'MISSING':<20} {distribution['missing']:>8,}")
    if distribution["unknown"]:
        print(f"  {'UNKNOWN':<20} {sum(distribution['unknown'].values()):>8,}")
    print()
    n_contam = sum(info["count"] for info in contamination.values())
    print(f"Cross-discipline contamination: {n_contam:,} issues")
    print(f"Allure mismatches: {allure_check['mismatches']['count']:,}")
    print(f"\nReport: {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
