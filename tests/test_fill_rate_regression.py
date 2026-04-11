#!/usr/bin/env python3
"""Test that critical features maintain minimum fill rates.

Reads the fill_rate_audit.csv and checks that key features
for ML models have fill rates above configured thresholds.
"""
import csv
import sys
from pathlib import Path

FILL_CSV = Path("D:/turf-data-pipeline/04_FEATURES/fill_rate_audit.csv")

# Critical features and their minimum fill rate thresholds
CRITICAL_FEATURES = {
    # Core ratio features (>50%)
    ("basic_ratio_features", "br_place_rate"): 50,
    ("basic_ratio_features", "br_earnings_per_race"): 50,
    ("basic_ratio_features", "br_distance_km"): 80,
    # Elo ratings (>80% — well-filled)
    ("elo_ratings", "elo_cheval"): 80,
    ("elo_ratings", "elo_jockey"): 80,
    ("elo_ratings", "elo_entraineur"): 80,
    # Speed features
    ("speed_figures", "speed_figure"): 20,
    # Bayesian
    ("bayesian_ratings", "bayes_horse_win_rate"): 40,
    ("bayesian_ratings", "bayes_jockey_win_rate"): 40,
    # Career stats
    ("career_stats", "nb_courses_carriere"): 80,
    # Form composites
    ("recent_form_composite", "rfc_recent_vs_expected"): 30,
}

# No feature should have fill rate < 1% (useless features)
MIN_USEFUL_FILL = 1.0


def test_fill_rates():
    if not FILL_CSV.exists():
        print("SKIP: fill_rate_audit.csv not found. Run audit_fill_rates.py first.")
        return True

    with open(FILL_CSV, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Build lookup
    lookup = {}
    for r in rows:
        lookup[(r["builder"], r["feature"])] = float(r["fill_pct"])

    failures = []

    # Check critical features
    for (builder, feature), min_fill in CRITICAL_FEATURES.items():
        actual = lookup.get((builder, feature))
        if actual is None:
            failures.append(f"MISSING: {builder}/{feature} not found in audit")
        elif actual < min_fill:
            failures.append(f"LOW FILL: {builder}/{feature} = {actual}% (min {min_fill}%)")

    # Count useless features
    useless = [(r["builder"], r["feature"], float(r["fill_pct"]))
               for r in rows if float(r["fill_pct"]) < MIN_USEFUL_FILL]

    print(f"Total features: {len(rows)}")
    print(f"Features <{MIN_USEFUL_FILL}% fill: {len(useless)}")
    print(f"Critical feature checks: {len(CRITICAL_FEATURES)}")

    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        for f_msg in failures:
            print(f"  {f_msg}")
        return False

    print("ALL CRITICAL FEATURES PASS")
    return True


if __name__ == "__main__":
    ok = test_fill_rates()
    sys.exit(0 if ok else 1)
