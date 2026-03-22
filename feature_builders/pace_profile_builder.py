"""
feature_builders.pace_profile_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Build pace / running-style profiles per horse and per race.

Temporal integrity: for any partant at date D, only races with date < D
are used (no future leakage).  History is capped at the 20 most recent
prior races per horse.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import Any, Optional

from utils.math import safe_mean as _safe_mean, safe_rate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_safe_rate = partial(safe_rate, ndigits=4)


# ---------------------------------------------------------------------------
# Per-horse style scoring (on historical window)
# ---------------------------------------------------------------------------

def _compute_style_scores(past_races: list[dict]) -> dict[str, Any]:
    """Compute front_runner_score and closer_score from historical races.

    Heuristics (proxy – no sectional data):
      * front-runner pattern: place_corde <= 4 AND position_arrivee <= 3
      * closer pattern:       place_corde > 6  AND position_arrivee <= 3
    """
    if not past_races:
        return {
            "front_runner_score": None,
            "closer_score": None,
            "style_course": None,
        }

    front_count = 0
    closer_count = 0
    valid = 0

    for r in past_races:
        pos = r.get("position_arrivee")
        corde = r.get("place_corde")
        if pos is None or corde is None:
            continue
        valid += 1
        if corde <= 4 and pos <= 3:
            front_count += 1
        if corde > 6 and pos <= 3:
            closer_count += 1

    fr_score = _safe_rate(front_count, valid)
    cl_score = _safe_rate(closer_count, valid)

    # Determine dominant style
    if fr_score is not None and cl_score is not None:
        if fr_score >= cl_score and fr_score > 0:
            style = "front"
        elif cl_score > fr_score:
            style = "closer"
        else:
            style = "mid"
    else:
        style = None

    return {
        "front_runner_score": fr_score,
        "closer_score": cl_score,
        "style_course": style,
    }


# ---------------------------------------------------------------------------
# Finish-speed proxy (reduction kilométrique)
# ---------------------------------------------------------------------------

def _compute_reduction_features(past_races: list[dict]) -> dict[str, Any]:
    """Compute speed features from reduction_km_ms history.

    - avg_reduction_km_5:  average over last 5 races
    - best_reduction_km_10: best (lowest) over last 10 races
    - reduction_km_trend:  'improving' / 'declining' / 'stable' / None
    """
    reductions = [
        r["reduction_km_ms"]
        for r in past_races
        if r.get("reduction_km_ms") is not None
    ]

    avg_5 = _safe_mean(reductions[-5:]) if reductions else None
    best_10 = min(reductions[-10:]) if reductions else None

    # Trend: compare last-3 avg to last-10 avg
    trend: Optional[str] = None
    if len(reductions) >= 4:
        avg_last3 = _safe_mean(reductions[-3:])
        avg_last10 = _safe_mean(reductions[-10:])
        if avg_last3 is not None and avg_last10 is not None:
            if avg_last3 < avg_last10:
                trend = "improving"
            elif avg_last3 > avg_last10:
                trend = "declining"
            else:
                trend = "stable"

    return {
        "avg_reduction_km_5": round(avg_5, 2) if avg_5 is not None else None,
        "best_reduction_km_10": round(best_10, 2) if best_10 is not None else None,
        "reduction_km_trend": trend,
    }


# ---------------------------------------------------------------------------
# Field-level pace features (second pass)
# ---------------------------------------------------------------------------

def _enrich_field_pace(race_partants: list[dict]) -> list[dict]:
    """Add field-level pace features to a group of partants sharing the same race.

    Mutates dicts in-place and returns them.
    """
    nb_partants = len(race_partants)
    nb_front = sum(
        1 for p in race_partants
        if p.get("front_runner_score") is not None and p["front_runner_score"] > 0.4
    )
    nb_closers = sum(
        1 for p in race_partants
        if p.get("closer_score") is not None and p["closer_score"] > 0.4
    )
    pace_pressure = round(nb_front / nb_partants, 4) if nb_partants else None

    if pace_pressure is not None:
        if pace_pressure > 0.3:
            pace_scenario = "fast"
        elif pace_pressure < 0.15:
            pace_scenario = "slow"
        else:
            pace_scenario = "moderate"
    else:
        pace_scenario = None

    # Probable leader = highest front_runner_score in the field
    max_fr = max(
        (p.get("front_runner_score") or 0.0 for p in race_partants),
        default=0.0,
    )

    for p in race_partants:
        p["nb_front_runners"] = nb_front
        p["nb_closers"] = nb_closers
        p["pace_pressure"] = pace_pressure
        p["pace_scenario"] = pace_scenario
        fr = p.get("front_runner_score") or 0.0
        p["is_probable_leader"] = (fr > 0 and fr == max_fr)

    return race_partants


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

_MAX_LOOKBACK = 20


def build_pace_profiles(partants: list[dict]) -> list[dict]:
    """Build pace profiles for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records from partants_normalises.json.

    Returns
    -------
    list[dict]
        One dict per partant with pace-profile features.
    """
    # Sort chronologically for correct history accumulation
    sorted_p = sorted(
        partants,
        key=lambda p: (
            p.get("date_reunion_iso", ""),
            p.get("course_uid", ""),
            p.get("num_pmu", 0),
        ),
    )

    # Accumulate history per horse (keyed by nom_cheval)
    horse_history: dict[str, list[dict]] = defaultdict(list)

    # First pass: per-horse features
    results: list[dict] = []

    for p in sorted_p:
        cheval = p.get("nom_cheval", "")
        date_iso = p.get("date_reunion_iso", "")

        # Strictly past races, sorted ascending by date, capped at last 20
        past = [
            r for r in horse_history.get(cheval, [])
            if r["date"] < date_iso
        ][-_MAX_LOOKBACK:]

        style = _compute_style_scores(past)
        speed = _compute_reduction_features(past)

        feat: dict[str, Any] = {
            "partant_uid": p.get("partant_uid"),
            "nom_cheval": cheval,
            "date_reunion_iso": date_iso,
            "course_uid": p.get("course_uid"),
        }
        feat.update(style)
        feat.update(speed)

        results.append(feat)

        # Append current race to horse history for future lookups
        horse_history[cheval].append({
            "date": date_iso,
            "position_arrivee": p.get("position_arrivee"),
            "place_corde": p.get("place_corde"),
            "reduction_km_ms": p.get("reduction_km_ms"),
        })

    # Second pass: field-level pace features grouped by course_uid
    course_groups: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        key = r.get("course_uid") or r.get("date_reunion_iso", "")
        course_groups[key].append(r)

    enriched: list[dict] = []
    for group in course_groups.values():
        enriched.extend(_enrich_field_pace(group))

    return enriched


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def _export(records: list[dict], out_dir: str) -> None:
    """Write JSON, Parquet and CSV to *out_dir*."""
    os.makedirs(out_dir, exist_ok=True)

    # JSON
    json_path = os.path.join(out_dir, "pace_profiles.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  JSON  -> {json_path}  ({len(records)} records)")

    # CSV + Parquet via pandas (optional dependency)
    try:
        import pandas as pd

        df = pd.DataFrame(records)
        csv_path = os.path.join(out_dir, "pace_profiles.csv")
        df.to_csv(csv_path, index=False)
        print(f"  CSV   -> {csv_path}")

        parquet_path = os.path.join(out_dir, "pace_profiles.parquet")
        df.to_parquet(parquet_path, index=False)
        print(f"  Parquet -> {parquet_path}")
    except ImportError:
        print("  [warn] pandas not available – CSV/Parquet export skipped.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build pace / running-style profiles per horse.",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Path to partants_normalises.json (default: output/02_liste_courses/partants_normalises.json)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: output/pace_profiles/)",
    )
    args = parser.parse_args()

    base = Path(__file__).resolve().parent.parent
    input_path = args.input or str(base / "output" / "02_liste_courses" / "partants_normalises.json")
    output_dir = args.output_dir or str(base / "output" / "pace_profiles")

    print(f"Loading partants from {input_path} ...")
    with open(input_path, encoding="utf-8") as f:
        partants = json.load(f)
    print(f"  {len(partants)} partants loaded.")

    profiles = build_pace_profiles(partants)
    print(f"Built {len(profiles)} pace profiles.")

    # Quick stats
    keys = [
        "front_runner_score", "closer_score", "style_course",
        "avg_reduction_km_5", "best_reduction_km_10", "reduction_km_trend",
        "pace_pressure", "pace_scenario", "is_probable_leader",
    ]
    for k in keys:
        filled = sum(1 for r in profiles if r.get(k) is not None)
        print(f"  {k}: {filled}/{len(profiles)} ({100 * filled / len(profiles):.1f}%)" if profiles else "")

    print(f"\nExporting to {output_dir} ...")
    _export(profiles, output_dir)
    print("Done.")


if __name__ == "__main__":
    main()
