"""
feature_builders.fatigue_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-horse cumulative fatigue features computed at race time.
Temporal integrity: for any partant at date D, only races with date < D are used.

Features
--------
- fatigue_30j              : Weighted sum of distances raced in last 30 days
- fatigue_60j              : Weighted sum of distances raced in last 60 days
- fatigue_90j              : Weighted sum of distances raced in last 90 days
- fatigue_distance_ponderee: Total distance raced weighted by recency (exp decay)
- intensite_recente        : Average allocation of recent races (higher = harder)
- sequence_courses         : Number of consecutive races without >30 day rest
- tendance_fatigue         : Slope of last 5 race intervals (positive = spacing out)
"""

from __future__ import annotations

import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from utils.math import safe_mean as _safe_mean


def _days_between(d1: str, d2: str) -> int:
    """Number of days between two ISO date strings."""
    dt1 = datetime.strptime(d1, "%Y-%m-%d")
    dt2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((dt2 - dt1).days)


def _weighted_distance_sum(past: list[dict], date_iso: str, window_days: int) -> float:
    """Sum of distances raced in last *window_days*, weighted by recency.

    Weight = 1 - (days_ago / window_days), so a race today has weight ~1
    and a race at the edge of the window has weight ~0.
    """
    total = 0.0
    for r in reversed(past):
        gap = _days_between(r["date"], date_iso)
        if gap > window_days:
            break
        if gap == 0:
            continue  # skip same-day (current race)
        dist = r.get("distance")
        if dist is None:
            continue
        weight = 1.0 - (gap / window_days)
        total += dist * weight
    return total


def _exp_decay_distance(past: list[dict], date_iso: str, half_life: float = 30.0) -> float:
    """Total distance raced weighted by exponential decay.

    Weight = exp(-ln(2) * days_ago / half_life).
    Half-life of 30 days means a race 30 days ago counts half as much.
    """
    total = 0.0
    decay_rate = math.log(2) / half_life
    for r in reversed(past):
        gap = _days_between(r["date"], date_iso)
        if gap == 0:
            continue
        # Stop when weight is negligible (< 1% contribution)
        if gap > half_life * 7:
            break
        dist = r.get("distance")
        if dist is None:
            continue
        weight = math.exp(-decay_rate * gap)
        total += dist * weight
    return total


def _sequence_courses(past: list[dict]) -> int:
    """Count consecutive races without >30 day rest, from most recent backwards."""
    count = 0
    for i in range(len(past) - 1, 0, -1):
        gap = _days_between(past[i - 1]["date"], past[i]["date"])
        if gap > 30:
            break
        count += 1
    # Include the last race itself if there's at least one race
    if past:
        count += 1
    return count


def _tendance_fatigue(past: list[dict], n: int = 5) -> Optional[float]:
    """Slope of inter-race intervals over last *n* races.

    Positive slope = intervals increasing = horse spacing out (less fatigue).
    Negative slope = intervals decreasing = horse racing more frequently.
    Uses simple linear regression slope.
    Returns None if fewer than 3 intervals available.
    """
    recent = past[-n:] if len(past) >= n else past
    if len(recent) < 3:
        return None

    # Compute intervals between consecutive races
    intervals = []
    for i in range(1, len(recent)):
        gap = _days_between(recent[i - 1]["date"], recent[i]["date"])
        intervals.append(gap)

    if len(intervals) < 2:
        return None

    # Simple linear regression: slope of intervals over index
    n_pts = len(intervals)
    x_mean = (n_pts - 1) / 2.0
    y_mean = sum(intervals) / n_pts

    num = 0.0
    den = 0.0
    for i, y in enumerate(intervals):
        num += (i - x_mean) * (y - y_mean)
        den += (i - x_mean) ** 2

    if den == 0:
        return 0.0
    return num / den


def build_fatigue_features(partants: list[dict]) -> list[dict]:
    """Build per-horse cumulative fatigue features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records, each containing at minimum:
        partant_uid, nom_cheval, date_reunion_iso, distance,
        allocation_totale (or similar gains field).

    Returns
    -------
    list[dict]
        One dict per partant_uid with computed fatigue features.
    """
    # Sort chronologically for temporal integrity
    sorted_p = sorted(
        partants,
        key=lambda p: (
            p.get("date_reunion_iso", ""),
            p.get("course_uid", ""),
            p.get("num_pmu", 0),
        ),
    )

    # Per-horse history: list of dicts with date, distance, allocation
    horse_history: dict[str, list[dict]] = defaultdict(list)

    results = []

    for p in sorted_p:
        uid = p.get("partant_uid")
        cheval = p.get("nom_cheval")
        date_iso = p.get("date_reunion_iso", "")

        # Past races strictly before current date
        past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]

        # --- fatigue weighted distance sums for 30/60/90 days ---
        fatigue_30 = _weighted_distance_sum(past, date_iso, 30) if past else 0.0
        fatigue_60 = _weighted_distance_sum(past, date_iso, 60) if past else 0.0
        fatigue_90 = _weighted_distance_sum(past, date_iso, 90) if past else 0.0

        # --- fatigue_distance_ponderee: exponential decay ---
        fatigue_exp = _exp_decay_distance(past, date_iso) if past else 0.0

        # --- intensite_recente: avg allocation of last 5 races ---
        intensite: Optional[float] = None
        if past:
            recent_allocs = [
                r["allocation"]
                for r in past[-5:]
                if r.get("allocation") is not None
            ]
            intensite = _safe_mean(recent_allocs)

        # --- sequence_courses: consecutive races without >30 day break ---
        seq = _sequence_courses(past) if past else 0

        # --- tendance_fatigue: slope of inter-race intervals ---
        tendance = _tendance_fatigue(past)

        feat = {
            "partant_uid": uid,
            "fatigue_30j": fatigue_30,
            "fatigue_60j": fatigue_60,
            "fatigue_90j": fatigue_90,
            "fatigue_distance_ponderee": fatigue_exp,
            "intensite_recente": intensite,
            "sequence_courses": seq,
            "tendance_fatigue": tendance,
        }
        results.append(feat)

        # --- Append current race to horse history ---
        allocation_val = (
            p.get("allocation_totale")
            or p.get("allocation")
            or p.get("gains_carriere_euros")
        )

        horse_history[cheval].append({
            "date": date_iso,
            "distance": p.get("distance"),
            "allocation": allocation_val,
        })

    return results


if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")
    path = os.path.join(base, "partants_normalises.json")
    with open(path, encoding="utf-8") as f:
        partants = json.load(f)
    feats = build_fatigue_features(partants)
    print(f"Built {len(feats)} fatigue feature records.")
    if feats:
        keys = [k for k in feats[0] if k != "partant_uid"]
        print(f"Features ({len(keys)}): {', '.join(keys)}")
        for k in keys:
            filled = sum(1 for r in feats if r.get(k) is not None)
            print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
