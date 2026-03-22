"""
feature_builders.recovery_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-horse recovery and rest features computed at race time.
Temporal integrity: for any partant at date D, only races with date < D are used.

Features
--------
- jours_repos              : Days since last race
- repos_optimal            : Is the rest period in the horse's historically optimal range? (bool)
- perf_apres_repos_court   : Historical win rate after <14 days rest
- perf_apres_repos_moyen   : Historical win rate after 14-45 days rest
- perf_apres_repos_long    : Historical win rate after >45 days rest
- repos_vs_moyenne          : Rest days vs horse's average rest between races
- nb_courses_30j           : Number of races in last 30 days
- nb_courses_60j           : Number of races in last 60 days
- nb_courses_90j           : Number of races in last 90 days
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from utils.math import safe_mean as _safe_mean, safe_rate as _safe_rate


def _days_between(d1: str, d2: str) -> int:
    """Number of days between two ISO date strings."""
    dt1 = datetime.strptime(d1, "%Y-%m-%d")
    dt2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((dt2 - dt1).days)


def _classify_rest(days: int) -> str:
    """Classify rest period into short / moyen / long."""
    if days < 14:
        return "court"
    elif days <= 45:
        return "moyen"
    return "long"


def _find_optimal_rest_range(past_with_rest: list[dict]) -> Optional[tuple[int, int]]:
    """Find the rest-day range that historically yielded the best win rate.

    Buckets rest days into [0-13], [14-45], [46+] and picks the bucket with
    the highest win rate (minimum 2 observations required).
    Returns (lo, hi) bounds or None if insufficient data.
    """
    buckets: dict[str, list[bool]] = {"court": [], "moyen": [], "long": []}
    for r in past_with_rest:
        rest = r.get("rest_days")
        if rest is None:
            continue
        cat = _classify_rest(rest)
        buckets[cat].append(r["gagnant"])

    best_rate = -1.0
    best_range: Optional[tuple[int, int]] = None
    ranges = {"court": (0, 13), "moyen": (14, 45), "long": (46, 9999)}

    for cat, outcomes in buckets.items():
        if len(outcomes) < 2:
            continue
        rate = sum(outcomes) / len(outcomes)
        if rate > best_rate:
            best_rate = rate
            best_range = ranges[cat]

    return best_range


def build_recovery_features(partants: list[dict]) -> list[dict]:
    """Build per-horse recovery/rest features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records, each containing at minimum:
        partant_uid, nom_cheval, date_reunion_iso, position_arrivee,
        is_gagnant.

    Returns
    -------
    list[dict]
        One dict per partant_uid with computed recovery features.
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

    # Per-horse history: list of dicts with date, gagnant, rest_days
    horse_history: dict[str, list[dict]] = defaultdict(list)

    results = []

    for p in sorted_p:
        uid = p.get("partant_uid")
        cheval = p.get("nom_cheval")
        date_iso = p.get("date_reunion_iso", "")

        # Past races strictly before current date
        past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]

        # --- jours_repos: days since last race ---
        jours_repos: Optional[int] = None
        if past:
            jours_repos = _days_between(past[-1]["date"], date_iso)

        # --- repos_optimal: is current rest in horse's best historical range? ---
        repos_optimal: Optional[bool] = None
        past_with_rest = [r for r in past if r.get("rest_days") is not None]
        if jours_repos is not None and len(past_with_rest) >= 3:
            optimal_range = _find_optimal_rest_range(past_with_rest)
            if optimal_range is not None:
                repos_optimal = optimal_range[0] <= jours_repos <= optimal_range[1]

        # --- perf_apres_repos by category ---
        # For each past race that had a known rest period, bucket win outcomes
        court_wins, court_total = 0, 0
        moyen_wins, moyen_total = 0, 0
        long_wins, long_total = 0, 0
        for r in past_with_rest:
            rest = r["rest_days"]
            cat = _classify_rest(rest)
            if cat == "court":
                court_total += 1
                court_wins += int(r["gagnant"])
            elif cat == "moyen":
                moyen_total += 1
                moyen_wins += int(r["gagnant"])
            else:
                long_total += 1
                long_wins += int(r["gagnant"])

        perf_court = _safe_rate(court_wins, court_total)
        perf_moyen = _safe_rate(moyen_wins, moyen_total)
        perf_long = _safe_rate(long_wins, long_total)

        # --- repos_vs_moyenne: current rest vs horse's average rest ---
        repos_vs_moyenne: Optional[float] = None
        if jours_repos is not None and past_with_rest:
            avg_rest = _safe_mean([r["rest_days"] for r in past_with_rest])
            if avg_rest is not None and avg_rest > 0:
                repos_vs_moyenne = jours_repos / avg_rest  # >1 = longer than usual

        # --- nb_courses in last 30/60/90 days ---
        nb_30 = 0
        nb_60 = 0
        nb_90 = 0
        if date_iso and past:
            for r in reversed(past):
                gap = _days_between(r["date"], date_iso)
                if gap <= 30:
                    nb_30 += 1
                if gap <= 60:
                    nb_60 += 1
                if gap <= 90:
                    nb_90 += 1
                if gap > 90:
                    break  # past is chronological, older entries won't match

        feat = {
            "partant_uid": uid,
            "jours_repos": jours_repos,
            "repos_optimal": repos_optimal,
            "perf_apres_repos_court": perf_court,
            "perf_apres_repos_moyen": perf_moyen,
            "perf_apres_repos_long": perf_long,
            "repos_vs_moyenne": repos_vs_moyenne,
            "nb_courses_30j": nb_30,
            "nb_courses_60j": nb_60,
            "nb_courses_90j": nb_90,
        }
        results.append(feat)

        # --- Append current race to horse history ---
        rest_days_for_record: Optional[int] = None
        if past:
            rest_days_for_record = _days_between(past[-1]["date"], date_iso)

        horse_history[cheval].append({
            "date": date_iso,
            "gagnant": bool(p.get("is_gagnant")),
            "rest_days": rest_days_for_record,
        })

    return results


if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")
    path = os.path.join(base, "partants_normalises.json")
    with open(path, encoding="utf-8") as f:
        partants = json.load(f)
    feats = build_recovery_features(partants)
    print(f"Built {len(feats)} recovery feature records.")
    if feats:
        keys = [k for k in feats[0] if k != "partant_uid"]
        print(f"Features ({len(keys)}): {', '.join(keys)}")
        for k in keys:
            filled = sum(1 for r in feats if r.get(k) is not None)
            print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
