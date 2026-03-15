"""
feature_builders.cheval_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-horse historical features computed at race time.
Temporal integrity: for any partant at date D, only races with date < D are used.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Optional


def _safe_mean(values: list) -> Optional[float]:
    """Mean of non-None numeric values, or None if empty."""
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _safe_rate(count: int, total: int) -> Optional[float]:
    if total == 0:
        return None
    return count / total


def _safe_stdev(values: list) -> Optional[float]:
    """Standard deviation of non-None numeric values, or None if < 2 values."""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    return statistics.stdev(clean)


def _days_between(d1: str, d2: str) -> int:
    """Number of days between two ISO date strings."""
    dt1 = datetime.strptime(d1, "%Y-%m-%d")
    dt2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((dt2 - dt1).days)


def _progression(positions: list) -> Optional[str]:
    """Determine trend from last 3 positions (most recent last).
    Improving = positions decreasing (lower = better).
    Declining = positions increasing.
    Stable = no clear trend.
    """
    clean = [p for p in positions if p is not None]
    if len(clean) < 2:
        return None
    recent = clean[-3:] if len(clean) >= 3 else clean
    if len(recent) < 2:
        return None
    diffs = [recent[i + 1] - recent[i] for i in range(len(recent) - 1)]
    avg_diff = sum(diffs) / len(diffs)
    if avg_diff < -0.5:
        return "improving"
    elif avg_diff > 0.5:
        return "declining"
    return "stable"


def _progression_numeric(positions: list, n: int) -> Optional[float]:
    """Numeric position trend over last N races.
    Negative = improving (positions getting lower/better).
    Positive = declining.
    Returns average difference between consecutive positions.
    """
    clean = [p for p in positions if p is not None]
    if len(clean) < 2:
        return None
    recent = clean[-n:] if len(clean) >= n else clean
    if len(recent) < 2:
        return None
    diffs = [recent[i + 1] - recent[i] for i in range(len(recent) - 1)]
    return sum(diffs) / len(diffs)


def _streak_count(past: list[dict], key: str) -> int:
    """Count consecutive True values for `key` from most recent race backwards."""
    count = 0
    for r in reversed(past):
        if r.get(key):
            count += 1
        else:
            break
    return count


def _races_since_last(past: list[dict], key: str) -> Optional[int]:
    """Count races since last True value for `key`. None if never happened."""
    for i, r in enumerate(reversed(past)):
        if r.get(key):
            return i
    return None


def build_cheval_features(partants: list[dict]) -> list[dict]:
    """Build per-horse historical features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records, each containing at minimum:
        partant_uid, nom_cheval, date_reunion_iso, position_arrivee,
        is_gagnant, is_place, hippodrome_normalise, distance, discipline,
        gains_carriere_euros.

    Returns
    -------
    list[dict]
        One dict per partant_uid with computed features.
    """
    # Sort by date then by course for determinism
    sorted_p = sorted(partants, key=lambda p: (p.get("date_reunion_iso", ""), p.get("course_uid", ""), p.get("num_pmu", 0)))

    # Group history per horse -- we accumulate as we iterate chronologically
    # Key: nom_cheval -> list of past-race records (dicts)
    horse_history: dict[str, list[dict]] = defaultdict(list)

    results = []

    for p in sorted_p:
        uid = p.get("partant_uid")
        cheval = p.get("nom_cheval")
        date_iso = p.get("date_reunion_iso", "")
        hippo = p.get("hippodrome_normalise", "")
        dist = p.get("distance")
        disc = p.get("discipline", "")

        # Get all PAST races for this horse (strictly < current date)
        past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]

        nb_avant = len(past)
        nb_victoires = sum(1 for r in past if r["gagnant"])
        nb_places = sum(1 for r in past if r["place"])

        # -- forme victoire / place over last N races --
        def _forme(n, key):
            if nb_avant == 0:
                return None
            window = past[-n:]
            total = len(window)
            return _safe_rate(sum(1 for r in window if r[key]), total)

        forme_v3 = _forme(3, "gagnant")
        forme_v5 = _forme(5, "gagnant")
        forme_v10 = _forme(10, "gagnant")
        forme_v20 = _forme(20, "gagnant")
        forme_p3 = _forme(3, "place")
        forme_p5 = _forme(5, "place")
        forme_p10 = _forme(10, "place")
        forme_p20 = _forme(20, "place")

        # -- jours_depuis_derniere --
        jours_dep = None
        derniere_pos = None
        if past:
            last = past[-1]
            jours_dep = _days_between(last["date"], date_iso)
            derniere_pos = last["position"]

        # -- avg_position over windows 3, 5, 10, 20 --
        def _avg_pos(n):
            positions = [r["position"] for r in past[-n:] if r["position"] is not None]
            return _safe_mean(positions)

        avg_pos_3 = _avg_pos(3)
        avg_pos_5 = _avg_pos(5)
        avg_pos_10 = _avg_pos(10)
        avg_pos_20 = _avg_pos(20)

        # -- rolling gains over windows 5, 10, 20 --
        def _rolling_gains(n):
            if nb_avant == 0:
                return None
            return sum(r.get("gains", 0) or 0 for r in past[-n:])

        gains_5 = _rolling_gains(5)
        gains_10 = _rolling_gains(10)
        gains_20 = _rolling_gains(20)

        # -- gains cumules (total career) --
        gains_cumules = sum(r.get("gains", 0) or 0 for r in past)

        # -- streaks --
        streak_victoires = _streak_count(past, "gagnant")
        streak_places = _streak_count(past, "place")
        streak_sans_victoire_val = _races_since_last(past, "gagnant")

        # -- DNF rate (did not finish) over windows --
        def _dnf_rate(n):
            if nb_avant == 0:
                return None
            window = past[-n:]
            total = len(window)
            return _safe_rate(sum(1 for r in window if r.get("dnf")), total)

        dnf_rate_10 = _dnf_rate(10)
        dnf_rate_20 = _dnf_rate(20)

        # -- average odds over windows --
        def _avg_cote(n):
            cotes = [r["cote"] for r in past[-n:] if r.get("cote") is not None]
            return _safe_mean(cotes)

        avg_cote_5 = _avg_cote(5)
        avg_cote_10 = _avg_cote(10)

        # -- progression (categorical + numeric windows) --
        all_positions = [r["position"] for r in past]
        prog = _progression(all_positions)
        prog_3 = _progression_numeric(all_positions, 3)
        prog_10 = _progression_numeric(all_positions, 10)

        # -- regularity_score: std deviation of positions (lower = more regular) --
        all_valid_pos = [p_val for p_val in all_positions if p_val is not None]
        regularity_score = _safe_stdev(all_valid_pos)

        # -- best / worst position in last 10 --
        pos_10 = [r["position"] for r in past[-10:] if r["position"] is not None]
        best_position_10 = min(pos_10) if pos_10 else None
        worst_position_10 = max(pos_10) if pos_10 else None

        # -- pct_top3 over windows 10, 20 --
        def _pct_top3(n):
            if nb_avant == 0:
                return None
            window = past[-n:]
            total = len(window)
            top3 = sum(1 for r in window if r["position"] is not None and r["position"] <= 3)
            return _safe_rate(top3, total)

        pct_top3_10 = _pct_top3(10)
        pct_top3_20 = _pct_top3(20)

        # -- days_since_first_race (career length) --
        days_since_first = None
        if past:
            days_since_first = _days_between(past[0]["date"], date_iso)

        # -- avg_distance_run (preferred distance) --
        past_distances = [r["distance"] for r in past if r["distance"] is not None]
        avg_distance_run = _safe_mean(past_distances)

        # -- distance_variety (number of distinct distances) --
        distance_variety = len(set(past_distances)) if past_distances else None

        # -- taux_victoire at hippodrome --
        past_hippo = [r for r in past if r["hippo"] == hippo]
        taux_v_hippo = _safe_rate(
            sum(1 for r in past_hippo if r["gagnant"]),
            len(past_hippo),
        )

        # -- taux_victoire at similar distance (+-200m) --
        past_dist = [r for r in past if dist is not None and r["distance"] is not None and abs(r["distance"] - dist) <= 200]
        taux_v_dist = _safe_rate(
            sum(1 for r in past_dist if r["gagnant"]),
            len(past_dist),
        )

        # -- taux_victoire discipline --
        past_disc = [r for r in past if r["discipline"] == disc]
        taux_v_disc = _safe_rate(
            sum(1 for r in past_disc if r["gagnant"]),
            len(past_disc),
        )

        feat = {
            "partant_uid": uid,
            "forme_victoire_3": forme_v3,
            "forme_victoire_5": forme_v5,
            "forme_victoire_10": forme_v10,
            "forme_victoire_20": forme_v20,
            "forme_place_3": forme_p3,
            "forme_place_5": forme_p5,
            "forme_place_10": forme_p10,
            "forme_place_20": forme_p20,
            "nb_courses_avant": nb_avant,
            "nb_victoires_avant": nb_victoires,
            "nb_places_avant": nb_places,
            "jours_depuis_derniere": jours_dep,
            "derniere_position": derniere_pos,
            "avg_position_3": avg_pos_3,
            "avg_position_5": avg_pos_5,
            "avg_position_10": avg_pos_10,
            "avg_position_20": avg_pos_20,
            "gains_5": gains_5,
            "gains_10": gains_10,
            "gains_20": gains_20,
            "gains_cumules": gains_cumules,
            "streak_victoires": streak_victoires,
            "streak_places": streak_places,
            "streak_sans_victoire": streak_sans_victoire_val,
            "dnf_rate_10": dnf_rate_10,
            "dnf_rate_20": dnf_rate_20,
            "avg_cote_5": avg_cote_5,
            "avg_cote_10": avg_cote_10,
            "progression": prog,
            "progression_3": prog_3,
            "progression_10": prog_10,
            "regularity_score": regularity_score,
            "best_position_10": best_position_10,
            "worst_position_10": worst_position_10,
            "pct_top3_10": pct_top3_10,
            "pct_top3_20": pct_top3_20,
            "days_since_first_race": days_since_first,
            "avg_distance_run": avg_distance_run,
            "distance_variety": distance_variety,
            "taux_victoire_hippo": taux_v_hippo,
            "taux_victoire_distance": taux_v_dist,
            "taux_victoire_discipline": taux_v_disc,
        }
        results.append(feat)

        # Add current race to horse history (will be available for future dates)
        pos_arrivee = p.get("position_arrivee")
        cote_val = p.get("cote_finale") or p.get("cote_reference")
        # DNF: position is None or position == 0 or specific status flags
        is_dnf = pos_arrivee is None or pos_arrivee == 0
        horse_history[cheval].append({
            "date": date_iso,
            "position": pos_arrivee,
            "gagnant": bool(p.get("is_gagnant")),
            "place": bool(p.get("is_place")),
            "hippo": hippo,
            "distance": dist,
            "discipline": disc,
            "gains": p.get("gains_carriere_euros", 0),
            "cote": cote_val,
            "dnf": is_dnf,
        })

    return results


if __name__ == "__main__":
    import os
    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")
    path = os.path.join(base, "partants_normalises.json")
    with open(path, encoding="utf-8") as f:
        partants = json.load(f)
    feats = build_cheval_features(partants)
    print(f"Built {len(feats)} cheval feature records.")
    if feats:
        keys = [k for k in feats[0] if k != "partant_uid"]
        print(f"Features ({len(keys)}): {', '.join(keys)}")
        # Show fill rates
        for k in keys:
            filled = sum(1 for r in feats if r.get(k) is not None)
            print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
