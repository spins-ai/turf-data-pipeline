"""
feature_builders.class_change_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Class transition and context-change features.
Tracks how the horse's current race compares to its previous races in terms
of allocation (prize money / class level), distance, discipline, hippodrome,
and surface.

Temporal integrity: for any partant at date D, only races with date < D are used.

Features produced (11):
- allocation_diff_vs_last: change in allocation from last race
- allocation_ratio_vs_last: ratio of current vs last allocation
- allocation_rank_career: percentile of this allocation vs career
- is_class_up: higher allocation than last race (boolean)
- is_class_down: lower allocation than last race (boolean)
- distance_diff_vs_last: distance change from last race (signed)
- distance_diff_abs: absolute distance change from last race
- discipline_change: switching discipline from last race (boolean)
- hippo_change: different hippodrome from last race (boolean)
- surface_change: different surface type from last race (boolean)
- nb_class_changes_5: count of class changes in last 5 races
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional


def _safe_percentile_rank(value: float, history_values: list[float]) -> Optional[float]:
    """Compute percentile rank of value within history_values.
    Returns a value in [0, 1] where 1 means this is the highest seen.
    """
    if not history_values:
        return None
    below = sum(1 for v in history_values if v < value)
    equal = sum(1 for v in history_values if v == value)
    return (below + 0.5 * equal) / len(history_values)


def build_class_change_features(partants: list[dict], courses: list[dict]) -> list[dict]:
    """Build class-change and context-transition features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records. Expected fields: partant_uid, nom_cheval,
        date_reunion_iso, course_uid, distance, discipline,
        hippodrome_normalise.
    courses : list[dict]
        All course records. Expected fields: course_uid, allocation_totale,
        type_piste.

    Returns
    -------
    list[dict]
        One dict per partant_uid with class-change features.
    """
    # Build course lookup for allocation and surface
    course_lookup: dict[str, dict] = {}
    for c in courses:
        cuid = c.get("course_uid")
        if cuid:
            course_lookup[cuid] = c

    # Sort chronologically for temporal integrity
    sorted_p = sorted(
        partants,
        key=lambda p: (
            p.get("date_reunion_iso", ""),
            p.get("course_uid", ""),
            p.get("num_pmu", 0),
        ),
    )

    # Accumulate per-horse history
    # Each entry: {"date": str, "allocation": float|None, "distance": int|None,
    #              "discipline": str, "hippo": str, "surface": str|None}
    horse_history: dict[str, list[dict]] = defaultdict(list)

    results = []

    for p in sorted_p:
        uid = p.get("partant_uid")
        cheval = p.get("nom_cheval", "")
        date_iso = p.get("date_reunion_iso", "")
        course_uid = p.get("course_uid", "")
        distance = p.get("distance")
        discipline = p.get("discipline", "")
        hippo = p.get("hippodrome_normalise", "")

        # Get course-level info
        course_info = course_lookup.get(course_uid, {})
        allocation = course_info.get("allocation_totale")
        surface = course_info.get("type_piste", "")

        # Get PAST races for this horse (strictly < current date)
        past = [r for r in horse_history.get(cheval, []) if r["date"] < date_iso]

        # --- Allocation-based features ---
        allocation_diff = None
        allocation_ratio = None
        allocation_rank = None
        is_class_up = None
        is_class_down = None

        if past and allocation is not None:
            last = past[-1]
            last_alloc = last.get("allocation")

            if last_alloc is not None and last_alloc > 0:
                allocation_diff = allocation - last_alloc
                allocation_ratio = allocation / last_alloc
                is_class_up = 1 if allocation > last_alloc else 0
                is_class_down = 1 if allocation < last_alloc else 0

            # Percentile rank of current allocation vs all past allocations
            past_allocs = [r["allocation"] for r in past if r["allocation"] is not None]
            if past_allocs:
                allocation_rank = _safe_percentile_rank(allocation, past_allocs)

        # --- Distance-based features ---
        distance_diff = None
        distance_diff_abs = None

        if past and distance is not None:
            last_dist = past[-1].get("distance")
            if last_dist is not None:
                distance_diff = distance - last_dist
                distance_diff_abs = abs(distance_diff)

        # --- Discipline change ---
        discipline_change = None
        if past and discipline:
            last_disc = past[-1].get("discipline", "")
            if last_disc:
                discipline_change = 1 if discipline != last_disc else 0

        # --- Hippodrome change ---
        hippo_change = None
        if past and hippo:
            last_hippo = past[-1].get("hippo", "")
            if last_hippo:
                hippo_change = 1 if hippo != last_hippo else 0

        # --- Surface change ---
        surface_change = None
        if past and surface:
            last_surface = past[-1].get("surface", "")
            if last_surface:
                surface_change = 1 if surface != last_surface else 0

        # --- Count class changes in last 5 races ---
        nb_class_changes_5 = None
        if len(past) >= 2:
            recent = past[-5:]  # last 5 (or fewer)
            changes = 0
            for i in range(1, len(recent)):
                a_prev = recent[i - 1].get("allocation")
                a_curr = recent[i].get("allocation")
                if a_prev is not None and a_curr is not None and a_prev != a_curr:
                    changes += 1
            nb_class_changes_5 = changes

        feat = {
            "partant_uid": uid,
            "allocation_diff_vs_last": allocation_diff,
            "allocation_ratio_vs_last": allocation_ratio,
            "allocation_rank_career": allocation_rank,
            "is_class_up": is_class_up,
            "is_class_down": is_class_down,
            "distance_diff_vs_last": distance_diff,
            "distance_diff_abs": distance_diff_abs,
            "discipline_change": discipline_change,
            "hippo_change": hippo_change,
            "surface_change": surface_change,
            "nb_class_changes_5": nb_class_changes_5,
        }

        results.append(feat)

        # Append current race to history
        horse_history[cheval].append({
            "date": date_iso,
            "allocation": allocation,
            "distance": distance,
            "discipline": discipline,
            "hippo": hippo,
            "surface": surface,
        })

    return results
