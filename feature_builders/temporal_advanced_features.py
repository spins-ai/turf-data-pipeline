"""
feature_builders.temporal_advanced_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Calendar and temporal context features extracted from each record.

These are simple per-record features with no history needed:
    - jour_semaine      : day of week (0=Mon, 6=Sun)
    - mois              : month (1-12)
    - saison            : season string (printemps, ete, automne, hiver)
    - is_weekend        : bool (samedi/dimanche)
    - is_quinte         : bool (course quinte du jour)
    - heure_course      : hour of race (int, 0-23)
    - position_dans_reunion : which race number in the meeting (1, 2, ...)

Some of these overlap with course_features (jour_semaine, mois, is_weekend,
heure_course) but are prefixed with ``temp_`` so they can be consumed
independently by the temporal feature group.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SEASON_MAP = {
    1: "hiver", 2: "hiver", 3: "printemps",
    4: "printemps", 5: "printemps", 6: "ete",
    7: "ete", 8: "ete", 9: "automne",
    10: "automne", 11: "automne", 12: "hiver",
}


def _extract_hour(heure_str: Optional[str]) -> Optional[int]:
    """Extract hour from strings like '14h30', '14:30', '14H00'."""
    if not heure_str:
        return None
    m = re.search(r"(\d{1,2})[hH:](\d{2})", str(heure_str))
    if m:
        return int(m.group(1))
    return None


def _detect_quinte(record: dict) -> Optional[bool]:
    """Detect whether this record belongs to a quinte race.

    Checks multiple possible field names from different data sources.
    Returns None if no information is available.
    """
    # Direct flag on the record (partant or course level)
    for key in ("is_quinte", "has_quinte", "quinte"):
        val = record.get(key)
        if val is not None:
            return bool(val)
    # Check paris_evenements list
    paris = record.get("paris_evenements") or []
    if paris:
        return any("QUINTE" in str(p).upper() for p in paris)
    return None


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

def build_temporal_advanced_features(
    partants: list[dict],
    courses: list[dict] | None = None,
    reunions: list[dict] | None = None,
) -> list[dict]:
    """Build temporal / calendar features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records.  Expected fields: ``partant_uid``,
        ``date_reunion_iso``, ``numero_course``, ``reunion_uid``.
        Optional: ``heure_depart``, ``heure``, ``is_quinte``, ``has_quinte``.
    courses : list[dict] | None
        Course records (optional).  Can supply ``heure_depart``, ``heure``,
        ``is_quinte``, ``has_quinte`` at course level.
    reunions : list[dict] | None
        Reunion records (optional).  Can supply ``has_quinte`` at reunion level.

    Returns
    -------
    list[dict]
        One dict per partant_uid with ``temp_`` prefixed features.
    """
    # Build lookup maps
    course_map: dict[str, dict] = {}
    if courses:
        for c in courses:
            cuid = c.get("course_uid")
            if cuid:
                course_map[cuid] = c

    reunion_map: dict[str, dict] = {}
    if reunions:
        for r in reunions:
            ruid = r.get("reunion_uid")
            if ruid:
                reunion_map[ruid] = r

    results: list[dict] = []

    for p in partants:
        uid = p.get("partant_uid")
        date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
        course_uid = p.get("course_uid", "")
        reunion_uid = p.get("reunion_uid", "")

        course = course_map.get(course_uid, {})
        reunion = reunion_map.get(reunion_uid, {})

        feat: dict = {"partant_uid": uid}

        # --- Calendar features from date ---
        jour_semaine: Optional[int] = None
        mois: Optional[int] = None
        saison: Optional[str] = None
        is_weekend: Optional[bool] = None

        if date_iso and len(date_iso) >= 10:
            try:
                dt = datetime.strptime(date_iso, "%Y-%m-%d")
                jour_semaine = dt.weekday()          # 0=Mon, 6=Sun
                mois = dt.month                       # 1-12
                saison = _SEASON_MAP.get(mois)
                is_weekend = jour_semaine >= 5        # Sat=5, Sun=6
            except (ValueError, TypeError):
                pass

        feat["temp_jour_semaine"] = jour_semaine
        feat["temp_mois"] = mois
        feat["temp_saison"] = saison
        feat["temp_is_weekend"] = is_weekend

        # --- is_quinte ---
        # Priority: partant -> course -> reunion
        is_quinte = _detect_quinte(p)
        if is_quinte is None:
            is_quinte = _detect_quinte(course)
        if is_quinte is None:
            is_quinte = _detect_quinte(reunion)
        feat["temp_is_quinte"] = is_quinte

        # --- heure_course ---
        heure_str = (
            p.get("heure_depart")
            or p.get("heure")
            or course.get("heure_depart")
            or course.get("heure")
        )
        feat["temp_heure_course"] = _extract_hour(heure_str)

        # --- position_dans_reunion ---
        # numero_course is the race number within the meeting (1-based)
        num_course = (
            p.get("numero_course")
            or course.get("numero_course")
        )
        if num_course is not None:
            try:
                feat["temp_position_dans_reunion"] = int(num_course)
            except (ValueError, TypeError):
                feat["temp_position_dans_reunion"] = None
        else:
            feat["temp_position_dans_reunion"] = None

        results.append(feat)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import os

    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")

    p_path = os.path.join(base, "partants_normalises.json")
    c_path = os.path.join(base, "courses_normalisees.json")
    r_path = os.path.join(base, "reunions_normalisees.json")

    partants: list[dict] = []
    courses: list[dict] = []
    reunions: list[dict] = []

    if os.path.exists(p_path):
        with open(p_path, encoding="utf-8") as f:
            partants = json.load(f)

    if os.path.exists(c_path):
        with open(c_path, encoding="utf-8") as f:
            courses = json.load(f)

    if os.path.exists(r_path):
        with open(r_path, encoding="utf-8") as f:
            reunions = json.load(f)

    feats = build_temporal_advanced_features(partants, courses, reunions)
    print(f"Built {len(feats)} temporal_advanced feature records.")
    if feats:
        keys = [k for k in feats[0] if k != "partant_uid"]
        print(f"Features ({len(keys)}): {', '.join(keys)}")
        for k in keys:
            filled = sum(1 for r in feats if r.get(k) is not None)
            print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
