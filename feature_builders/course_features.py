"""
feature_builders.course_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-race context features (same value for all partants in a given race),
including conditions features and hippodrome features.
"""

from __future__ import annotations

import json
import math
import os
import re
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Conditions encodings
# ---------------------------------------------------------------------------
_PENETROMETRE_MAP = {
    "bon": 1, "leger": 2, "souple": 3, "tres souple": 4, "très souple": 4,
    "lourd": 5, "collant": 6, "sec": 1, "standard": 2,
}

_TYPE_PISTE_MAP = {
    "cendree": 1, "cendrée": 1,
    "herbe": 2,
    "sable": 3,
    "gazon": 4,
    "pst": 5, "polytrack": 5, "fibresand": 5, "tapeta": 5,
}

_CORDE_MAP = {
    "gauche": 0, "droite": 1,
}

_MODE_DEPART_MAP = {
    "autostart": 1, "volte": 2, "elastique": 3,
}


# ---------------------------------------------------------------------------
# Hippodrome DB loader
# ---------------------------------------------------------------------------
def _load_hippodromes_db() -> dict[str, dict]:
    """Load the hippodromes database from hippodromes_db.py."""
    db_path = os.path.join(os.path.dirname(__file__), "..", "hippodromes_db.py")
    if not os.path.exists(db_path):
        return {}
    ns: dict[str, Any] = {}
    with open(db_path, encoding="utf-8") as f:
        exec(f.read(), ns)
    return ns.get("HIPPODROMES_DB", {})


def _safe_stdev(values: list) -> Optional[float]:
    if len(values) < 2:
        return None
    return statistics.stdev(values)


def _distance_category(distance: Optional[int]) -> Optional[str]:
    """Classify race distance into a category."""
    if distance is None:
        return None
    if distance <= 1200:
        return "sprint"
    elif distance <= 1600:
        return "mile"
    elif distance <= 2200:
        return "intermediaire"
    elif distance <= 3000:
        return "long"
    else:
        return "marathon"


def _nb_partants_category(nb: int) -> str:
    """Classify field size into small/medium/large."""
    if nb <= 8:
        return "small"
    elif nb <= 14:
        return "medium"
    else:
        return "large"


def _extract_hour(heure_str: Optional[str]) -> Optional[int]:
    """Extract hour of day from a time string like '14h30' or '14:30'."""
    if not heure_str:
        return None
    m = re.search(r'(\d{1,2})[hH:](\d{2})', str(heure_str))
    if m:
        return int(m.group(1))
    return None


def _is_handicap(conditions: Optional[str]) -> Optional[bool]:
    """Detect if race is a handicap from conditions text."""
    if not conditions:
        return None
    return bool(re.search(r'handicap', str(conditions), re.IGNORECASE))


def _is_groupe(categorie: Optional[str]) -> Optional[bool]:
    """Detect if race is a Groupe race from categorie."""
    if not categorie:
        return None
    return bool(re.search(r'groupe|group', str(categorie), re.IGNORECASE))


def _is_listed(categorie: Optional[str]) -> Optional[bool]:
    """Detect if race is a Listed race from categorie."""
    if not categorie:
        return None
    return bool(re.search(r'listed|list[ée]', str(categorie), re.IGNORECASE))


def build_course_features(partants: list[dict], courses: list[dict]) -> list[dict]:
    """Build per-race context features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records.
    courses : list[dict]
        All course records containing course_uid, allocation_totale, discipline, etc.

    Returns
    -------
    list[dict]
        One dict per partant_uid with race-level features.
    """
    # Load hippodromes database
    hippo_db = _load_hippodromes_db()

    # Build course lookup
    course_map: dict[str, dict] = {}
    for c in courses:
        course_map[c.get("course_uid", "")] = c

    # Compute median allocation per discipline for allocation_relative
    disc_allocations: dict[str, list[float]] = defaultdict(list)
    for c in courses:
        alloc = c.get("allocation_totale")
        disc = c.get("discipline", "")
        if alloc is not None and alloc > 0 and disc:
            disc_allocations[disc].append(alloc)
    disc_median: dict[str, float] = {}
    for disc, vals in disc_allocations.items():
        disc_median[disc] = statistics.median(vals) if vals else 1.0

    # Group partants by course_uid
    course_partants: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        course_partants[p.get("course_uid", "")].append(p)

    # Pre-compute per-course features
    course_features_cache: dict[str, dict] = {}

    for course_uid, runners in course_partants.items():
        course = course_map.get(course_uid, {})
        discipline = course.get("discipline", "")
        distance = course.get("distance")
        alloc = course.get("allocation_totale")

        nb_partants = len(runners)

        # allocation_relative
        median_alloc = disc_median.get(discipline, 1.0)
        allocation_relative = None
        if alloc is not None and median_alloc and median_alloc > 0:
            allocation_relative = alloc / median_alloc

        # allocation_log (log of allocation)
        allocation_log = None
        if alloc is not None and alloc > 0:
            allocation_log = math.log(alloc)

        # Horse ratings: nb_victoires / nb_courses
        ratings = []
        for r in runners:
            nb_c = r.get("nb_courses_carriere")
            nb_v = r.get("nb_victoires_carriere", 0) or 0
            if nb_c is not None and nb_c > 0:
                ratings.append(nb_v / nb_c)
            else:
                ratings.append(0.0)

        force_champ = statistics.mean(ratings) if ratings else None
        dispersion_champ = _safe_stdev(ratings)

        # Odds-based features
        cotes = []
        for r in runners:
            cote = r.get("cote_finale") or r.get("cote_reference")
            if cote is not None and cote > 0:
                cotes.append(cote)

        nb_favoris = sum(1 for c in cotes if c < 5)
        nb_outsiders = sum(1 for c in cotes if c > 20)
        cote_favori = min(cotes) if cotes else None

        dist_cat = _distance_category(distance)
        partants_cat = _nb_partants_category(nb_partants)

        # -- Time/calendar features --
        date_iso = course.get("date_reunion_iso", "")
        heure_str = course.get("heure_depart") or course.get("heure")
        heure_course = _extract_hour(heure_str)

        jour_semaine = None
        mois = None
        is_weekend = None
        if date_iso:
            try:
                dt = datetime.strptime(date_iso, "%Y-%m-%d")
                jour_semaine = dt.weekday()  # 0=Mon, 6=Sun
                mois = dt.month
                is_weekend = jour_semaine >= 5
            except (ValueError, TypeError):
                pass

        # -- Race type features --
        conditions = course.get("conditions", "")
        categorie = course.get("categorie", "")
        is_handicap_val = _is_handicap(conditions)
        is_groupe_val = _is_groupe(categorie)
        is_listed_val = _is_listed(categorie)

        # -- Conditions features --
        penetrometre_raw = (course.get("penetrometre") or "").lower().strip()
        cond_penetrometre = _PENETROMETRE_MAP.get(penetrometre_raw, None)

        type_piste_raw = (course.get("type_piste") or "").lower().strip()
        cond_type_piste = _TYPE_PISTE_MAP.get(type_piste_raw, None)

        corde_raw = (course.get("corde") or "").lower().strip()
        cond_corde = _CORDE_MAP.get(corde_raw, None)

        mode_depart_raw = (course.get("mode_depart") or "").lower().strip()
        cond_mode_depart = _MODE_DEPART_MAP.get(mode_depart_raw, None)

        # -- Hippodrome features --
        hippo_nom = (course.get("hippodrome_normalise")
                     or course.get("hippodrome") or "").lower().strip()
        hippo_info = hippo_db.get(hippo_nom, {})

        hippo_altitude = hippo_info.get("altitude")
        hippo_lat = hippo_info.get("lat")
        hippo_lon = hippo_info.get("lon")
        hippo_nb_courses_hist = hippo_info.get("nb_courses")
        hippo_dist_min = hippo_info.get("distance_min")
        hippo_dist_max = hippo_info.get("distance_max")

        hippo_pays = (hippo_info.get("pays") or "").lower().strip()
        is_hippo_etranger = None
        hippo_pays_encoded = None
        if hippo_pays:
            is_hippo_etranger = 0 if hippo_pays == "france" else 1
            _pays_enc_map = {
                "france": 0, "royaume-uni": 1, "irlande": 2, "allemagne": 3,
                "suede": 4, "suède": 4, "norvege": 5, "norvège": 5,
                "belgique": 6, "suisse": 7, "italie": 8, "etats-unis": 9,
                "états-unis": 9, "australie": 10, "japon": 11,
            }
            hippo_pays_encoded = _pays_enc_map.get(hippo_pays, 99)

        # If type_piste not from course, try from hippo_db
        if cond_type_piste is None and hippo_info.get("type_piste"):
            cond_type_piste = _TYPE_PISTE_MAP.get(
                hippo_info["type_piste"].lower().strip(), None
            )
        if cond_corde is None and hippo_info.get("corde"):
            cond_corde = _CORDE_MAP.get(
                hippo_info["corde"].lower().strip(), None
            )

        course_features_cache[course_uid] = {
            "nb_partants": nb_partants,
            "allocation_relative": allocation_relative,
            "allocation_log": allocation_log,
            "force_champ": force_champ,
            "dispersion_champ": dispersion_champ,
            "nb_favoris": nb_favoris,
            "nb_outsiders": nb_outsiders,
            "cote_favori": cote_favori,
            "distance_category": dist_cat,
            "nb_partants_category": partants_cat,
            "heure_course": heure_course,
            "jour_semaine": jour_semaine,
            "mois": mois,
            "is_weekend": is_weekend,
            "is_handicap": is_handicap_val,
            "is_groupe": is_groupe_val,
            "is_listed": is_listed_val,
            # Conditions features
            "cond_penetrometre_encoded": cond_penetrometre,
            "cond_type_piste_encoded": cond_type_piste,
            "cond_corde_encoded": cond_corde,
            "cond_mode_depart_encoded": cond_mode_depart,
            # Hippodrome features
            "hippo_altitude": hippo_altitude,
            "hippo_lat": hippo_lat,
            "hippo_lon": hippo_lon,
            "hippo_nb_courses_historique": hippo_nb_courses_hist,
            "hippo_distance_min": hippo_dist_min,
            "hippo_distance_max": hippo_dist_max,
            "hippo_is_etranger": is_hippo_etranger,
            "hippo_pays_encoded": hippo_pays_encoded,
        }

    # Build output: one record per partant
    results = []
    for p in partants:
        course_uid = p.get("course_uid", "")
        cf = course_features_cache.get(course_uid, {})
        feat = {"partant_uid": p.get("partant_uid")}
        feat.update(cf)
        results.append(feat)

    return results


if __name__ == "__main__":
    import os
    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")
    p_path = os.path.join(base, "partants_normalises.json")
    c_path = os.path.join(base, "courses_normalisees.json")
    with open(p_path, encoding="utf-8") as f:
        partants = json.load(f)
    with open(c_path, encoding="utf-8") as f:
        courses = json.load(f)
    feats = build_course_features(partants, courses)
    print(f"Built {len(feats)} course feature records.")
    if feats:
        keys = [k for k in feats[0] if k != "partant_uid"]
        print(f"Features ({len(keys)}): {', '.join(keys)}")
        for k in keys:
            filled = sum(1 for r in feats if r.get(k) is not None)
            print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
