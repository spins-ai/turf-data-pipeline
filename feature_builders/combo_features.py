"""
feature_builders.combo_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Jockey-Trainer-Horse combination features.
Computes win rates, place rates, and experience metrics for entity pairs
(jockey+trainer, jockey+horse, trainer+horse) and entity+context pairs
(jockey+hippodrome, trainer+hippodrome, jockey+distance, trainer+distance).

Temporal integrity: for any partant at date D, only races with date < D are used.

Features produced (13):
- jockey_trainer_nb_courses: how often this jockey+trainer pair has raced together
- jockey_trainer_taux_victoire: win rate of this jockey+trainer pair
- jockey_trainer_taux_place: place rate of this jockey+trainer pair
- jockey_cheval_nb_courses: how often this jockey has ridden this horse
- jockey_cheval_taux_victoire: jockey's win rate on this horse
- trainer_cheval_taux_victoire: trainer's win rate with this horse
- jockey_hippo_taux_victoire: jockey's win rate at this hippodrome
- trainer_hippo_taux_victoire: trainer's win rate at this hippodrome
- jockey_distance_taux_victoire: jockey's win rate at this distance category
- trainer_distance_taux_victoire: trainer's win rate at this distance category
- is_new_jockey: first time this jockey rides this horse (boolean)
- is_new_trainer: first time this trainer trains this horse (boolean)
- jockey_change: different jockey from last race on this horse (boolean)
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional


def _safe_rate(count: int, total: int) -> Optional[float]:
    """Win/place rate, or None if no data."""
    if total == 0:
        return None
    return count / total


def _distance_category(dist: Optional[int]) -> Optional[str]:
    """Bucket distance into sprint/mile/intermediate/staying."""
    if dist is None:
        return None
    if dist < 1400:
        return "sprint"
    elif dist < 1800:
        return "mile"
    elif dist < 2400:
        return "intermediate"
    else:
        return "staying"


def build_combo_features(partants: list[dict]) -> list[dict]:
    """Build jockey-trainer-horse combination features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records. Expected fields: partant_uid, nom_cheval,
        jockey_driver, entraineur, date_reunion_iso, hippodrome_normalise,
        distance, position_arrivee, is_gagnant, is_place.

    Returns
    -------
    list[dict]
        One dict per partant_uid with combo features.
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

    # Accumulate histories for each combo key as we iterate chronologically
    # Each history entry: {"date": str, "gagnant": bool, "place": bool}
    jt_history: dict[str, list[dict]] = defaultdict(list)   # jockey+trainer
    jh_history: dict[str, list[dict]] = defaultdict(list)   # jockey+horse
    th_history: dict[str, list[dict]] = defaultdict(list)   # trainer+horse
    j_hippo_history: dict[str, list[dict]] = defaultdict(list)  # jockey+hippo
    t_hippo_history: dict[str, list[dict]] = defaultdict(list)  # trainer+hippo
    j_dist_history: dict[str, list[dict]] = defaultdict(list)   # jockey+dist_cat
    t_dist_history: dict[str, list[dict]] = defaultdict(list)   # trainer+dist_cat

    # Track last jockey per horse for jockey_change detection
    horse_last_jockey: dict[str, str] = {}

    results = []

    for p in sorted_p:
        uid = p.get("partant_uid")
        cheval = p.get("nom_cheval", "")
        jockey = p.get("jockey_driver", "")
        trainer = p.get("entraineur", "")
        date_iso = p.get("date_reunion_iso", "")
        hippo = p.get("hippodrome_normalise", "")
        dist = p.get("distance")
        dist_cat = _distance_category(dist)

        is_gagnant = bool(p.get("is_gagnant"))
        is_place = bool(p.get("is_place"))
        pos = p.get("position_arrivee")

        # -- Build combo keys --
        jt_key = f"{jockey}||{trainer}" if jockey and trainer else None
        jh_key = f"{jockey}||{cheval}" if jockey and cheval else None
        th_key = f"{trainer}||{cheval}" if trainer and cheval else None
        j_hippo_key = f"{jockey}||{hippo}" if jockey and hippo else None
        t_hippo_key = f"{trainer}||{hippo}" if trainer and hippo else None
        j_dist_key = f"{jockey}||{dist_cat}" if jockey and dist_cat else None
        t_dist_key = f"{trainer}||{dist_cat}" if trainer and dist_cat else None

        # -- Retrieve PAST records (strictly < current date) --
        def _past(history, key):
            if key is None:
                return []
            return [r for r in history.get(key, []) if r["date"] < date_iso]

        jt_past = _past(jt_history, jt_key)
        jh_past = _past(jh_history, jh_key)
        th_past = _past(th_history, th_key)
        j_hippo_past = _past(j_hippo_history, j_hippo_key)
        t_hippo_past = _past(t_hippo_history, t_hippo_key)
        j_dist_past = _past(j_dist_history, j_dist_key)
        t_dist_past = _past(t_dist_history, t_dist_key)

        # -- Compute features --
        jt_nb = len(jt_past)
        jt_wins = sum(1 for r in jt_past if r["gagnant"])
        jt_places = sum(1 for r in jt_past if r["place"])

        jh_nb = len(jh_past)
        jh_wins = sum(1 for r in jh_past if r["gagnant"])

        th_nb = len(th_past)
        th_wins = sum(1 for r in th_past if r["gagnant"])

        j_hippo_nb = len(j_hippo_past)
        j_hippo_wins = sum(1 for r in j_hippo_past if r["gagnant"])

        t_hippo_nb = len(t_hippo_past)
        t_hippo_wins = sum(1 for r in t_hippo_past if r["gagnant"])

        j_dist_nb = len(j_dist_past)
        j_dist_wins = sum(1 for r in j_dist_past if r["gagnant"])

        t_dist_nb = len(t_dist_past)
        t_dist_wins = sum(1 for r in t_dist_past if r["gagnant"])

        # Jockey change: different jockey from last time this horse raced
        last_jockey = horse_last_jockey.get(cheval)
        jockey_change = None
        if last_jockey is not None and jockey:
            jockey_change = 1 if last_jockey != jockey else 0

        feat = {
            "partant_uid": uid,
            # Jockey-Trainer combo
            "jockey_trainer_nb_courses": jt_nb if jt_key else None,
            "jockey_trainer_taux_victoire": _safe_rate(jt_wins, jt_nb),
            "jockey_trainer_taux_place": _safe_rate(jt_places, jt_nb),
            # Jockey-Horse combo
            "jockey_cheval_nb_courses": jh_nb if jh_key else None,
            "jockey_cheval_taux_victoire": _safe_rate(jh_wins, jh_nb),
            # Trainer-Horse combo
            "trainer_cheval_taux_victoire": _safe_rate(th_wins, th_nb),
            # Jockey at hippodrome
            "jockey_hippo_taux_victoire": _safe_rate(j_hippo_wins, j_hippo_nb),
            # Trainer at hippodrome
            "trainer_hippo_taux_victoire": _safe_rate(t_hippo_wins, t_hippo_nb),
            # Jockey at distance category
            "jockey_distance_taux_victoire": _safe_rate(j_dist_wins, j_dist_nb),
            # Trainer at distance category
            "trainer_distance_taux_victoire": _safe_rate(t_dist_wins, t_dist_nb),
            # First-time flags
            "is_new_jockey": 1 if (jh_key and jh_nb == 0) else (0 if jh_key else None),
            "is_new_trainer": 1 if (th_key and th_nb == 0) else (0 if th_key else None),
            # Jockey change
            "jockey_change": jockey_change,
        }

        results.append(feat)

        # -- Append current race to histories for future use --
        record = {"date": date_iso, "gagnant": is_gagnant, "place": is_place}

        if jt_key:
            jt_history[jt_key].append(record)
        if jh_key:
            jh_history[jh_key].append(record)
        if th_key:
            th_history[th_key].append(record)
        if j_hippo_key:
            j_hippo_history[j_hippo_key].append(record)
        if t_hippo_key:
            t_hippo_history[t_hippo_key].append(record)
        if j_dist_key:
            j_dist_history[j_dist_key].append(record)
        if t_dist_key:
            t_dist_history[t_dist_key].append(record)

        # Update last jockey for this horse
        if cheval and jockey:
            horse_last_jockey[cheval] = jockey

    return results
