"""
feature_builders.interaction_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-feature interaction terms.
These multiply or combine features from different builders to capture
non-linear relationships that tree models might not find on their own.

This builder operates on an ALREADY-MERGED feature matrix (i.e., after all
other builders have been merged). It reads existing feature columns and
produces new interaction columns.

Features produced (10):
- forme_x_cote: recent form * implied probability (form-value interaction)
- age_x_distance: age * distance (maturity-stamina interaction)
- poids_x_distance: weight carried * distance (weight impact grows with distance)
- jockey_taux_x_cheval_taux: jockey win rate * horse win rate (synergy)
- forme_x_terrain: form * surface affinity (form on preferred surface)
- cote_x_nb_partants: odds rank percentile * field size (value in big fields)
- allocation_x_forme: allocation * recent form (class-form interaction)
- rest_x_forme: days rest * form (freshness-form interaction)
- age_x_nb_courses: age * experience (maturity-experience)
- is_favori_x_forme: favorite flag * form (favorite quality)
"""

from __future__ import annotations

import math
from typing import Optional


def _get_float(row: dict, key: str) -> Optional[float]:
    """Safely extract a numeric value from a row dict."""
    val = row.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _multiply(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Multiply two values, returning None if either is None."""
    if a is None or b is None:
        return None
    return a * b


def build_interaction_features(merged: list[dict]) -> list[dict]:
    """Build interaction features from an already-merged feature matrix.

    Parameters
    ----------
    merged : list[dict]
        The merged feature matrix containing features from all prior builders.
        Expected features (will gracefully handle missing ones):
        - forme_victoire_5 or musique_taux_victoire (recent form)
        - proba_implicite or cote_finale (odds)
        - profil_age or age (horse age)
        - distance (race distance)
        - poids_porte_kg (weight carried)
        - jockey_taux_victoire_90j (jockey win rate)
        - forme_victoire_5 (horse win rate)
        - affin_disc_taux_victoire or forme_victoire_5 (surface affinity)
        - rang_cote_pct or rang_cote (odds rank)
        - nb_partants (field size)
        - allocation_relative (class level)
        - jours_depuis_derniere (rest days)
        - profil_nb_courses_carriere or nb_courses_avant (experience)
        - is_favori (favorite flag)

    Returns
    -------
    list[dict]
        One dict per record with partant_uid + interaction features.
    """
    results = []

    for row in merged:
        uid = row.get("partant_uid")

        # Gather source features (try multiple possible column names)
        forme = (
            _get_float(row, "forme_victoire_5")
            or _get_float(row, "musique_taux_victoire")
            or _get_float(row, "taux_victoire_carriere")
        )
        proba = _get_float(row, "proba_implicite") or _get_float(row, "proba_normalisee")
        cote = _get_float(row, "cote_finale")
        age = _get_float(row, "profil_age") or _get_float(row, "age")
        distance = _get_float(row, "distance")
        poids = _get_float(row, "poids_porte_kg")
        jockey_taux = (
            _get_float(row, "jockey_taux_victoire_90j")
            or _get_float(row, "jockey_taux_victoire_365j")
        )
        cheval_taux = (
            _get_float(row, "forme_victoire_5")
            or _get_float(row, "taux_victoire_carriere")
        )
        affin_terrain = (
            _get_float(row, "affin_disc_taux_victoire")
            or _get_float(row, "affin_hippo_taux_victoire")
        )
        rang_cote_pct = _get_float(row, "rang_cote_pct") or _get_float(row, "rang_cote")
        nb_partants = _get_float(row, "nb_partants")
        allocation_rel = (
            _get_float(row, "allocation_relative")
            or _get_float(row, "allocation_diff_vs_last")
        )
        jours_repos = _get_float(row, "jours_depuis_derniere")
        nb_courses = (
            _get_float(row, "profil_nb_courses_carriere")
            or _get_float(row, "nb_courses_avant")
        )
        is_favori = _get_float(row, "is_favori")

        # Normalize distance for interaction (divide by 1000 to keep scale reasonable)
        dist_norm = distance / 1000.0 if distance is not None else None

        # Normalize rest days (log scale to dampen extreme values)
        rest_norm = None
        if jours_repos is not None and jours_repos >= 0:
            rest_norm = math.log1p(jours_repos)

        # Normalize nb_courses (log scale)
        exp_norm = None
        if nb_courses is not None and nb_courses >= 0:
            exp_norm = math.log1p(nb_courses)

        feat = {
            "partant_uid": uid,
            # 1. Form * odds: high form + high proba = strong signal
            "forme_x_cote": _multiply(forme, proba),
            # 2. Age * distance: older horses may struggle at longer distances
            "age_x_distance": _multiply(age, dist_norm),
            # 3. Weight * distance: weight burden increases with distance
            "poids_x_distance": _multiply(poids, dist_norm),
            # 4. Jockey quality * horse quality: synergy term
            "jockey_taux_x_cheval_taux": _multiply(jockey_taux, cheval_taux),
            # 5. Form on preferred terrain/discipline
            "forme_x_terrain": _multiply(forme, affin_terrain),
            # 6. Odds adjusted for field size
            "cote_x_nb_partants": _multiply(rang_cote_pct, nb_partants),
            # 7. Class level * form
            "allocation_x_forme": _multiply(allocation_rel, forme),
            # 8. Rest * form (fresh + in-form = dangerous)
            "rest_x_forme": _multiply(rest_norm, forme),
            # 9. Age * experience
            "age_x_nb_courses": _multiply(age, exp_norm),
            # 10. Favorite status * form quality
            "is_favori_x_forme": _multiply(is_favori, forme),
        }

        results.append(feat)

    return results
