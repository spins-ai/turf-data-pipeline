#!/usr/bin/env python3
"""
Feature Engineering — Pedigree x Discipline Match

10 features: lineage fit for discipline, stamina index, precocity.

Pour chaque partant, evalue l'adequation du pedigree avec la discipline
et la distance de la course.

Features produites (~10) :
  - pdm_sire_disc_fit        -> adequation pere x discipline (0-1)
  - pdm_damsire_disc_fit     -> adequation pere_mere x discipline
  - pdm_stamina_index        -> indice d'endurance herite
  - pdm_speed_index          -> indice de vitesse herite
  - pdm_precocity_index      -> indice de precocite
  - pdm_dist_x_stamina       -> interaction distance x stamina (bon/mauvais)
  - pdm_age_x_precocity      -> interaction age x precocite
  - pdm_lineage_class        -> classe estimee du pedigree (A/B/C/D)
  - pdm_sire_discipline_runs -> nb courses du pere dans cette discipline
  - pdm_dam_discipline_runs  -> nb courses produits mere dans cette discipline
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def _norm(name):
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 else None


# Known sire-discipline affinities (French racing context)
# discipline -> {sire -> score 0-1}
SIRE_DISC_SCORES = {
    "plat": {
        "SIYOUNI": 0.90, "WOOTTON BASSETT": 0.85, "LOPE DE VEGA": 0.85,
        "DUBAWI": 0.80, "FRANKEL": 0.80, "GALILEO": 0.75,
        "DEEP IMPACT": 0.80, "KINGMAN": 0.85, "SHAMARDAL": 0.80,
    },
    "obstacle": {
        "SAINT DES SAINTS": 0.90, "KAPGARDE": 0.85, "NETWORK": 0.80,
        "TURGEON": 0.80, "POLIGLOTE": 0.75, "GREAT PRETENDER": 0.80,
        "MARTALINE": 0.80, "SADDLER MAKER": 0.75,
    },
    "trot_attele": {
        "READY CASH": 0.90, "BOLD EAGLE": 0.85, "TIMOKO": 0.80,
        "LOVE YOU": 0.85, "JASMIN DE FLORE": 0.80, "ROYAL DREAM": 0.75,
        "SEVERINO": 0.75, "HURRICANE DU PONT": 0.70,
    },
    "trot_monte": {
        "READY CASH": 0.85, "BOLD EAGLE": 0.80, "LOVE YOU": 0.80,
        "TIMOKO": 0.80, "JASMIN DE FLORE": 0.75,
    },
}


def compute_pedigree_discipline_match(partants):
    """
    Calcule les features pedigree x discipline.
    Les partants DOIVENT etre tries par date.
    """
    log.info(f"Calcul pedigree-discipline match sur {len(partants)} partants...")

    # Accumulate sire offspring performance by discipline
    sire_by_disc = defaultdict(lambda: defaultdict(lambda: {"total": 0, "wins": 0, "gains": 0.0}))
    dam_by_disc = defaultdict(lambda: defaultdict(lambda: {"total": 0, "wins": 0}))

    enriched = 0
    for i, p in enumerate(partants):
        pere = _norm(p.get("pere") or p.get("nom_pere") or p.get("sire"))
        mere = _norm(p.get("mere") or p.get("nom_mere") or p.get("dam"))
        pere_mere = _norm(p.get("pere_mere") or p.get("dam_sire") or p.get("broodmare_sire"))
        discipline = (p.get("rapport_discipline_norm") or p.get("discipline_norm") or "").lower().strip()
        distance = p.get("distance") or p.get("rapport_distance_m")
        age = p.get("age") or p.get("age_cheval")

        try:
            distance = int(distance)
        except (ValueError, TypeError):
            distance = None
        try:
            age = int(age)
        except (ValueError, TypeError):
            age = None

        feat = {}
        has_data = False

        # --- Static sire x discipline fit ---
        if pere and discipline:
            disc_key = discipline.split("_")[0] if "_" in discipline else discipline
            disc_scores = SIRE_DISC_SCORES.get(disc_key, {})
            if pere in disc_scores:
                has_data = True
                feat["pdm_sire_disc_fit"] = disc_scores[pere]

        if pere_mere and discipline:
            disc_key = discipline.split("_")[0] if "_" in discipline else discipline
            disc_scores = SIRE_DISC_SCORES.get(disc_key, {})
            if pere_mere in disc_scores:
                has_data = True
                feat["pdm_damsire_disc_fit"] = disc_scores[pere_mere]

        # --- Dynamic sire stats from accumulated data ---
        if pere and discipline:
            sire_d = sire_by_disc.get(pere, {}).get(discipline)
            if sire_d and sire_d["total"] > 0:
                has_data = True
                feat["pdm_sire_discipline_runs"] = sire_d["total"]
                feat["pdm_sire_disc_win_rate"] = round(sire_d["wins"] / sire_d["total"], 4)

        if mere and discipline:
            dam_d = dam_by_disc.get(mere, {}).get(discipline)
            if dam_d and dam_d["total"] > 0:
                has_data = True
                feat["pdm_dam_discipline_runs"] = dam_d["total"]

        # --- Stamina / Speed index ---
        # Use known sire profiles
        stamina_idx = p.get("ped_sire_stamina_idx")
        precocity_idx = p.get("ped_sire_precocity_idx")

        if stamina_idx is not None:
            has_data = True
            feat["pdm_stamina_index"] = stamina_idx
            feat["pdm_speed_index"] = round(1.0 - stamina_idx, 2)

            # Distance x stamina interaction
            if distance:
                if distance > 2400 and stamina_idx < 0.5:
                    feat["pdm_dist_x_stamina"] = "mismatch_short_stamina"
                elif distance < 1400 and stamina_idx > 0.7:
                    feat["pdm_dist_x_stamina"] = "mismatch_high_stamina"
                elif distance > 2400 and stamina_idx > 0.7:
                    feat["pdm_dist_x_stamina"] = "good_fit_stayer"
                elif distance < 1400 and stamina_idx < 0.4:
                    feat["pdm_dist_x_stamina"] = "good_fit_sprinter"
                else:
                    feat["pdm_dist_x_stamina"] = "neutral"

        if precocity_idx is not None:
            has_data = True
            feat["pdm_precocity_index"] = precocity_idx

            # Age x precocity interaction
            if age:
                if age <= 3 and precocity_idx > 0.7:
                    feat["pdm_age_x_precocity"] = "precocious_young"
                elif age <= 3 and precocity_idx < 0.4:
                    feat["pdm_age_x_precocity"] = "late_developer_young"
                elif age >= 5 and precocity_idx > 0.7:
                    feat["pdm_age_x_precocity"] = "past_peak"
                else:
                    feat["pdm_age_x_precocity"] = "neutral"

        # --- Lineage class estimate ---
        sire_fit = feat.get("pdm_sire_disc_fit", 0)
        damsire_fit = feat.get("pdm_damsire_disc_fit", 0)
        avg_fit = (sire_fit + damsire_fit) / 2 if (sire_fit or damsire_fit) else None
        if avg_fit is not None:
            has_data = True
            if avg_fit >= 0.80:
                feat["pdm_lineage_class"] = "A"
            elif avg_fit >= 0.60:
                feat["pdm_lineage_class"] = "B"
            elif avg_fit >= 0.40:
                feat["pdm_lineage_class"] = "C"
            else:
                feat["pdm_lineage_class"] = "D"

        if has_data:
            enriched += 1
            p.update(feat)

        # --- Update accumulators ---
        classement = None
        for k in ("classement", "arrivee", "place"):
            v = p.get(k)
            if v is not None:
                try:
                    classement = int(v)
                    break
                except (ValueError, TypeError):
                    pass

        gains_val = 0
        try:
            gains_val = float(p.get("gains_course") or p.get("gains") or 0)
        except (ValueError, TypeError):
            pass

        if pere and discipline:
            sire_by_disc[pere][discipline]["total"] += 1
            if classement == 1:
                sire_by_disc[pere][discipline]["wins"] += 1
            sire_by_disc[pere][discipline]["gains"] += gains_val

        if mere and discipline:
            dam_by_disc[mere][discipline]["total"] += 1
            if classement == 1:
                dam_by_disc[mere][discipline]["wins"] += 1

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {enriched} enrichis")

    log.info(f"  -> {enriched}/{len(partants)} enrichis ({enriched*100/max(len(partants),1):.1f}%)")
    return partants
