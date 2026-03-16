#!/usr/bin/env python3
"""
Feature Engineering — Module Temporel / Saisonnalité

Analyse les patterns temporels : certains chevaux sont meilleurs
en hiver, le dimanche, ou sur certaines périodes.

Features produites (~40) :

  SAISONNALITÉ CHEVAL :
    - temp_cheval_vic_saison       → taux victoire de CE cheval dans CETTE saison
    - temp_cheval_nb_saison        → nb courses de ce cheval dans cette saison
    - temp_cheval_is_specialist    → spécialiste de saison (>3 courses, taux > 25%)
    - temp_cheval_vic_mois         → taux victoire ce mois
    - temp_cheval_vic_jour         → taux victoire ce jour de semaine

  SAISONNALITÉ HIPPODROME :
    - temp_hippo_vic_saison        → taux victoire moyen sur cet hippodrome cette saison
    - temp_hippo_nb_saison         → nb courses sur cet hippodrome cette saison

  TENDANCE RÉCENTE DU CHEVAL :
    - temp_gains_trend_3m          → gains 3 derniers mois vs 3 mois d'avant (tendance)
    - temp_vic_trend_3m            → victoires récentes vs avant
    - temp_is_peaking              → le cheval est en pic de forme
    - temp_is_declining            → le cheval est en déclin

  CYCLE DE L'ANNÉE :
    - temp_sin_mois                → sin(2π × mois/12) — capture la cyclicité
    - temp_cos_mois                → cos(2π × mois/12)
    - temp_sin_jour                → sin(2π × jour/7)
    - temp_cos_jour                → cos(2π × jour/7)

  CALENDRIER COURSE :
    - temp_is_weekend              → samedi ou dimanche
    - temp_is_ferie               → jour férié / vacances
    - temp_nb_courses_reunion     → nb de courses dans cette réunion
    - temp_position_reunion       → position de la course dans la réunion (1ère, 5ème...)
"""

import logging, math
from collections import defaultdict
from datetime import datetime

log = logging.getLogger(__name__)

# Jours fériés français (fixes)
JOURS_FERIES = [
    (1, 1),   # Jour de l'an
    (5, 1),   # Fête du travail
    (5, 8),   # Victoire 1945
    (7, 14),  # Fête nationale
    (8, 15),  # Assomption
    (11, 1),  # Toussaint
    (11, 11), # Armistice
    (12, 25), # Noël
]


def compute_temporel(partants):
    """
    Calcule les features temporelles.
    Les partants DOIVENT être triés par date.
    """
    log.info(f"Calcul des features temporelles sur {len(partants)} partants...")

    # Historique par cheval × saison / mois / jour
    cheval_saison = defaultdict(lambda: defaultdict(lambda: {"total": 0, "vic": 0}))
    cheval_mois = defaultdict(lambda: defaultdict(lambda: {"total": 0, "vic": 0}))
    cheval_jour = defaultdict(lambda: defaultdict(lambda: {"total": 0, "vic": 0}))

    # Historique par hippodrome × saison
    hippo_saison = defaultdict(lambda: defaultdict(lambda: {"total": 0, "vic": 0}))

    # Gains par trimestre pour détecter les tendances
    cheval_gains_trim = defaultdict(lambda: defaultdict(float))

    enriched = 0
    for i, partant in enumerate(partants):
        nom = (partant.get("nom_cheval") or "").upper().strip()
        date_str = str(partant.get("date_reunion_iso", ""))[:10]
        hippo = (partant.get("hippodrome") or "").lower().strip()

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            continue

        mois = dt.month
        jour = dt.weekday()  # 0=lundi
        saison = partant.get("rapport_saison") or (
            "hiver" if mois in (12, 1, 2) else
            "printemps" if mois in (3, 4, 5) else
            "ete" if mois in (6, 7, 8) else "automne"
        )
        trimestre = f"{dt.year}-Q{(mois-1)//3+1}"

        has_data = False

        # ── Features cycliques (toujours calculées) ──
        partant["temp_sin_mois"] = round(math.sin(2 * math.pi * mois / 12), 4)
        partant["temp_cos_mois"] = round(math.cos(2 * math.pi * mois / 12), 4)
        partant["temp_sin_jour"] = round(math.sin(2 * math.pi * jour / 7), 4)
        partant["temp_cos_jour"] = round(math.cos(2 * math.pi * jour / 7), 4)

        # ── Calendrier ──
        partant["temp_is_weekend"] = jour >= 5  # samedi ou dimanche
        partant["temp_is_ferie"] = (mois, dt.day) in JOURS_FERIES

        # Position dans la réunion
        num_course = partant.get("rapport_num_course") or partant.get("num_course")
        if num_course is not None:
            try:
                partant["temp_position_reunion"] = int(num_course)
                partant["temp_is_premiere_course"] = int(num_course) == 1
            except (ValueError, TypeError):
                pass

        # ── Saisonnalité du cheval ──
        if nom:
            # Taux victoire par saison
            cs = cheval_saison[nom][saison]
            if cs["total"] > 0:
                has_data = True
                partant["temp_cheval_vic_saison"] = round(cs["vic"] / cs["total"], 4)
                partant["temp_cheval_nb_saison"] = cs["total"]
                partant["temp_cheval_specialist_saison"] = cs["total"] >= 3 and cs["vic"] / cs["total"] >= 0.25

            # Taux victoire par mois
            cm = cheval_mois[nom][mois]
            if cm["total"] > 0:
                partant["temp_cheval_vic_mois"] = round(cm["vic"] / cm["total"], 4)

            # Taux victoire par jour
            cj = cheval_jour[nom][jour]
            if cj["total"] > 0:
                partant["temp_cheval_vic_jour"] = round(cj["vic"] / cj["total"], 4)

            # Tendance gains (ce trimestre vs le précédent)
            trims = cheval_gains_trim[nom]
            current_q = f"{dt.year}-Q{(mois-1)//3+1}"
            prev_q_num = (mois - 1) // 3  # 0-3
            if prev_q_num == 0:
                prev_q = f"{dt.year - 1}-Q4"
            else:
                prev_q = f"{dt.year}-Q{prev_q_num}"

            if prev_q in trims and current_q in trims:
                prev_gains = trims[prev_q]
                curr_gains = trims[current_q]
                if prev_gains > 0:
                    trend = (curr_gains - prev_gains) / prev_gains
                    partant["temp_gains_trend"] = round(trend, 3)
                    partant["temp_is_peaking"] = trend > 0.5
                    partant["temp_is_declining"] = trend < -0.5

        # ── Saisonnalité hippodrome ──
        if hippo:
            hs = hippo_saison[hippo][saison]
            if hs["total"] > 0:
                partant["temp_hippo_vic_saison"] = round(hs["vic"] / hs["total"], 4)
                partant["temp_hippo_nb_saison"] = hs["total"]

        if has_data:
            enriched += 1

        # ── Enregistrer le résultat ──
        classement = partant.get("classement") or partant.get("arrivee") or partant.get("place")
        try:
            classement = int(classement) if classement is not None else None
        except (ValueError, TypeError):
            classement = None

        gains = 0
        try:
            gains = float(partant.get("gains_course") or partant.get("gains") or 0)
        except (ValueError, TypeError):
            pass

        if nom:
            cheval_saison[nom][saison]["total"] += 1
            if classement == 1:
                cheval_saison[nom][saison]["vic"] += 1

            cheval_mois[nom][mois]["total"] += 1
            if classement == 1:
                cheval_mois[nom][mois]["vic"] += 1

            cheval_jour[nom][jour]["total"] += 1
            if classement == 1:
                cheval_jour[nom][jour]["vic"] += 1

            cheval_gains_trim[nom][trimestre] += gains

        if hippo:
            hippo_saison[hippo][saison]["total"] += 1
            if classement == 1:
                hippo_saison[hippo][saison]["vic"] += 1

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)} traités...")

    log.info(f"  → {enriched}/{len(partants)} enrichis")
    return partants
