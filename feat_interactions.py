#!/usr/bin/env python3
"""
Feature Engineering — Module Interactions + Signaux Marché

Features qui combinent plusieurs champs entre eux
+ features dérivées du marché des paris.

Features produites (~60) :

  INTERACTIONS :
    - inter_cote_x_forme5         → cote × taux victoire récent (value bet indicator)
    - inter_poids_x_distance      → poids relatif × distance (fatigue indicator)
    - inter_age_x_distance        → âge × catégorie distance
    - inter_forme_x_terrain       → forme récente × spécialiste terrain
    - inter_equipment_x_forme     → changement équipement × forme (signal tactique)
    - inter_repos_x_forme         → jours de repos × forme récente
    - inter_cote_x_jockey         → cote × taux victoire jockey

  FORCE DU CHAMP (relativement aux autres partants de la course) :
    - field_nb_partants           → nombre de partants dans la course
    - field_rang_forme            → rang du cheval par forme dans la course
    - field_rang_gains            → rang par gains totaux
    - field_pct_favoris           → % de chevaux avec cote < 6
    - field_cote_min              → cote du favori
    - field_cote_max              → cote du plus gros outsider
    - field_ecart_favori          → écart de cote avec le favori

  SIGNAUX MARCHÉ :
    - mkt_proba_rank              → rang de probabilité implicite
    - mkt_value_bet               → proba implicite vs taux victoire historique
    - mkt_is_value                → True si value bet positif
    - mkt_consensus_vs_forme      → le marché est d'accord avec la forme ?
    - mkt_smart_money             → signal de smart money (cote basse + popularité haute)

  PEDIGREE INTERACTIONS :
    - ped_pere_x_terrain          → père × terrain (certaines lignées aiment le lourd)
    - ped_pere_x_distance         → père × distance
    - ped_age_category            → catégorie d'âge pour la discipline
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def compute_interactions(partants):
    """
    Calcule les features d'interactions.
    Certaines features nécessitent de voir TOUS les partants d'une même course.
    """
    log.info(f"Calcul des features interactions sur {len(partants)} partants...")

    # ════════════════════════════════════
    # PHASE 1 : Features individuelles (interactions simples)
    # ════════════════════════════════════
    log.info("  Phase 1: Interactions individuelles...")
    for partant in partants:
        # ── Cote × Forme ──
        cote = partant.get("marche_cote_finale")
        forme5 = partant.get("hist_taux_vic_5")
        if cote is not None and forme5 is not None:
            try:
                c = float(cote)
                f = float(forme5)
                # Value bet : bonne forme mais cote haute = opportunité
                partant["inter_cote_x_forme5"] = round(c * f, 4)
                # Si forme élevée et cote élevée → le marché sous-estime
                if f > 0.2 and c > 8:
                    partant["inter_is_underrated"] = True
            except (ValueError, TypeError):
                pass

        # ── Poids × Distance ──
        poids = partant.get("poids_porte_kg")
        distance = partant.get("rapport_distance_m")
        if poids is not None and distance is not None:
            try:
                p = float(poids)
                d = float(distance)
                # Plus la distance est longue, plus le poids pèse
                partant["inter_poids_x_distance"] = round(p * d / 1000, 2)
                # Poids par km
                if d > 0:
                    partant["inter_poids_par_km"] = round(p / (d / 1000), 2)
            except (ValueError, TypeError):
                pass

        # ── Repos × Forme ──
        repos = partant.get("hist_jours_depuis_derniere")
        if repos is not None and forme5 is not None:
            try:
                r = float(repos)
                f = float(forme5)
                partant["inter_repos_x_forme"] = round(r * f, 2)
                # Cheval en forme qui revient vite = dangereux
                partant["inter_is_fresh_and_fit"] = r <= 21 and f > 0.2
                # Longue absence + mauvaise forme = attention
                partant["inter_is_rusty"] = r > 90 and f < 0.1
            except (ValueError, TypeError):
                pass

        # ── Changement équipement × Forme ──
        equip_score = partant.get("equipment_change_score", 0)
        if equip_score and equip_score > 0 and forme5 is not None:
            try:
                f = float(forme5)
                # L'entraîneur change l'équipement quand le cheval est en méforme
                partant["inter_equip_x_forme"] = round(equip_score * (1 - f), 2)
                partant["inter_tactique_change"] = equip_score >= 2 and f < 0.15
            except (ValueError, TypeError):
                pass

        # ── Première œillères (signal très fort) ──
        if partant.get("signal_premiere_oeilleres") or partant.get("premiere_oeilleres"):
            partant["inter_first_blinkers"] = True
            # En combo avec la forme
            if forme5 is not None:
                try:
                    f = float(forme5)
                    partant["inter_first_blinkers_on_bad_form"] = f < 0.10
                except (ValueError, TypeError):
                    pass

        # ── Cote × Jockey ──
        jock_taux = partant.get("jock_taux_vic")
        if cote is not None and jock_taux is not None:
            try:
                c = float(cote)
                j = float(jock_taux)
                partant["inter_cote_x_jockey"] = round(c * j, 4)
            except (ValueError, TypeError):
                pass

        # ── Âge × Discipline ──
        age = partant.get("ped_annee_naissance")
        discipline = partant.get("rapport_discipline_norm")
        if age is not None and discipline:
            try:
                # Calculer l'âge approximatif
                date = partant.get("date_reunion_iso", "")[:4]
                if date:
                    age_val = int(date) - int(age)
                    partant["inter_age_course"] = age_val
                    # Catégorie d'âge par discipline
                    if discipline in ("plat",):
                        partant["inter_age_ideal"] = 3 <= age_val <= 5
                    elif discipline in ("haie", "steeple", "cross"):
                        partant["inter_age_ideal"] = 4 <= age_val <= 8
                    elif discipline in ("trot_attele", "trot_monte"):
                        partant["inter_age_ideal"] = 4 <= age_val <= 10
            except (ValueError, TypeError):
                pass

        # ── Value Bet (marché vs historique) ──
        proba = partant.get("marche_proba_implicite")
        hist_taux = partant.get("hist_taux_vic_10") or partant.get("stats_taux_victoire")
        if proba is not None and hist_taux is not None:
            try:
                p = float(proba)
                h = float(hist_taux)
                if p > 0:
                    value = h / p  # > 1 = le cheval gagne plus souvent que le marché pense
                    partant["mkt_value_bet"] = round(value, 3)
                    partant["mkt_is_value"] = value > 1.2
                    partant["mkt_is_overbet"] = value < 0.5
            except (ValueError, TypeError):
                pass

        # ── Smart Money Signal ──
        popularite = partant.get("marche_popularite")
        rang_cote = partant.get("marche_rang_cote")
        if popularite == "star" and rang_cote is not None:
            try:
                partant["mkt_smart_money"] = int(rang_cote) <= 3
            except (ValueError, TypeError):
                pass

    # ════════════════════════════════════
    # PHASE 2 : Features relatives au champ (même course)
    # ════════════════════════════════════
    log.info("  Phase 2: Features relatives au champ...")

    # Grouper les partants par course
    courses = defaultdict(list)
    for idx, partant in enumerate(partants):
        uid = partant.get("course_uid")
        if uid:
            courses[uid].append(idx)

    log.info(f"    → {len(courses)} courses identifiées")

    for course_uid, indices in courses.items():
        n = len(indices)
        if n < 2:
            continue

        # Récupérer les cotes de la course
        cotes = []
        formes = []
        gains_list = []
        for idx in indices:
            p = partants[idx]
            cote = p.get("marche_cote_finale")
            forme = p.get("hist_taux_vic_5") or p.get("stats_taux_victoire")
            gains = p.get("stats_gains_total_euros") or p.get("hist_gains_total_10")

            cotes.append((idx, float(cote) if cote is not None else 999))
            formes.append((idx, float(forme) if forme is not None else 0))
            gains_list.append((idx, float(gains) if gains is not None else 0))

        # Trier par cote
        cotes.sort(key=lambda x: x[1])
        formes.sort(key=lambda x: -x[1])  # meilleur en premier
        gains_list.sort(key=lambda x: -x[1])

        cote_min = cotes[0][1] if cotes else 999
        cote_max = cotes[-1][1] if cotes else 0
        nb_favoris = sum(1 for _, c in cotes if c < 6)

        for rank_cote, (idx, cote_val) in enumerate(cotes):
            p = partants[idx]
            p["field_nb_partants"] = n
            p["field_rang_cote"] = rank_cote + 1
            p["field_cote_min"] = cote_min
            p["field_cote_max"] = cote_max
            p["field_ecart_favori"] = round(cote_val - cote_min, 2)
            p["field_pct_favoris"] = round(nb_favoris / n, 2)

        for rank_forme, (idx, _) in enumerate(formes):
            partants[idx]["field_rang_forme"] = rank_forme + 1

        for rank_gains, (idx, _) in enumerate(gains_list):
            partants[idx]["field_rang_gains"] = rank_gains + 1

        # Consensus marché vs forme
        for idx in indices:
            p = partants[idx]
            rang_c = p.get("field_rang_cote")
            rang_f = p.get("field_rang_forme")
            if rang_c is not None and rang_f is not None:
                diff = abs(rang_c - rang_f)
                p["field_consensus_ecart"] = diff
                p["field_consensus_ok"] = diff <= 2
                p["field_marche_vs_forme"] = rang_c - rang_f  # négatif = marché plus optimiste

    enriched = sum(1 for p in partants if p.get("field_nb_partants"))
    log.info(f"  → {enriched}/{len(partants)} partants avec features de champ")

    return partants
