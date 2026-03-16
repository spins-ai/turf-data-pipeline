#!/usr/bin/env python3
"""
Feature Engineering — Module Séquences / Patterns

Analyse les PATTERNS dans les résultats récents d'un cheval.
La "musique" en hippisme (ex: 1p2p5a3a) cache des patterns prédictifs.

Features produites (~30) :

  MUSIQUE (séquence de résultats) :
    - seq_musique_5               → les 5 derniers classements en string "1-3-2-5-1"
    - seq_pattern_type            → "gagnant_regulier", "place_regulier", "irregulier", "en_progres", "en_chute"
    - seq_derniere_victoire_ago   → nombre de courses depuis la dernière victoire
    - seq_derniere_place_ago      → nombre de courses depuis le dernier top 3
    - seq_alternance              → le cheval alterne bon/mauvais résultats ?
    - seq_trend_classement        → pente de la droite de régression des classements

  PATTERNS SPÉCIAUX :
    - seq_back_from_win           → revient après une victoire (pression ?)
    - seq_after_dnf               → revient après un DNF (disqualification)
    - seq_consecutive_same_hippo  → courses consécutives sur le même hippodrome
    - seq_change_discipline       → changement de discipline (trot → galop)
    - seq_change_distance         → changement de catégorie de distance
    - seq_montee_en_classe        → passe à une classe supérieure

  RÉGULARITÉ :
    - seq_ecart_type_5            → écart-type des 5 derniers classements
    - seq_range_5                 → max - min des 5 derniers classements
    - seq_coefficient_variation   → coefficient de variation (régularité)
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def linear_trend(values):
    """Calcule la pente de la tendance linéaire d'une liste de valeurs"""
    n = len(values)
    if n < 2:
        return 0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return 0
    return num / den


def detect_pattern(classements):
    """Détecte le pattern dans une séquence de classements"""
    if not classements:
        return "inconnu"
    n = len(classements)

    vic = sum(1 for c in classements if c == 1)
    places = sum(1 for c in classements if c <= 3)

    if vic >= n * 0.4:
        return "gagnant_regulier"
    if places >= n * 0.6:
        return "place_regulier"

    # Progression (classements qui baissent = mieux)
    if n >= 3:
        trend = linear_trend(classements)
        if trend < -0.5:
            return "en_progres"
        if trend > 0.5:
            return "en_chute"

    # Alternance
    if n >= 4:
        changes = sum(1 for i in range(1, n) if (classements[i] <= 3) != (classements[i-1] <= 3))
        if changes >= n * 0.6:
            return "alternant"

    return "irregulier"


def compute_sequences(partants):
    """
    Calcule les features de séquences/patterns.
    Les partants DOIVENT être triés par date.
    """
    log.info(f"Calcul des features séquences sur {len(partants)} partants...")

    # Historique structuré par cheval
    horse_seq = defaultdict(list)  # nom → [{classement, hippo, distance, discipline, is_dnf}]

    enriched = 0
    for i, partant in enumerate(partants):
        nom = (partant.get("nom_cheval") or "").upper().strip()
        if not nom:
            continue

        history = horse_seq[nom]

        if len(history) > 0:
            enriched += 1

            # ── Musique (5 derniers résultats) ──
            recent = history[-5:]
            classements = [h["classement"] for h in recent if h["classement"] is not None]

            if classements:
                partant["seq_musique_5"] = "-".join(str(c) for c in classements[-5:])
                partant["seq_pattern_type"] = detect_pattern(classements)

                # Stats sur les classements récents
                partant["seq_classement_moy_5"] = round(sum(classements[-5:]) / len(classements[-5:]), 2)
                if len(classements) >= 2:
                    moy = sum(classements) / len(classements)
                    var = sum((c - moy) ** 2 for c in classements) / len(classements)
                    partant["seq_ecart_type_5"] = round(var ** 0.5, 2)
                    partant["seq_range_5"] = max(classements) - min(classements)
                    if moy > 0:
                        partant["seq_coefficient_variation"] = round((var ** 0.5) / moy, 3)

                # Tendance
                if len(classements) >= 3:
                    partant["seq_trend_classement"] = round(linear_trend(classements), 3)

            # ── Depuis dernière victoire / place ──
            courses_depuis_vic = 0
            courses_depuis_place = 0
            for h in reversed(history):
                if h["classement"] == 1:
                    break
                courses_depuis_vic += 1
            for h in reversed(history):
                if h["classement"] is not None and h["classement"] <= 3:
                    break
                courses_depuis_place += 1
            partant["seq_derniere_victoire_ago"] = courses_depuis_vic
            partant["seq_derniere_place_ago"] = courses_depuis_place
            partant["seq_jamais_gagne"] = all(h["classement"] != 1 for h in history if h["classement"])

            # ── Patterns spéciaux ──
            last = history[-1]

            # Revient après une victoire
            partant["seq_back_from_win"] = last.get("classement") == 1

            # Revient après un DNF
            partant["seq_after_dnf"] = last.get("is_dnf", False)

            # Même hippodrome que la dernière fois
            current_hippo = (partant.get("hippodrome") or "").lower().strip()
            last_hippo = (last.get("hippo") or "").lower().strip()
            partant["seq_same_hippo"] = current_hippo == last_hippo and current_hippo != ""

            # Changement de discipline
            current_disc = partant.get("rapport_discipline_norm") or partant.get("discipline_norm")
            last_disc = last.get("discipline")
            if current_disc and last_disc:
                partant["seq_change_discipline"] = current_disc != last_disc

            # Changement de distance
            current_dist = partant.get("rapport_distance_category")
            last_dist = last.get("distance_cat")
            if current_dist and last_dist:
                partant["seq_change_distance"] = current_dist != last_dist

            # ── Compteur courses consécutives même hippodrome ──
            consec_hippo = 0
            for h in reversed(history):
                if (h.get("hippo") or "").lower().strip() == current_hippo and current_hippo:
                    consec_hippo += 1
                else:
                    break
            partant["seq_consecutive_same_hippo"] = consec_hippo

            # ── Alternance bon/mauvais ──
            if len(history) >= 4:
                recent_results = [h["classement"] for h in history[-4:] if h["classement"]]
                if len(recent_results) >= 4:
                    bon_mauvais = [c <= 3 for c in recent_results]
                    changes = sum(1 for j in range(1, len(bon_mauvais)) if bon_mauvais[j] != bon_mauvais[j-1])
                    partant["seq_alternance"] = changes >= len(bon_mauvais) - 1

        # ── Enregistrer ──
        classement = partant.get("classement") or partant.get("arrivee") or partant.get("place")
        try:
            classement = int(classement) if classement is not None else None
        except (ValueError, TypeError):
            classement = None

        is_dnf = classement is None or classement == 0

        horse_seq[nom].append({
            "classement": classement,
            "hippo": partant.get("hippodrome"),
            "distance_cat": partant.get("rapport_distance_category"),
            "discipline": partant.get("rapport_discipline_norm"),
            "is_dnf": is_dnf,
        })

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)} traités...")

    log.info(f"  → {enriched}/{len(partants)} enrichis")
    return partants
