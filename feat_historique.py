#!/usr/bin/env python3
"""
Feature Engineering — Module Historique Glissant

Pour chaque partant, calcule sa forme AVANT la course en cours.
Utilise UNIQUEMENT les courses PASSÉES (point-in-time safe).

Features produites (~80) :
  CHEVAL :
    - hist_victoires_5/10/20     → victoires sur les N dernières courses
    - hist_places_5/10/20        → places (top 3) sur les N dernières
    - hist_taux_vic_5/10/20      → taux de victoire glissant
    - hist_taux_place_5/10/20    → taux de place glissant
    - hist_gains_30j/60j/90j     → gains sur les X derniers jours
    - hist_gains_moy_5/10/20     → gains moyens par course
    - hist_nb_courses_30j/60j/90j → nb courses récentes (activité)
    - hist_jours_depuis_derniere → jours depuis la dernière course
    - hist_classement_moy_5/10   → classement moyen
    - hist_regularite_5/10       → écart-type du classement (régularité)
    - hist_progression           → tendance (amélioration ou déclin)
    - hist_streak_victoires      → série de victoires en cours
    - hist_streak_places         → série de places en cours
    - hist_meilleur_classement   → meilleur résultat récent
    - hist_pire_classement       → pire résultat récent
    - hist_dnf_5/10              → nombre de non-finishers (disqualifié, tombé)
    - hist_pct_complete_5/10     → % de courses terminées
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


def compute_historique(partants):
    """
    Calcule les features historiques pour chaque partant.

    IMPORTANT : les partants DOIVENT être triés par date (passé → futur).
    On maintient un dictionnaire de l'historique de chaque cheval au fur et à mesure.
    """
    log.info(f"Calcul des features historiques sur {len(partants)} partants...")

    # Historique par cheval : nom_norm → liste de résultats passés [{date, classement, gains, ...}]
    horse_history = defaultdict(list)

    enriched = 0
    for i, partant in enumerate(partants):
        nom = partant.get("nom_cheval", "")
        if not nom:
            continue
        nom_norm = nom.upper().strip()
        date_str = str(partant.get("date_reunion_iso", ""))[:10]

        try:
            date_course = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            continue

        # ── Récupérer l'historique PASSÉ de ce cheval ──
        history = horse_history[nom_norm]

        if len(history) > 0:
            enriched += 1

            # === JOURS DEPUIS DERNIÈRE COURSE ===
            last = history[-1]
            jours = (date_course - last["date"]).days
            partant["hist_jours_depuis_derniere"] = jours
            partant["hist_repos_category"] = (
                "enchaine" if jours <= 7 else
                "frais" if jours <= 21 else
                "repose" if jours <= 60 else
                "longue_absence" if jours <= 180 else
                "tres_longue_absence"
            )

            # === FEATURES GLISSANTES (5, 10, 20 dernières courses) ===
            for window_name, window in [("5", 5), ("10", 10), ("20", 20)]:
                recent = history[-window:]
                n = len(recent)
                if n == 0:
                    continue

                # Victoires et places
                victoires = sum(1 for r in recent if r.get("classement") == 1)
                places = sum(1 for r in recent if r.get("classement") is not None and r["classement"] <= 3)
                finishers = [r for r in recent if r.get("classement") is not None]
                dnf = n - len(finishers)

                partant[f"hist_victoires_{window_name}"] = victoires
                partant[f"hist_places_{window_name}"] = places
                partant[f"hist_taux_vic_{window_name}"] = round(victoires / n, 4) if n else 0
                partant[f"hist_taux_place_{window_name}"] = round(places / n, 4) if n else 0
                partant[f"hist_dnf_{window_name}"] = dnf
                partant[f"hist_pct_complete_{window_name}"] = round(len(finishers) / n, 4) if n else 0

                # Gains
                gains = [r.get("gains", 0) or 0 for r in recent]
                partant[f"hist_gains_total_{window_name}"] = sum(gains)
                partant[f"hist_gains_moy_{window_name}"] = round(sum(gains) / n, 2) if n else 0

                # Classement moyen
                classements = [r["classement"] for r in finishers]
                if classements:
                    moy = sum(classements) / len(classements)
                    partant[f"hist_classement_moy_{window_name}"] = round(moy, 2)
                    partant[f"hist_meilleur_{window_name}"] = min(classements)
                    partant[f"hist_pire_{window_name}"] = max(classements)

                    # Régularité (écart-type)
                    if len(classements) > 1:
                        variance = sum((c - moy) ** 2 for c in classements) / len(classements)
                        partant[f"hist_regularite_{window_name}"] = round(variance ** 0.5, 2)

            # === FEATURES TEMPORELLES (30j, 60j, 90j) ===
            for days_name, days_limit in [("30j", 30), ("60j", 60), ("90j", 90), ("180j", 180)]:
                cutoff = date_course - timedelta(days=days_limit)
                recent_time = [r for r in history if r["date"] >= cutoff]
                partant[f"hist_nb_courses_{days_name}"] = len(recent_time)
                partant[f"hist_gains_{days_name}"] = sum(r.get("gains", 0) or 0 for r in recent_time)
                if recent_time:
                    vic_time = sum(1 for r in recent_time if r.get("classement") == 1)
                    partant[f"hist_taux_vic_{days_name}"] = round(vic_time / len(recent_time), 4)

            # === STREAKS (séries en cours) ===
            streak_vic = 0
            streak_place = 0
            for r in reversed(history):
                if r.get("classement") == 1:
                    streak_vic += 1
                else:
                    break
            for r in reversed(history):
                if r.get("classement") is not None and r["classement"] <= 3:
                    streak_place += 1
                else:
                    break
            partant["hist_streak_victoires"] = streak_vic
            partant["hist_streak_places"] = streak_place

            # === PROGRESSION (tendance) ===
            if len(history) >= 5:
                recent_5 = [r["classement"] for r in history[-5:] if r.get("classement")]
                older_5 = [r["classement"] for r in history[-10:-5] if r.get("classement")]
                if recent_5 and older_5:
                    moy_recent = sum(recent_5) / len(recent_5)
                    moy_older = sum(older_5) / len(older_5)
                    # Négatif = progression (classement baisse = mieux)
                    partant["hist_progression"] = round(moy_recent - moy_older, 2)
                    partant["hist_is_improving"] = moy_recent < moy_older
                    partant["hist_is_declining"] = moy_recent > moy_older + 2

            # === NB COURSES TOTAL À CE STADE ===
            partant["hist_nb_courses_avant"] = len(history)
            partant["hist_is_debutant"] = len(history) <= 2

        else:
            # Première course connue
            partant["hist_jours_depuis_derniere"] = None
            partant["hist_nb_courses_avant"] = 0
            partant["hist_is_debutant"] = True

        # ── Ajouter cette course à l'historique (pour les courses futures) ──
        # On essaie de récupérer le classement et les gains de cette course
        classement = partant.get("rapport_combinaison_gagnant")
        # Tenter de déduire le classement depuis d'autres champs
        if classement is None:
            # Si on a le résultat direct
            classement = partant.get("classement") or partant.get("arrivee") or partant.get("place")
        if classement is not None:
            try:
                classement = int(classement)
            except (ValueError, TypeError):
                classement = None

        gains = partant.get("gains_course") or partant.get("gains") or 0
        try:
            gains = float(gains)
        except (ValueError, TypeError):
            gains = 0

        horse_history[nom_norm].append({
            "date": date_course,
            "classement": classement,
            "gains": gains,
            "hippodrome": partant.get("hippodrome"),
            "distance": partant.get("distance") or partant.get("rapport_distance_m"),
            "terrain": partant.get("meteo_terrain_category"),
            "discipline": partant.get("rapport_discipline_norm"),
        })

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis...")

    log.info(f"  → {enriched}/{len(partants)} partants avec historique ({enriched*100/max(len(partants),1):.1f}%)")
    return partants
