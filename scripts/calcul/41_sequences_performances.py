#!/usr/bin/env python3
"""
Script 41 — Séquences de performances pour LSTM/GRU/TFT
Calcul local, 0 requête API.

Input :
  - output/22_performances_detaillees/performances_detaillees.jsonl (ou .json)
  - output/02_liste_courses/partants_normalises.jsonl (ou .json)

Output : output/41_sequences/
  - sequences_performances.jsonl

Features calculées par cheval (séquence des N dernières courses) :
  - positions : [3, 1, 2, 5, ...] (séquence classements)
  - cotes : [4.5, 3.2, 8.0, ...] (séquence cotes)
  - gains : [450, 1200, 0, ...] (séquence gains)
  - distances : [2400, 1600, 2000, ...] (séquence distances)
  - red_km : [72000, 71500, ...] (réductions kilométriques)
  - jours_repos : [14, 21, 7, ...] (jours entre courses)
  - trend_position : pente linéaire des positions récentes
  - trend_cote : pente linéaire des cotes récentes
  - volatilite_position : std des 5 dernières positions
  - serie_victoires : nb victoires consécutives en cours
  - serie_places : nb places consécutives en cours
  - serie_non_places : nb non-placés consécutifs
  - progression_score : score composite de progression
  - momentum : accélération de la forme
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import os
import sys
import math
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "../../output", "41_sequences")
os.makedirs(OUTPUT_DIR, exist_ok=True)

log = setup_logging("41_sequences_performances")

MAX_SEQ_LEN = 20  # Nombre max de courses dans la séquence


def load_partants():
    """Charge les partants en streaming — uniquement les champs nécessaires."""
    KEEP = {"nom_cheval", "horse_id", "date_reunion_iso", "course_uid", "partant_uid",
            "position_arrivee", "cote_finale", "gains_carriere_euros", "gains_annee_euros",
            "distance", "discipline", "hippodrome_normalise", "reduction_km_ms",
            "temps_ms", "is_gagnant", "is_place", "is_disqualifie",
            "jockey_driver", "poids_porte_kg", "oeilleres", "deferre",
            "numero_reunion", "numero_course"}

    partants = []

    # JSONL d'abord
    for path in [os.path.join(BASE_DIR, "../../output", "02_liste_courses", "partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "02_liste_courses", "partants_normalises.json")]:
        if not os.path.exists(path):
            continue

        log.info(f"Chargement: {path}")
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        p = json.loads(line)
                        partants.append({k: p[k] for k in KEEP if k in p})
                    except json.JSONDecodeError:
                        continue
        else:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for p in data:
                partants.append({k: p[k] for k in KEEP if k in p})
            del data

        log.info(f"  {len(partants)} partants chargés")
        break

    return partants


def linear_slope(values):
    """Calcule la pente de régression linéaire simple."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den > 0 else 0.0


def compute_sequences(partants):
    """Calcule les features de séquence pour chaque partant."""
    # Trier par date
    partants.sort(key=lambda p: (p.get("date_reunion_iso", ""), p.get("numero_reunion", 0), p.get("numero_course", 0)))

    log.info(f"Calcul séquences sur {len(partants)} partants...")

    # Historique par cheval
    horse_history = defaultdict(list)
    enriched = 0

    output_file = os.path.join(OUTPUT_DIR, "sequences_performances.jsonl")

    with open(output_file, "w", encoding="utf-8", newline="\n") as fout:
        for i, partant in enumerate(partants):
            nom = (partant.get("nom_cheval") or "").upper().strip()
            if not nom:
                continue

            history = horse_history[nom]

            result = {
                "partant_uid": partant.get("partant_uid", ""),
                "course_uid": partant.get("course_uid", ""),
                "nom_cheval": nom,
                "date_reunion_iso": partant.get("date_reunion_iso", ""),
                "nb_courses_historique": len(history),
            }

            enriched += 1
            if len(history) >= 1:

                # Séquences brutes (dernières N courses)
                recent = history[-MAX_SEQ_LEN:]

                positions = [h["pos"] for h in recent if h["pos"] is not None]
                cotes = [h["cote"] for h in recent if h["cote"] is not None]
                gains = [h["gain"] for h in recent if h["gain"] is not None]
                distances = [h["dist"] for h in recent if h["dist"] is not None]
                red_km = [h["red_km"] for h in recent if h["red_km"] is not None]

                result["seq_positions"] = positions[-MAX_SEQ_LEN:]
                result["seq_cotes"] = cotes[-MAX_SEQ_LEN:]
                result["seq_distances"] = distances[-MAX_SEQ_LEN:]

                # Tendances (sur les 5 dernières)
                pos_5 = positions[-5:]
                cotes_5 = cotes[-5:]

                result["trend_position_5"] = round(linear_slope(pos_5), 4) if len(pos_5) >= 2 else None
                result["trend_cote_5"] = round(linear_slope(cotes_5), 4) if len(cotes_5) >= 2 else None
                result["trend_position_10"] = round(linear_slope(positions[-10:]), 4) if len(positions) >= 3 else None

                # Volatilité
                if len(pos_5) >= 2:
                    mean_p = sum(pos_5) / len(pos_5)
                    result["volatilite_position_5"] = round(math.sqrt(sum((x - mean_p)**2 for x in pos_5) / len(pos_5)), 3)
                else:
                    result["volatilite_position_5"] = None

                # Moyenne position récente
                result["position_moy_5"] = round(sum(pos_5) / len(pos_5), 2) if pos_5 else None
                result["position_moy_10"] = round(sum(positions[-10:]) / len(positions[-10:]), 2) if positions else None

                # Séries (victoires, places, non-placés consécutifs)
                serie_vic = 0
                serie_place = 0
                serie_np = 0
                for h in reversed(recent):
                    if h.get("gagnant"):
                        serie_vic += 1
                        serie_place += 1
                    elif h.get("place"):
                        serie_vic = 0  # reset
                        serie_place += 1
                    else:
                        break
                for h in reversed(recent):
                    if not h.get("place") and not h.get("gagnant"):
                        serie_np += 1
                    else:
                        break

                result["serie_victoires"] = serie_vic
                result["serie_places"] = serie_place
                result["serie_non_places"] = serie_np

                # Jours de repos
                dates = [h["date"] for h in recent if h["date"]]
                jours_repos = []
                for j in range(1, len(dates)):
                    try:
                        d1 = datetime.strptime(dates[j-1], "%Y-%m-%d")
                        d2 = datetime.strptime(dates[j], "%Y-%m-%d")
                        jours_repos.append((d2 - d1).days)
                    except (ValueError, TypeError):
                        pass

                result["seq_jours_repos"] = jours_repos[-MAX_SEQ_LEN:]
                result["repos_moy"] = round(sum(jours_repos) / len(jours_repos), 1) if jours_repos else None

                # Dernier repos
                if dates and partant.get("date_reunion_iso"):
                    try:
                        d_last = datetime.strptime(dates[-1], "%Y-%m-%d")
                        d_now = datetime.strptime(partant["date_reunion_iso"][:10], "%Y-%m-%d")
                        result["jours_depuis_derniere"] = (d_now - d_last).days
                    except (ValueError, TypeError):
                        result["jours_depuis_derniere"] = None
                else:
                    result["jours_depuis_derniere"] = None

                # Réduction km moyenne
                if red_km:
                    result["red_km_moy_5"] = round(sum(red_km[-5:]) / len(red_km[-5:]))
                    result["red_km_best"] = min(red_km)
                    result["red_km_trend"] = round(linear_slope(red_km[-5:]), 2) if len(red_km) >= 2 else None
                else:
                    result["red_km_moy_5"] = None
                    result["red_km_best"] = None
                    result["red_km_trend"] = None

                # Progression score composite
                if len(pos_5) >= 3:
                    trend = linear_slope(pos_5)
                    # Trend négatif = progression (position diminue = monte au classement)
                    result["progression_score"] = round(-trend * 10 + (5 - (sum(pos_5) / len(pos_5))), 2)
                else:
                    result["progression_score"] = None

                # Momentum (accélération)
                if len(positions) >= 6:
                    slope_old = linear_slope(positions[-6:-3])
                    slope_new = linear_slope(positions[-3:])
                    result["momentum"] = round(slope_old - slope_new, 4)  # positif = accélère
                else:
                    result["momentum"] = None

                # Best/worst positions
                result["best_position"] = min(positions) if positions else None
                result["worst_position"] = max(positions) if positions else None
                result["nb_victoires_recent_5"] = sum(1 for h in recent[-5:] if h.get("gagnant"))
                result["nb_places_recent_5"] = sum(1 for h in recent[-5:] if h.get("place") or h.get("gagnant"))

            else:
                # Pas d'historique — valeurs par défaut
                result["seq_positions"] = []
                result["seq_cotes"] = []
                result["seq_distances"] = []
                result["trend_position_5"] = None
                result["trend_cote_5"] = None
                result["trend_position_10"] = None
                result["volatilite_position_5"] = None
                result["position_moy_5"] = None
                result["position_moy_10"] = None
                result["serie_victoires"] = 0
                result["serie_places"] = 0
                result["serie_non_places"] = 0
                result["derniere_victoire_courses_ago"] = None
                result["derniere_place_courses_ago"] = None
                result["tx_victoire_hist"] = None
                result["tx_place_hist"] = None
                result["gain_moyen_hist"] = None
                result["red_km_moy_hist"] = None
                result["red_km_best_hist"] = None
                result["nb_hippodromes_differents"] = 0
                result["pct_meme_hippo"] = None
                result["pct_meme_discipline"] = None
                result["momentum"] = None
                result["best_position"] = None
                result["worst_position"] = None
                result["nb_victoires_recent_5"] = 0
                result["nb_places_recent_5"] = 0

            fout.write(json.dumps(result, ensure_ascii=False) + "\n")

            # Ajouter au historique
            horse_history[nom].append({
                "pos": partant.get("position_arrivee"),
                "cote": partant.get("cote_finale"),
                "gain": partant.get("gains_annee_euros"),
                "dist": partant.get("distance"),
                "red_km": partant.get("reduction_km_ms"),
                "date": (partant.get("date_reunion_iso") or "")[:10],
                "gagnant": partant.get("is_gagnant", False),
                "place": partant.get("is_place", False),
                "hippo": partant.get("hippodrome_normalise", ""),
                "disc": partant.get("discipline", ""),
            })

            if (i + 1) % 200000 == 0:
                log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis")

    log.info(f"Séquences terminées: {enriched}/{len(partants)} enrichis ({100*enriched/len(partants):.1f}%)")
    log.info(f"Output: {output_file}")


def main():
    log.info("=" * 60)
    log.info("SCRIPT 41 — Séquences performances (LSTM/GRU/TFT)")
    log.info("=" * 60)

    partants = load_partants()
    if not partants:
        log.error("Aucun partant trouvé")
        sys.exit(1)

    compute_sequences(partants)

    log.info("=" * 60)
    log.info("TERMINÉ")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
