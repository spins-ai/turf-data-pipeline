#!/usr/bin/env python3
"""
Script 46 — Track Bias + Speed Figures + Class Ratings
Calcul local, 0 requête API.

Input :
  - output/02_liste_courses/partants_normalises.jsonl
  - output/02_liste_courses/courses_normalisees.jsonl

Output : output/46_track_bias_speed/
  - track_bias_speed_class.jsonl

Features :
  Track Bias :
    - bias_corde_int : avantage numéro de corde intérieur (1-3) vs extérieur
    - bias_corde_ext : idem extérieur
    - bias_stalle_gagnant_moy : position de corde moyenne des gagnants
    - bias_front_runner : % de gagnants partis devant
    - hippo_avg_speed : vitesse moyenne sur cet hippodrome
    - hippo_avg_ecart : écart type des vitesses

  Speed Figures :
    - speed_figure : vitesse normalisée par distance/hippodrome
    - speed_figure_best : meilleure speed figure
    - speed_figure_moy_5 : speed figure moyenne des 5 dernières
    - speed_figure_trend : tendance speed figure
    - speed_relative : speed figure vs moyenne du lot

  Class Ratings :
    - class_rating : rating classe estimé depuis gains + performances
    - class_rating_moy_5 : rating moyen des 5 dernières
    - class_change : changement de classe vs dernière course
    - is_class_drop : descente en classe
    - is_class_rise : montée en classe
    - field_strength : force du lot (somme class ratings des adversaires)
"""

import json
import logging
import math
import os
import sys
from collections import defaultdict

OUTPUT_DIR = "output/46_track_bias_speed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)


def load_partants():
    KEEP = {"partant_uid", "course_uid", "nom_cheval", "horse_id",
            "date_reunion_iso", "hippodrome_normalise", "distance",
            "discipline", "position_arrivee", "is_gagnant", "is_place",
            "cote_finale", "proba_implicite", "gains_carriere_euros",
            "gains_annee_euros", "nb_courses_carriere", "nb_victoires_carriere",
            "reduction_km_ms", "temps_ms", "place_corde", "nombre_partants",
            "allocation_totale", "poids_porte_kg",
            "numero_reunion", "numero_course"}
    partants = []
    for path in ["output/02_liste_courses/partants_normalises.jsonl",
                 "output/02_liste_courses/partants_normalises.json"]:
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
        break
    log.info(f"  {len(partants)} partants")
    return partants


def linear_slope(values):
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den > 0 else 0.0


def compute_speed_figure(red_km, distance, hippo_avg, hippo_std):
    """Calcule une speed figure normalisée.

    Speed figure = 100 + (avg_hippo - red_km) / std_hippo * 15
    Plus la red_km est basse, plus le cheval est rapide → figure élevée.
    """
    if red_km is None or hippo_avg is None or hippo_std is None or hippo_std == 0:
        return None
    return round(100 + (hippo_avg - red_km) / hippo_std * 15, 1)


def compute_class_rating(gains_carriere, nb_courses, nb_victoires, allocation):
    """Estime un class rating depuis les gains et performances."""
    score = 0

    # Composante gains (normaliser en milliers d'euros)
    if gains_carriere:
        try:
            g = float(gains_carriere)
            score += min(g / 5000, 50)  # Cap à 50 points
        except (ValueError, TypeError):
            pass

    # Composante win rate
    if nb_courses and nb_victoires:
        try:
            win_rate = int(nb_victoires) / max(int(nb_courses), 1)
            score += win_rate * 30  # Max 30 points
        except (ValueError, TypeError):
            pass

    # Composante allocation
    if allocation:
        try:
            a = float(allocation)
            score += min(a / 2000, 20)  # Max 20 points
        except (ValueError, TypeError):
            pass

    return round(score, 1)


def compute_features(partants):
    """Calcule track bias, speed figures et class ratings."""
    partants.sort(key=lambda p: (p.get("date_reunion_iso", ""), p.get("numero_reunion", 0), p.get("numero_course", 0)))

    log.info("Phase 1: Calcul des moyennes par hippodrome+distance...")

    # Première passe : moyennes par hippo+distance pour speed figures
    hippo_dist_speeds = defaultdict(list)  # (hippo, dist_cat) -> [red_km]
    for p in partants:
        red_km = p.get("reduction_km_ms")
        if red_km is None:
            continue
        hippo = (p.get("hippodrome_normalise") or "").lower()
        dist = p.get("distance")
        if not hippo or not dist:
            continue
        # Catégoriser distance par tranches de 200m
        try:
            dist_cat = int(dist) // 200 * 200
        except (ValueError, TypeError):
            continue
        hippo_dist_speeds[(hippo, dist_cat)].append(red_km)

    # Calculer moyennes et écart types
    hippo_dist_stats = {}
    for key, speeds in hippo_dist_speeds.items():
        if len(speeds) < 10:
            continue
        avg = sum(speeds) / len(speeds)
        std = math.sqrt(sum((s - avg) ** 2 for s in speeds) / len(speeds))
        hippo_dist_stats[key] = {"avg": avg, "std": max(std, 1)}  # std min 1 pour éviter div/0

    log.info(f"  {len(hippo_dist_stats)} paires hippo/distance avec stats")
    del hippo_dist_speeds

    log.info("Phase 2: Calcul track bias par hippodrome...")

    # Track bias par hippodrome (position de corde des gagnants)
    hippo_corde_wins = defaultdict(list)  # hippo -> [place_corde des gagnants]
    hippo_corde_all = defaultdict(list)
    for p in partants:
        hippo = (p.get("hippodrome_normalise") or "").lower()
        corde = p.get("place_corde")
        if not hippo or corde is None:
            continue
        hippo_corde_all[hippo].append(corde)
        if p.get("is_gagnant"):
            hippo_corde_wins[hippo].append(corde)

    hippo_bias = {}
    for hippo in hippo_corde_all:
        wins = hippo_corde_wins.get(hippo, [])
        all_c = hippo_corde_all[hippo]
        if len(wins) >= 10 and len(all_c) >= 30:
            avg_win = sum(wins) / len(wins)
            avg_all = sum(all_c) / len(all_c)
            hippo_bias[hippo] = {
                "avg_corde_gagnant": round(avg_win, 2),
                "avg_corde_tous": round(avg_all, 2),
                "bias_interieur": round(avg_win / max(avg_all, 1), 3),
            }

    log.info(f"  {len(hippo_bias)} hippodromes avec bias calculé")

    log.info("Phase 3: Calcul features par partant...")

    # Historiques par cheval
    horse_speeds = defaultdict(list)     # nom -> [speed_figure]
    horse_classes = defaultdict(list)    # nom -> [class_rating]

    # Features par course pour field_strength
    course_partants = defaultdict(list)  # course_uid -> [{nom, class_rating}]

    output_file = os.path.join(OUTPUT_DIR, "track_bias_speed_class.jsonl")
    enriched = 0

    with open(output_file, "w", encoding="utf-8") as fout:
        for i, p in enumerate(partants):
            nom = (p.get("nom_cheval") or "").upper().strip()
            hippo = (p.get("hippodrome_normalise") or "").lower()
            dist = p.get("distance")

            result = {
                "partant_uid": p.get("partant_uid", ""),
                "course_uid": p.get("course_uid", ""),
                "nom_cheval": p.get("nom_cheval", ""),
            }

            # === Track Bias ===
            bias = hippo_bias.get(hippo, {})
            if bias:
                result["bias_corde_gagnant_moy"] = bias["avg_corde_gagnant"]
                result["bias_interieur"] = bias["bias_interieur"]

                # Position de corde relative
                corde = p.get("place_corde")
                if corde is not None:
                    result["corde_relative"] = round(corde / max(bias["avg_corde_tous"], 1), 3)
                    result["corde_avantage"] = corde <= bias["avg_corde_gagnant"]

            # === Speed Figure ===
            red_km = p.get("reduction_km_ms")
            speed_fig = None
            if dist:
                try:
                    dist_cat = int(dist) // 200 * 200
                except (ValueError, TypeError):
                    dist_cat = None

                if dist_cat and (hippo, dist_cat) in hippo_dist_stats:
                    stats = hippo_dist_stats[(hippo, dist_cat)]
                    speed_fig = compute_speed_figure(red_km, dist, stats["avg"], stats["std"])
                    result["speed_figure"] = speed_fig

            # Speed figure historique
            if nom:
                hist_speeds = horse_speeds[nom]
                if hist_speeds:
                    enriched += 1
                    result["speed_figure_best"] = max(hist_speeds)
                    result["speed_figure_moy_5"] = round(sum(hist_speeds[-5:]) / len(hist_speeds[-5:]), 1)
                    result["speed_figure_trend"] = round(linear_slope(hist_speeds[-5:]), 3) if len(hist_speeds) >= 2 else None

                    # Speed figure relative au lot
                    if speed_fig is not None and len(hist_speeds) >= 3:
                        avg_recent = sum(hist_speeds[-3:]) / len(hist_speeds[-3:])
                        result["speed_relative_to_self"] = round(speed_fig - avg_recent, 1) if speed_fig else None

            # === Class Rating ===
            cr = compute_class_rating(
                p.get("gains_carriere_euros"),
                p.get("nb_courses_carriere"),
                p.get("nb_victoires_carriere"),
                p.get("allocation_totale"),
            )
            result["class_rating"] = cr

            if nom:
                hist_classes = horse_classes[nom]
                if hist_classes:
                    result["class_rating_moy_5"] = round(sum(hist_classes[-5:]) / len(hist_classes[-5:]), 1)
                    last_cr = hist_classes[-1]
                    result["class_change"] = round(cr - last_cr, 1)
                    result["is_class_drop"] = cr < last_cr - 5
                    result["is_class_rise"] = cr > last_cr + 5

            # Sauver pour field_strength
            course_uid = p.get("course_uid", "")
            if course_uid:
                course_partants[course_uid].append({"nom": nom, "cr": cr})

            fout.write(json.dumps(result, ensure_ascii=False) + "\n")

            # Mettre à jour historiques APRÈS
            if nom and speed_fig is not None:
                horse_speeds[nom].append(speed_fig)
            if nom:
                horse_classes[nom].append(cr)

            if (i + 1) % 200000 == 0:
                log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis")

    log.info(f"Track bias/speed/class terminé: {enriched}/{len(partants)} enrichis")

    # Phase 4 : Field strength (re-lecture et enrichissement)
    log.info("Phase 4: Calcul field_strength...")
    _enrich_field_strength(output_file, course_partants)


def _enrich_field_strength(output_file, course_partants):
    """Ajoute field_strength en relisant le JSONL."""
    # Calculer field_strength par course
    course_strength = {}
    for cuid, plist in course_partants.items():
        ratings = [p["cr"] for p in plist if p["cr"] > 0]
        if ratings:
            course_strength[cuid] = {
                "avg_cr": round(sum(ratings) / len(ratings), 1),
                "max_cr": max(ratings),
                "std_cr": round(math.sqrt(sum((r - sum(ratings)/len(ratings))**2 for r in ratings) / len(ratings)), 1) if len(ratings) > 1 else 0,
                "nb_runners": len(ratings),
            }

    # Relire et enrichir
    tmp_file = output_file + ".tmp"
    enriched = 0
    with open(output_file, "r", encoding="utf-8") as fin, \
         open(tmp_file, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                fout.write(line + "\n")
                continue

            cuid = r.get("course_uid", "")
            cs = course_strength.get(cuid)
            if cs:
                r["field_strength_avg"] = cs["avg_cr"]
                r["field_strength_max"] = cs["max_cr"]
                r["field_strength_std"] = cs["std_cr"]
                cr = r.get("class_rating", 0)
                r["class_vs_field"] = round(cr - cs["avg_cr"], 1) if cr else None
                enriched += 1

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    os.replace(tmp_file, output_file)
    log.info(f"  Field strength ajouté à {enriched} records")


def main():
    log.info("=" * 60)
    log.info("SCRIPT 46 — Track Bias + Speed Figures + Class Ratings")
    log.info("=" * 60)

    partants = load_partants()
    if not partants:
        log.error("Aucun partant")
        sys.exit(1)

    compute_features(partants)
    log.info("TERMINÉ")


if __name__ == "__main__":
    main()
