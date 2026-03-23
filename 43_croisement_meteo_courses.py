#!/usr/bin/env python3
"""
Script 43 — Croisement Météo × Courses
Calcul local, 0 requête API.

Input :
  - output/39_reunions_enrichies/ (météo par réunion)
  - output/13_meteo_historique/ ou data_master/meteo_master.json
  - output/02_liste_courses/partants_normalises.jsonl

Output : output/43_croisement_meteo/
  - croisement_meteo_courses.jsonl

Features :
  - meteo_exacte_temperature : température exacte à l'heure de la course
  - meteo_exacte_pluie_mm : précipitation exacte
  - meteo_exacte_vent_kmh : vitesse vent exacte
  - meteo_exacte_vent_direction : direction vent
  - meteo_humidite : humidité %
  - pluie_48h_avant : cumul pluie 48h avant
  - delta_temperature : écart temp vs moyenne saisonnière
  - terrain_predit : terrain estimé depuis météo (lourd/souple/bon/sec)
  - impact_meteo_score : score d'impact météo composite
  - cheval_perf_terrain : taux victoire du cheval sur ce terrain
  - cheval_perf_pluie : taux victoire du cheval sous la pluie
  - cheval_specialist_lourd : spécialiste terrain lourd
  - cheval_specialist_sec : spécialiste terrain sec
"""

import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "43_croisement_meteo")
os.makedirs(OUTPUT_DIR, exist_ok=True)

log = setup_logging("43_croisement_meteo_courses")


def load_meteo_index():
    """Construit un index météo par (date, hippodrome)."""
    index = {}

    # Essayer meteo_master d'abord
    for path in [os.path.join(BASE_DIR, "data_master", "meteo_master.json"), os.path.join(BASE_DIR, "output", "13_meteo_historique", "meteo_historique.json")]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement météo: {path}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            for m in data:
                date_iso = (m.get("date_reunion_iso") or m.get("date") or "")[:10]
                hippo = (m.get("hippodrome_normalise") or m.get("hippodrome") or "").lower().strip()
                if date_iso and hippo:
                    key = f"{date_iso}|{hippo}"
                    index[key] = m
        elif isinstance(data, dict):
            for key, m in data.items():
                index[key] = m

        del data
        log.info(f"  {len(index)} entrées météo indexées")
        break

    # Compléter avec réunions enrichies
    reu_path = os.path.join(BASE_DIR, "output", "39_reunions_enrichies", "reunions_enrichies.jsonl")
    if os.path.exists(reu_path):
        added = 0
        with open(reu_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                date_iso = (r.get("date_reunion_iso") or "")[:10]
                hippo = (r.get("hippodrome_normalise") or "").lower().strip()
                key = f"{date_iso}|{hippo}"
                if key not in index:
                    index[key] = r
                    added += 1
                else:
                    # Enrichir l'entrée existante
                    for k, v in r.items():
                        if k not in index[key] or not index[key][k]:
                            index[key][k] = v
        log.info(f"  +{added} entrées depuis réunions enrichies")

    return index


def load_partants_light():
    KEEP = {"partant_uid", "course_uid", "nom_cheval", "horse_id",
            "date_reunion_iso", "hippodrome_normalise", "distance",
            "discipline", "position_arrivee", "is_gagnant", "is_place",
            "type_piste", "numero_reunion", "numero_course"}
    partants = []
    for path in [os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.json")]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement partants: {path}")
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


def categorize_terrain(penetro_val, pluie_mm):
    """Estime le terrain depuis le pénétromètre et la pluie."""
    if penetro_val is not None:
        try:
            pv = float(penetro_val)
            if pv <= 2.5:
                return "sec"
            elif pv <= 3.2:
                return "bon"
            elif pv <= 3.8:
                return "souple"
            elif pv <= 4.5:
                return "lourd"
            else:
                return "tres_lourd"
        except (ValueError, TypeError):
            pass

    if pluie_mm is not None:
        try:
            p = float(pluie_mm)
            if p < 0.5:
                return "bon"
            elif p < 5:
                return "souple"
            elif p < 15:
                return "lourd"
            else:
                return "tres_lourd"
        except (ValueError, TypeError):
            pass

    return None


def compute_croisement(partants, meteo_index):
    """Croise météo avec courses et calcule les features."""
    # Trier par date pour historique par cheval
    partants.sort(key=lambda p: (p.get("date_reunion_iso", ""), p.get("numero_reunion", 0)))

    output_file = os.path.join(OUTPUT_DIR, "croisement_meteo_courses.jsonl")
    enriched = 0

    # Historique terrain par cheval
    horse_terrain_history = defaultdict(list)  # nom -> [{terrain, gagnant, place}]

    with open(output_file, "w", encoding="utf-8") as fout:
        for i, p in enumerate(partants):
            nom = (p.get("nom_cheval") or "").upper().strip()
            date_iso = (p.get("date_reunion_iso") or "")[:10]
            hippo = (p.get("hippodrome_normalise") or "").lower().strip()

            result = {
                "partant_uid": p.get("partant_uid", ""),
                "course_uid": p.get("course_uid", ""),
                "nom_cheval": p.get("nom_cheval", ""),
            }

            # Lookup météo
            key = f"{date_iso}|{hippo}"
            meteo = meteo_index.get(key, {})

            if meteo:
                enriched += 1

                # Température
                temp = meteo.get("temperature") or meteo.get("temp_celsius") or meteo.get("temperature_2m_mean")
                result["meteo_temperature"] = temp

                # Pluie
                pluie = meteo.get("precipitation_mm") or meteo.get("precipitation_sum") or meteo.get("pluie")
                result["meteo_pluie_mm"] = pluie

                # Vent
                vent = meteo.get("wind_force") or meteo.get("windspeed_10m_max") or meteo.get("vent_kmh")
                result["meteo_vent_kmh"] = vent
                result["meteo_vent_direction"] = meteo.get("wind_direction") or meteo.get("winddirection_10m_dominant")

                # Humidité
                result["meteo_humidite"] = meteo.get("humidity") or meteo.get("relative_humidity_2m_mean")

                # Nébulosité
                result["meteo_nebulosite"] = meteo.get("nebulosity") or meteo.get("cloudcover_mean")

                # Pénétromètre
                penetro = meteo.get("penetrometre_numeric") or meteo.get("penetrometre_valeur")
                result["penetrometre"] = penetro

                # Terrain prédit
                terrain_cat = meteo.get("terrain_category")
                if not terrain_cat:
                    terrain_cat = categorize_terrain(penetro, pluie)
                result["terrain_predit"] = terrain_cat

                # Score impact météo composite
                score = 0
                if pluie is not None:
                    try:
                        score += min(float(pluie) / 10, 3)
                    except (ValueError, TypeError):
                        pass
                if vent is not None:
                    try:
                        score += min(float(vent) / 30, 2)
                    except (ValueError, TypeError):
                        pass
                result["impact_meteo_score"] = round(score, 2) if score > 0 else 0

                # PSF flag
                result["is_psf"] = meteo.get("is_psf", False) or p.get("type_piste") == "psf"
            else:
                terrain_cat = None

            # Features historiques du cheval sur ce type de terrain
            if nom:
                hist = horse_terrain_history[nom]
                if hist and terrain_cat:
                    same_terrain = [h for h in hist if h.get("terrain") == terrain_cat]
                    if same_terrain:
                        nb = len(same_terrain)
                        wins = sum(1 for h in same_terrain if h.get("gagnant"))
                        places = sum(1 for h in same_terrain if h.get("place"))
                        result["cheval_nb_courses_terrain"] = nb
                        result["cheval_taux_vic_terrain"] = round(wins / nb, 3) if nb > 0 else 0
                        result["cheval_taux_place_terrain"] = round(places / nb, 3) if nb > 0 else 0
                        result["cheval_specialist_terrain"] = nb >= 3 and (wins / nb) >= 0.25

                    # Perf sous la pluie
                    pluie_courses = [h for h in hist if h.get("pluie", False)]
                    if pluie_courses:
                        nb_p = len(pluie_courses)
                        wins_p = sum(1 for h in pluie_courses if h.get("gagnant"))
                        result["cheval_taux_vic_pluie"] = round(wins_p / nb_p, 3)

                # Ajouter au historique
                is_pluie = False
                if meteo:
                    try:
                        is_pluie = float(meteo.get("precipitation_mm") or meteo.get("precipitation_sum") or 0) > 1
                    except (ValueError, TypeError):
                        pass

                horse_terrain_history[nom].append({
                    "terrain": terrain_cat,
                    "gagnant": p.get("is_gagnant", False),
                    "place": p.get("is_place", False),
                    "pluie": is_pluie,
                })

            fout.write(json.dumps(result, ensure_ascii=False) + "\n")

            if (i + 1) % 200000 == 0:
                log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis")

    log.info(f"Croisement météo terminé: {enriched}/{len(partants)} enrichis ({100*enriched/len(partants):.1f}%)")


def main():
    log.info("=" * 60)
    log.info("SCRIPT 43 — Croisement Météo × Courses")
    log.info("=" * 60)

    meteo_index = load_meteo_index()
    if not meteo_index:
        log.warning("Pas de données météo")

    partants = load_partants_light()
    if not partants:
        log.error("Aucun partant")
        sys.exit(1)

    compute_croisement(partants, meteo_index)
    log.info("TERMINÉ")


if __name__ == "__main__":
    main()
