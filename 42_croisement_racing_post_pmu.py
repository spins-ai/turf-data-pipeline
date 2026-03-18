#!/usr/bin/env python3
"""
Script 42 — Croisement Racing Post × PMU
Calcul local, 0 requête API.

Input :
  - output/37_racing_post/racing_post_fr.jsonl (ou .json)
  - output/02_liste_courses/partants_normalises.jsonl (ou .json)

Output : output/42_croisement_rp/
  - croisement_rp_pmu.jsonl

Features :
  - rpr_rating : Racing Post Rating du cheval
  - topspeed_rating : TopSpeed du cheval
  - rpr_moy_5 : RPR moyen sur 5 dernières courses RP
  - topspeed_moy_5 : TopSpeed moyen sur 5 dernières
  - rpr_best : meilleur RPR jamais
  - topspeed_best : meilleur TopSpeed
  - rpr_trend : tendance RPR
  - class_rating_rp : classe estimée Racing Post
  - rp_courses_count : nb de courses trouvées sur RP
  - rp_win_rate : taux victoire sur RP
"""

import json
import logging
import os
import re
import sys
from collections import defaultdict

OUTPUT_DIR = "output/42_croisement_rp"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)


def load_racing_post():
    """Charge les données Racing Post."""
    records = []
    for path in ["output/37_racing_post/racing_post_fr.jsonl",
                 "output/37_racing_post/racing_post_fr.json"]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement RP: {path}")
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        else:
            with open(path, "r", encoding="utf-8") as f:
                records = json.load(f)
        break

    log.info(f"  {len(records)} records Racing Post")
    return records


def load_partants_light():
    """Charge les partants avec seulement les champs nécessaires."""
    KEEP = {"partant_uid", "course_uid", "nom_cheval", "date_reunion_iso",
            "hippodrome_normalise", "distance", "discipline", "position_arrivee",
            "is_gagnant", "is_place", "cote_finale"}
    partants = []
    for path in ["output/02_liste_courses/partants_normalises.jsonl",
                 "output/02_liste_courses/partants_normalises.json"]:
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


def normalize_name(name):
    """Normalise un nom de cheval pour matching."""
    if not name:
        return ""
    import unicodedata
    name = name.strip().upper()
    nfkd = unicodedata.normalize("NFKD", name)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def build_rp_index(rp_records):
    """Construit un index des données RP par nom de cheval normalisé."""
    index = defaultdict(list)

    for r in rp_records:
        raw_text = r.get("raw_text", "")
        rpr = r.get("rpr")
        topspeed = r.get("topspeed")
        position = r.get("position")
        date_str = r.get("date", "")

        # Extraire le nom du cheval depuis raw_text
        # Pattern typique: "1st | HORSE NAME | ..."
        parts = raw_text.split("|")
        horse_name = ""
        for part in parts[1:3]:
            cleaned = part.strip()
            # Skip les numéros, odds, etc.
            if cleaned and not re.match(r'^\d', cleaned) and len(cleaned) > 2:
                # Skip les termes techniques
                if cleaned.lower() not in ("evens", "fav", "nf"):
                    horse_name = cleaned
                    break

        if not horse_name:
            continue

        name_norm = normalize_name(horse_name)
        if not name_norm:
            continue

        index[name_norm].append({
            "date": date_str,
            "rpr": rpr,
            "topspeed": topspeed,
            "position": position,
            "odds": r.get("odds", ""),
        })

    log.info(f"  Index RP: {len(index)} chevaux uniques")
    return index


def linear_slope(values):
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den > 0 else 0.0


def compute_croisement(partants, rp_index):
    """Croise Racing Post avec PMU pour chaque partant."""
    output_file = os.path.join(OUTPUT_DIR, "croisement_rp_pmu.jsonl")
    enriched = 0

    with open(output_file, "w", encoding="utf-8") as fout:
        for i, p in enumerate(partants):
            nom = normalize_name(p.get("nom_cheval", ""))
            result = {
                "partant_uid": p.get("partant_uid", ""),
                "course_uid": p.get("course_uid", ""),
                "nom_cheval": p.get("nom_cheval", ""),
            }

            rp_data = rp_index.get(nom, [])

            # Filtrer par date (point-in-time safe)
            date_iso = p.get("date_reunion_iso", "")
            if date_iso:
                rp_data = [r for r in rp_data if r.get("date", "") < date_iso[:10]]

            if rp_data:
                enriched += 1

                rprs = [r["rpr"] for r in rp_data if r.get("rpr")]
                topspeeds = [r["topspeed"] for r in rp_data if r.get("topspeed")]
                positions = [r["position"] for r in rp_data if r.get("position")]

                result["rp_courses_count"] = len(rp_data)

                if rprs:
                    result["rpr_best"] = max(rprs)
                    result["rpr_moy_5"] = round(sum(rprs[-5:]) / len(rprs[-5:]), 1)
                    result["rpr_last"] = rprs[-1]
                    result["rpr_trend"] = round(linear_slope(rprs[-5:]), 3) if len(rprs) >= 2 else None

                if topspeeds:
                    result["topspeed_best"] = max(topspeeds)
                    result["topspeed_moy_5"] = round(sum(topspeeds[-5:]) / len(topspeeds[-5:]), 1)
                    result["topspeed_last"] = topspeeds[-1]
                    result["topspeed_trend"] = round(linear_slope(topspeeds[-5:]), 3) if len(topspeeds) >= 2 else None

                if positions:
                    wins = sum(1 for p2 in positions if p2 == 1)
                    result["rp_win_rate"] = round(wins / len(positions), 3)
                    result["rp_place_rate"] = round(sum(1 for p2 in positions if p2 <= 3) / len(positions), 3)

                # Class rating estimé (RPR moyen + topspeed moyen) / 2
                if rprs and topspeeds:
                    result["class_rating_rp"] = round((sum(rprs[-3:]) / len(rprs[-3:]) + sum(topspeeds[-3:]) / len(topspeeds[-3:])) / 2, 1)

            fout.write(json.dumps(result, ensure_ascii=False) + "\n")

            if (i + 1) % 200000 == 0:
                log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis")

    log.info(f"Croisement RP terminé: {enriched}/{len(partants)} enrichis ({100*enriched/len(partants):.1f}%)")


def main():
    log.info("=" * 60)
    log.info("SCRIPT 42 — Croisement Racing Post × PMU")
    log.info("=" * 60)

    rp_records = load_racing_post()
    if not rp_records:
        log.warning("Pas de données Racing Post — fichier vide créé")
        with open(os.path.join(OUTPUT_DIR, "croisement_rp_pmu.jsonl"), "w") as f:
            pass
        return

    rp_index = build_rp_index(rp_records)
    del rp_records

    partants = load_partants_light()
    if not partants:
        log.error("Aucun partant")
        sys.exit(1)

    compute_croisement(partants, rp_index)

    log.info("TERMINÉ")


if __name__ == "__main__":
    main()
