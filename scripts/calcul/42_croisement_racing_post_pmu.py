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

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.normalize import normalize_name

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "../../output", "42_croisement_rp")
os.makedirs(OUTPUT_DIR, exist_ok=True)

log = setup_logging("42_croisement_racing_post_pmu")


def load_racing_post():
    """Charge les données Racing Post."""
    records = []
    for path in [os.path.join(BASE_DIR, "../../output", "37_racing_post", "racing_post_fr.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "37_racing_post", "racing_post_fr.json"),
                 os.path.join(BASE_DIR, "../../data_master", "racing_post_master.json")]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement RP: {path}")
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        else:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = list(data.values()) if data else []
        break

    log.info(f"  {len(records)} records Racing Post")
    return records


def load_partants_light():
    """Charge les partants avec seulement les champs nécessaires."""
    KEEP = {"partant_uid", "course_uid", "nom_cheval", "date_reunion_iso",
            "hippodrome_normalise", "distance", "discipline", "position_arrivee",
            "is_gagnant", "is_place", "cote_finale"}
    partants = []
    for path in [os.path.join(BASE_DIR, "../../data_master", "partants_master.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "02_liste_courses", "partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "02_liste_courses", "partants_normalises.json")]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement partants: {path}")
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
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
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for p in data:
                partants.append({k: p[k] for k in KEEP if k in p})
            del data
        break

    log.info(f"  {len(partants)} partants")
    return partants


def extract_horse_name_from_raw(raw_text):
    """Extrait le nom du cheval depuis raw_text.

    Format réel des données Racing Post scrappées :
        '2.\\n                                                Kingcormac | 11/8F | ...'
    Le nom du cheval se trouve AVANT le premier '|', après le numéro de position.
    """
    if not raw_text:
        return ""
    parts = raw_text.split("|")
    # Le nom est dans la première partie, après le numéro de position
    first_part = parts[0].strip()
    # Retirer le numéro de position en début (ex: "2.\n   Kingcormac")
    # Le format est "N." suivi de whitespace/newline puis le nom
    m = re.match(r'^\d+\.\s*', first_part)
    if m:
        name = first_part[m.end():].strip()
        if name and len(name) > 1:
            return name
    # Fallback: si pas de numéro de position, prendre tel quel
    if first_part and len(first_part) > 2 and not re.match(r'^\d+$', first_part):
        return first_part
    return ""


def parse_fractional_odds(odds_str):
    """Convertit des cotes fractionnelles UK (ex: '11/8', '3/1F') en décimal."""
    if not odds_str:
        return None
    # Retirer F (favori), J, C, etc. en fin de cote
    cleaned = re.sub(r'[A-Za-z]+$', '', str(odds_str).strip())
    if '/' in cleaned:
        try:
            num, den = cleaned.split('/')
            return round(float(num) / float(den) + 1, 2)
        except (ValueError, ZeroDivisionError):
            return None
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def build_rp_index(rp_records):
    """Construit un index des données RP par nom de cheval normalisé.

    Les données Racing Post scrappées contiennent :
      - date, course_id, race_name, race_info, position, odds, raw_text
    Il n'y a PAS de champs rpr/topspeed dans le scraping actuel.
    On extrait le nom du cheval depuis raw_text et on utilise odds + position.
    """
    index = defaultdict(list)

    for r in rp_records:
        rpr = r.get("rpr") or r.get("racing_post_rating")
        topspeed = r.get("topspeed") or r.get("top_speed") or r.get("ts")
        position = r.get("position")
        date_str = r.get("date", "") or r.get("date_reunion_iso", "") or ""

        # Convertir rpr/topspeed en int si possible
        try:
            rpr = int(rpr) if rpr else None
        except (ValueError, TypeError):
            rpr = None
        try:
            topspeed = int(topspeed) if topspeed else None
        except (ValueError, TypeError):
            topspeed = None
        try:
            position = int(position) if position else None
        except (ValueError, TypeError):
            position = None

        # Convertir les cotes fractionnelles UK en décimal
        odds_decimal = parse_fractional_odds(r.get("odds"))

        # Priorité 1 : champ nom_cheval / horse_name direct
        horse_name = (r.get("nom_cheval") or r.get("horse_name") or "").strip()

        # Priorité 2 : extraction depuis raw_text
        if not horse_name:
            horse_name = extract_horse_name_from_raw(r.get("raw_text", ""))

        if not horse_name:
            continue

        name_norm = normalize_name(horse_name)
        if not name_norm:
            continue

        index[name_norm].append({
            "date": str(date_str)[:10],
            "rpr": rpr,
            "topspeed": topspeed,
            "position": position,
            "odds": r.get("odds", ""),
            "odds_decimal": odds_decimal,
            "race_name": r.get("race_name", ""),
            "course_id": r.get("course_id"),
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

    with open(output_file, "w", encoding="utf-8", newline="\n") as fout:
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
                odds_list = [r["odds_decimal"] for r in rp_data if r.get("odds_decimal")]

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
                    result["rp_avg_position"] = round(sum(positions) / len(positions), 2)

                # Class rating estimé (RPR moyen + topspeed moyen) / 2
                if rprs and topspeeds:
                    result["class_rating_rp"] = round((sum(rprs[-3:]) / len(rprs[-3:]) + sum(topspeeds[-3:]) / len(topspeeds[-3:])) / 2, 1)

                # Odds-based features (disponibles même sans RPR/TopSpeed)
                if odds_list:
                    result["rp_odds_moy_5"] = round(sum(odds_list[-5:]) / len(odds_list[-5:]), 2)
                    result["rp_odds_last"] = odds_list[-1]
                    result["rp_odds_best"] = min(odds_list)  # cote la plus basse = meilleur favori
                    result["rp_odds_trend"] = round(linear_slope(odds_list[-5:]), 3) if len(odds_list) >= 2 else None

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
        with open(os.path.join(OUTPUT_DIR, "croisement_rp_pmu.jsonl"), "w", encoding="utf-8", errors="replace", newline="\n") as f:
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
