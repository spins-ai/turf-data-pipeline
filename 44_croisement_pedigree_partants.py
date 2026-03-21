#!/usr/bin/env python3
"""
Script 44 — Croisement Pedigree × Partants
Calcul local, 0 requête API.

Input :
  - data_master/pedigree_master.json (1.4M chevaux)
  - output/02_liste_courses/partants_normalises.jsonl

Output : output/44_croisement_pedigree/
  - croisement_pedigree_partants.jsonl

Features :
  - sire_win_rate : taux victoire des produits du père
  - sire_place_rate : taux placé des produits du père
  - sire_nb_runners : nb de coureurs du père
  - sire_gains_moy : gains moyens des produits du père
  - dam_sire_win_rate : taux victoire des produits du père de la mère
  - sire_win_rate_distance : taux victoire du père sur cette distance
  - sire_win_rate_terrain : taux victoire du père sur ce terrain
  - sire_win_rate_discipline : taux victoire du père dans cette discipline
  - inbreeding_score : coefficient de consanguinité estimé
  - stamina_index : indice d'endurance basé sur le pedigree
  - speed_index : indice de vitesse basé sur le pedigree
  - lignee_adaptee : 1 si la lignée est adaptée à la distance/discipline
"""

import json
import logging
import os
import sys
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "44_croisement_pedigree")
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

# Distances catégorisées
DIST_SPRINT = 1400
DIST_MILE = 1800
DIST_INTER = 2200
DIST_STAYER = 2800


def categorize_distance(dist):
    if not dist:
        return "unknown"
    try:
        d = int(dist)
    except (ValueError, TypeError):
        return "unknown"
    if d <= DIST_SPRINT:
        return "sprint"
    elif d <= DIST_MILE:
        return "mile"
    elif d <= DIST_INTER:
        return "intermediaire"
    elif d <= DIST_STAYER:
        return "long"
    else:
        return "tres_long"


def load_pedigree_index():
    """Charge le pedigree master et indexe par nom normalisé."""
    index = {}
    for path in [os.path.join(BASE_DIR, "data_master", "pedigree_master.json"),
                 os.path.join(BASE_DIR, "output", "14_pedigree", "pedigrees_pq.jsonl"),
                 os.path.join(BASE_DIR, "output", "14_pedigree", "pedigrees_pq.json")]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement pedigree: {path}")

        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        p = json.loads(line)
                        nom = (p.get("nom_cheval") or "").upper().strip()
                        if nom:
                            index[nom] = p
                    except json.JSONDecodeError:
                        continue
        else:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, list):
                for p in data:
                    nom = (p.get("nom_cheval") or p.get("nom") or "").upper().strip()
                    if nom:
                        index[nom] = p
            elif isinstance(data, dict):
                for key, p in data.items():
                    nom = (p.get("nom_cheval") or key or "").upper().strip()
                    if nom:
                        index[nom] = p
            del data

        log.info(f"  {len(index)} chevaux dans l'index pedigree")
        break

    return index


def load_partants():
    KEEP = {"partant_uid", "course_uid", "nom_cheval", "horse_id",
            "date_reunion_iso", "hippodrome_normalise", "distance",
            "discipline", "position_arrivee", "is_gagnant", "is_place",
            "pere", "mere", "pere_mere", "race", "type_piste",
            "numero_reunion", "numero_course"}
    partants = []
    for path in [os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.json")]:
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


def build_sire_stats(partants):
    """Construit les statistiques par père à partir des résultats de courses."""
    sire_stats = defaultdict(lambda: {
        "total": 0, "wins": 0, "places": 0, "gains_total": 0,
        "by_dist": defaultdict(lambda: {"total": 0, "wins": 0, "places": 0}),
        "by_disc": defaultdict(lambda: {"total": 0, "wins": 0, "places": 0}),
    })

    # Trier par date pour stats point-in-time
    partants_sorted = sorted(partants, key=lambda p: p.get("date_reunion_iso", ""))

    for p in partants_sorted:
        pere = (p.get("pere") or "").upper().strip()
        if not pere:
            continue

        s = sire_stats[pere]
        s["total"] += 1
        if p.get("is_gagnant"):
            s["wins"] += 1
        if p.get("is_place"):
            s["places"] += 1

        # Par distance
        dist_cat = categorize_distance(p.get("distance"))
        sd = s["by_dist"][dist_cat]
        sd["total"] += 1
        if p.get("is_gagnant"):
            sd["wins"] += 1
        if p.get("is_place"):
            sd["places"] += 1

        # Par discipline
        disc = (p.get("discipline") or "").lower().strip()
        if disc:
            dd = s["by_disc"][disc]
            dd["total"] += 1
            if p.get("is_gagnant"):
                dd["wins"] += 1
            if p.get("is_place"):
                dd["places"] += 1

    log.info(f"  Stats pour {len(sire_stats)} pères")
    return sire_stats


# Étalons connus pour le stamina/speed (heuristique simple)
STAMINA_SIRES = {"SADLER'S WELLS", "GALILEO", "MONSUN", "CAMELOT", "SEA THE STARS",
                 "DUBAWI", "FRANKEL", "DEEP IMPACT", "GOLDIKOVA", "MONTJEU"}
SPEED_SIRES = {"DANEHILL", "STORM CAT", "SCAT DADDY", "NO NAY NEVER", "ZOFFANY",
               "KODIAC", "MEHMAS", "DARK ANGEL", "ACCLAMATION", "SHOWCASING"}


def compute_croisement(partants, pedigree_index, sire_stats):
    """Calcule les features de croisement pedigree."""
    partants.sort(key=lambda p: (p.get("date_reunion_iso", ""), p.get("numero_reunion", 0)))

    output_file = os.path.join(OUTPUT_DIR, "croisement_pedigree_partants.jsonl")
    enriched = 0

    # Snapshot progressif des sire stats (point-in-time)
    sire_snapshot = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0,
                                         "by_dist": defaultdict(lambda: {"total": 0, "wins": 0}),
                                         "by_disc": defaultdict(lambda: {"total": 0, "wins": 0})})

    with open(output_file, "w", encoding="utf-8") as fout:
        for i, p in enumerate(partants):
            nom = (p.get("nom_cheval") or "").upper().strip()
            pere = (p.get("pere") or "").upper().strip()
            mere = (p.get("mere") or "").upper().strip()
            pere_mere = (p.get("pere_mere") or "").upper().strip()

            result = {
                "partant_uid": p.get("partant_uid", ""),
                "course_uid": p.get("course_uid", ""),
                "nom_cheval": p.get("nom_cheval", ""),
            }

            # Pedigree du cheval
            ped = pedigree_index.get(nom, {})
            if ped:
                enriched += 1
                result["has_pedigree"] = True
                result["pedigree_found_pq"] = ped.get("found", False)

                # Grand-parents
                result["grand_pere_paternel"] = ped.get("grand_pere_paternel", "")
                result["grand_pere_maternel"] = ped.get("grand_pere_maternel", "")

                # Consanguinité simple (même ancêtre dans les 2 branches)
                ancestors_pat = {ped.get(k, "") for k in
                                 ["pere", "grand_pere_paternel", "grand_mere_paternelle",
                                  "arriere_gpp_pp", "arriere_gpm_pp", "arriere_gpp_mp", "arriere_gpm_mp"]
                                 if ped.get(k)}
                ancestors_mat = {ped.get(k, "") for k in
                                 ["mere", "grand_pere_maternel", "grand_mere_maternelle",
                                  "arriere_gpp_pm", "arriere_gpm_pm", "arriere_gpp_mm", "arriere_gpm_mm"]
                                 if ped.get(k)}

                common = ancestors_pat & ancestors_mat
                common.discard("")
                result["inbreeding_count"] = len(common)
                result["inbreeding_score"] = round(len(common) / max(len(ancestors_pat | ancestors_mat), 1), 3)
            else:
                result["has_pedigree"] = False

            # Stats du père (point-in-time depuis snapshot progressif)
            if pere:
                ss = sire_snapshot[pere]
                if ss["total"] >= 3:
                    result["sire_nb_runners"] = ss["total"]
                    result["sire_win_rate"] = round(ss["wins"] / ss["total"], 3)
                    result["sire_place_rate"] = round(ss["places"] / ss["total"], 3)

                    # Par distance
                    dist_cat = categorize_distance(p.get("distance"))
                    sd = ss["by_dist"].get(dist_cat, {"total": 0, "wins": 0})
                    if sd["total"] >= 2:
                        result["sire_win_rate_distance"] = round(sd["wins"] / sd["total"], 3)

                    # Par discipline
                    disc = (p.get("discipline") or "").lower().strip()
                    dd = ss["by_disc"].get(disc, {"total": 0, "wins": 0})
                    if dd["total"] >= 2:
                        result["sire_win_rate_discipline"] = round(dd["wins"] / dd["total"], 3)

                # Stamina/Speed index
                result["sire_stamina_flag"] = pere in STAMINA_SIRES
                result["sire_speed_flag"] = pere in SPEED_SIRES

                if pere_mere:
                    result["dam_sire_stamina_flag"] = pere_mere in STAMINA_SIRES
                    result["dam_sire_speed_flag"] = pere_mere in SPEED_SIRES

                # Stamina index composite
                stamina = 0
                speed = 0
                if pere in STAMINA_SIRES:
                    stamina += 2
                if pere in SPEED_SIRES:
                    speed += 2
                if pere_mere in STAMINA_SIRES:
                    stamina += 1
                if pere_mere in SPEED_SIRES:
                    speed += 1

                gpp = (ped.get("grand_pere_paternel") or "").upper()
                gpm = (ped.get("grand_pere_maternel") or "").upper()
                if gpp in STAMINA_SIRES:
                    stamina += 0.5
                if gpp in SPEED_SIRES:
                    speed += 0.5
                if gpm in STAMINA_SIRES:
                    stamina += 0.5
                if gpm in SPEED_SIRES:
                    speed += 0.5

                result["stamina_index"] = round(stamina, 1)
                result["speed_index"] = round(speed, 1)

                # Lignée adaptée à la distance
                dist_cat = categorize_distance(p.get("distance"))
                if dist_cat in ("long", "tres_long"):
                    result["lignee_adaptee_distance"] = stamina > speed
                elif dist_cat == "sprint":
                    result["lignee_adaptee_distance"] = speed > stamina
                else:
                    result["lignee_adaptee_distance"] = True  # intermédiaire = tout va

            # Stats du père de la mère
            if pere_mere:
                ds = sire_snapshot[pere_mere]
                if ds["total"] >= 3:
                    result["dam_sire_win_rate"] = round(ds["wins"] / ds["total"], 3)
                    result["dam_sire_place_rate"] = round(ds["places"] / ds["total"], 3)
                    result["dam_sire_nb_runners"] = ds["total"]

            fout.write(json.dumps(result, ensure_ascii=False) + "\n")

            # Mettre à jour le snapshot sire stats APRÈS (point-in-time safe)
            if pere:
                ss = sire_snapshot[pere]
                ss["total"] += 1
                if p.get("is_gagnant"):
                    ss["wins"] += 1
                if p.get("is_place"):
                    ss["places"] += 1
                dist_cat = categorize_distance(p.get("distance"))
                ss["by_dist"][dist_cat]["total"] += 1
                if p.get("is_gagnant"):
                    ss["by_dist"][dist_cat]["wins"] += 1
                disc = (p.get("discipline") or "").lower().strip()
                if disc:
                    ss["by_disc"][disc]["total"] += 1
                    if p.get("is_gagnant"):
                        ss["by_disc"][disc]["wins"] += 1

            if pere_mere:
                ds = sire_snapshot[pere_mere]
                ds["total"] += 1
                if p.get("is_gagnant"):
                    ds["wins"] += 1
                if p.get("is_place"):
                    ds["places"] += 1

            if (i + 1) % 200000 == 0:
                log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis")

    log.info(f"Croisement pedigree terminé: {enriched}/{len(partants)} enrichis")


def main():
    log.info("=" * 60)
    log.info("SCRIPT 44 — Croisement Pedigree × Partants")
    log.info("=" * 60)

    pedigree_index = load_pedigree_index()
    partants = load_partants()
    if not partants:
        log.error("Aucun partant")
        sys.exit(1)

    sire_stats = build_sire_stats(partants)
    compute_croisement(partants, pedigree_index, sire_stats)
    log.info("TERMINÉ")


if __name__ == "__main__":
    main()
