#!/usr/bin/env python3
"""
Script 49 — Écart cotes Internet vs National + Market Efficiency
Calcul local, 0 requête API.

Input :
  - output/38_rapports_internet/rapports_internet.jsonl (ou .json)
  - output/21_rapports_definitifs/rapports_definitifs.jsonl (ou .json)
  - output/02_liste_courses/partants_normalises.jsonl (ou .json)
  - output/07_cotes_marche/cotes_marche.json

Output : output/49_ecart_cotes/
  - ecart_cotes_market.jsonl

Features :
  - ecart_internet_national : différence de cote internet vs nationale
  - ratio_internet_national : ratio des cotes
  - rapport_internet_gagnant : rapport gagnant internet
  - rapport_national_gagnant : rapport gagnant national
  - overbet_score : surenchère (cote basse vs attendu)
  - underbet_score : sous-enchère (cote haute vs attendu)
  - market_efficiency : efficience du marché (calibration cote vs résultat)
  - sharp_money_indicator : argent intelligent (mouvement de cote)
  - steam_move : mouvement brutal de cote (steam = argent massif)
  - cote_mouvement_pct : % de mouvement de cote (première vs dernière)
  - clv_closing_line_value : valeur par rapport à la cote de clôture
  - public_overbet : surestimation publique (favori trop joué)
  - longshot_bias : biais longshot (outsiders sous-payés)
"""

import json
import logging
import math
import os
import sys
from collections import defaultdict

OUTPUT_DIR = "output/49_ecart_cotes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)


def load_rapports_internet():
    """Charge les rapports internet indexés par course_uid."""
    index = {}
    for path in ["output/38_rapports_internet/rapports_internet.jsonl",
                 "output/38_rapports_internet/rapports_internet.json"]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement rapports internet: {path}")
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                        uid = r.get("course_uid", "")
                        if uid:
                            index[uid] = r
                    except json.JSONDecodeError:
                        continue
        else:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for r in data:
                    uid = r.get("course_uid", "")
                    if uid:
                        index[uid] = r
            del data
        break
    log.info(f"  {len(index)} rapports internet indexés")
    return index


def load_rapports_nationaux():
    """Charge les rapports nationaux indexés par course_uid."""
    index = {}
    for path in ["output/21_rapports_definitifs/rapports_definitifs.jsonl",
                 "output/21_rapports_definitifs/rapports_definitifs.json"]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement rapports nationaux: {path}")
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                        uid = r.get("course_uid", "")
                        if uid:
                            index[uid] = r
                    except json.JSONDecodeError:
                        continue
        else:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for r in data:
                    uid = r.get("course_uid", "")
                    if uid:
                        index[uid] = r
            del data
        break
    log.info(f"  {len(index)} rapports nationaux indexés")
    return index


def load_partants():
    KEEP = {"partant_uid", "course_uid", "nom_cheval", "horse_id",
            "date_reunion_iso", "hippodrome_normalise", "num_pmu",
            "cote_finale", "cote_reference", "proba_implicite",
            "position_arrivee", "is_gagnant", "is_place",
            "distance", "discipline", "nombre_partants",
            "numero_reunion", "numero_course"}
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


def load_cotes_marche():
    """Charge les cotes marché pour détecter les mouvements."""
    index = {}
    path = "output/07_cotes_marche/cotes_marche.json"
    if not os.path.exists(path):
        return index
    log.info(f"Chargement cotes marché: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        for c in data:
            uid = c.get("partant_uid") or c.get("course_uid", "")
            if uid:
                index[uid] = c
    elif isinstance(data, dict):
        index = data
    del data
    log.info(f"  {len(index)} entrées cotes marché")
    return index


def compute_ecart_features(partants, rapports_internet, rapports_nationaux, cotes_marche):
    """Calcule les features d'écart de cotes et market efficiency."""
    partants.sort(key=lambda p: (p.get("date_reunion_iso", ""), p.get("numero_reunion", 0), p.get("numero_course", 0)))

    log.info("Calcul des écarts de cotes...")

    # Grouper les partants par course pour calculer les stats de marché
    course_partants = defaultdict(list)
    for p in partants:
        cuid = p.get("course_uid", "")
        if cuid:
            course_partants[cuid].append(p)

    output_file = os.path.join(OUTPUT_DIR, "ecart_cotes_market.jsonl")
    enriched = 0

    # Historique cote pour CLV
    horse_cotes = defaultdict(list)  # nom -> [cotes finales]

    with open(output_file, "w", encoding="utf-8") as fout:
        for i, p in enumerate(partants):
            nom = (p.get("nom_cheval") or "").upper().strip()
            cuid = p.get("course_uid", "")
            num_pmu = p.get("num_pmu")

            result = {
                "partant_uid": p.get("partant_uid", ""),
                "course_uid": cuid,
                "nom_cheval": p.get("nom_cheval", ""),
            }

            cote_finale = p.get("cote_finale")
            cote_ref = p.get("cote_reference")

            # === Rapports Internet vs National ===
            ri = rapports_internet.get(cuid, {})
            rn = rapports_nationaux.get(cuid, {})

            rapport_int_gagnant = ri.get("rapport_simple_gagnant")
            rapport_nat_gagnant = rn.get("rapport_simple_gagnant")

            if rapport_int_gagnant is not None and rapport_nat_gagnant is not None:
                try:
                    ri_f = float(rapport_int_gagnant)
                    rn_f = float(rapport_nat_gagnant)
                    if rn_f > 0:
                        result["rapport_internet_gagnant"] = ri_f
                        result["rapport_national_gagnant"] = rn_f
                        result["ecart_internet_national"] = round(ri_f - rn_f, 2)
                        result["ratio_internet_national"] = round(ri_f / rn_f, 3)
                        enriched += 1
                except (ValueError, TypeError):
                    pass

            # === Overbet / Underbet ===
            if cote_finale is not None and cote_ref is not None:
                try:
                    cf = float(cote_finale)
                    cr = float(cote_ref)
                    if cr > 0 and cf > 0:
                        ratio = cf / cr
                        result["cote_ratio_final_ref"] = round(ratio, 3)
                        # < 1 = cote a baissé (overbet/steam)
                        # > 1 = cote a monté (underbet/drift)
                        result["overbet_score"] = round(max(0, 1 - ratio) * 100, 1)
                        result["underbet_score"] = round(max(0, ratio - 1) * 100, 1)

                        # Steam move (mouvement brutal > 20%)
                        mouvement_pct = (cf - cr) / cr * 100
                        result["cote_mouvement_pct"] = round(mouvement_pct, 1)
                        result["steam_move"] = mouvement_pct < -20
                        result["drift_move"] = mouvement_pct > 20

                        # Sharp money indicator
                        # Cote qui baisse beaucoup = argent intelligent
                        result["sharp_money_indicator"] = mouvement_pct < -15
                except (ValueError, TypeError):
                    pass

            # === Market Efficiency (calibration au niveau de la course) ===
            course_p = course_partants.get(cuid, [])
            if len(course_p) >= 3 and cote_finale:
                try:
                    cf = float(cote_finale)
                    if cf > 0:
                        # Somme des probas implicites (overround)
                        probas = []
                        for cp in course_p:
                            c = cp.get("cote_finale")
                            if c and float(c) > 0:
                                probas.append(1 / float(c))

                        if probas:
                            overround = sum(probas)
                            result["market_overround"] = round(overround, 3)
                            # Proba ajustée (sans overround)
                            proba_brute = 1 / cf
                            result["proba_ajustee"] = round(proba_brute / overround, 4) if overround > 0 else None

                            # Public overbet (favori surjoué)
                            # Si cote < 3 et nb_partants > 10, le public surjoue probablement
                            nb = p.get("nombre_partants") or len(course_p)
                            if cf < 3 and nb > 10:
                                result["public_overbet"] = True
                            else:
                                result["public_overbet"] = False

                            # Longshot bias
                            if cf > 20:
                                result["longshot_bias"] = True
                            else:
                                result["longshot_bias"] = False
                except (ValueError, TypeError):
                    pass

            # === CLV (Closing Line Value) ===
            # Comparer la cote de référence (matin) vs cote finale (fermeture)
            if cote_ref and cote_finale:
                try:
                    cr = float(cote_ref)
                    cf = float(cote_finale)
                    if cr > 0 and cf > 0:
                        # CLV positif = on aurait eu meilleure cote en pariant tôt
                        clv = (1/cr - 1/cf) * 100  # en points de pourcentage
                        result["clv_closing_line_value"] = round(clv, 2)
                except (ValueError, TypeError):
                    pass

            # === Historique cote du cheval ===
            if nom and cote_finale:
                hist = horse_cotes[nom]
                if hist:
                    avg_cote = sum(hist) / len(hist)
                    try:
                        cf = float(cote_finale)
                        result["cote_vs_historique"] = round(cf - avg_cote, 2)
                        result["cote_historique_moy"] = round(avg_cote, 2)
                        # Cote plus basse que d'habitude = confiance accrue
                        result["confiance_accrue"] = cf < avg_cote * 0.8
                    except (ValueError, TypeError):
                        pass

                # Ajouter au historique
                try:
                    horse_cotes[nom].append(float(cote_finale))
                except (ValueError, TypeError):
                    pass

            # === Cotes marché ===
            cm = cotes_marche.get(p.get("partant_uid", ""), {})
            if cm:
                # Mouvement de cote depuis l'ouverture
                cote_ouverture = cm.get("cote_ouverture") or cm.get("cote_premiere")
                if cote_ouverture and cote_finale:
                    try:
                        co = float(cote_ouverture)
                        cf = float(cote_finale)
                        if co > 0:
                            result["cote_mouvement_ouverture_pct"] = round((cf - co) / co * 100, 1)
                    except (ValueError, TypeError):
                        pass

            fout.write(json.dumps(result, ensure_ascii=False) + "\n")

            if (i + 1) % 200000 == 0:
                log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis")

    log.info(f"Écart cotes terminé: {enriched}/{len(partants)} enrichis ({100*enriched/len(partants):.1f}%)")


def main():
    log.info("=" * 60)
    log.info("SCRIPT 49 — Écart cotes Internet vs National + Market Efficiency")
    log.info("=" * 60)

    rapports_internet = load_rapports_internet()
    rapports_nationaux = load_rapports_nationaux()
    cotes_marche = load_cotes_marche()
    partants = load_partants()

    if not partants:
        log.error("Aucun partant")
        sys.exit(1)

    compute_ecart_features(partants, rapports_internet, rapports_nationaux, cotes_marche)
    log.info("TERMINÉ")


if __name__ == "__main__":
    main()
