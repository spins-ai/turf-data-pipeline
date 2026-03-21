#!/usr/bin/env python3
"""
Script 45 — Graphe de relations pour GNN
Calcul local, 0 requête API.

Input :
  - output/02_liste_courses/partants_normalises.jsonl

Output : output/45_graphe_gnn/
  - edges_cheval_jockey.jsonl
  - edges_cheval_entraineur.jsonl
  - edges_cheval_hippodrome.jsonl
  - edges_cheval_pere.jsonl
  - edges_jockey_entraineur.jsonl
  - node_features.jsonl (features par nœud)
  - graph_features_partants.jsonl (features GNN par partant)

Features par partant :
  - duo_cheval_jockey_nb : nb courses ensemble
  - duo_cheval_jockey_win_rate : taux victoire ensemble
  - duo_cheval_entraineur_nb : nb courses ensemble
  - duo_cheval_entraineur_win_rate : taux victoire
  - duo_jockey_entraineur_nb : nb courses ensemble
  - duo_jockey_entraineur_win_rate : taux victoire
  - cheval_degree : nb de connexions du cheval (jockeys, entraineurs, hippos)
  - jockey_nb_chevaux : nb de chevaux montés par le jockey
  - entraineur_nb_chevaux : nb de chevaux entraînés
  - hippo_familiarite : nb de fois le cheval a couru sur cet hippodrome
  - hippo_win_rate : taux victoire du cheval sur cet hippodrome
  - cluster_id : cluster communauté (jockey-entraineur)
"""

import json
import logging
import os
import sys
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "45_graphe_gnn")
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)


def load_partants():
    KEEP = {"partant_uid", "course_uid", "nom_cheval", "horse_id",
            "date_reunion_iso", "hippodrome_normalise", "jockey_driver",
            "entraineur", "pere", "mere", "distance", "discipline",
            "position_arrivee", "is_gagnant", "is_place",
            "numero_reunion", "numero_course"}
    partants = []
    for path in [os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "output", "02_liste_courses", "partants_normalises.json")]:
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


def compute_graph_features(partants):
    """Construit le graphe et calcule les features GNN par partant."""
    partants.sort(key=lambda p: (p.get("date_reunion_iso", ""), p.get("numero_reunion", 0)))

    log.info("Construction du graphe de relations...")

    # Compteurs progressifs (point-in-time safe)
    # duo -> {total, wins, places}
    cheval_jockey = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0})
    cheval_entraineur = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0})
    jockey_entraineur = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0})
    cheval_hippo = defaultdict(lambda: {"total": 0, "wins": 0, "places": 0})
    cheval_pere_stats = defaultdict(lambda: {"total": 0, "wins": 0})

    # Degré des nœuds
    jockey_chevaux = defaultdict(set)
    entraineur_chevaux = defaultdict(set)
    cheval_connections = defaultdict(set)  # tous les voisins

    # Cluster jockey-entraineur
    je_pairs = defaultdict(int)

    output_file = os.path.join(OUTPUT_DIR, "graph_features_partants.jsonl")
    enriched = 0

    # Fichiers edges
    edges_cj = open(os.path.join(OUTPUT_DIR, "edges_cheval_jockey.jsonl"), "w", encoding="utf-8")
    edges_ce = open(os.path.join(OUTPUT_DIR, "edges_cheval_entraineur.jsonl"), "w", encoding="utf-8")
    edges_ch = open(os.path.join(OUTPUT_DIR, "edges_cheval_hippodrome.jsonl"), "w", encoding="utf-8")

    with open(output_file, "w", encoding="utf-8") as fout:
        for i, p in enumerate(partants):
            nom = (p.get("nom_cheval") or "").upper().strip()
            jockey = (p.get("jockey_driver") or "").strip()
            entraineur = (p.get("entraineur") or "").strip()
            hippo = (p.get("hippodrome_normalise") or "").lower().strip()
            pere = (p.get("pere") or "").upper().strip()

            if not nom:
                continue

            result = {
                "partant_uid": p.get("partant_uid", ""),
                "course_uid": p.get("course_uid", ""),
                "nom_cheval": p.get("nom_cheval", ""),
            }

            # Lire les stats AVANT mise à jour (point-in-time)
            if jockey:
                cj_key = f"{nom}|{jockey}"
                cj = cheval_jockey[cj_key]
                if cj["total"] > 0:
                    enriched += 1
                    result["duo_cheval_jockey_nb"] = cj["total"]
                    result["duo_cheval_jockey_win_rate"] = round(cj["wins"] / cj["total"], 3)
                    result["duo_cheval_jockey_place_rate"] = round(cj["places"] / cj["total"], 3)

                result["jockey_nb_chevaux"] = len(jockey_chevaux[jockey])

            if entraineur:
                ce_key = f"{nom}|{entraineur}"
                ce = cheval_entraineur[ce_key]
                if ce["total"] > 0:
                    result["duo_cheval_entraineur_nb"] = ce["total"]
                    result["duo_cheval_entraineur_win_rate"] = round(ce["wins"] / ce["total"], 3)

                result["entraineur_nb_chevaux"] = len(entraineur_chevaux[entraineur])

            if jockey and entraineur:
                je_key = f"{jockey}|{entraineur}"
                je = jockey_entraineur[je_key]
                if je["total"] > 0:
                    result["duo_jockey_entraineur_nb"] = je["total"]
                    result["duo_jockey_entraineur_win_rate"] = round(je["wins"] / je["total"], 3)

            if hippo:
                ch_key = f"{nom}|{hippo}"
                ch = cheval_hippo[ch_key]
                if ch["total"] > 0:
                    result["hippo_familiarite"] = ch["total"]
                    result["hippo_win_rate"] = round(ch["wins"] / ch["total"], 3)
                    result["hippo_place_rate"] = round(ch["places"] / ch["total"], 3)

            # Degré du cheval
            result["cheval_degree"] = len(cheval_connections[nom])

            # Première fois ?
            result["premier_jockey"] = jockey and nom not in {c for c in jockey_chevaux[jockey]}
            result["premier_hippo"] = hippo and cheval_hippo[f"{nom}|{hippo}"]["total"] == 0

            fout.write(json.dumps(result, ensure_ascii=False) + "\n")

            # Mettre à jour les stats APRÈS (point-in-time safe)
            is_win = p.get("is_gagnant", False)
            is_place = p.get("is_place", False)

            if jockey:
                cj = cheval_jockey[f"{nom}|{jockey}"]
                cj["total"] += 1
                if is_win: cj["wins"] += 1
                if is_place: cj["places"] += 1
                jockey_chevaux[jockey].add(nom)
                cheval_connections[nom].add(f"J:{jockey}")

                edges_cj.write(json.dumps({
                    "cheval": nom, "jockey": jockey,
                    "date": p.get("date_reunion_iso", ""),
                    "win": is_win, "place": is_place,
                }, ensure_ascii=False) + "\n")

            if entraineur:
                ce = cheval_entraineur[f"{nom}|{entraineur}"]
                ce["total"] += 1
                if is_win: ce["wins"] += 1
                if is_place: ce["places"] += 1
                entraineur_chevaux[entraineur].add(nom)
                cheval_connections[nom].add(f"E:{entraineur}")

                edges_ce.write(json.dumps({
                    "cheval": nom, "entraineur": entraineur,
                    "date": p.get("date_reunion_iso", ""),
                    "win": is_win, "place": is_place,
                }, ensure_ascii=False) + "\n")

            if jockey and entraineur:
                je = jockey_entraineur[f"{jockey}|{entraineur}"]
                je["total"] += 1
                if is_win: je["wins"] += 1
                if is_place: je["places"] += 1
                je_pairs[f"{jockey}|{entraineur}"] += 1

            if hippo:
                ch = cheval_hippo[f"{nom}|{hippo}"]
                ch["total"] += 1
                if is_win: ch["wins"] += 1
                if is_place: ch["places"] += 1
                cheval_connections[nom].add(f"H:{hippo}")

                edges_ch.write(json.dumps({
                    "cheval": nom, "hippodrome": hippo,
                    "date": p.get("date_reunion_iso", ""),
                    "win": is_win, "place": is_place,
                }, ensure_ascii=False) + "\n")

            if pere:
                cheval_connections[nom].add(f"P:{pere}")

            if (i + 1) % 200000 == 0:
                log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis, "
                         f"{len(cheval_connections)} chevaux, {len(jockey_chevaux)} jockeys")

    edges_cj.close()
    edges_ce.close()
    edges_ch.close()

    log.info(f"Graphe terminé: {enriched}/{len(partants)} enrichis")
    log.info(f"  Chevaux: {len(cheval_connections)}, Jockeys: {len(jockey_chevaux)}, "
             f"Entraîneurs: {len(entraineur_chevaux)}")

    # Sauver les node features
    node_file = os.path.join(OUTPUT_DIR, "node_features.jsonl")
    with open(node_file, "w", encoding="utf-8") as f:
        for jockey, chevaux in jockey_chevaux.items():
            f.write(json.dumps({
                "type": "jockey", "name": jockey, "nb_chevaux": len(chevaux),
            }, ensure_ascii=False) + "\n")
        for entraineur, chevaux in entraineur_chevaux.items():
            f.write(json.dumps({
                "type": "entraineur", "name": entraineur, "nb_chevaux": len(chevaux),
            }, ensure_ascii=False) + "\n")

    log.info(f"Node features: {node_file}")


def main():
    log.info("=" * 60)
    log.info("SCRIPT 45 — Graphe de relations GNN")
    log.info("=" * 60)

    partants = load_partants()
    if not partants:
        log.error("Aucun partant")
        sys.exit(1)

    compute_graph_features(partants)
    log.info("TERMINÉ")


if __name__ == "__main__":
    main()
