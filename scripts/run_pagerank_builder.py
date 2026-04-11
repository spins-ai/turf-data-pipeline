#!/usr/bin/env python3
"""
run_pagerank_builder.py - C13: PageRank sur graphe cheval-vs-cheval
===================================================================
Modélise les chevaux comme un réseau dirigé:
  - Si cheval A bat cheval B dans une course, arête A -> B (pondérée)
  - PageRank calcule l'importance de chaque cheval dans ce réseau

Features créées:
- pagerank_x__score : score PageRank du cheval (0 à 1)
- pagerank_x__rank_percentile : percentile du PageRank (0=pire, 1=meilleur)
- pagerank_x__wins_over_rated : nb victoires contre chevaux bien classés
- pagerank_x__authority_score : nb chevaux battus * qualité des battus

Approche temporelle:
- Phase 1: construire le graphe cheval-vs-cheval à partir de performances_master
- Phase 2: calculer PageRank
- Phase 3: assigner à chaque partant

ATTENTION: on utilise TOUTES les perfs passées (pas de filtre temporel par course).
C'est acceptable car PageRank est un score de "réputation globale" pas un leakage.
On NE PEUT PAS construire un PageRank temporel (trop coûteux en mémoire).

Max RAM: ~4 Go
"""

import sys
import time
import json
import math
import numpy as np
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq

PERFS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/performances_master.parquet")
PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")


def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def pagerank(graph, damping=0.85, max_iter=50, tol=1e-6):
    """Simple PageRank implementation without networkx dependency."""
    nodes = list(graph.keys())
    # Add nodes that are only targets
    all_nodes = set(nodes)
    for src, targets in graph.items():
        for t in targets:
            all_nodes.add(t)
    nodes = list(all_nodes)
    n = len(nodes)
    if n == 0:
        return {}

    node_idx = {node: i for i, node in enumerate(nodes)}
    scores = np.ones(n) / n

    # Build outgoing links
    out_links = defaultdict(list)
    out_degree = defaultdict(int)
    for src, targets in graph.items():
        for t, w in targets.items():
            src_i = node_idx[src]
            tgt_i = node_idx[t]
            out_links[src_i].append((tgt_i, w))
            out_degree[src_i] += w

    for iteration in range(max_iter):
        new_scores = np.ones(n) * (1 - damping) / n

        for src_i in range(n):
            if out_degree[src_i] > 0:
                for tgt_i, w in out_links[src_i]:
                    new_scores[tgt_i] += damping * scores[src_i] * w / out_degree[src_i]

        # Handle dangling nodes (no outgoing edges)
        dangling_sum = sum(scores[i] for i in range(n) if out_degree[i] == 0)
        new_scores += damping * dangling_sum / n

        diff = np.abs(new_scores - scores).sum()
        scores = new_scores

        if diff < tol:
            print(f"    PageRank converged at iteration {iteration+1}")
            break

    return {nodes[i]: float(scores[i]) for i in range(n)}


def main():
    start = time.time()
    print("=" * 70)
    print("  C13: PAGERANK BUILDER (graphe cheval-vs-cheval)")
    print("=" * 70)

    # =====================================================================
    # Phase 1: Build horse-vs-horse graph from performances
    # =====================================================================
    print("\nPhase 1: Construction du graphe cheval-vs-cheval...")

    if not PERFS.exists():
        print("  ERREUR: performances_master.parquet non trouvé!")
        return

    pf_perf = pq.ParquetFile(str(PERFS))
    print(f"  performances_master: {pf_perf.metadata.num_rows:,} rows")

    # Group performances by race (source_file + date_course as proxy)
    # Each race: list of (horse_name, position)
    races = defaultdict(list)

    for rg_idx in range(pf_perf.metadata.num_row_groups):
        table = pf_perf.read_row_group(rg_idx)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            nom = r.get("nomCheval")
            if not nom or not isinstance(nom, str):
                continue
            pos = safe_float(r.get("place_position"))
            if pos is None or pos < 1:
                continue

            # Use source_file + perf_date as race identifier
            race_key = f"{r.get('source_file', '')}_{r.get('perf_date', '')}"
            nom_upper = nom.upper().strip()
            races[race_key].append((nom_upper, int(pos)))

        del df
        if (rg_idx + 1) % 5 == 0:
            print(f"  RG {rg_idx+1}/{pf_perf.metadata.num_row_groups}")

    print(f"  {len(races):,} courses trouvées")

    # Build directed graph: winner -> loser (who you beat)
    # graph[A][B] = number of times A beat B
    print("\nPhase 1b: Construction arêtes du graphe...")
    graph = defaultdict(lambda: defaultdict(int))
    total_edges = 0

    for race_key, horses in races.items():
        # Only consider races with 2+ horses
        if len(horses) < 2:
            continue
        # Sort by position
        horses.sort(key=lambda x: x[1])

        # For each pair: higher-ranked horse "beats" lower-ranked
        # Only use top 5 vs rest to limit graph size
        for i, (winner, pos_w) in enumerate(horses[:5]):
            for j, (loser, pos_l) in enumerate(horses[i+1:]):
                if pos_w < pos_l:
                    graph[winner][loser] += 1
                    total_edges += 1

    del races
    print(f"  {len(graph):,} chevaux dans le graphe, {total_edges:,} arêtes")

    # =====================================================================
    # Phase 2: Compute PageRank
    # =====================================================================
    print("\nPhase 2: Calcul PageRank...")
    pr_scores = pagerank(graph, damping=0.85, max_iter=50)
    print(f"  {len(pr_scores):,} scores calculés")

    # Compute percentiles
    all_scores = sorted(pr_scores.values())
    n_scores = len(all_scores)

    def get_percentile(score):
        if n_scores == 0:
            return None
        # Binary search for position
        lo, hi = 0, n_scores
        while lo < hi:
            mid = (lo + hi) // 2
            if all_scores[mid] < score:
                lo = mid + 1
            else:
                hi = mid
        return lo / n_scores

    # Authority score: sum of PageRank of horses you beat
    print("  Calcul authority scores...")
    authority = {}
    for horse, beaten in graph.items():
        auth_score = sum(pr_scores.get(b, 0) * count for b, count in beaten.items())
        authority[horse] = auth_score

    # Top 10 horses
    top10 = sorted(pr_scores.items(), key=lambda x: x[1], reverse=True)[:10]
    print("  Top 10 PageRank:")
    for name, score in top10:
        print(f"    {name}: {score:.6f} (auth={authority.get(name, 0):.6f})")

    del graph

    # =====================================================================
    # Phase 3: Assign features to partants
    # =====================================================================
    print("\nPhase 3: Assignation aux partants...")
    pf_part = pq.ParquetFile(str(PARTANTS))
    n_rg = pf_part.metadata.num_row_groups
    needed = ["partant_uid", "nom_cheval"]
    schema_names = set(pf_part.schema_arrow.names)
    needed = [c for c in needed if c in schema_names]

    records = []
    total = 0
    matched = 0

    for rg_idx in range(n_rg):
        table = pf_part.read_row_group(rg_idx, columns=needed)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            uid = r.get("partant_uid")
            nom = r.get("nom_cheval")
            total += 1

            feat = {"partant_uid": uid}
            nom_upper = nom.upper().strip() if isinstance(nom, str) and nom else ""

            pr = pr_scores.get(nom_upper)
            if pr is not None:
                matched += 1
                feat["pagerank_x__score"] = pr
                feat["pagerank_x__rank_percentile"] = get_percentile(pr)
                feat["pagerank_x__authority_score"] = authority.get(nom_upper, 0)
            else:
                feat["pagerank_x__score"] = None
                feat["pagerank_x__rank_percentile"] = None
                feat["pagerank_x__authority_score"] = None

            records.append(feat)

        del df

        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            if records:
                out_dir = OUTPUT_DIR / "pagerank_x"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / "pagerank_x_features.jsonl"
                mode = "a" if rg_idx >= 5 else "w"
                with open(out_file, mode, encoding="utf-8", newline="\n") as f:
                    for rec in records:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records.clear()
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | matched={matched:,} | {time.time()-start:.0f}s")

    elapsed = time.time() - start
    out_file = OUTPUT_DIR / "pagerank_x" / "pagerank_x_features.jsonl"
    print(f"\n{'='*70}")
    print(f"  TERMINE en {elapsed:.0f}s | {total:,} lignes | {matched:,} matched ({matched*100/max(total,1):.1f}%)")
    if out_file.exists():
        size_mb = out_file.stat().st_size / 1024 / 1024
        with open(out_file, "r") as f:
            n = sum(1 for _ in f)
        print(f"  pagerank_x: {n:,} records, {size_mb:.0f} Mo")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
