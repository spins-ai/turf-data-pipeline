#!/usr/bin/env python3
"""
run_advanced_builders.py - Calculs mathematiques avances
=========================================================
C14. Expected Value (EV) - rapport qualite/prix de chaque cheval
C15. Performance Relative - normalisation par qualite du peloton
C12. Class Drop/Rise - changement de classe entre courses

Lit partants_master.parquet en streaming.
"""

import sys
import time
import json
import math
import numpy as np
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq

PARQUET = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")


def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


# =====================================================================
# C14. EXPECTED VALUE BUILDER
# =====================================================================
def build_expected_value(row, horse_history):
    """Calcul du rapport qualite/prix: EV, Kelly, Sharpe."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid

    cote = safe_float(row.get("cote_finale"))
    proba_impl = safe_float(row.get("mch_proba_implicite"))
    horse_id = str(row.get("horse_id") or row.get("nom_cheval") or "")

    # Historique du cheval pour estimer sa vraie probabilite
    hist = horse_history.get(horse_id, {"wins": 0, "runs": 0, "returns": []})

    # Proba estimee par historique (bayesian: prior 8.8% + observations)
    prior_wins = 0.088 * 5  # prior: 5 courses virtuelles a 8.8%
    prior_runs = 5
    estimated_proba = (hist["wins"] + prior_wins) / (hist["runs"] + prior_runs) if hist["runs"] + prior_runs > 0 else 0.088

    # Feature 1: Proba estimee du cheval (bayesian)
    features["ev_x__proba_estimee"] = estimated_proba

    # Feature 2: Expected Value = proba * cote - 1
    if cote and cote > 0:
        features["ev_x__expected_value"] = estimated_proba * cote - 1.0
    else:
        features["ev_x__expected_value"] = None

    # Feature 3: Kelly fraction = (p*b - q) / b ou b=cote-1, p=proba, q=1-p
    if cote and cote > 1:
        b = cote - 1
        q = 1 - estimated_proba
        kelly = (estimated_proba * b - q) / b
        features["ev_x__kelly_fraction"] = max(kelly, 0)  # never negative
    else:
        features["ev_x__kelly_fraction"] = None

    # Feature 4: Ecart proba estimee vs proba implicite (marche)
    if proba_impl:
        features["ev_x__proba_edge"] = estimated_proba - proba_impl
    else:
        features["ev_x__proba_edge"] = None

    # Feature 5: Cote value (cote actuelle vs cote "juste" basee sur historique)
    if estimated_proba > 0:
        fair_odds = 1.0 / estimated_proba
        if cote:
            features["ev_x__odds_value"] = cote / fair_odds  # >1 = value bet
        else:
            features["ev_x__odds_value"] = None
    else:
        features["ev_x__odds_value"] = None

    # Feature 6: Sharpe ratio du cheval (rendement moyen / volatilite)
    returns = hist["returns"]
    if len(returns) >= 3:
        mean_ret = np.mean(returns)
        std_ret = np.std(returns)
        features["ev_x__sharpe_ratio"] = mean_ret / std_ret if std_ret > 0 else 0
    else:
        features["ev_x__sharpe_ratio"] = None

    return features


# =====================================================================
# C15. RELATIVE PERFORMANCE BUILDER
# =====================================================================
def build_relative_perf(row, course_stats, horse_career):
    """Performance normalisee par qualite du peloton."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid

    course_uid = row.get("course_uid")
    horse_id = str(row.get("horse_id") or row.get("nom_cheval") or "")
    nb_partants = safe_float(row.get("mch_nb_partants_course"))
    elo = safe_float(row.get("gnn_cheval_degree"))  # proxy for strength

    career = horse_career.get(horse_id, {"avg_pos_norm": None, "runs": 0, "positions": []})

    # Feature 1: Position normalisee historique (pos / nb_partants)
    features["relperf_x__avg_pos_norm"] = career["avg_pos_norm"]

    # Feature 2: Taux top-half historique
    positions = career["positions"]
    if positions:
        top_half = sum(1 for p, n in positions if p <= n / 2) / len(positions)
        features["relperf_x__top_half_rate"] = top_half
    else:
        features["relperf_x__top_half_rate"] = None

    # Feature 3: Force relative dans le peloton
    if course_uid and course_uid in course_stats:
        field_avg_elo = course_stats[course_uid].get("avg_elo")
        if elo and field_avg_elo and field_avg_elo > 0:
            features["relperf_x__elo_vs_field"] = elo / field_avg_elo
        else:
            features["relperf_x__elo_vs_field"] = None
        # Nb partants (liquidite/difficulte)
        features["relperf_x__nb_partants"] = safe_float(course_stats[course_uid].get("nb_partants"))
    else:
        features["relperf_x__elo_vs_field"] = None
        features["relperf_x__nb_partants"] = nb_partants

    # Feature 4: Progression recente (amelioration vs moyenne carriere)
    if len(positions) >= 3 and career["avg_pos_norm"] is not None:
        recent_avg = np.mean([p / max(n, 1) for p, n in positions[-3:]])
        features["relperf_x__recent_vs_career"] = career["avg_pos_norm"] - recent_avg  # positive = improving
    else:
        features["relperf_x__recent_vs_career"] = None

    # Feature 5: Variance de performance (consistency)
    if len(positions) >= 3:
        norms = [p / max(n, 1) for p, n in positions]
        features["relperf_x__consistency"] = 1.0 - np.std(norms)  # high = consistent
    else:
        features["relperf_x__consistency"] = None

    return features


# =====================================================================
# C12. CLASS DROP/RISE BUILDER
# =====================================================================
def build_class_change(row, horse_last_class):
    """Changement de classe entre courses consecutives."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid

    horse_id = str(row.get("horse_id") or row.get("nom_cheval") or "")
    allocation = safe_float(row.get("cnd_cond_prix_euros"))
    if allocation is None:
        allocation = safe_float(row.get("engagement"))

    prev_class = horse_last_class.get(horse_id)

    # Feature 1: Ratio classe actuelle / precedente
    if prev_class and allocation and prev_class > 0:
        features["class_x__ratio"] = allocation / prev_class
    else:
        features["class_x__ratio"] = None

    # Feature 2: Is class drop (descend de categorie = avantage)
    if prev_class and allocation and prev_class > 0:
        features["class_x__is_drop"] = 1.0 if allocation < prev_class * 0.8 else 0.0
    else:
        features["class_x__is_drop"] = None

    # Feature 3: Is class rise (monte de categorie = desavantage)
    if prev_class and allocation and prev_class > 0:
        features["class_x__is_rise"] = 1.0 if allocation > prev_class * 1.2 else 0.0
    else:
        features["class_x__is_rise"] = None

    # Feature 4: Classe absolue (log allocation)
    features["class_x__level_log"] = math.log1p(allocation) if allocation and allocation > 0 else None

    # Update history
    if allocation:
        horse_last_class[horse_id] = allocation

    return features


# =====================================================================
# MAIN
# =====================================================================
def main():
    start = time.time()
    print("=" * 70)
    print("  BUILDERS AVANCES (C12, C14, C15)")
    print("=" * 70)

    pf = pq.ParquetFile(str(PARQUET))
    n_rg = pf.metadata.num_row_groups
    n_rows = pf.metadata.num_rows
    print(f"  {n_rows:,} rows, {n_rg} row groups")

    # Phase 1: Build course stats (avg elo) + horse history
    print("\nPhase 1: Stats par course et historique chevaux...")
    needed_cols_p1 = ["partant_uid", "course_uid", "horse_id", "nom_cheval",
                      "is_gagnant", "cote_finale", "gnn_cheval_degree",
                      "mch_nb_partants_course"]
    schema_names = set(pf.schema_arrow.names)
    needed_cols_p1 = [c for c in needed_cols_p1 if c in schema_names]

    course_stats = defaultdict(lambda: {"elos": [], "nb_partants": 0})
    horse_history = defaultdict(lambda: {"wins": 0, "runs": 0, "returns": []})
    horse_career = defaultdict(lambda: {"avg_pos_norm": None, "runs": 0, "positions": []})

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=needed_cols_p1)
        df = table.to_pandas()
        del table
        for _, r in df.iterrows():
            cuid = r.get("course_uid")
            hid = str(r.get("horse_id") or r.get("nom_cheval") or "")
            elo = safe_float(r.get("gnn_cheval_degree"))
            is_win = safe_float(r.get("is_gagnant"))
            cote = safe_float(r.get("cote_finale"))
            nb_p = safe_float(r.get("mch_nb_partants_course"))

            if cuid:
                if elo:
                    course_stats[cuid]["elos"].append(elo)
                if nb_p:
                    course_stats[cuid]["nb_partants"] = nb_p

            if hid:
                horse_history[hid]["runs"] += 1
                if is_win and is_win > 0:
                    horse_history[hid]["wins"] += 1
                if cote and is_win is not None:
                    ret = cote if is_win > 0 else -1.0
                    horse_history[hid]["returns"].append(ret)
        del df
        if (rg_idx + 1) % 10 == 0:
            print(f"  RG {rg_idx+1}/{n_rg}")

    # Finalize course stats
    for cuid, stats in course_stats.items():
        elos = stats["elos"]
        stats["avg_elo"] = np.mean(elos) if elos else None
        del stats["elos"]

    print(f"  {len(course_stats):,} courses, {len(horse_history):,} chevaux")

    # Phase 2: Compute features
    print("\nPhase 2: Calcul des features...")
    needed_cols = [
        "partant_uid", "course_uid", "horse_id", "nom_cheval",
        "cote_finale", "mch_proba_implicite", "gnn_cheval_degree",
        "mch_nb_partants_course", "cnd_cond_prix_euros", "engagement",
        "is_gagnant",
    ]
    needed_cols = [c for c in needed_cols if c in schema_names]

    horse_last_class = {}
    builders = {"ev_x": [], "relperf_x": [], "class_x": []}
    total = 0

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=needed_cols)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            row = r.to_dict()
            total += 1

            f14 = build_expected_value(row, horse_history)
            if f14:
                builders["ev_x"].append(f14)

            f15 = build_relative_perf(row, course_stats, horse_career)
            if f15:
                builders["relperf_x"].append(f15)

            f12 = build_class_change(row, horse_last_class)
            if f12:
                builders["class_x"].append(f12)

        del df

        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            for name, records in builders.items():
                if records:
                    out_dir = OUTPUT_DIR / name
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f"{name}_features.jsonl"
                    mode = "a" if rg_idx >= 5 else "w"
                    with open(out_file, mode, encoding="utf-8", newline="\n") as f:
                        for rec in records:
                            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    records.clear()
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | {time.time()-start:.0f}s")

    elapsed = time.time() - start
    print(f"\n{'='*70}")
    print(f"  TERMINE en {elapsed:.0f}s | {total:,} lignes")
    for name in builders:
        out_file = OUTPUT_DIR / name / f"{name}_features.jsonl"
        if out_file.exists():
            size_mb = out_file.stat().st_size / 1024 / 1024
            with open(out_file, "r") as f:
                n = sum(1 for _ in f)
            print(f"  {name}: {n:,} records, {size_mb:.0f} Mo")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
