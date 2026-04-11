#!/usr/bin/env python3
"""
validate_pipeline_output.py -- Garde-fou automatique post-pipeline
===================================================================
Verifie que features_selected.parquet est propre et pret pour les modeles.
A lancer apres chaque execution du pipeline.

Checks:
  1. Le fichier existe et est lisible
  2. Nombre de lignes = 2,930,290 (ou proche)
  3. Colonnes obligatoires presentes (partant_uid, is_gagnant, target_roi)
  4. ZERO colonne post-course (position_arrivee, ecart_lengths, etc.)
  5. Pas de colonne avec >50% NaN
  6. Target is_gagnant a ~8.8% positifs (pas de desequilibre anormal)
  7. Aucune feature avec correlation >0.99 avec la target (leakage)

Exit code 0 = tout OK, 1 = probleme detecte
"""

import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

PARQUET = Path("D:/turf-data-pipeline/04_FEATURES/features_selected.parquet")
EXPECTED_ROWS = 2_930_290
REQUIRED_COLS = ["partant_uid", "comblage__is_gagnant"]

# Colonnes post-course interdites (patterns)
FORBIDDEN_PATTERNS = [
    "position_arrivee", "ecart_length", "bl_ecart",
    "wmf_position_margin", "wmf_relative_speed_figure",
    "wmf_horse_avg_time_behind", "rtm_time_vs_winner",
    "rtm_is_fastest", "rtm_speed_rank",
    "wmf_horse_in_top_quarter",
    "pf_race_competitiveness", "rrp_exacta",
    "log_rapport_simple_gagnant",
]

def main():
    errors = []
    warnings = []

    print("=" * 60)
    print("VALIDATION POST-PIPELINE")
    print("=" * 60)

    # 1. Fichier existe
    if not PARQUET.exists():
        print(f"ERREUR: {PARQUET} n'existe pas!")
        sys.exit(1)

    pf = pq.ParquetFile(str(PARQUET))
    names = pf.schema_arrow.names
    n_rows = pf.metadata.num_rows
    n_cols = len(names)
    print(f"Fichier: {PARQUET.name} ({n_cols} cols, {n_rows:,} rows)")

    # 2. Nombre de lignes
    if abs(n_rows - EXPECTED_ROWS) > 1000:
        errors.append(f"Rows: {n_rows:,} (attendu ~{EXPECTED_ROWS:,})")
    else:
        print(f"  [OK] Rows: {n_rows:,}")

    # 3. Colonnes obligatoires
    for col in REQUIRED_COLS:
        if col not in names:
            errors.append(f"Colonne manquante: {col}")
        else:
            print(f"  [OK] {col} presente")

    # 4. Zero colonne post-course
    forbidden_found = []
    for n in names:
        nl = n.lower()
        for pat in FORBIDDEN_PATTERNS:
            if pat in nl:
                forbidden_found.append(n)
                break
    if forbidden_found:
        errors.append(f"Colonnes post-course detectees: {forbidden_found}")
    else:
        print(f"  [OK] Zero colonne post-course")

    # 5. NaN check (echantillon sur premier row group)
    table = pf.read_row_group(0)
    high_nan = []
    for i, col_name in enumerate(names):
        col = table.column(i)
        null_count = col.null_count
        if table.num_rows > 0 and null_count / table.num_rows > 0.5:
            high_nan.append((col_name, null_count / table.num_rows))
    if high_nan:
        warnings.append(f"{len(high_nan)} colonnes avec >50% NaN (echantillon RG0)")
        for cn, pct in high_nan[:5]:
            warnings.append(f"  {cn}: {pct:.1%} NaN")
    else:
        print(f"  [OK] Pas de colonne >50% NaN (RG0)")

    # 6. Target balance
    if "comblage__is_gagnant" in names:
        target = table.column("comblage__is_gagnant").to_pandas()
        pos_rate = target.astype(float).mean()
        if pos_rate < 0.05 or pos_rate > 0.15:
            errors.append(f"Target desequilibree: {pos_rate:.2%} positifs (attendu ~8.8%)")
        else:
            print(f"  [OK] Target balance: {pos_rate:.1%} positifs")

    # 7. Leakage check (correlation avec target)
    if "comblage__is_gagnant" in names:
        target_arr = table.column("comblage__is_gagnant").to_pandas().astype(float).values
        leakage_cols = []
        for col_name in names:
            if col_name in REQUIRED_COLS or col_name == "partant_uid":
                continue
            field = pf.schema_arrow.field(col_name)
            if not (pa.types.is_floating(field.type) or pa.types.is_integer(field.type)):
                continue
            col_arr = table.column(col_name).to_pandas().astype(float).values
            mask = ~(np.isnan(col_arr) | np.isnan(target_arr))
            if mask.sum() < 100:
                continue
            corr = abs(np.corrcoef(col_arr[mask], target_arr[mask])[0, 1])
            if corr > 0.5:
                leakage_cols.append((col_name, corr))
        if leakage_cols:
            errors.append(f"Leakage potentiel ({len(leakage_cols)} cols avec |corr|>0.5):")
            for cn, c in sorted(leakage_cols, key=lambda x: -x[1])[:5]:
                errors.append(f"  {cn}: corr={c:.4f}")
        else:
            print(f"  [OK] Pas de leakage detecte (|corr|<0.5)")

    del table

    # Resume
    print()
    if errors:
        print("ERREURS:")
        for e in errors:
            print(f"  {e}")
    if warnings:
        print("WARNINGS:")
        for w in warnings:
            print(f"  {w}")

    if errors:
        print(f"\nRESULTAT: ECHEC ({len(errors)} erreurs)")
        sys.exit(1)
    else:
        print(f"RESULTAT: OK - Dataset pret pour les modeles")
        sys.exit(0)


if __name__ == "__main__":
    main()
