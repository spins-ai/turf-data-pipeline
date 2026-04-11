# Definitive TODO - Turf Data Pipeline

Last updated: 2026-04-11

---

## COMPLETED

### Data Quality & Audits
- [x] Fix 7 OOM builders (streaming refactoring)
- [x] Create 14 new feature builders (all 2,930,290 rows)
- [x] Builder output integrity check (311 valid builders)
- [x] Fill rate audit (3,994 features, CSV at 04_FEATURES/fill_rate_audit.csv)
- [x] Feature catalog auto-generated (JSON + MD)
- [x] Temporal leakage detection (0 real leaks found)
- [x] Feature deduplication (504 exact + 263 near-duplicates identified)
- [x] High correlation audit (683 features to drop, r>0.95)
- [x] Schema consistency audit (1,339 missing keys, 56 mixed types, 54 empty strings)
- [x] Outlier capping thresholds (1,090 features flagged, CSV ready)
- [x] Type casting audit (all issues documented)
- [x] Quality tests created (fill rate regression, completeness, ordering)
- [x] Clean empty builder_outputs directories (3 removed)
- [x] Collection scripts verified (syntax/imports OK)
- [x] Missing data imputation strategy documented

### Documentation
- [x] DATA_SOURCES_GUIDE.md
- [x] VERSION_MANIFEST.json
- [x] FEATURE_CATALOG.md (auto-generated, 3,994 features)
- [x] IMPUTATION_STRATEGY.md
- [x] TEMPORAL_LEAKAGE_AUDIT.md
- [x] DEDUP_AUDIT.md
- [x] Pipeline reproductible (run_full_pipeline.sh)

### Scripts & Pipeline Execution
- [x] prepare_targets.py — targets.jsonl generated
- [x] prepare_temporal_split.py — temporal split done
- [x] audit_data_drift.py — no critical drift detected
- [x] convert_master_to_parquet.py — master converted to Parquet
- [x] consolidate_features_parquet.py — 80 GB JSONL → ~5 GB Parquet (503 cols final)
- [x] create_duckdb_index.py — DuckDB index operational
- [x] generate_feature_catalog_md.py — catalog regenerated
- [x] Leakage fix applied — 503 cols after removing leaky features
- [x] Feature selection — 449 features selected for ML

### Codebase Cleanup (2026-04-11)
- [x] Delete obsolete DuckDB scripts (consolidate_features_duckdb.py, consolidate_features.py, convert_to_duckdb.py)
- [x] Delete duplicate _v2 builders (8 files — non-v2 versions kept)
- [x] Delete one-shot wave/launch shell scripts (10 files)

---

## TODO (optionnel, faible priorite)

### Data improvements
1. [ ] Recuperer 3 colonnes manquantes (rap_dividend_moyen, rap_market_concentration, rap_nb_gagnants_simple)
   - Deja disponibles dans master original + rapport_payout builder
   - Necessite re-enrichissement complet (25 GB x2 = tres lourd)
2. [ ] Ameliorer fill rate poids_porte_kg
   - 100% pour galop, 0% pour trot attele (normal, pas de poids en trot attele)
   - Pas d'amelioration possible
3. [ ] Ameliorer fill rate temps_ms/reduction_km_ms (39%)
   - Disponible seulement pour trot (67%), 0% pour galop
   - Limitation structurelle des donnees PMU
4. [ ] Merger 83_letrot dans donnees trot
5. [ ] Consolider 5 variantes partants_master en 1 seul
6. [ ] Archiver donnees historiques pre-2015
7. [ ] Compression JSONL → garder seulement Parquet

### Infrastructure (optionnel)
8. [ ] Setup cron daily PMU API fetch

---

## Stats pipeline finales
- **2,930,290 records** dans partants_master.jsonl (26.7 GB)
- **311 builders** avec sortie JSONL valide
- **503 colonnes** dans le Parquet consolide final (apres leakage fix)
- **449 features** selectionnees pour ML (apres dedup + correlation + fill rate)
- **~5 GB** Parquet consolide (vs ~80 GB JSONL builders)
- **0 temporal leakage** detecte et corrige

## Prochaine etape
→ Pipeline data COMPLET — pret pour ML/DL
→ CatBoost, XGBoost, LightGBM (nouveau dossier MODELES/)
→ Stacking ensemble + meta selector
