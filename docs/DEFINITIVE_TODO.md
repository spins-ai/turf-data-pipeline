# Definitive TODO - Turf Data Pipeline

Last updated: 2026-04-09

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

### Scripts Ready (not yet run)
- [x] prepare_targets.py (running, ~50% done)
- [x] prepare_temporal_split.py
- [x] audit_data_drift.py
- [x] convert_master_to_parquet.py
- [x] consolidate_features_parquet.py
- [x] create_duckdb_index.py
- [x] generate_feature_catalog_md.py

---

## IN PROGRESS

### Target Variables
- [ ] prepare_targets.py en cours (lecture 26 GB master JSONL)
  - Output: D:/turf-data-pipeline/04_FEATURES/targets/targets.jsonl

---

## TODO (scripts prets, a lancer sequentiellement)

### Execution sequentielle (1 a la fois, verifier RAM avant chaque)
1. [ ] Temporal split (prepare_temporal_split.py) — ~10 min
2. [ ] Data drift audit (audit_data_drift.py) — ~30 min, lit master + builders
3. [ ] Parquet master conversion (convert_master_to_parquet.py) — ~20 min, besoin 5-10 GB RAM
4. [ ] Consolidation Parquet (consolidate_features_parquet.py) — ~60 min, besoin ~30 GB RAM
5. [ ] DuckDB index (create_duckdb_index.py) — ~5 min, besoin Parquet consolide

### Data improvements (optionnel, faible priorite)
6. [ ] Recuperer 3 colonnes manquantes (rap_dividend_moyen, rap_market_concentration, rap_nb_gagnants_simple)
   - Deja disponibles dans master original + rapport_payout builder
   - Necessite re-enrichissement complet (25 GB x2 = tres lourd)
7. [ ] Ameliorer fill rate poids_porte_kg
   - 100% pour galop, 0% pour trot attele (normal, pas de poids en trot attele)
   - Pas d'amelioration possible
8. [ ] Ameliorer fill rate temps_ms/reduction_km_ms (39%)
   - Disponible seulement pour trot (67%), 0% pour galop
   - Limitation structurelle des donnees PMU
9. [ ] Merger 83_letrot dans donnees trot
10. [ ] Consolider 5 variantes partants_master en 1 seul
11. [ ] Archiver donnees historiques pre-2015
12. [ ] Compression JSONL → garder seulement Parquet

### Infrastructure (optionnel)
13. [ ] Setup cron daily PMU API fetch
14. [ ] Git commit propre + documentation finale

---

## Stats pipeline actuelles
- **2,930,290 records** dans partants_master.jsonl (26.7 GB)
- **311 builders** avec sortie JSONL valide
- **3,994 features** au total
- **~80 GB** de builder outputs JSONL
- **683 features** a supprimer (correlation >0.95)
- **237 features** avec fill rate <10%
- **0 temporal leakage** detecte

## Prochaine etape critique
→ Consolidation Parquet (80 GB JSONL → ~5 GB Parquet)
→ Puis DuckDB index pour requetes instantanees
→ Puis ML/DL models (CatBoost, XGBoost, LightGBM)
