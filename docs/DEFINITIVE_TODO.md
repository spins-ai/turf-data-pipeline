# Definitive TODO - Turf Data Pipeline

Last updated: 2026-03-26

---

## Section 1: IMMEDIATE (can do right now, no blockers)

### 1.1 Run the 28 remaining feature builders

These builders exist in `feature_builders/` but have NOT been run yet.
They are listed in FEATURE_CATALOG.md (21 of them) but have zero output.

**Builders to run (estimated 2-4 hours each on full dataset):**

```bash
PYTHON="/c/Users/celia/AppData/Local/Programs/Python/Python312/python.exe"

# Group A: Horse/form features (~30 min each)
$PYTHON -m feature_builders.fatigue_features
$PYTHON -m feature_builders.recovery_features
$PYTHON -m feature_builders.momentum_builder
$PYTHON -m feature_builders.career_stats_builder
$PYTHON -m feature_builders.first_time_events_builder

# Group B: Jockey/trainer features (~30 min each)
$PYTHON -m feature_builders.jockey_form_builder
$PYTHON -m feature_builders.trainer_form_builder
$PYTHON -m feature_builders.jockey_horse_affinity_builder
$PYTHON -m feature_builders.trainer_horse_compatibility_builder
$PYTHON -m feature_builders.hippodrome_expertise_builder

# Group C: Track/course features (~30 min each)
$PYTHON -m feature_builders.course_context_builder
$PYTHON -m feature_builders.going_preference_builder
$PYTHON -m feature_builders.distance_preference_builder
$PYTHON -m feature_builders.pace_profile_builder
$PYTHON -m feature_builders.track_bias_detector

# Group D: Market/odds features (~30 min each)
$PYTHON -m feature_builders.odds_movement_features
$PYTHON -m feature_builders.closing_line_value_builder
$PYTHON -m feature_builders.market_entropy_features
$PYTHON -m feature_builders.market_divergence_builder
$PYTHON -m feature_builders.market_inefficiency_builder

# Group E: Advanced/derived features (~1 hour each)
$PYTHON -m feature_builders.temporal_advanced_features
$PYTHON -m feature_builders.temporal_context_features
$PYTHON -m feature_builders.pedigree_distance_aptitude
$PYTHON -m feature_builders.pedigree_advanced_builder
$PYTHON -m feature_builders.derived_features_builder
$PYTHON -m feature_builders.pattern_discovery_builder
$PYTHON -m feature_builders.advanced_encoding_builder
$PYTHON -m feature_builders.perf_detaillees_builder
```

**Expected result:** Each produces a JSONL file in `output/<builder_name>/` with ~2.9M records.
**Time estimate:** 8-12 hours total (can be parallelized).

### 1.2 Rebuild features_matrix with all builders

After running the 28 missing builders:

```bash
$PYTHON -m feature_builders.master_feature_builder
```

**Expected result:** `output/features/features_matrix.jsonl` grows from 383 columns to ~640+ columns.
**Time estimate:** 2-4 hours.

### 1.3 Copy features_matrix and training_labels to data_master/

These critical files exist in `output/` but NOT in `data_master/`:

```bash
# After rebuild
cp output/features/features_matrix_clean.jsonl data_master/features_matrix.jsonl
cp output/labels/training_labels.jsonl data_master/training_labels.jsonl
```

**Expected result:** `data_master/features_matrix.jsonl` and `data_master/training_labels.jsonl` exist.
**Time estimate:** 5 minutes.

### 1.4 Clean up 44 dead imports

44 Python files have unused imports. Not critical but should be cleaned for code quality.

```bash
$PYTHON -c "
# Files with dead imports (non-exhaustive, see quality audit):
# feature_builders/betting_edge_features_builder.py: math
# feature_builders/field_strength_builder.py: json
# feature_builders/market_divergence_builder.py: os
# feature_builders/pattern_discovery_builder.py: math
# feature_builders/seasonality_builder.py: struct, tempfile
# ... and 39 more in scripts/collection/*.py
"
```

**Time estimate:** 30 minutes.

### 1.5 Refresh Parquet exports

Parquet exports in `data_master/` exist for secondary tables but NOT for partants_master (too large for single Parquet without chunking).

```bash
$PYTHON scripts/refresh_parquet_exports.py
```

**Expected result:** All 12 Parquet files in `data_master/` updated.
**Time estimate:** 10 minutes.

### 1.6 Fix fill rate issues on key fields

Fields with low fill rates that could be improved:
- `poids_porte_kg`: 44.4% (only for galop, expected)
- `cote_finale`: 70.7% (could be improved by cross-referencing rapports)
- `is_gagnant`: 8.6% (note: this is correct for boolean - only winners are True, but 0/False records show as 0% fill because of the sampling method)

No action needed on `is_gagnant` - it is correct (binary field).

---

## Section 2: WAITING (needs a process to complete first)

### 2.1 PMU enrichment completion

**Status:** The `partants_master_enrichi.jsonl` (24.4 GB, 2,930,290 records) exists but only has 178 columns vs the 181 in `partants_master.jsonl`. The enrichment dropped 3 columns.

**Waiting on:** Verify which columns were dropped and whether the enrichment script `scripts/collection/40_enrichissement_partants.py` needs re-running with corrections.

### 2.2 Cross-reference with external sources

`partants_master_crossref.jsonl` has same record count (2,930,290) and 181 columns. Cross-referencing with Sporting Life / Timeform data is partially done:
- `partants_master_enrichi_sl.jsonl`: 140 cols (Sporting Life enriched - FEWER columns, likely a slim version)
- `partants_master_enrichi_tf.jsonl`: 140 cols (Timeform enriched - FEWER columns)

**Waiting on:** Decision on whether slim versions are intentional or if full cross-reference merge is needed.

### 2.3 Features matrix column expansion

The current `output/features/features_matrix.jsonl` has 383 columns. FEATURE_CATALOG.md promises 640+. The gap of ~257 columns will be filled by running the 28 missing builders (Section 1.1) then rebuilding (Section 1.2).

**Waiting on:** Completion of Section 1.1 and 1.2.

---

## Section 3: BLOCKED (paid APIs, external resources)

### 3.1 Racing Post full data

`output/102_racing_post/racing_post_data.jsonl` exists but is tiny (0.0 MB).
The builder `racing_post_builder.py` and scraper `scripts/collection/37_racing_post.py` exist.

**Blocked by:** Racing Post requires paid API access or scraping is rate-limited/blocked.

### 3.2 Smarkets exchange data

`output/30_smarkets_exchange/smarkets_exchange.jsonl` is only 0.3 MB.

**Blocked by:** Smarkets API requires authentication; limited French racing coverage.

### 3.3 ProForm Racing

`output/106_proform_racing/proform_data.jsonl` exists but is tiny.

**Blocked by:** ProForm requires paid subscription.

### 3.4 International scrapers (low priority)

Several international scrapers have minimal data:
- HKJC (0.6 MB), JRA (0.0 MB), Keeneland (0.0 MB), Singapore Pools (0.1 MB)

**Blocked by:** These are non-French sources, useful only for international horses.

### 3.5 Meteo France API

`scripts/collection/35_meteo_france_api.py` exists but real-time weather data requires API key.

**Blocked by:** Meteo France API subscription. NASA/OpenMeteo fallback is already working.

---

## Section 4: ML/MODELS (next project phase)

### 4.1 Feature selection (Phase 3)

```
pipeline/phase_03_feature_selection/
  19_selection_auto_features/
  20_feature_subset_optimizer/
```

**Prerequisite:** Complete features_matrix with 640+ columns.
**Task:** Run automated feature selection to reduce from 640+ to optimal subset.

### 4.2 ML core models (Phase 4)

```
pipeline/phase_04_ml_core/
  21_logistic_regression/
  22_random_forest/
  23_xgboost/
  24_lightgbm/
  25_catboost/
```

All module skeletons exist. Data placeholders (1 KB) exist. Need real data piped in.

### 4.3 Deep learning models (Phase 5)

```
pipeline/phase_05_deep_learning/
  26_mlp/
  27_lstm/
  28_gru/
  29_tabnet/
  30_tft/
```

**Prerequisite:** Completed feature matrix + training labels + GPU resources.

### 4.4 Advanced models (Phase 6)

GNN, Bayesian NN, Survival model, Quantile regressor.

### 4.5 AutoML (Phase 7)

AutoGluon, TPOT, H2O. Requires large memory / compute.

### 4.6 Ensemble / Fusion (Phase 8)

Stacking, Blending, Meta-model. Requires trained base models.

### 4.7 Calibration (Phase 9)

Platt scaling, Isotonic calibration. Requires trained models.

### 4.8 Outsider detection (Phase 10)

Anomaly detector, Retour de forme, GAN-Turf.

### 4.9 Betting strategy (Phase 11)

ROI predictor, Value Hunter RL, Meta-selector, ZURI Outsider Engine.

### 4.10 Simulation & Bet Sizing (Phase 12-13)

Monte Carlo, Race simulation, Kelly strategy, Ticket optimizer.

### 4.11 Adaptation & Monitoring (Phase 14-15)

Auto-recalibration, Model decay detection, Concept drift, Dashboard.

---

## Section 5: NICE TO HAVE (improvements, not critical)

### 5.1 Remove duplicate builder output mappings

Some builders map to the same output file (e.g., `combo_features.py` and `combo_triple_builder.py` both map to `combo_triple_features/`). Clarify ownership.

### 5.2 DuckDB conversion

`scripts/convert_to_duckdb.py` exists. Converting the 25 GB partants_master to DuckDB would dramatically speed up queries.

```bash
$PYTHON scripts/convert_to_duckdb.py
```

### 5.3 Consolidate enriched master variants

Currently 5 variants of partants_master exist (110+ GB total):
- `partants_master.jsonl` (24.8 GB)
- `partants_master_crossref.jsonl` (24.8 GB)
- `partants_master_enrichi.jsonl` (24.4 GB)
- `partants_master_enrichi_sl.jsonl` (16.6 GB)
- `partants_master_enrichi_tf.jsonl` (16.5 GB)

Consider keeping only the most complete version and archiving the rest.

### 5.4 Quality test suite expansion

7 quality tests exist in `quality/`. Consider adding:
- Fill rate regression tests
- Feature distribution drift tests
- Builder output consistency tests

### 5.5 Documentation refresh

37 doc files exist. Some may be outdated:
- `docs/FILL_RATES.md` (modified, uncommitted)
- `docs/COVERAGE_REPORT.md`
- `docs/STATS.md`

### 5.6 Letrot merge script

`scripts/merge_letrot_to_master.py` exists as untracked file. Needs review and commit.

### 5.7 Pipeline orchestration

`pipeline/phase_16_orchestration/` has 6 modules. Not yet wired to run the full pipeline end-to-end.

### 5.8 CI/CD improvements

`scripts/ci_check.py` exists. Could be integrated into GitHub Actions for automated quality checks.
