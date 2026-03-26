# Project Status - Turf Data Pipeline

Last updated: 2026-03-26

---

## 1. Repository Overview

| Metric | Value |
|--------|-------|
| Total commits | 341 |
| Current branch | `claude/naughty-bardeen` |
| Main branch | `main` |
| Tags | `data-v1.0-ready`, `data-v2.0-enriched` |
| Uncommitted changes | 2 modified, 2 untracked |
| Total files | 2,324 |
| Python files | 752 |
| Lines of Python | 205,013 |
| Markdown docs | 45 |
| Doc files in docs/ | 37 |
| Quality test files | 7 + 22 quality modules |
| Total data on disk | 721.1 GB |

## 2. Codebase Breakdown

### Feature Builders: 98 total

| Category | Count |
|----------|-------|
| Builders WITH output (JSONL > 1 KB) | 63 |
| Builders without standalone output (orchestrators/joiners) | 7 |
| Builders NOT YET RUN (no output anywhere) | 28 |

### Scripts: ~160 Python scripts

| Category | Count |
|----------|-------|
| Collection/scraping scripts | 60 |
| Merge scripts | 20 |
| Calculation scripts (41-60) | 8 |
| Utility scripts | 15 |
| Pipeline/audit scripts | 10 |
| Other (feature selection, monitoring, etc.) | ~47 |

### Pipeline Phases: 16 phases, 90 modules

| Phase | Name | Modules | Status |
|-------|------|---------|--------|
| 01 | Infrastructure | 10 | DONE - data ingested and cleaned |
| 02 | Feature Engineering | 16 | PARTIAL - 63/98 builders run |
| 03 | Feature Selection | 3 | NOT STARTED - needs full matrix |
| 03b | Preprocessing | 2 | DONE |
| 04 | ML Core | 6 | NOT STARTED - skeleton only |
| 05 | Deep Learning | 6 | NOT STARTED - skeleton only |
| 06 | Advanced Models | 5 | NOT STARTED - skeleton only |
| 07 | AutoML | 4 | NOT STARTED - skeleton only |
| 08 | Fusion/Ensemble | 4 | NOT STARTED - skeleton only |
| 09 | Calibration | 4 | NOT STARTED - skeleton only |
| 10 | Outsider Detection | 4 | NOT STARTED - skeleton only |
| 11 | Betting Strategy | 5 | NOT STARTED - skeleton only |
| 12 | Simulation | 3 | NOT STARTED - skeleton only |
| 13 | Bet Sizing | 6 | NOT STARTED - skeleton only |
| 14 | Adaptation | 4 | NOT STARTED - skeleton only |
| 15 | Monitoring | 4 | NOT STARTED - skeleton only |
| 16 | Orchestration | 6 | NOT STARTED - skeleton only |

## 3. Data Assets

### Primary Data (data_master/)

| File | Records | Size | Columns | Date Range |
|------|---------|------|---------|------------|
| partants_master.jsonl | 2,930,290 | 24.8 GB | 181 | 2013-02-19 to 2026-03-08 |
| partants_master_enrichi.jsonl | 2,930,290 | 24.4 GB | 178 | same |
| partants_master_crossref.jsonl | 2,930,290 | 24.8 GB | 181 | same |
| partants_master_enrichi_sl.jsonl | 2,930,290 | 16.6 GB | 140 | same |
| partants_master_enrichi_tf.jsonl | 2,930,290 | 16.5 GB | 140 | same |
| courses_master.jsonl | - | 375.4 MB | - | same |
| equipements_master.jsonl | - | 215.4 MB | - | - |
| horse_career_stats.jsonl | - | 226.4 MB | - | - |
| jockey_stats.jsonl | - | 21.5 MB | - | - |
| trainer_stats.jsonl | - | 22.1 MB | - | - |
| course_profiles.jsonl | - | 0.7 MB | - | - |
| **data_master/ total** | | **114.3 GB** | | |

### Features & Labels (output/)

| File | Size | Columns |
|------|------|---------|
| features_matrix.jsonl | 44.6 GB | 383 |
| features_matrix_clean.jsonl | 41.0 GB | 340 |
| features_matrix_improved.jsonl | 36.7 GB | - |
| training_labels.jsonl | 940.5 MB | - |
| advanced_labels.jsonl | 1,008.6 MB | - |
| supplementary_labels.jsonl | 927.0 MB | - |
| **output/ total** | **606.8 GB** | |

### CSV Masters (data_master/)

| File | Lines | Size |
|------|-------|------|
| courses_master.csv | 257,807 | 225 MB |
| equipements_master.csv | 573,112 | 57 MB |
| marche_master.csv | 186,631 | 26 MB |
| meteo_master.csv | 257,807 | 39 MB |
| pedigree_master.csv | 1,413,914 | 186 MB |
| rapports_master.csv | 221,526 | 4.2 GB |

### Parquet Exports (data_master/)

12 Parquet files exist for secondary tables. Total ~170 MB.
partants_master is NOT exported to Parquet (too large for single file).

### Indexes (data_master/indexes/)

| Index | Size |
|-------|------|
| course_index.json | 58 MB |
| horse_index.json | 166 MB |
| jockey_index.json | 2.6 MB |
| hippodrome_index.json | 72 KB |

## 4. Feature Engineering Status

### FEATURE_CATALOG.md Summary

- **Documented features:** 640+ across 90 builder categories
- **Currently in features_matrix:** 383 columns (340 after cleaning)
- **Gap:** ~257 columns from 28 unrun builders

### Feature Groups in features_matrix (383 cols)

| Prefix | Columns | Description |
|--------|---------|-------------|
| rap | 44 | Rapport/payout features |
| aff | 29 | Affinity features |
| pgr | 23 | Pedigree registry |
| musique | 23 | Music/form string |
| ped | 21 | Pedigree features |
| profil | 21 | Horse profile |
| ent | 17 | Trainer features |
| poids | 16 | Weight features |
| temps | 15 | Time features |
| equip | 15 | Equipment features |
| pc | 14 | Pre-computed features |
| combo | 12 | Combination features |
| is | 9 | Binary flags |
| spd | 8 | Speed/class features |
| cnd | 8 | Condition features |
| vb | 7 | Value base features |
| mch | 7 | Market features |
| seq | 6 | Sequence features |
| gnn | 5 | Graph neural features |
| Other | ~73 | Various single/small groups |

### Builders NOT yet run (28)

fatigue_features, recovery_features, momentum_builder, career_stats_builder,
first_time_events_builder, jockey_form_builder, trainer_form_builder,
jockey_horse_affinity_builder, trainer_horse_compatibility_builder,
hippodrome_expertise_builder, course_context_builder, going_preference_builder,
distance_preference_builder, pace_profile_builder, track_bias_detector,
odds_movement_features, closing_line_value_builder, market_entropy_features,
market_divergence_builder, market_inefficiency_builder, temporal_advanced_features,
temporal_context_features, pedigree_distance_aptitude, pedigree_advanced_builder,
derived_features_builder, pattern_discovery_builder, advanced_encoding_builder,
perf_detaillees_builder

## 5. Data Quality

### Compilation: ALL 754 Python files compile successfully

### Dead Imports: 44 files with unused imports

Mostly `HTTPAdapter`/`Retry` in collection scripts (harmless), plus `math`, `json`, `os` in a few feature builders.

### Syntax Errors: ZERO

### Key Field Fill Rates (sampled 2,931 records)

| Field | Fill Rate | Notes |
|-------|-----------|-------|
| partant_uid | 100.0% | |
| course_uid | 100.0% | |
| date_reunion_iso | 100.0% | |
| hippodrome_normalise | 100.0% | |
| nom_cheval | 100.0% | |
| jockey_driver | 100.0% | |
| entraineur | 100.0% | |
| distance | 100.0% | |
| discipline | 100.0% | |
| nombre_partants | 100.0% | |
| musique | 95.9% | |
| pere | 95.2% | |
| mere | 95.2% | |
| nb_courses_carriere | 81.5% | |
| position_arrivee | 80.9% | Scratched horses have no position |
| gains_carriere_euros | 80.0% | |
| cote_finale | 70.7% | Missing for some historical races |
| poids_porte_kg | 44.4% | Galop only, trot has no weight |
| is_place | 26.7% | Binary field - correct |
| is_gagnant | 8.6% | Binary field - correct (~1 winner per race) |

### Lowest Fill Rate Fields (0%)

| Field | Explanation |
|-------|-------------|
| cnd_cond_is_quinte | Boolean, 0 counts as empty |
| cnd_cond_is_tierce | Boolean, 0 counts as empty |
| met_is_psf | Boolean, 0 counts as empty |
| ped_inbreeding_count | Only available for subset with pedigree data |
| jument_pleine | Rare condition (pregnant mare) |
| rap_ri_* fields | Internet rapport detail fields, sparse |

## 6. Data Collection Sources

### Fully collected (significant data)

| Source | Output Size | Records |
|--------|-------------|---------|
| PMU API (101) | 1,930 MB | Multiple files |
| Rapports definitifs (21) | 2,913 MB | - |
| Performances detaillees (22) | 4,607 MB | - |
| Citations enjeux (27) | 5,288 MB | - |
| Combinaisons marche (28) | 1,273 MB | - |
| Racing Post FR (37) | 1,272 MB | - |
| Rapports internet (38) | 678 MB | - |
| Reunions enrichies (39) | 1,995 MB | - |
| Sequences (41) | 2,479 MB | - |
| Graphe GNN (45) | 1,824 MB | - |
| Track bias/speed (46) | 1,274 MB | - |
| Letrot (02b, 83) | 658 MB | - |
| Zeturf (51) | 96 MB | - |
| Turfinfo (54) | 164 MB | - |
| Pronostics (23) | 165 MB | - |
| Conditions (48) | 121 MB | - |

### Partially collected (small data)

| Source | Output Size | Notes |
|--------|-------------|-------|
| Geny (26) | 28 MB | Flat file generated |
| TurfoStats (25) | 12 MB | Flat file generated |
| CanalTurf (24) | 4 MB | |
| Paris Turf (53) | 29 MB | |
| Equidia (55) | 9 MB | |
| Timeform (56) | 10 MB | |
| Sporting Life (57) | 5 MB | |
| Turfomania (52) | 2 MB | |
| Smarkets (30) | 0.3 MB | |

### Blocked/minimal

Racing Post (102), ProForm (106), Oddschecker (60), JRA (67), Keeneland (75)

## 7. Overall Progress Summary

| Phase | Status | Completion |
|-------|--------|------------|
| Data Collection | DONE | 95% |
| Data Cleaning/Merge | DONE | 95% |
| Feature Engineering | IN PROGRESS | 65% (63/98 builders, 383/640 features) |
| Feature Selection | NOT STARTED | 0% |
| ML Modeling | NOT STARTED | 0% (skeletons ready) |
| Deep Learning | NOT STARTED | 0% |
| Ensemble/Fusion | NOT STARTED | 0% |
| Betting Strategy | NOT STARTED | 0% |
| Monitoring | NOT STARTED | 0% |
| **Overall Pipeline** | **Phase 2 in progress** | **~35%** |

## 8. Key Numbers

- **13 years** of historical data (2013-2026)
- **2,930,290** race entries (partants)
- **~257,807** race meetings
- **181** columns in master dataset
- **383** features currently computed (target: 640+)
- **98** feature builders (63 run, 7 orchestrators, 28 pending)
- **721 GB** total data on disk
- **752** Python files, **205,013** lines of code
- **0** syntax errors, **0** compilation errors
- **44** dead imports (cosmetic)
