# Pipeline Guide: Running turf-data-pipeline from A to Z

Complete guide to execute the horse racing data pipeline end-to-end.

---

## 1. Prerequisites

### Python Version

- Python 3.10+ (tested on 3.11 and 3.12)

### Install Dependencies

```bash
pip install -r requirements.txt
```

**Core packages** (from `requirements.txt`):

| Package        | Purpose                                     |
|----------------|---------------------------------------------|
| requests       | HTTP calls (PMU API, scrapers)              |
| beautifulsoup4 | HTML parsing (scrapers 12-39)               |
| lxml           | Fast XML/HTML parser                        |
| ijson          | Streaming JSON parsing (large files)        |
| pandas         | DataFrames for merge/analysis               |
| numpy          | Numerical operations                        |
| meteostat      | Weather data retrieval                      |
| scikit-learn   | ML utilities                                |
| scipy          | Statistical functions                       |
| xgboost        | Gradient boosting (future ML phase)         |
| lightgbm       | Gradient boosting (future ML phase)         |
| catboost       | Gradient boosting (future ML phase)         |
| pyarrow        | Parquet read/write                          |
| pyyaml         | YAML config parsing                         |

### Disk Space

Budget **~100 GB** total across all phases:

| Directory        | Estimated Size | Contents                        |
|------------------|----------------|---------------------------------|
| `output/`        | ~40 GB         | Raw scraper outputs (JSONL/JSON)|
| `data_master/`   | ~25 GB         | Merged master files             |
| `output/features/`| ~20 GB        | Feature matrix                  |
| `output/labels/` | ~2 GB          | Training labels                 |
| `logs/`          | ~500 MB        | Log files                       |
| Cache dirs       | ~10 GB         | Scraper HTTP caches             |

### RAM

- Most scripts stream data line-by-line and use under 2 GB RAM.
- `mega_merge_partants_master.py` loads several indexes in memory: allow **6-8 GB**.
- `master_feature_builder.py` uses a two-pass streaming approach: allow **4-6 GB**.
- **Rule**: never run more than 3 heavy scripts simultaneously.

---

## 2. Pipeline Overview

The pipeline has **9 phases** executed in order. The orchestrator `run_pipeline.py` manages everything, including parallel execution and crash recovery.

```
Phase 1: Audit           --> Validate existing raw data
Phase 2: Nettoyage       --> Clean & normalize
Phase 3: Deduplication   --> Remove duplicates across sources
Phase 4: Comblage        --> Fill missing fields from cross-sources
Phase 5: Merges          --> Merge sources into domain masters (parallel)
Phase 6: Mega merge      --> Fuse all masters into partants_master.jsonl
Phase 7: Features        --> Compute 400+ features (parallel)
Phase 8: Master features --> Consolidate into features_matrix.jsonl
Phase 9: Quality         --> Run all quality tests
```

---

## 3. Quick Start: Run Everything

```bash
# Full pipeline (uses checkpoint for crash recovery)
python run_pipeline.py

# See what will run without executing
python run_pipeline.py --dry-run

# List all steps
python run_pipeline.py --list

# Check current progress
python run_pipeline.py --status
```

---

## 4. Step-by-Step Execution

### Phase 0: Data Collection (Scrapers)

The numbered scripts `00_` through `40_` are **data collectors**. They scrape APIs and websites to populate the `output/` directory. They are **not** part of `run_pipeline.py` and must be run manually before the pipeline.

```bash
# Core PMU data (run these first)
python 00_enrichissement_meteo.py
python 01_calendrier_reunions.py
python 02_liste_courses.py              # courses + partants
python 04_resultats.py
python 05_historique_chevaux.py
python 06_historique_jockeys.py
python 07_cotes_marche.py
python 08_pedigree.py
python 09_equipements.py
python 10_poids_handicaps.py
python 11_sectionals.py

# Pedigree scrapers (multiple sources)
python 12_pedigree_scraper.py
python 14_pedigree_scraper.py
python 36_pedigree_query.py

# Weather
python 13_meteo_historique.py
python 35_meteo_france_api.py

# External datasets
python 15_download_external_datasets.py
python 16_collecte_nanaelie_2004_2013.py
python 17_process_sire.py
python 18_letrot_records.py
python 19_boturfers_stats.py
python 20_ifce_stats.py

# Rapports & pronostics
python 21_rapports_definitifs.py
python 22_performances_detaillees.py
python 23_pronostics_equidia.py

# External turf sites
python 24_canalturf_scraper.py
python 25_turfostats_scraper.py
python 26_geny_scraper.py
python 27_citations_enjeux.py
python 28_combinaisons_marche.py

# More external sources
python 29_arqana_ventes.py
python 30_smarkets_exchange.py
python 31_zone_turf.py
python 32_turfomania.py
python 33_turf_fr.py
python 34_unibet_cotes.py

# Racing Post
python 37_rpscrape_racing_post.py

# Internet rapports & enrichment
python 38_rapports_internet.py
python 39_reunions_enrichies.py
python 40_enrichissement_partants.py
```

**Expected outputs from scraping:**

| Script | Output Directory | Key File(s) | Est. Size |
|--------|-----------------|-------------|-----------|
| 00 | `output/00_meteo/` | meteo data | ~200 MB |
| 01 | `output/01_calendrier/` | reunions.jsonl | ~50 MB |
| 02 | `output/02_liste_courses/` | courses_normalisees.jsonl, partants_normalises.jsonl | ~5 GB |
| 04 | `output/04_resultats/` | resultats.jsonl | ~2 GB |
| 05 | `output/05_historique_chevaux/` | historique_chevaux.json | ~324 MB |
| 06 | `output/06_historique_jockeys/` | historique_jockeys.json | ~200 MB |
| 07 | `output/07_cotes_marche/` | cotes_marche.json | ~286 MB |
| 08 | `output/08_pedigree/` | pedigree_peres.json, pedigree_meres.json | ~100 MB |
| 09 | `output/09_equipements/` | equipements_historique.json | ~319 MB |
| 10 | `output/10_poids_handicaps/` | poids_handicaps.json | ~141 MB |
| 11 | `output/11_sectionals/` | sectionals.json | ~133 MB |
| 21 | `output/21_rapports_definitifs/` | rapports_definitifs.json | ~500 MB |
| 22 | `output/22_performances_detaillees/` | performances_detaillees.jsonl | ~12 GB |
| 24 | `output/24_canalturf/` | canalturf.jsonl | ~41 MB |
| 25 | `output/25_turfostats/` | turfostats.jsonl | ~27 MB |
| 26 | `output/26_geny/` | geny.jsonl | ~44 MB |
| 30 | `output/30_smarkets/` | smarkets.jsonl | ~640 KB |
| 37 | `output/37_racing_post/` | racing_post.jsonl | ~5.6 GB |
| 38 | `output/38_rapports_internet/` | rapports_internet.json | ~500 MB |
| 39 | `output/39_reunions_enrichies/` | reunions.jsonl | ~200 MB |
| 40 | `output/40_partants_enrichis/` | partants_enrichis.jsonl | ~655 MB |

**Estimated scraping time:** Days to weeks depending on date range and API rate limits. Each scraper has its own checkpoint system for resumption.

### Phase 1: Audit

```bash
python audit_data_integrity.py
```

**What it does:** Validates all raw data files (JSON/JSONL integrity, record counts, date ranges, outliers, fill rates).

**Output:**
- `output/audit/audit_report.md` -- human-readable report
- `output/audit/audit_stats.json` -- machine-readable stats

**Estimated time:** 5-15 minutes (reads all output files).

### Phase 2: Nettoyage (Cleaning)

```bash
python nettoyage_global.py
```

**What it does:** Fixes UTF-8 encoding, normalizes names (horses, jockeys, hippodromes), standardizes dates to ISO 8601, cleans nulls, trims whitespace.

**Output:**
- `output/nettoyage/partants_nettoyes.jsonl`
- `output/nettoyage/nettoyage_report.json`

**Estimated time:** 10-30 minutes (streaming, low RAM).

### Phase 3: Deduplication

```bash
python deduplication.py
```

**What it does:** Deduplicates courses (02 + 02b), partants, pedigrees (08+12+14+36), rapports (21+38). Keeps the most complete version for each duplicate.

**Output:**
- `output/dedup/` -- deduplicated files + report

**Estimated time:** 10-20 minutes.

### Phase 4: Comblage (Gap Filling)

```bash
python comblage_trous.py
```

**What it does:** Fills missing fields by cross-referencing other sources (e.g., penetrometre from meteo, pays_cheval from SIRE, type_piste from hippodromes_db).

**Output:**
- `output/comblage/partants_combles.jsonl`
- `output/comblage/comblage_report.json`

**Estimated time:** 15-30 minutes.

### Phase 5: Merges (Parallel)

These run in parallel (up to 4 workers by default):

```bash
# Domain-specific merges -- all run concurrently
python merge_02_02b_courses_master.py     # -> data_master/courses_master.jsonl
python merge_pedigree_master.py           # -> data_master/pedigree_master.jsonl
python merge_rapports_21_38.py            # -> output/rapports_merged/rapports_complets.json
python merge_rapports_master.py           # -> data_master/rapports_master.json (depends on merge_rapports_21_38)
python merge_meteo.py                     # -> output/meteo_complete/meteo_complete.json
python merge_meteo_master.py              # -> data_master/meteo_master.json (depends on merge_meteo)
python merge_equipements_master.py        # -> data_master/equipements_master.json
python merge_marche_master.py             # -> data_master/marche_master.json
python merge_performances_master.py       # -> data_master/performances_master.json
python merge_stats_externes_master.py     # -> data_master/stats_externes_master.json
```

**Output summary:**

| Script | Output File | Sources Merged |
|--------|------------|----------------|
| merge_02_02b_courses_master | `data_master/courses_master.jsonl` | PMU courses + Le Trot 2004-2013 |
| merge_pedigree_master | `data_master/pedigree_master.jsonl` | Scripts 08, 12, 14, 36 |
| merge_rapports_21_38 | `output/rapports_merged/rapports_complets.json` | Rapports 21 + 38 |
| merge_rapports_master | `data_master/rapports_master.json` | Merged rapports |
| merge_meteo | `output/meteo_complete/meteo_complete.json` | 6 meteo sources |
| merge_meteo_master | `data_master/meteo_master.json` | Consolidated meteo |
| merge_equipements_master | `data_master/equipements_master.json` | Scripts 09 + 10 |
| merge_marche_master | `data_master/marche_master.json` | Scripts 07, 28, 30, 40 |
| merge_performances_master | `data_master/performances_master.json` | Scripts 05, 22, 11 |
| merge_stats_externes_master | `data_master/stats_externes_master.json` | Scripts 24, 25, 26, 37 |

**Estimated time:** 20-60 minutes total (parallel).

### Phase 6: Mega Merge

```bash
python mega_merge_partants_master.py
```

**What it does:** Fuses ALL domain masters into a single file. Joins cleaned/deduped/gap-filled partants with every data source (historique, cotes, pedigree, equipements, meteo, performances, rapports, stats externes, enrichissement, etc.).

**Output:**
- `data_master/partants_master.jsonl` -- THE master file (~17 GB)

**Estimated time:** 30-90 minutes. Uses ~6-8 GB RAM for indexes.

### Phase 7: Features (Parallel)

All feature builders run in parallel after mega merge:

```bash
# Feature builders (feature_builders/*.py) -- 27 scripts
python feature_builders/cheval_features.py
python feature_builders/course_features.py
python feature_builders/jockey_features.py
python feature_builders/pedigree_features.py
python feature_builders/marche_features.py
python feature_builders/musique_features.py
python feature_builders/temps_features.py
python feature_builders/meteo_features.py
python feature_builders/equipement_features.py
python feature_builders/poids_features.py
python feature_builders/combo_features.py
python feature_builders/interaction_features.py
python feature_builders/profil_cheval_features.py
python feature_builders/class_change_features.py
python feature_builders/field_strength_builder.py
python feature_builders/pace_profile_builder.py
python feature_builders/track_bias_detector.py
python feature_builders/perf_detaillees_builder.py
python feature_builders/smarkets_builder.py
python feature_builders/racing_post_builder.py
python feature_builders/reunions_builder.py
python feature_builders/enrichissement_builder.py
python feature_builders/pedigree_advanced_builder.py
python feature_builders/canalturf_builder.py
python feature_builders/turfostats_builder.py
python feature_builders/geny_builder.py
python feature_builders/precomputed_partant_joiner.py
python feature_builders/precomputed_entity_joiner.py

# Standalone feature scripts (feat_*.py) -- 16 scripts
python feat_croisements.py
python feat_historique.py
python feat_interactions.py
python feat_jockey.py
python feat_pedigree.py
python feat_sequences.py
python feat_temporel.py
python feat_cheval_jockey_affinity.py
python feat_cheval_hippodrome_affinity.py
python feat_cheval_distance_affinity.py
python feat_cheval_terrain_affinity.py
python feat_jockey_entraineur_combo.py
python feat_entraineur_hippodrome.py
python feat_value_betting.py
python feat_meteo_terrain_interaction.py
python feat_pedigree_discipline_match.py
python feat_field_strength.py

# Calculation scripts (41-49) -- 8 scripts
python 41_sequences_performances.py
python 42_croisement_racing_post_pmu.py
python 43_croisement_meteo_courses.py
python 44_croisement_pedigree_partants.py
python 45_graphe_relations_gnn.py
python 46_track_bias_speed_class.py
python 48_parse_conditions_texte.py
python 49_ecart_cotes_internet_national.py
```

**Estimated time:** 1-3 hours total (parallel, 4 workers).

### Phase 8: Master Feature Builder

```bash
python master_feature_builder.py
```

**What it does:** Two-pass streaming over `partants_master.jsonl`. Pass 1 builds lightweight indexes (2-4 GB RAM). Pass 2 computes all 400+ features per record and writes immediately to output.

**Output:**
- `output/features/features_matrix.jsonl` -- the final feature matrix

**Estimated time:** 1-3 hours. Uses ~4-6 GB RAM.

### Phase 9: Quality Tests

```bash
python quality/run_all_tests.py
```

**What it does:** Runs 7 test suites: JSON integrity, zero-byte files, record counts, feature quality (NaN/Inf), date validity, value ranges, cross-source consistency.

**Output:**
- `quality/report.md` -- test results report

**Estimated time:** 10-30 minutes.

### Post-Pipeline: Generate Labels

```bash
python generate_labels.py
```

**What it does:** Generates training labels from `partants_master.jsonl` (is_winner, is_place, position, is_dnf, roi_final_odds, value_label).

**Output:**
- `output/labels/training_labels.jsonl`
- Optionally: `output/labels/training_labels.parquet` (with `--format parquet`)

**Estimated time:** 15-30 minutes (streaming).

---

## 5. Checkpoint System and Crash Recovery

### How Checkpoints Work

The orchestrator saves progress to `pipeline_checkpoint.json` after every step. It tracks:
- **completed**: list of successfully finished step names
- **failed**: dict of step names with error messages
- **timings**: duration in seconds per step
- **started_at** / **finished_at**: timestamps

### Resuming After a Crash

```bash
# Simply re-run -- completed steps are skipped automatically
python run_pipeline.py

# Resume from a specific step (marks all ancestors as done)
python run_pipeline.py --from mega_merge

# Re-run only one specific step
python run_pipeline.py --only master_features

# Check what is left to do
python run_pipeline.py --status
```

### Starting Over

```bash
# Delete checkpoint and re-run everything
python run_pipeline.py --restart
```

### Continue Despite Failures

```bash
# Don't stop on first failure -- run everything possible
python run_pipeline.py --no-stop-on-failure
```

### Individual Scraper Checkpoints

Each scraper (00-40) has its own checkpoint system, typically a JSON file in its output directory (e.g., `output/02_liste_courses/checkpoint.json`). To re-scrape a specific source, delete its checkpoint file and re-run.

---

## 6. Orchestrator CLI Reference

```
python run_pipeline.py [OPTIONS]

Options:
  --restart              Clear checkpoint and start from scratch
  --from STEP            Resume from a specific step (ancestors marked as done)
  --only STEP            Run a single step (ignores dependencies)
  --dry-run              Show execution plan without running anything
  --no-stop-on-failure   Continue even if a step fails
  --list                 List all pipeline steps and exit
  --status               Show checkpoint status and exit
  --workers N            Number of parallel workers (default: 4)
```

### Step Names

Use `python run_pipeline.py --list` to see all step names. Key ones:

| Step Name | Script | Phase |
|-----------|--------|-------|
| audit | audit_data_integrity.py | 1 |
| nettoyage | nettoyage_global.py | 2 |
| dedup | deduplication.py | 3 |
| comblage | comblage_trous.py | 4 |
| merge_courses_master | merge_02_02b_courses_master.py | 5 |
| merge_pedigree_master | merge_pedigree_master.py | 5 |
| merge_rapports_master | merge_rapports_master.py | 5 |
| merge_meteo_master | merge_meteo_master.py | 5 |
| merge_equipements_master | merge_equipements_master.py | 5 |
| merge_marche_master | merge_marche_master.py | 5 |
| merge_performances_master | merge_performances_master.py | 5 |
| merge_stats_externes_master | merge_stats_externes_master.py | 5 |
| mega_merge | mega_merge_partants_master.py | 6 |
| fb_cheval_features | feature_builders/cheval_features.py | 7 |
| master_features | master_feature_builder.py | 8 |
| quality | quality/run_all_tests.py | 9 |

---

## 7. Logging

All pipeline execution logs go to:
- **Console** (stdout): real-time progress
- **`logs/pipeline.log`**: full execution log with timestamps

Each individual script also writes its own log file under `logs/` (e.g., `logs/nettoyage_global.log`).

---

## 8. How to Add a New Data Source

### Step 1: Create the Scraper

Create a new numbered script (e.g., `50_new_source.py`) that:
- Saves output to `output/50_new_source/` as JSONL
- Uses a checkpoint for resumability
- Includes `partant_uid` or `course_uid` as join keys

### Step 2: Create a Merge (if needed)

If the data fits into an existing domain (e.g., market data), add it to the relevant merge script (e.g., `merge_marche_master.py`). Otherwise, create a new `merge_new_source_master.py`.

### Step 3: Register in the DAG

Edit `run_pipeline.py`, inside `build_dag()`:

```python
# In Phase 5 (Merges), add:
add("merge_new_source", "merge_new_source_master.py", depends_on=merge_deps, phase=5)

# Add to the all_merges list for mega_merge dependency:
all_merges = [
    ...,
    "merge_new_source",
]
```

### Step 4: Add to Mega Merge

Edit `mega_merge_partants_master.py` to load and join the new master file using the appropriate join key (`partant_uid`, `course_uid`, or `(date, hippodrome)`).

### Step 5: Create a Feature Builder (optional)

Create `feature_builders/new_source_builder.py` and register it in `build_dag()` under Phase 7:

```python
feature_builder_scripts = [
    ...,
    "feature_builders/new_source_builder.py",
]
```

### Step 6: Update Master Feature Builder

Add the new features to `master_feature_builder.py` so they appear in the final feature matrix.

---

## 9. Estimated Total Pipeline Time

| Phase | Duration (approx.) |
|-------|-------------------|
| Scrapers (Phase 0) | Days to weeks (first run) |
| Phase 1: Audit | 5-15 min |
| Phase 2: Nettoyage | 10-30 min |
| Phase 3: Deduplication | 10-20 min |
| Phase 4: Comblage | 15-30 min |
| Phase 5: Merges | 20-60 min |
| Phase 6: Mega Merge | 30-90 min |
| Phase 7: Features | 1-3 hours |
| Phase 8: Master Features | 1-3 hours |
| Phase 9: Quality | 10-30 min |
| **Total (Phases 1-9)** | **~3-8 hours** |

Times depend on hardware, data volume (2004-2026 covers ~800k courses), and disk speed (SSD strongly recommended).

---

## 10. Troubleshooting

### Out of Memory

- Reduce `--workers` to 1 or 2
- Most scripts stream data; if one doesn't, check for `.json` files that load entirely into memory
- Never run more than 3 heavy scripts concurrently

### Script Not Found

- Run from the project root directory
- Ensure `PYTHON_EXE` environment variable is not set to a wrong interpreter

### Checkpoint Corruption

```bash
# Delete and restart
del pipeline_checkpoint.json   # Windows
rm pipeline_checkpoint.json    # Unix
python run_pipeline.py
```

### Partial Scraper Data

- Each scraper has its own checkpoint; re-run the scraper to complete missing data
- Use `--export` flag on scrapers 24, 25, 26 to regenerate JSONL from cache
