# Pedigree Data Documentation

## Overview

Pedigree data tracks horse lineage (sire, dam, dam-sire, breed, birth country/year) and is used to compute predictive features based on ancestral performance patterns. The pipeline merges four primary sources into a unified `pedigree_master.json` in `data_master/`.

## Data Sources

| Source | Script | Description | Volume |
|--------|--------|-------------|--------|
| SIRE/IFCE | `17_sire_ifce` | Official French breeding registry (SIRE database). Highest priority. | 1.1 GB |
| PedigreeQuery | `36_pedigree_query` | Online pedigree database, 4-generation deep. HTML-contaminated, needs cleaning. | 39 MB |
| AllBreedPedigree | `71_allbreedpedigree_scraper` | Global breed pedigree data (all breeds). | varies |
| France Galop | `80_france_galop_scraper` | Official French flat racing authority pedigree records. | varies |
| PMU partants | `08_pedigree` | Sire/dam stats from PMU race entries (win rates, descendants). | 20 MB |
| Pedigree scraper | `14_pedigree_scraper` | 4-generation pedigree scraper (~89K/250K horses scraped). | varies |
| PedigreeQuery Playwright | `115` (Playwright) | Cloudflare-resistant version of script 36. | varies |

### Source Priority (highest to lowest)

1. **SIRE/IFCE** (official government registry)
2. **partants_enrichis** (2.7M records with pere/mere/race/robe/sexe)
3. **PMU 08** (basic sire/dam data)
4. **Scraper 14** (4-gen deep pedigree)
5. **CanalTurf 24**
6. **PedigreeQuery 36**

## Merge Pipeline

The merge is handled by two scripts:

- **`merge_all_pedigree.py`** -- Merges sources 08, 12, 14, and 36 into `output/pedigree_complete/pedigree_complet.json`.
- **`merge_pedigree_master.py`** -- Final master merge incorporating all sources into `data_master/pedigree_master.json` (also `.parquet` and `.csv`). Join key is normalized horse name.

## Fields Available

### Core Lineage

| Field | Type | Description |
|-------|------|-------------|
| `pere` | string | Sire (father) name |
| `mere` | string | Dam (mother) name |
| `pere_mere` | string | Dam-sire (maternal grandfather) name |
| `race` | string | Breed (Pur-Sang, Trotteur Francais, AQPS, etc.) |
| `robe` | string | Coat color |
| `sexe` | string | Sex (M/F/H) |
| `eleveur` | string | Breeder name |

### Birth & Origin

| Field | Type | Description |
|-------|------|-------------|
| `pays_naissance` / `sire_pays_naissance` | string | Country of birth |
| `annee_naissance` / `sire_annee_naissance` | int | Year of birth |
| `date_naissance` / `sire_date_naissance` | string | Full date of birth |
| `pays_cheval` | string | Horse country (may differ from birth country) |

### Extended Pedigree (from script 36 / AllBreedPedigree)

| Field | Type | Description |
|-------|------|-------------|
| `lignee_male` | list | Patrilineal line (father's father's father...) |
| `dosage_profile` | string | Dosage Profile (DP) from pedigree analysis |
| `dosage_index` | float | Dosage Index (DI) |
| `center_of_distribution` | float | Center of Distribution (CD) |

### SIRE/IFCE Specific

| Field | Type | Description |
|-------|------|-------------|
| `sire_vivant` | bool | Whether the sire is alive |
| `sire_consommation` | string | Sire breeding status |
| `jument_pleine` | bool | Whether the dam is in foal |

## Coverage

- **Total partants with pedigree data**: sourced from 2.7M enriched race entries
- **Full 3-generation pedigree** (pere + mere + pere_mere): available for horses where all three fields are non-null after cross-source merge. Coverage of `pere_mere` was enriched from 44.8% to 57.4% via pedigree_master.
- **4-generation pedigree**: available for ~24K horses from scraper 14, plus records from PedigreeQuery.
- **Script 14 progress**: ~89K/250K horses scraped (~35%), still in progress.

## Pedigree Feature Engineering

Feature computation is handled by **`feat_pedigree.py`**, which produces approximately 40 features per runner.

### Sire Line Features (`ped_pere_*`)

Tracks historical performance of all descendants of a given sire seen so far in the dataset (computed incrementally, respecting temporal order):

- `ped_pere_nb_descendants_vus` -- number of descendants seen
- `ped_pere_taux_vic` -- sire's descendant win rate
- `ped_pere_taux_place` -- sire's descendant place rate (top 3)
- `ped_pere_gains_moy` -- average earnings of descendants
- `ped_pere_taux_vic_terrain_X` -- win rate on current going
- `ped_pere_taux_vic_dist_X` -- win rate at current distance
- `ped_pere_taux_vic_disc_X` -- win rate in current discipline
- `ped_pere_is_top` -- sire in top-20 sires

### Dam-Sire Features (`ped_pm_*`)

Same tracker logic applied to the maternal grandfather:

- `ped_pm_nb_descendants_vus`
- `ped_pm_taux_vic`

### Breed Features (`ped_race_*`)

- `ped_race_norm` -- normalized breed name (PURSANG, TROTTEUR_FR, AQPS, SELLE_FR, ANGLO_ARABE)
- `ped_race_taux_vic_dist` -- breed win rate at this distance
- `ped_race_taux_vic_terrain` -- breed win rate on this going

### Coat Features (`ped_robe_*`)

- `ped_robe_norm` -- normalized coat color
- `ped_robe_taux_vic` -- coat color win rate (some coats have statistically different rates)

### Age Features (`ped_age_*`)

Computed from `annee_naissance` and race date:

- `ped_age_exact` -- exact age in years (validated 1-25 range)
- `ped_age_category` -- jeune (<=3), prime (4-6), mature (7-10), veteran (>10)
- `ped_age_ideal_disc` -- whether age is ideal for the discipline:
  - Flat: 3-5 years
  - Hurdle/steeplechase: 4-8 years
  - Trot: 3-10 years

### Inbreeding Detection (`ped_inbreeding`)

- `ped_inbreeding` -- True if a common ancestor is detected in the pedigree tree (requires 3+ generation data)

### Discipline Match Features

Handled by **`feat_pedigree_discipline_match.py`** (10 additional features), which evaluates how well a horse's pedigree matches the current race discipline.

## Cross-Reference with Other Data

Pedigree data is joined to race entries via **`44_croisement_pedigree_partants.py`**, which matches horses by normalized name and enriches each `partant` record with pedigree fields before feature computation.
