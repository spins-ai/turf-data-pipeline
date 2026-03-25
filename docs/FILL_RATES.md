# Fill Rate Dashboard - partants_master.jsonl

**Generated**: 2026-03-25 21:33
**Total records**: 2,930,290
**Sample size**: 5,000

## 1. Key ML Fields (30 most important)

| # | Field | Fill Rate | Status |
|---|-------|-----------|--------|
| 1 | `is_gagnant` | [##########] 100.0% | OK |
| 2 | `nombre_partants` | [##########] 100.0% | OK |
| 3 | `distance` | [##########] 100.0% | OK |
| 4 | `discipline` | [##########] 100.0% | OK |
| 5 | `hippodrome_normalise` | [##########] 100.0% | OK |
| 6 | `nom_cheval` | [##########] 100.0% | OK |
| 7 | `jockey_driver` | [##########] 100.0% | OK |
| 8 | `entraineur` | [##########] 100.0% | OK |
| 9 | `age` | [##########] 100.0% | OK |
| 10 | `sexe` | [##########] 100.0% | OK |
| 11 | `race` | [#########.] 99.8% | OK |
| 12 | `musique` | [#########.] 96.3% | OK |
| 13 | `pere` | [#########.] 95.4% | OK |
| 14 | `mere` | [#########.] 95.3% | OK |
| 15 | `nb_courses_carriere` | [#########.] 91.5% | OK |
| 16 | `nb_victoires_carriere` | [#########.] 91.5% | OK |
| 17 | `gains_carriere_euros` | [########..] 87.4% | MEDIUM |
| 18 | `oeilleres` | [########..] 85.8% | MEDIUM |
| 19 | `position_arrivee` | [########..] 82.3% | MEDIUM |
| 20 | `robe` | [#######...] 76.9% | MEDIUM |
| 21 | `proba_implicite` | [#######...] 71.0% | MEDIUM |
| 22 | `cote_finale` | [#######...] 70.1% | MEDIUM |
| 23 | `cote_reference` | [######....] 62.1% | MEDIUM |
| 24 | `poids_porte_kg` | [####......] 46.4% | LOW |
| 25 | `temps_ms` | [###.......] 39.1% | LOW |
| 26 | `reduction_km_ms` | [###.......] 39.1% | LOW |
| 27 | `deferre` | [##........] 28.8% | LOW |
| 28 | `handicap_valeur` | [##........] 21.4% | LOW |
| 29 | `incident` | [#.........] 15.3% | LOW |
| 30 | `avis_entraineur` | [..........] 8.7% | LOW |

**Summary**: 16 fields >= 90%, 7 fields 50-89%, 7 fields < 50%

### Fill Rates by Era (pre-2020 vs 2020+)

PMU enriched data covers 2020-2021, PMU participants covers 2020-2026.
Pre-2020 records have no PMU API source available.

- **Pre-2020**: 2,533 records in sample
- **2020+**: 2,467 records in sample

| Field | Pre-2020 | 2020+ | Delta |
|-------|----------|-------|-------|
| `is_gagnant` | 100.0% | 100.0% | 0.0% |
| `nombre_partants` | 100.0% | 100.0% | 0.0% |
| `distance` | 100.0% | 100.0% | 0.0% |
| `discipline` | 100.0% | 100.0% | 0.0% |
| `hippodrome_normalise` | 100.0% | 100.0% | 0.0% |
| `nom_cheval` | 100.0% | 100.0% | 0.0% |
| `jockey_driver` | 100.0% | 100.0% | 0.0% |
| `entraineur` | 100.0% | 100.0% | 0.0% |
| `age` | 100.0% | 100.0% | 0.0% |
| `sexe` | 100.0% | 100.0% | 0.0% |
| `race` | 99.7% | 100.0% | +0.3% |
| `musique` | 96.5% | 96.0% | -0.5% |
| `pere` | 92.7% | 98.1% | +5.3% |
| `mere` | 92.7% | 98.1% | +5.4% |
| `nb_courses_carriere` | 93.3% | 89.7% | -3.6% |
| `nb_victoires_carriere` | 93.3% | 89.7% | -3.6% |
| `gains_carriere_euros` | 84.8% | 90.1% | +5.3% |
| `oeilleres` | 82.0% | 89.7% | +7.6% |
| `position_arrivee` | 80.3% | 84.4% | +4.1% |
| `robe` | 82.3% | 71.3% | -11.0% |
| `proba_implicite` | 67.3% | 74.9% | +7.6% |
| `cote_finale` | 65.4% | 74.9% | +9.5% |
| `cote_reference` | 60.0% | 64.2% | +4.2% |
| `poids_porte_kg` | 45.2% | 47.5% | +2.3% |
| `temps_ms` | 40.2% | 37.9% | -2.3% |
| `reduction_km_ms` | 40.2% | 37.9% | -2.3% |
| `deferre` | 23.1% | 34.8% | +11.7% |
| `handicap_valeur` | 17.4% | 25.5% | +8.0% |
| `incident` | 16.3% | 14.3% | -2.0% |
| `avis_entraineur` | 0.0% | 17.6% | +17.6% |

## 2. Fields Below 50% - Potential Sources

| Field | Current Rate | Potential Sources |
|-------|-------------|-------------------|
| `poids_porte_kg` | 46.4% | `101_pmu_enriched:poidsConditionMonte`, `26_geny:poids` |
| `temps_ms` | 39.1% | `101_pmu_enriched:tempsObtenu`, `101_pmu_participants:tempsObtenu` |
| `reduction_km_ms` | 39.1% | `101_pmu_enriched:reductionKilometrique`, `101_pmu_participants:reductionKm` |
| `deferre` | 28.8% | `101_pmu_enriched:deferre`, `101_pmu_participants:deferre` |
| `handicap_valeur` | 21.4% | `101_pmu_enriched:handicapValeur` |
| `incident` | 15.3% | _No external source identified_ |
| `avis_entraineur` | 8.7% | `101_pmu_enriched:avisEntraineur`, `101_pmu_participants:avisEntraineur` |

### Fields 50-89% (improvement candidates)

| Field | Current Rate | Potential Sources |
|-------|-------------|-------------------|
| `gains_carriere_euros` | 87.4% | `101_pmu_participants:gainsCarriere` |
| `oeilleres` | 85.8% | `101_pmu_enriched:oeilleres`, `101_pmu_participants:oeilleres` |
| `position_arrivee` | 82.3% | `101_pmu_participants:ordreArrivee` |
| `robe` | 76.9% | _Derived / no external source_ |
| `proba_implicite` | 71.0% | _Derived / no external source_ |
| `cote_finale` | 70.1% | `101_pmu_participants:cote_direct` |
| `cote_reference` | 62.1% | `101_pmu_participants:cote_reference` |

## 3. PMU Enriched Data Join Analysis

- **PMU enriched** (pmu_participants_enriched.jsonl): ~235K records
  - Join key: `(date, numReunion, numCourse, numPmu)`
  - Match rate on sample: **7.7%** (386/5,000)
- **PMU participants** (pmu_participants.jsonl): ~1.39M records
  - Join key: `(date, num_reunion, num_course, numPmu)`
  - Match rate on sample: **44.5%** (2,226/5,000)

## 4. Potential Gains from PMU Data Merge

| Field | Current | After Merge | Gain | Fillable Records |
|-------|---------|-------------|------|-----------------|
| `gains_carriere_euros` | 87.4% | 92.3% | +4.9% | 245/5,000 |
| `poids_porte_kg` | 46.4% | 46.8% | +0.4% | 21/5,000 |
| `cote_finale` | 70.1% | 70.4% | +0.3% | 14/5,000 |
| `cote_reference` | 62.1% | 62.4% | +0.3% | 14/5,000 |
| `nb_courses_carriere` | 91.5% | 91.8% | +0.3% | 14/5,000 |
| `nb_victoires_carriere` | 91.5% | 91.8% | +0.3% | 14/5,000 |
| `oeilleres` | 85.8% | 86.1% | +0.3% | 14/5,000 |
| `avis_entraineur` | 8.7% | 8.8% | +0.1% | 4/5,000 |
| `pere` | 95.4% | 95.4% | +0.0% | 2/5,000 |
| `mere` | 95.3% | 95.4% | +0.0% | 2/5,000 |

**1 fields** could gain > 1% fill rate from PMU data merge.

## 5. Full Field Inventory (all fields, top 50 by fill rate)

| # | Field | Fill Rate |
|---|-------|-----------|
| 1 | `age` | 100.0% |
| 2 | `allure` | 100.0% |
| 3 | `cle_partant` | 100.0% |
| 4 | `cnd_cond_is_international` | 100.0% |
| 5 | `cnd_cond_is_quinte` | 100.0% |
| 6 | `cnd_cond_is_tierce` | 100.0% |
| 7 | `cnd_cond_nb_features_extraites` | 100.0% |
| 8 | `cnd_conditions_texte_original` | 100.0% |
| 9 | `course_uid` | 100.0% |
| 10 | `date_reunion_iso` | 100.0% |
| 11 | `discipline` | 100.0% |
| 12 | `distance` | 100.0% |
| 13 | `engagement` | 100.0% |
| 14 | `entraineur` | 100.0% |
| 15 | `gnn_cheval_degree` | 100.0% |
| 16 | `gnn_entraineur_nb_chevaux` | 100.0% |
| 17 | `gnn_jockey_nb_chevaux` | 100.0% |
| 18 | `gnn_premier_hippo` | 100.0% |
| 19 | `gnn_premier_jockey` | 100.0% |
| 20 | `hippodrome_normalise` | 100.0% |
| 21 | `horse_id` | 100.0% |
| 22 | `is_disqualifie` | 100.0% |
| 23 | `is_gagnant` | 100.0% |
| 24 | `is_inedit` | 100.0% |
| 25 | `is_place` | 100.0% |
| 26 | `jockey_driver` | 100.0% |
| 27 | `jockey_driver_change` | 100.0% |
| 28 | `jument_pleine` | 100.0% |
| 29 | `met_impact_meteo_score` | 100.0% |
| 30 | `met_is_psf` | 100.0% |
| 31 | `nom_cheval` | 100.0% |
| 32 | `nombre_partants` | 100.0% |
| 33 | `num_pmu` | 100.0% |
| 34 | `numero_course` | 100.0% |
| 35 | `numero_reunion` | 100.0% |
| 36 | `partant_uid` | 100.0% |
| 37 | `ped_has_pedigree` | 100.0% |
| 38 | `poids_monte_change` | 100.0% |
| 39 | `proprietaire` | 100.0% |
| 40 | `reunion_uid` | 100.0% |
| 41 | `seq_nb_courses_historique` | 100.0% |
| 42 | `seq_nb_places_recent_5` | 100.0% |
| 43 | `seq_nb_victoires_recent_5` | 100.0% |
| 44 | `seq_serie_non_places` | 100.0% |
| 45 | `seq_serie_places` | 100.0% |
| 46 | `seq_serie_victoires` | 100.0% |
| 47 | `sexe` | 100.0% |
| 48 | `source` | 100.0% |
| 49 | `spd_class_rating` | 100.0% |
| 50 | `statut` | 100.0% |

*Total fields found: 6737*

## 6. Lowest Fill Rate Fields (bottom 30)

| # | Field | Fill Rate |
|---|-------|-----------|
| 1 | `rap_ri_simple_place_international_91_combinaison` | 0.0% |
| 2 | `rap_ri_simple_place_international_91_dividende` | 0.0% |
| 3 | `rap_ri_simple_place_international_91_mise_base` | 0.0% |
| 4 | `rap_ri_simple_place_international_92_combinaison` | 0.0% |
| 5 | `rap_ri_simple_place_international_92_dividende` | 0.0% |
| 6 | `rap_ri_simple_place_international_92_mise_base` | 0.0% |
| 7 | `rap_ri_simple_place_international_93_combinaison` | 0.0% |
| 8 | `rap_ri_simple_place_international_93_dividende` | 0.0% |
| 9 | `rap_ri_simple_place_international_93_mise_base` | 0.0% |
| 10 | `rap_ri_simple_place_international_94_combinaison` | 0.0% |
| 11 | `rap_ri_simple_place_international_94_dividende` | 0.0% |
| 12 | `rap_ri_simple_place_international_94_mise_base` | 0.0% |
| 13 | `rap_ri_simple_place_international_95_combinaison` | 0.0% |
| 14 | `rap_ri_simple_place_international_95_dividende` | 0.0% |
| 15 | `rap_ri_simple_place_international_95_mise_base` | 0.0% |
| 16 | `rap_ri_simple_place_international_96_combinaison` | 0.0% |
| 17 | `rap_ri_simple_place_international_96_dividende` | 0.0% |
| 18 | `rap_ri_simple_place_international_96_mise_base` | 0.0% |
| 19 | `rap_ri_simple_place_international_97_combinaison` | 0.0% |
| 20 | `rap_ri_simple_place_international_97_dividende` | 0.0% |
| 21 | `rap_ri_simple_place_international_97_mise_base` | 0.0% |
| 22 | `rap_ri_simple_place_international_98_combinaison` | 0.0% |
| 23 | `rap_ri_simple_place_international_98_dividende` | 0.0% |
| 24 | `rap_ri_simple_place_international_98_mise_base` | 0.0% |
| 25 | `rap_ri_simple_place_international_99_combinaison` | 0.0% |
| 26 | `rap_ri_simple_place_international_99_dividende` | 0.0% |
| 27 | `rap_ri_simple_place_international_99_mise_base` | 0.0% |
| 28 | `rap_ri_simple_place_international_9_combinaison` | 0.0% |
| 29 | `rap_ri_simple_place_international_9_dividende` | 0.0% |
| 30 | `rap_ri_simple_place_international_9_mise_base` | 0.0% |

## 7. Recommendations

### Quick Wins (merge existing data)
- **`gains_carriere_euros`**: +4.9% from `pmu_participants`

### Scraper Improvements Needed
- **`incident`** (15.3%): No external source identified - consider adding a scraper or deriving from existing data

### Data Pipeline Actions
1. Run `scripts/merge/merge_pmu_enriched.py` to integrate PMU enriched data
2. Cross-reference with `output/26_geny/geny_flat.jsonl` for cotes and equipment data
3. Run `scripts/enrich_deferre.py` and `scripts/enrich_incident.py` for missing equipment/incident data
4. Consider re-scraping PMU API for dates with missing `temps_ms` and `reduction_km_ms`
