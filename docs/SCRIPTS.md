# Scripts Reference

Complete listing of all 204 Python scripts in the project root.

Legend: Resume = supports `--resume` flag for checkpoint-based restart.

## Numbered Scripts (102)

| # | Script | Purpose | Resume |
|---|--------|---------|:------:|
| 1 | `00_enrichissement_meteo.py` | 00_enrichissement_meteo.py — Enrichissement météo des réunions normalisées. | - |
| 2 | `01_calendrier_reunions.py` | Script de collecte multi-sources du calendrier des réunions de courses hippiques. | - |
| 3 | `02_liste_courses.py` | Collecte multi-sources des courses et partants par réunion. | - |
| 4 | `02b_liste_courses_2013.py` | 02_liste_courses.py | - |
| 5 | `02b_scraper_letrot.py` | Collecte des courses et partants de trot hors-PMU depuis Le Trot. | - |
| 6 | `04_resultats.py` | Collecte des rapports définitifs (résultats officiels / cotes finales) par course. | - |
| 7 | `05_historique_chevaux.py` | Reconstruit l'historique complet de chaque cheval a partir des partants normalises. | - |
| 8 | `06_historique_jockeys.py` | Reconstruit l'historique de chaque jockey/driver et de chaque entraineur | - |
| 9 | `07_cotes_marche.py` | Calcule des features derivees du marche des cotes pour chaque partant. | - |
| 10 | `08_pedigree.py` | Calcule les statistiques de performance par pere et par mere | - |
| 11 | `09_equipements.py` | Reconstruit l'historique d'equipements (oeilleres, deferre) par cheval | - |
| 12 | `100_magic_millions_scraper.py` | Script 100 -- Scraping Magic Millions (Playwright version) | Yes |
| 13 | `101_pmu_api_scraper.py` | Script 101 — PMU Official API Scraper | Yes |
| 14 | `102_racing_post_scraper.py` | Script 102 — Racing Post Results Scraper | Yes |
| 15 | `10_poids_handicaps.py` | Calcul des metriques poids / handicaps par partant | - |
| 16 | `11_sectionals.py` | Calcul des metriques de temps / vitesse / reduction kilometrique | - |
| 17 | `12_pedigree_scraper.py` | Enrichit les donnees de pedigree des chevaux en scrappant des sources en ligne. | Yes |
| 18 | `13_meteo_historique.py` | Collecte des donnees meteo historiques pour chaque course hippique. | - |
| 19 | `14_pedigree_scraper.py` | Scrape les pedigrees complets des pur-sang depuis PedigreeQuery.com. | - |
| 20 | `15_download_external_datasets.py` | Download freely available horse racing datasets from multiple sources. | - |
| 21 | `16_collecte_nanaelie_2004_2013.py` | Collecte des resultats de courses PMU via l'API gratuite Open PMU (nanaelie) | - |
| 22 | `17_process_sire.py` | Script 17 — Processing fichier SIRE/IFCE (4M chevaux) | - |
| 23 | `18_letrot_records.py` | Script 18 — Scraping des records de piste LeTrot | - |
| 24 | `19_boturfers_stats.py` | Script 19 — Scraping stats hippodromes Boturfers | - |
| 25 | `20_ifce_stats.py` | Script 20 — IFCE Stats & Cartes | - |
| 26 | `21_rapports_definitifs.py` | Script 21 — Rapports définitifs PMU (dividendes) | - |
| 27 | `22_performances_detaillees.py` | Script 22 — Performances détaillées PMU (9 dernières courses par cheval) | - |
| 28 | `23_pronostics_equidia.py` | Script 23 — Pronostics : collecte multi-source | - |
| 29 | `24_canalturf_scraper.py` | Script 24 — Scraping Canalturf fiches chevaux | - |
| 30 | `25_turfostats_scraper.py` | Script 25 — Scraping Turfostats stats galop | - |
| 31 | `26_geny_scraper.py` | Script 26 — Scraping Geny.com (PMU Group) | Yes |
| 32 | `27_citations_enjeux.py` | Script 27 — Citations & Enjeux PMU (distribution des paris par cheval) | - |
| 33 | `28_combinaisons_marche.py` | Script 28 — Combinaisons & Masse d'enjeux PMU (structure du marché des paris) | - |
| 34 | `29_arqana_ventes.py` | Script 29 — Arqana : Ventes de chevaux en France | - |
| 35 | `30_smarkets_exchange.py` | Script 30 — Smarkets Exchange : Cotes back/lay courses FR | - |
| 36 | `31_zone_turf.py` | Script 31 — Zone-Turf : Pronostics communautaires + stats chevaux | - |
| 37 | `32_turfomania.py` | Script 32 — Turfomania : Indices de confiance, Turf Machine IA, fiches techniques | - |
| 38 | `33_turf_fr.py` | Script 33 — Turf-FR : Pronostics presse, % adversaires battus, stats | - |
| 39 | `34_unibet_cotes.py` | Script 34 — Unibet FR : Cotes bookmaker français | - |
| 40 | `35_meteo_france_api.py` | Script 35 — Météo-France API via Open-Meteo (AROME 1.5km) | - |
| 41 | `36_pedigree_query.py` | Script 36 — Pedigree Query : Pedigrees 5 générations internationaux | - |
| 42 | `37_rpscrape_racing_post.py` | Script 37 — Racing Post via rpscrape : Form, ratings, résultats internationaux | - |
| 43 | `38_rapports_internet.py` | Script 38 — Rapports Définitifs Internet (e-paris, spécialisation INTERNET) | - |
| 44 | `39_reunions_enrichies.py` | Script 39 — Reunions enrichies : meteo, incidents, conditions, duree, commentaires, paris | - |
| 45 | `40_enrichissement_partants.py` | Lit tous les fichiers JSON du cache des reunions (output/02_liste_courses/cache/) | - |
| 46 | `41_sequences_performances.py` | Script 41 — Séquences de performances pour LSTM/GRU/TFT | - |
| 47 | `42_croisement_racing_post_pmu.py` | Script 42 — Croisement Racing Post × PMU | - |
| 48 | `43_croisement_meteo_courses.py` | Script 43 — Croisement Météo × Courses | - |
| 49 | `44_croisement_pedigree_partants.py` | Script 44 — Croisement Pedigree × Partants | - |
| 50 | `45_graphe_relations_gnn.py` | Script 45 — Graphe de relations pour GNN | - |
| 51 | `46_track_bias_speed_class.py` | Script 46 — Track Bias + Speed Figures + Class Ratings | - |
| 52 | `48_parse_conditions_texte.py` | Script 48 — Parse conditions texte (regex → features structurées) | - |
| 53 | `49_ecart_cotes_internet_national.py` | Script 49 — Écart cotes Internet vs National + Market Efficiency | - |
| 54 | `51_zeturf_scraper.py` | Script 51 — Scraping ZeTurf.fr | Yes |
| 55 | `52_turfomania_scraper.py` | Script 52 — Scraping Turfomania.fr (Playwright) | - |
| 56 | `53_paris_turf_scraper.py` | Script 53 — Scraping Paris-Turf.com (Playwright version) | - |
| 57 | `54_turfinfo_scraper.py` | Script 54 — Scraping TurfInfo.fr (Playwright) | Yes |
| 58 | `55_equidia_data_scraper.py` | Script 55 — Scraping Equidia.fr (Playwright version) | Yes |
| 59 | `56_timeform_scraper.py` | Script 56 — Scraping Timeform.com (UK Racing) | Yes |
| 60 | `57_sporting_life_scraper.py` | Script 57 — Scraping Sporting Life (UK Racing) | Yes |
| 61 | `58_at_the_races_scraper.py` | Script 58 — Scraping At The Races (UK/IRE Racing) | Yes |
| 62 | `59_racing_tv_scraper.py` | Script 59 — Scraping Racing TV (Playwright version) | Yes |
| 63 | `60_oddschecker_scraper.py` | Script 60 — Scraping Oddschecker (Odds Comparison) | Yes |
| 64 | `61_equibase_scraper.py` | Script 61 — Scraping Equibase.com (US Racing) | Yes |
| 65 | `62_horse_racing_nation_scraper.py` | Script 62 — Scraping HorseRacingNation.com (US Racing) | Yes |
| 66 | `63_daily_racing_form_scraper.py` | Script 63 — Scraping DRF.com (Daily Racing Form - US) | Yes |
| 67 | `64_punters_scraper.py` | Script 64 — Scraping Punters.com.au (Australian Racing) | Yes |
| 68 | `65_racenet_scraper.py` | Script 65 — Scraping Racenet.com.au (Australian Racing) | Yes |
| 69 | `66_hkjc_scraper.py` | Script 66 — Scraping racing.hkjc.com (Playwright version) | Yes |
| 70 | `67_jra_scraper.py` | Script 67 — Scraping jra.go.jp (Japan Racing Association) | Yes |
| 71 | `68_betfair_exchange_scraper.py` | Script 68 — Scraping Betfair Exchange API (Global Betting Exchange) | Yes |
| 72 | `69_oddsportal_scraper.py` | Script 69 — Scraping OddsPortal.com (Playwright version) | Yes |
| 73 | `70_betexplorer_scraper.py` | Script 70 — Scraping BetExplorer.com | Yes |
| 74 | `71_allbreedpedigree_scraper.py` | Script 71 — Scraping AllBreedPedigree.com | Yes |
| 75 | `72_tattersalls_scraper.py` | Script 72 — Scraping Tattersalls.com | Yes |
| 76 | `73_goffs_scraper.py` | Script 73 — Scraping Goffs.com | Yes |
| 77 | `74_arqana_scraper.py` | Script 74 — Scraping Arqana.com (ventes de chevaux France) | Yes |
| 78 | `75_keeneland_scraper.py` | Script 75 — Scraping Keeneland.com (ventes de chevaux US) | Yes |
| 79 | `76_usta_trot_scraper.py` | Script 76 — Scraping USTrotting.com (trotting US) | Yes |
| 80 | `77_kaggle_datasets.py` | Script 77 — Download ALL horse racing datasets from Kaggle | Yes |
| 81 | `78_goingstick_scraper.py` | Script 78 — Scraping Going/Terrain data (UK sources) | Yes |
| 82 | `79_trainer_stats_scraper.py` | Script 79 — Scraping Trainer Performance Statistics | Yes |
| 83 | `80_france_galop_scraper.py` | Script 80 — Scraping france-galop.com (donnees officielles courses FR) | Yes |
| 84 | `81_pronosoft_scraper.py` | Script 81 — Scraping Pronosoft.com | Yes |
| 85 | `82_turf_fr_scraper.py` | Script 82 — Scraping Turf-FR.com | Yes |
| 86 | `83_letrot_scraper.py` | Script 83 — Scraping LeTrot.com (corrigé) | Yes |
| 87 | `84_turfoo_scraper.py` | Script 84 — Scraping Turfoo.fr (corrigé) | Yes |
| 88 | `85_racing_and_sports_scraper.py` | Script 85 — Scraping RacingAndSports.com | Yes |
| 89 | `86_smartform_scraper.py` | Script 86 — Scraping SmartForm.co.uk | Yes |
| 90 | `87_bloodstock_scraper.py` | Script 87 — Scraping Bloodstock News (BloodHorse + TDN) | Yes |
| 91 | `88_weatherbys_scraper.py` | Script 88 — Scraping Weatherbys.co.uk | Yes |
| 92 | `89_singapore_pools_scraper.py` | Script 89 — Scraping Singapore Pools (Racing) | Yes |
| 93 | `90_korea_racing_scraper.py` | Script 90 — Scraping Korea Racing Authority (KRA) | Yes |
| 94 | `91_equiratings_scraper.py` | Script 91 -- Scraping EquiRatings.com (Playwright version) | Yes |
| 95 | `92_optixeq_scraper.py` | Script 92 -- Scraping OptixEQ | Yes |
| 96 | `93_raceform_scraper.py` | Script 93 -- Scraping Raceform.co.uk | Yes |
| 97 | `94_harness_au_scraper.py` | Script 94 -- Scraping Harness Racing Australia (Playwright version) | Yes |
| 98 | `95_standardbred_ca_scraper.py` | Script 95 -- Scraping Standardbred Canada (Playwright version) | Yes |
| 99 | `96_noaa_weather_scraper.py` | Script 96 -- NOAA Historical Weather API Scraper | Yes |
| 100 | `97_meteostat_scraper.py` | Script 97 -- Meteostat API Scraper | Yes |
| 101 | `98_turftrax_scraper.py` | Script 98 -- Scraping TurfTrax | Yes |
| 102 | `99_clerk_of_course_scraper.py` | Script 99 -- Clerk of Course Reports Scraper | Yes |

## Feature Builders (17)

| # | Script | Purpose | Resume |
|---|--------|---------|:------:|
| 1 | `feat_cheval_distance_affinity.py` | Feature Engineering — Cheval x Distance Affinity | - |
| 2 | `feat_cheval_hippodrome_affinity.py` | Feature Engineering — Cheval x Hippodrome Affinity | - |
| 3 | `feat_cheval_jockey_affinity.py` | Feature Engineering — Cheval x Jockey Affinity | - |
| 4 | `feat_cheval_terrain_affinity.py` | Feature Engineering — Cheval x Terrain Affinity | - |
| 5 | `feat_croisements.py` | Feature Engineering — Module Croisements Cheval × Contexte | - |
| 6 | `feat_entraineur_hippodrome.py` | Feature Engineering — Entraineur x Hippodrome | - |
| 7 | `feat_field_strength.py` | Feature Engineering — Field Strength | - |
| 8 | `feat_historique.py` | Feature Engineering — Module Historique Glissant | - |
| 9 | `feat_interactions.py` | Feature Engineering — Module Interactions + Signaux Marché | - |
| 10 | `feat_jockey.py` | Feature Engineering — Module Jockey / Entraîneur | - |
| 11 | `feat_jockey_entraineur_combo.py` | Feature Engineering — Jockey x Entraineur Combo | - |
| 12 | `feat_meteo_terrain_interaction.py` | Feature Engineering — Meteo x Terrain Interaction | - |
| 13 | `feat_pedigree.py` | Feature Engineering — Module Pedigree Avancé | - |
| 14 | `feat_pedigree_discipline_match.py` | Feature Engineering — Pedigree x Discipline Match | - |
| 15 | `feat_sequences.py` | Feature Engineering — Module Séquences / Patterns | - |
| 16 | `feat_temporel.py` | Feature Engineering — Module Temporel / Saisonnalité | - |
| 17 | `feat_value_betting.py` | Feature Engineering — Value Betting | - |

## Merge Scripts (13)

| # | Script | Purpose | Resume |
|---|--------|---------|:------:|
| 1 | `merge_02_02b.py` | Merge PMU (02) and Le Trot (02b) normalised courses and partants into unified files. | - |
| 2 | `merge_02_02b_courses_master.py` | merge_02_02b_courses_master.py — Étape 5.1 du TODO | - |
| 3 | `merge_all_enrichments.py` | Merge TOUS les fichiers enrichis en un seul partants_master_final.jsonl. | - |
| 4 | `merge_all_pedigree.py` | Merge 4 pedigree data sources into one unified pedigree file. | - |
| 5 | `merge_equipements_master.py` | Merge Équipements Master — Fusionne équipements + poids/handicaps | - |
| 6 | `merge_marche_master.py` | Merge Marché Master — Fusionne TOUTES les données de cotes/paris/marché | - |
| 7 | `merge_meteo.py` | merge_meteo.py — Consolidate all météo sources into one comprehensive file. | - |
| 8 | `merge_meteo_master.py` | Merge Météo Master — Fusionne TOUTES les sources météo | - |
| 9 | `merge_pedigree_master.py` | Merge Pedigree Master v2 — Fusionne TOUTES les sources pedigree | - |
| 10 | `merge_performances_master.py` | Merge Performances Master — Fusionne TOUTES les données de performances/historique | - |
| 11 | `merge_rapports_21_38.py` | Merge rapports_definitifs (21) and rapports_internet (38) into a single | - |
| 12 | `merge_rapports_master.py` | Merge Rapports Master — Fusionne TOUS les rapports/résultats | - |
| 13 | `merge_stats_externes_master.py` | Merge Stats Externes Master — Fusionne TOUTES les sources externes (hors PMU) | - |

## Post-Processing (5)

| # | Script | Purpose | Resume |
|---|--------|---------|:------:|
| 1 | `postprocess_equipements.py` | Post-processing équipements — Enrichit equipements_master.json avec : | - |
| 2 | `postprocess_horse_stats.py` | Post-processing horse_stats — Enrichit horse_stats_master.json avec : | - |
| 3 | `postprocess_marche.py` | Post-processing marché — Enrichit marche_master.json avec : | - |
| 4 | `postprocess_meteo.py` | Post-processing météo — Ajoute les flags calculés manquants | - |
| 5 | `postprocess_rapports.py` | Post-processing rapports — Enrichit rapports_master.json avec : | - |

## Patches (5)

| # | Script | Purpose | Resume |
|---|--------|---------|:------:|
| 1 | `patch_brutes_geny.py` | patch_brutes_geny.py — Patch les brutes Geny pour enrichir avec pénétromètre et terrain. | - |
| 2 | `patch_brutes_letrot.py` | patch_brutes_letrot.py — Patch les brutes Le Trot pour enrichir avec les données courses. | - |
| 3 | `patch_brutes_pmu.py` | patch_brutes_pmu.py — Enrichit les brutes PMU existantes avec les nouveaux champs | - |
| 4 | `patch_condition_pmu.py` | patch_condition_pmu.py — Enrichit condition, type_piste, corde via l'API PMU. | - |
| 5 | `patch_terrain_equidia.py` | patch_terrain_equidia.py — Comble les trous de terrain/condition via Equidia. | - |

## Quality Pillars (7)

| # | Script | Purpose | Resume |
|---|--------|---------|:------:|
| 1 | `pilier_audit_trail.py` | pilier_audit_trail.py — Pilier 5 : Traçabilité complète | - |
| 2 | `pilier_auto_repair.py` | pilier_auto_repair.py -- Pilier Qualite : Reparation automatique | - |
| 3 | `pilier_coverage_matrix.py` | pilier_coverage_matrix.py -- Pilier Qualite : Matrice de couverture | - |
| 4 | `pilier_data_freshness.py` | pilier_data_freshness.py -- Pilier Qualite : Fraicheur des donnees | - |
| 5 | `pilier_drift_detection.py` | pilier_drift_detection.py -- Pilier Qualite : Detection de drift | - |
| 6 | `pilier_golden_records.py` | pilier_golden_records.py -- Pilier Qualite : Reconciliation Golden Records | - |
| 7 | `pilier_performance_profiler.py` | pilier_performance_profiler.py -- Pilier Qualite : Profilage de performance | - |

## Utilities and Other (55)

| # | Script | Purpose | Resume |
|---|--------|---------|:------:|
| 1 | `audit_02.py` | audit_02.py — Audit qualite des donnees produites par 02_liste_courses.py. | - |
| 2 | `audit_data_integrity.py` | audit_data_integrity.py — Étape 2 du TODO | - |
| 3 | `audit_html_vs_json.py` | audit_html_vs_json.py  (Etape 8.0) | - |
| 4 | `batch_scraper.py` | Batch scraper — Scrape multiple horse racing sites in one pass. | - |
| 5 | `build_course_profiles.py` | Construit les profils de course par hippodrome a partir des donnees historiques. | - |
| 6 | `build_horse_career_stats.py` | Construit les statistiques de carriere par cheval a partir de partants_master. | - |
| 7 | `build_jockey_stats.py` | Construit les statistiques par jockey/driver a partir de partants_master. | - |
| 8 | `build_trainer_stats.py` | Construit les statistiques par entraineur a partir de partants_master. | - |
| 9 | `comblage_trous.py` | comblage_trous.py — Étape 4 du TODO | - |
| 10 | `convert_features_parquet.py` | Convertit les 11 fichiers features JSONL (~253 GB total) en format Parquet | - |
| 11 | `convert_stable_jsonl_to_parquet.py` | Converts stable JSONL files (NOT features_matrix.jsonl) to Parquet with snappy compression. | - |
| 12 | `data_completeness_report.py` | Genere un rapport detaille de completude des donnees. | - |
| 13 | `deduplication.py` | deduplication.py — Étape 3.3 du TODO | - |
| 14 | `enrichissement_champs.py` | Enrichit les 14 champs à faible taux de remplissage dans partants_master.jsonl. | - |
| 15 | `enrichissement_meteo_nasa.py` | enrichissement_meteo_nasa.py — Enrichissement météo via NASA POWER (gratuit, sans limite). | - |
| 16 | `entity_resolution.py` | Entity Resolution — Relie TOUS les masters en une seule grande table. | - |
| 17 | `export_parquet_chunks.py` | Convertit un gros fichier JSONL en Parquet par chunks de 50K lignes. | - |
| 18 | `export_triple_format.py` | export_triple_format.py — Etape 9.4 : Export triple format | - |
| 19 | `feature_engineering.py` | Feature Engineering PRINCIPAL — Orchestre tous les modules de features. | - |
| 20 | `fetch_openmeteo_missing.py` | Fetch missing meteo data from Open-Meteo Archive API for all hippodromes. | - |
| 21 | `fill_empty_fields.py` | Fills empty/missing fields in partants_normalises.json and courses_normalisees.json | - |
| 22 | `fix_14_consolidate.py` | Reads ALL JSON files from output/14_pedigree/cache/, | - |
| 23 | `fix_output_permissions.py` | Corrige le probleme de junction/read-only sur le dossier output/. | - |
| 24 | `generate_labels.py` | Genere les labels d'entrainement a partir de partants_master.jsonl. | - |
| 25 | `hippodromes_db.py` | Base de donnees des hippodromes avec coordonnees GPS, altitude et donnees piste. | - |
| 26 | `integrate_new_sources.py` | integrate_new_sources.py  (Etape 8.3) | - |
| 27 | `integrate_sporting_life.py` | integrate_sporting_life.py  (Etape 8.2) | - |
| 28 | `integrate_timeform.py` | integrate_timeform.py  (Etape 8.1) | - |
| 29 | `json_to_jsonl.py` | json_to_jsonl.py — Convertit les gros fichiers JSON (array) en JSONL. | - |
| 30 | `master_feature_builder.py` | master_feature_builder.py  (STREAMING version) | - |
| 31 | `mega_merge_all_sources.py` | Enriches partants_normalises.json (2.9M records, 4.6GB) by merging data from | - |
| 32 | `mega_merge_courses.py` | Enriches courses_normalisees.json by merging all available course-level data sources. | - |
| 33 | `mega_merge_partants_master.py` | mega_merge_partants_master.py — Étape 5 du TODO | - |
| 34 | `monitor_pipeline.py` | Moniteur en temps reel du pipeline turf-data. | - |
| 35 | `nettoyage_global.py` | nettoyage_global.py — Étape 3 du TODO | - |
| 36 | `normalize_disciplines.py` | Normalise les noms de disciplines a travers tous les fichiers de donnees. | - |
| 37 | `normalize_hippodromes.py` | Normalise les noms d'hippodromes a travers toutes les sources. | - |
| 38 | `organize_model_data.py` | Creates model-specific data folders under pipeline/data/ with RELATIVE symlinks | - |
| 39 | `organize_pipeline.py` | Creates the complete 16-phase, 68-module pipeline folder structure | - |
| 40 | `organize_project.py` | Reorganise le projet turf-data-pipeline en une arborescence propre. | - |
| 41 | `parse_02b_letrot.py` | Parse Le Trot HTML cache files into normalised courses and partants JSON. | - |
| 42 | `rebuild_all_indexes.py` | Reconstruit tous les index de lookup a partir des donnees brutes. | - |
| 43 | `rebuild_mega_merge.py` | Re-run mega merge using partants_master_enrichi.jsonl (au lieu de l'original). | - |
| 44 | `remove_empty_fields.py` | remove_empty_fields.py — Etape 3.4 : Suppression des donnees inutiles | - |
| 45 | `renormaliser.py` | renormaliser.py — Re-normalise les brutes existantes sans re-scraper. | - |
| 46 | `run_pipeline.py` | run_pipeline.py - Orchestrateur principal du pipeline turf-data. | - |
| 47 | `scraper_results_audit.py` | Audit de ce que chaque scraper a reellement collecte. | - |
| 48 | `setup.py` | Script d'installation et de verification du projet turf-data-pipeline. | - |
| 49 | `stats_finales.py` | stats_finales.py — Étape 11.2 | - |
| 50 | `status_report.py` | status_report.py — Genere un rapport de statut complet du pipeline. | - |
| 51 | `test_endpoints.py` | Systematic test of ALL possible PMU API endpoints to discover available data. | - |
| 52 | `validate_cross_sources.py` | validate_cross_sources.py — Étape 11.3 | - |
| 53 | `validate_data_final.py` | validate_data_final.py  --  One-command validation of the turf-data-pipeline master data. | - |
| 54 | `validate_data_quality.py` | validate_data_quality.py — Validation qualite des donnees du pipeline. | - |
| 55 | `verify_fusion.py` | verify_fusion.py — Etape 5.3 : Verification post-fusion | - |
