# Changelog

Generated from git history -- 187 commits.

## Initial Setup (5 commits)

- `198f4d5 Initial commit — pipeline data courses hippiques`
- `84900b2 Sauvegarde complète — scripts JSONL + contexte pour 2ème session`
- `3259f6b Sauvegarde liste complete 68 modules + 16 phases + 23 piliers (pour plus tard)`
- `b83454e Sauvegarde Phase 3-4 ML (7 modules) - PAS EXECUTE, data d'abord`
- `cb3843e Deduplicate save helpers in 01_calendrier_reunions Sauvegarder class`

## Data Collection (Scrapers) (59 commits)

- `462a2a9 Session 2 complète : 11 builders + master orchestrator + 5 scrapers FR + 8 tests qualité + docs`
- `a0576c4 Ajout 5 scrapers UK (56-60) : Timeform, Sporting Life, At The Races, Racing TV, Oddschecker`
- `d7277ca Ajout 8 scrapers internationaux (61-68) : US, AU, HK, JP, Betfair`
- `28205be Ajout 5 scrapers (69-73) : OddsPortal, BetExplorer, AllBreedPedigree, Tattersalls, Goffs`
- `b1dba73 Ajout 7 scrapers (74-80)`
- `b608aaa Enrichissement 5 scrapers : extraction complète (JSON embarqué, commentaires, sectionals, GPS, stats)`
- `a3632c0 Enrichissement 10 scrapers : extraction complète JSON/commentaires/stats/sectionals`
- `1acf08a Ajout 10 scrapers (81-90) : Pronosoft, Turf-FR, LeTrot, Turfoo, Racing&Sports, SmartForm, Bloodstock, Weatherbys, Singapore, Korea`
- `840ec35 6 scripts data: rebuild_mega, parquet_chunks, fix_permissions, scraper_audit, indexes, completeness`
- `faee138 5 scrapers Playwright pour sites bloques (Zeturf, Equidia, Oddschecker, France Galop + base class)`
- `e2cf6c2 100 scrapers! Ajout 91-100: EquiRatings, OptixEQ, Raceform, Harness AU, Standardbred CA, NOAA, Meteostat, TurfTrax, Clerk of Course, Magic Millions`
- `23100b9 10 scrapers mis a jour: cloudscraper (bypass Cloudflare) + HTML cache`
- `a1fadab Completeness report + indexes rebuilds + scraper audit`
- `8c52551 Scrapers session: 6 scrapers fixes + 3 nouveaux + batch scraper + quality updates`
- `73ddd56 Fix deprecated datetime.utcnow() in letrot scraper`
- `d1fb49e Fix 16 bugs across 14 scrapers: encoding, paths, cloudscraper safety`
- `4eb3694 Fix relative paths in 51 scrapers: use os.path.abspath for OUTPUT_DIR`
- `3a9e9fb Add source field to PMU API scraper records`
- `5f884c0 Add encoding="utf-8" to remaining write calls in turfostats scraper`
- `4c86574 Fix PMU API scraper crash on corrupted cache files`
- `2c408fe fix: add missing 'import requests' to 10 scrapers with cloudscraper fallback`
- `a68de2a Add utils/scraping.py with shared scraper utilities`
- `c44c13d Fix PMU scraper --resume ignoring checkpoint before default start date`
- `76b4543 Improve PMU scraper cache error handling with OSError and logging`
- `4c40506 Fix --resume checkpoint bug in 33 scrapers`
- `d591266 Migrate logging.basicConfig to utils.logging_setup (scrapers 21-30)`
- `9b5eed9 Migrate logging.basicConfig to utils.logging_setup (scrapers 31-39)`
- `ae1dfb3 Migrate logging.basicConfig to utils.logging_setup (scrapers 60-70)`
- `7d74771 Migrate logging.basicConfig to utils.logging_setup (scrapers 51-59)`
- `b5ce52d Migrate logging.basicConfig to utils.logging_setup (scrapers 71-80)`
- `210cdb4 Migrate logging.basicConfig to utils.logging_setup (scrapers 81-99)`
- `5ac84f5 Migrate logging.basicConfig to utils.logging_setup (scrapers 100-102 + audit/batch files)`
- `6b5ad40 Migrate smart_pause to utils.scraping (scrapers 61-70)`
- `755a667 Migrate smart_pause to utils.scraping (scrapers 51-60)`
- `0b2d4b6 Migrate smart_pause to utils.scraping (scrapers 71-80)`
- `e4bd026 Migrate smart_pause to utils.scraping (scrapers 81-99)`
- `d77f6eb Migrate smart_pause to utils.scraping (remaining scrapers 21-39 + 100-102)`
- `6a7d52e Migrate checkpoint + append_jsonl to utils.scraping (scrapers 51-70)`
- `da0d5ab Migrate checkpoint + append_jsonl to utils.scraping (scrapers 71-99)`
- `47efdc9 Remove unused imports after utils.scraping migration (scrapers 51-70)`
- `a6bd429 Fix PMU scraper crash on Windows file lock when removing corrupt cache`
- `fb2eac3 Fix scrapers 24-26 to produce JSONL output from cache data`
- `9b4e225 Fix scrapers 59, 63, 78, 86, 90 to produce JSONL output from cache data`
- `f07bfed Fix scrapers 13, 16, 19, 20 to produce JSONL output from cache data`
- `75e66db Fix 02b_scraper_letrot to produce JSONL output from cache data`
- `2d05732 Add --export flag to scrapers 24, 25, 26 for cache-to-JSONL export`
- `a3a2f33 Migrate 55_equidia_data_scraper from requests/cloudscraper to Playwright`
- `b5631f3 Update 60_oddschecker_scraper to use Playwright instead of requests`
- `e27dd8f Migrate scrapers 58, 62, 64, 65 from requests/cloudscraper to Playwright`
- `aa15aed Migrate scrapers 52, 54, 80 from requests/cloudscraper to Playwright`
- `c166261 Migrate scrapers 53, 59, 66, 69 from requests/cloudscraper to Playwright`
- `9cf2ee3 Add shared utils/playwright.py module for Playwright scraper helpers`
- `3d8df79 Migrate 13 scrapers to use shared utils/playwright.py helpers`
- `ff10144 Add shared create_session() to utils/scraping.py and migrate scrapers 20-39`
- `d74f3b2 Migrate scrapers 51-99 and clean up 20-26 to use shared create_session()`
- `c251460 Remove dead imports from utils.* across 11 scraper/pipeline files`
- `a34ca13 Update SOURCES.md with latest scraper counts and add Feature Builders section`
- `f896a92 Migrate scrapers 91, 94, 95, 100 from requests to Playwright`
- `5c1e9f1 Add missing __init__.py to scrapers_playwright/ and scripts/ packages`

## Data Processing & Merging (38 commits)

- `9ab6576 Ajout pipeline complet étapes 2-5 : audit, nettoyage, comblage, mega-merge`
- `35665ba Ajout déduplication + merge courses_master + comblage par inférence`
- `4d10236 Ajout run_pipeline.py (DAG 70 étapes) + organize_project.py (réorg auto)`
- `65102aa Ajout monitor_pipeline + generate_labels + setup.py + .env.example`
- `37c6db3 Fix mega_merge: encodage latin-1 fallback + limite 2GB + fix merge_meteo UTF-8`
- `f7e9b61 Ajout section 4.5 enrichissement: 14 champs à combler avec taux réels mesurés`
- `834dd61 Ajout 5 scripts: enrichissement_champs, convert_parquet, stats_finales, validate_cross, audit_trail`
- `8630b7e Stats finales: 2.93M partants, 4700 jours (2013-2026), 527 hippodromes`
- `a34fdd6 6 piliers qualite + enrichissement (pays +73%, ecart +63%)`
- `72a7f5f TODO: ajout section 4.8 taches reportees (Parquet, Playwright, APIs, permissions)`
- `259de60 7 scripts data: normalize hippo/disc, career stats cheval/jockey/trainer, course profiles, merge final`
- `ccc33a0 Add status_report.py for pipeline health monitoring`
- `b4bfe23 Add .gitattributes for consistent LF line endings`
- `43a87d6 Add encoding="utf-8" to write-mode open() in 31 files`
- `0b29b98 Fix input paths in 7 merge scripts to use BASE_DIR`
- `498174c Fix remaining data_master output paths in 7 merge scripts`
- `63c2dbe Fix remaining hardcoded paths in mega_merge and process_sire scripts`
- `948ad03 Fix paths in 10 utility scripts: normalize, rebuild, pilier, merge`
- `c4147e6 Replace 24 bare except clauses with specific exception types`
- `2ca7ff0 Add missing __init__.py files across pipeline/ package structure`
- `c6673d1 refactor: create shared utils/normalize.py, deduplicate normalize_name() across 11 files`
- `42a1759 refactor: add strip_accents() and normalize_date() to shared utils/normalize.py`
- `f494728 fix: close resource leaks in 45_graphe_relations_gnn.py and mega_merge_partants_master.py`
- `f25b474 Add newline="\n" to JSONL writers to prevent CRLF on Windows`
- `fcf0955 Add newline="\n" to remaining 78 JSONL writers (batch 2)`
- `5367676 Add error logging to silent exception handlers across 10 files`
- `776e7fc Add error logging to silent exception handlers (batch 2, 5 files)`
- `cde0156 Add error logging to silent exception handlers (batch 3, 3 files)`
- `e416f76 Add error logging to silent exception handlers (batch 4, 2 files)`
- `4b689fa Add error logging to remaining silent exception handlers (batch 5, 11 files)`
- `d2a6df5 Migrate logging.basicConfig to utils.logging_setup (merge/processing files)`
- `46d15aa Add __all__ exports to utils package modules`
- `08e151b Add DATA_DICTIONARY.md with all partants_master fields`
- `46bd62b Replace local load_json_safe/load_json_or_jsonl with imports from utils.loaders`
- `5b6db51 Add NOTE comments for incompatible local safe_float/safe_int/normalize_name`
- `378c628 Deduplicate sauver_parquet into utils/output.py`
- `33ca359 Update requirements.txt with missing packages and organize by category`
- `17096f6 Improve .gitignore with broader backup, packaging, and IDE patterns`

## Feature Engineering (22 commits)

- `80a8094 Add all remaining scripts, feature builders, pipeline, docs`
- `e39751d Ajout post-processing (50+ features calculées) + audit + questions expert`
- `7f19386 Ajout Entity Resolution + Feature Engineering complet (4 modules)`
- `242c866 Ajout 3 modules Feature Engineering supplémentaires (pedigree, temporel, séquences)`
- `ca80f1a Ajout 9 builders features + 10 scripts affinités croisées (étapes 6.2 + 6.3)`
- `a98985b Mise à jour TODO — 19 builders + pipeline cochés, CONTEXT mis à jour`
- `05a0d48 Fix 11 builders: logger et meteo_index rendus optionnels`
- `6a4634a Recode master_feature_builder en streaming 2 passes (~3 GB RAM vs 50 GB)`
- `de23636 Features matrix COMPLETE: 36 GB, 2.93M records, streaming 2-pass`
- `5ad6a2b refactor: create shared utils/loaders.py, deduplicate load_json_or_jsonl across 14 feature_builders`
- `e870c9b refactor: deduplicate setup_logging across 22 feature_builders + master_feature_builder`
- `707c851 Add debug logging to 16 silent exception handlers in master_feature_builder`
- `47d9038 Add debug logging to catboost feature importance exception handler`
- `d9cf0ca Migrate logging.basicConfig to utils.logging_setup (models + feature_builders)`
- `00f5e8d Add docs/FEATURE_CATALOG.md with complete 291-feature catalog`
- `2f3d5fc Add SHA256 checksums for all master data, labels, and features files`
- `bdddb17 Add Elo rating feature builder for horses, jockeys and trainers`
- `2991cca Add recovery and fatigue feature builders for horse racing analysis`
- `9b68392 Fix sys.path in recovery and fatigue feature builders`
- `2c5dfe2 Add elo_rating_builder, recovery_features, fatigue_features to pipeline DAG`
- `25e3c39 Update FEATURE_CATALOG.md with 22 new features from Elo, Recovery, Fatigue builders`
- `f4a1715 Fix data format mismatches in scripts 42 and 49 that caused 0% feature output`

## Quality & Validation (13 commits)

- `b05c854 Ajout 5 scripts: remove_empty_fields, verify_fusion, export_triple, checksums, DAG visualizer`
- `46bface Validation croisee PASS + DAG diagram + audit trail + checksums + field rates`
- `c261e44 4 scripts integration: Timeform 68K, Sporting Life 32K, generic integrator, audit HTML`
- `ca45bfb Fix input paths in 17 pipeline scripts + add validate_data_quality.py`
- `5696496 Fix paths in 13 more scripts: core (01-14), patches, audit`
- `ca20a73 Fix relative paths in models/ and quality/ modules`
- `c1e1a9a Fix remaining 20 relative Path declarations in models/, patch_*, post_course/, quality/`
- `a0ef3f9 refactor: deduplicate setup_logging across 33 remaining files (models, quality, post_course, patches)`
- `d99f45c Exclude cache directories from quality tests to prevent timeouts`
- `e34b2db Add step 16 - final validation checklist for DATA folder completion`
- `1490455 Add data coverage report for partants_master.jsonl`
- `27e2525 Add validate_data_final.py for one-command data validation`
- `f9c6529 Regenerate CHECKSUMS.sha256 after pipeline re-run`

## Bug Fixes (27 commits)

- `5b4021b Patch JSONL 3 scripts lourds + création 8 scripts de calcul (41-49)`
- `e2543b6 Fix chemins Mac hardcodés + fix syntaxe nettoyage_global.py`
- `a2bed1c Fix 42 (jointure RP), 44 (encodage latin-1), 48 (NoneType), 41 (100% enrichment)`
- `f1a983b Fix encodage UTF-8 sur 8 scripts originaux pour Windows`
- `4efe790 TODO: 255 tâches cochées (était 86) + fix remove_empty_fields encodage`
- `a939531 Fix Unicode chars (fleches, emojis) pour compatibilite terminal Windows cp1252`
- `2caa774 Fix OSError sur gros fichiers Windows: readline + buffering 1MB`
- `1da7ace Fix 52_turfomania: nouveau flux 3 etapes (reunions -> courses -> partants)`
- `011f894 Fix deprecated datetime.utcnow() across 60 Python files`
- `772b18f Fix relative paths in 32 pipeline/data scripts: use os.path.abspath`
- `4c7b83d Add encoding="utf-8" to 28 files + json_to_jsonl converter + fix paths`
- `559c5ad Fix data_master paths in 15 scripts: use os.path.abspath`
- `7e0ea9a Fix input paths in 4 more scripts: pronostics, canalturf, meteo, reunions`
- `3132829 Fix paths in 12 core pipeline scripts (00-16): use Path(__file__).resolve()`
- `8b56ad0 Fix OUTPUT_DIR path in renormaliser.py`
- `7b11d4b Fix hardcoded relative paths in 3 remaining files`
- `0f32ffd Fix encoding, resource leak, and relative path bugs in 7 files`
- `6f74f43 Fix 2 compilation errors: leakage_detector global decl, hippodromes_db stub`
- `c03435b Fix json_to_jsonl.py crash on Decimal types from ijson`
- `adddd20 Fix 20 remaining relative LOG_DIR and path declarations`
- `b635380 fix: prevent CRLF line endings in generate_labels.py JSONL output`
- `aac1be2 Fix setup_logging imports in model files to use utils.logging_setup directly`
- `b842733 Fix hardcoded Python path, unused exception var, missing encoding params`
- `ef653b4 Fix missing hippodrome/discipline stats in nettoyage JSON branch`
- `5eba034 Fix Windows encoding issue in logging_setup console handler`
- `fbea7d4 Fix 23_pronostics_equidia to produce JSONL output from cache data`
- `e70955e Fix 02b aggregate_cache_to_jsonl to parse HTML cache files`

## Refactoring & Cleanup (18 commits)

- `e7344b5 refactor: create shared utils/types.py, deduplicate safe_int/safe_float across 8 files`
- `808baee refactor: create shared utils/logging_setup.py, deduplicate setup_logging across 16 core scripts`
- `aeb39c1 cleanup: remove 24 unused 'import sys' left behind by setup_logging refactoring`
- `b1790b6 cleanup: remove unused imports (unicodedata, re) from 7 files`
- `205b902 Migrate logging.basicConfig to utils.logging_setup (scripts 41-49)`
- `47b7a6d Migrate logging.basicConfig to utils.logging_setup (remaining utility files)`
- `59d0565 Migrate remaining smart_pause definitions to utils.scraping (5 files)`
- `4f24476 Migrate fetch_with_retry to utils.scraping (batch 1)`
- `6a1e71f Migrate remaining checkpoint/append_jsonl/fetch_with_retry to utils.scraping`
- `e2d22ac Migrate last duplicated checkpoint/fetch functions to utils.scraping`
- `cf40e35 Remove unused imports after utils.scraping migration (scripts 21-49 + 100-102)`
- `ab7200b Deduplicate save_jsonl, sauver_json, sauver_csv into utils/output.py`
- `2c5dc9b Migrate remaining new_session() calls to create_session() from utils.scraping`
- `aa3a0df Deduplicate safe_mean, safe_rate, safe_stdev into utils/math.py`
- `0dea80c Deduplicate utc_now_iso and normaliser_texte into shared utils modules`
- `646e006 Deduplicate extract_embedded_json and extract_data_attributes into utils/html_parsing.py`
- `4cf3220 Deduplicate rotate_session and aggregate_cache_to_jsonl into utils/scraping.py`
- `98ed599 Remove dead 'import logging' from 73 files that use setup_logging`

## Documentation (5 commits)

- `b89aa30 Mise à jour TODO_MACHINE_PUISSANTE.md — 56 tâches cochées (était 21)`
- `b346050 TODO: sections 4.5-4.7 mises a jour avec resultats reels + blocages identifies`
- `8292d05 docs: add module listing to utils/__init__.py`
- `99b430a Add PIPELINE_README.md with complete pipeline guide`
- `90255e7 Update docs/SOURCES.md with current data for all 92+ sources`

## Full Chronological Log

1. `198f4d5 Initial commit — pipeline data courses hippiques`
2. `80a8094 Add all remaining scripts, feature builders, pipeline, docs`
3. `e39751d Ajout post-processing (50+ features calculées) + audit + questions expert`
4. `7f19386 Ajout Entity Resolution + Feature Engineering complet (4 modules)`
5. `242c866 Ajout 3 modules Feature Engineering supplémentaires (pedigree, temporel, séquences)`
6. `b89aa30 Mise à jour TODO_MACHINE_PUISSANTE.md — 56 tâches cochées (était 21)`
7. `84900b2 Sauvegarde complète — scripts JSONL + contexte pour 2ème session`
8. `5b4021b Patch JSONL 3 scripts lourds + création 8 scripts de calcul (41-49)`
9. `9ab6576 Ajout pipeline complet étapes 2-5 : audit, nettoyage, comblage, mega-merge`
10. `35665ba Ajout déduplication + merge courses_master + comblage par inférence`
11. `ca80f1a Ajout 9 builders features + 10 scripts affinités croisées (étapes 6.2 + 6.3)`
12. `a98985b Mise à jour TODO — 19 builders + pipeline cochés, CONTEXT mis à jour`
13. `462a2a9 Session 2 complète : 11 builders + master orchestrator + 5 scrapers FR + 8 tests qualité + docs`
14. `4d10236 Ajout run_pipeline.py (DAG 70 étapes) + organize_project.py (réorg auto)`
15. `a0576c4 Ajout 5 scrapers UK (56-60) : Timeform, Sporting Life, At The Races, Racing TV, Oddschecker`
16. `65102aa Ajout monitor_pipeline + generate_labels + setup.py + .env.example`
17. `d7277ca Ajout 8 scrapers internationaux (61-68) : US, AU, HK, JP, Betfair`
18. `e2543b6 Fix chemins Mac hardcodés + fix syntaxe nettoyage_global.py`
19. `37c6db3 Fix mega_merge: encodage latin-1 fallback + limite 2GB + fix merge_meteo UTF-8`
20. `05a0d48 Fix 11 builders: logger et meteo_index rendus optionnels`
21. `6a4634a Recode master_feature_builder en streaming 2 passes (~3 GB RAM vs 50 GB)`
22. `a2bed1c Fix 42 (jointure RP), 44 (encodage latin-1), 48 (NoneType), 41 (100% enrichment)`
23. `f1a983b Fix encodage UTF-8 sur 8 scripts originaux pour Windows`
24. `28205be Ajout 5 scrapers (69-73) : OddsPortal, BetExplorer, AllBreedPedigree, Tattersalls, Goffs`
25. `b1dba73 Ajout 7 scrapers (74-80)`
26. `b608aaa Enrichissement 5 scrapers : extraction complète (JSON embarqué, commentaires, sectionals, GPS, stats)`
27. `a3632c0 Enrichissement 10 scrapers : extraction complète JSON/commentaires/stats/sectionals`
28. `1acf08a Ajout 10 scrapers (81-90) : Pronosoft, Turf-FR, LeTrot, Turfoo, Racing&Sports, SmartForm, Bloodstock, Weatherbys, Singapore, Korea`
29. `de23636 Features matrix COMPLETE: 36 GB, 2.93M records, streaming 2-pass`
30. `f7e9b61 Ajout section 4.5 enrichissement: 14 champs à combler avec taux réels mesurés`
31. `b05c854 Ajout 5 scripts: remove_empty_fields, verify_fusion, export_triple, checksums, DAG visualizer`
32. `4efe790 TODO: 255 tâches cochées (était 86) + fix remove_empty_fields encodage`
33. `834dd61 Ajout 5 scripts: enrichissement_champs, convert_parquet, stats_finales, validate_cross, audit_trail`
34. `a939531 Fix Unicode chars (fleches, emojis) pour compatibilite terminal Windows cp1252`
35. `46bface Validation croisee PASS + DAG diagram + audit trail + checksums + field rates`
36. `8630b7e Stats finales: 2.93M partants, 4700 jours (2013-2026), 527 hippodromes`
37. `a34fdd6 6 piliers qualite + enrichissement (pays +73%, ecart +63%)`
38. `c261e44 4 scripts integration: Timeform 68K, Sporting Life 32K, generic integrator, audit HTML`
39. `b346050 TODO: sections 4.5-4.7 mises a jour avec resultats reels + blocages identifies`
40. `3259f6b Sauvegarde liste complete 68 modules + 16 phases + 23 piliers (pour plus tard)`
41. `b83454e Sauvegarde Phase 3-4 ML (7 modules) - PAS EXECUTE, data d'abord`
42. `72a7f5f TODO: ajout section 4.8 taches reportees (Parquet, Playwright, APIs, permissions)`
43. `840ec35 6 scripts data: rebuild_mega, parquet_chunks, fix_permissions, scraper_audit, indexes, completeness`
44. `faee138 5 scrapers Playwright pour sites bloques (Zeturf, Equidia, Oddschecker, France Galop + base class)`
45. `259de60 7 scripts data: normalize hippo/disc, career stats cheval/jockey/trainer, course profiles, merge final`
46. `e2cf6c2 100 scrapers! Ajout 91-100: EquiRatings, OptixEQ, Raceform, Harness AU, Standardbred CA, NOAA, Meteostat, TurfTrax, Clerk of Course, Magic Millions`
47. `23100b9 10 scrapers mis a jour: cloudscraper (bypass Cloudflare) + HTML cache`
48. `a1fadab Completeness report + indexes rebuilds + scraper audit`
49. `2caa774 Fix OSError sur gros fichiers Windows: readline + buffering 1MB`
50. `8c52551 Scrapers session: 6 scrapers fixes + 3 nouveaux + batch scraper + quality updates`
51. `1da7ace Fix 52_turfomania: nouveau flux 3 etapes (reunions -> courses -> partants)`
52. `73ddd56 Fix deprecated datetime.utcnow() in letrot scraper`
53. `011f894 Fix deprecated datetime.utcnow() across 60 Python files`
54. `ccc33a0 Add status_report.py for pipeline health monitoring`
55. `b4bfe23 Add .gitattributes for consistent LF line endings`
56. `d1fb49e Fix 16 bugs across 14 scrapers: encoding, paths, cloudscraper safety`
57. `4eb3694 Fix relative paths in 51 scrapers: use os.path.abspath for OUTPUT_DIR`
58. `772b18f Fix relative paths in 32 pipeline/data scripts: use os.path.abspath`
59. `4c7b83d Add encoding="utf-8" to 28 files + json_to_jsonl converter + fix paths`
60. `43a87d6 Add encoding="utf-8" to write-mode open() in 31 files`
61. `0b29b98 Fix input paths in 7 merge scripts to use BASE_DIR`
62. `559c5ad Fix data_master paths in 15 scripts: use os.path.abspath`
63. `498174c Fix remaining data_master output paths in 7 merge scripts`
64. `3a9e9fb Add source field to PMU API scraper records`
65. `5f884c0 Add encoding="utf-8" to remaining write calls in turfostats scraper`
66. `63c2dbe Fix remaining hardcoded paths in mega_merge and process_sire scripts`
67. `ca45bfb Fix input paths in 17 pipeline scripts + add validate_data_quality.py`
68. `7e0ea9a Fix input paths in 4 more scripts: pronostics, canalturf, meteo, reunions`
69. `3132829 Fix paths in 12 core pipeline scripts (00-16): use Path(__file__).resolve()`
70. `5696496 Fix paths in 13 more scripts: core (01-14), patches, audit`
71. `948ad03 Fix paths in 10 utility scripts: normalize, rebuild, pilier, merge`
72. `8b56ad0 Fix OUTPUT_DIR path in renormaliser.py`
73. `7b11d4b Fix hardcoded relative paths in 3 remaining files`
74. `0f32ffd Fix encoding, resource leak, and relative path bugs in 7 files`
75. `4c86574 Fix PMU API scraper crash on corrupted cache files`
76. `c4147e6 Replace 24 bare except clauses with specific exception types`
77. `6f74f43 Fix 2 compilation errors: leakage_detector global decl, hippodromes_db stub`
78. `ca20a73 Fix relative paths in models/ and quality/ modules`
79. `c03435b Fix json_to_jsonl.py crash on Decimal types from ijson`
80. `adddd20 Fix 20 remaining relative LOG_DIR and path declarations`
81. `c1e1a9a Fix remaining 20 relative Path declarations in models/, patch_*, post_course/, quality/`
82. `2ca7ff0 Add missing __init__.py files across pipeline/ package structure`
83. `c6673d1 refactor: create shared utils/normalize.py, deduplicate normalize_name() across 11 files`
84. `e7344b5 refactor: create shared utils/types.py, deduplicate safe_int/safe_float across 8 files`
85. `5ad6a2b refactor: create shared utils/loaders.py, deduplicate load_json_or_jsonl across 14 feature_builders`
86. `42a1759 refactor: add strip_accents() and normalize_date() to shared utils/normalize.py`
87. `f494728 fix: close resource leaks in 45_graphe_relations_gnn.py and mega_merge_partants_master.py`
88. `808baee refactor: create shared utils/logging_setup.py, deduplicate setup_logging across 16 core scripts`
89. `e870c9b refactor: deduplicate setup_logging across 22 feature_builders + master_feature_builder`
90. `a0ef3f9 refactor: deduplicate setup_logging across 33 remaining files (models, quality, post_course, patches)`
91. `aeb39c1 cleanup: remove 24 unused 'import sys' left behind by setup_logging refactoring`
92. `b1790b6 cleanup: remove unused imports (unicodedata, re) from 7 files`
93. `8292d05 docs: add module listing to utils/__init__.py`
94. `b635380 fix: prevent CRLF line endings in generate_labels.py JSONL output`
95. `2c408fe fix: add missing 'import requests' to 10 scrapers with cloudscraper fallback`
96. `aac1be2 Fix setup_logging imports in model files to use utils.logging_setup directly`
97. `f25b474 Add newline="\n" to JSONL writers to prevent CRLF on Windows`
98. `fcf0955 Add newline="\n" to remaining 78 JSONL writers (batch 2)`
99. `b842733 Fix hardcoded Python path, unused exception var, missing encoding params`
100. `a68de2a Add utils/scraping.py with shared scraper utilities`
101. `ef653b4 Fix missing hippodrome/discipline stats in nettoyage JSON branch`
102. `c44c13d Fix PMU scraper --resume ignoring checkpoint before default start date`
103. `76b4543 Improve PMU scraper cache error handling with OSError and logging`
104. `4c40506 Fix --resume checkpoint bug in 33 scrapers`
105. `5367676 Add error logging to silent exception handlers across 10 files`
106. `776e7fc Add error logging to silent exception handlers (batch 2, 5 files)`
107. `707c851 Add debug logging to 16 silent exception handlers in master_feature_builder`
108. `cde0156 Add error logging to silent exception handlers (batch 3, 3 files)`
109. `e416f76 Add error logging to silent exception handlers (batch 4, 2 files)`
110. `47d9038 Add debug logging to catboost feature importance exception handler`
111. `d591266 Migrate logging.basicConfig to utils.logging_setup (scrapers 21-30)`
112. `4b689fa Add error logging to remaining silent exception handlers (batch 5, 11 files)`
113. `205b902 Migrate logging.basicConfig to utils.logging_setup (scripts 41-49)`
114. `9b5eed9 Migrate logging.basicConfig to utils.logging_setup (scrapers 31-39)`
115. `ae1dfb3 Migrate logging.basicConfig to utils.logging_setup (scrapers 60-70)`
116. `7d74771 Migrate logging.basicConfig to utils.logging_setup (scrapers 51-59)`
117. `b5ce52d Migrate logging.basicConfig to utils.logging_setup (scrapers 71-80)`
118. `210cdb4 Migrate logging.basicConfig to utils.logging_setup (scrapers 81-99)`
119. `d2a6df5 Migrate logging.basicConfig to utils.logging_setup (merge/processing files)`
120. `5ac84f5 Migrate logging.basicConfig to utils.logging_setup (scrapers 100-102 + audit/batch files)`
121. `d9cf0ca Migrate logging.basicConfig to utils.logging_setup (models + feature_builders)`
122. `47b7a6d Migrate logging.basicConfig to utils.logging_setup (remaining utility files)`
123. `5eba034 Fix Windows encoding issue in logging_setup console handler`
124. `6b5ad40 Migrate smart_pause to utils.scraping (scrapers 61-70)`
125. `755a667 Migrate smart_pause to utils.scraping (scrapers 51-60)`
126. `0b2d4b6 Migrate smart_pause to utils.scraping (scrapers 71-80)`
127. `e4bd026 Migrate smart_pause to utils.scraping (scrapers 81-99)`
128. `d77f6eb Migrate smart_pause to utils.scraping (remaining scrapers 21-39 + 100-102)`
129. `59d0565 Migrate remaining smart_pause definitions to utils.scraping (5 files)`
130. `4f24476 Migrate fetch_with_retry to utils.scraping (batch 1)`
131. `6a7d52e Migrate checkpoint + append_jsonl to utils.scraping (scrapers 51-70)`
132. `da0d5ab Migrate checkpoint + append_jsonl to utils.scraping (scrapers 71-99)`
133. `6a1e71f Migrate remaining checkpoint/append_jsonl/fetch_with_retry to utils.scraping`
134. `e2d22ac Migrate last duplicated checkpoint/fetch functions to utils.scraping`
135. `46d15aa Add __all__ exports to utils package modules`
136. `47efdc9 Remove unused imports after utils.scraping migration (scrapers 51-70)`
137. `cf40e35 Remove unused imports after utils.scraping migration (scripts 21-49 + 100-102)`
138. `a6bd429 Fix PMU scraper crash on Windows file lock when removing corrupt cache`
139. `fbea7d4 Fix 23_pronostics_equidia to produce JSONL output from cache data`
140. `fb2eac3 Fix scrapers 24-26 to produce JSONL output from cache data`
141. `9b4e225 Fix scrapers 59, 63, 78, 86, 90 to produce JSONL output from cache data`
142. `f07bfed Fix scrapers 13, 16, 19, 20 to produce JSONL output from cache data`
143. `d99f45c Exclude cache directories from quality tests to prevent timeouts`
144. `75e66db Fix 02b_scraper_letrot to produce JSONL output from cache data`
145. `2d05732 Add --export flag to scrapers 24, 25, 26 for cache-to-JSONL export`
146. `e70955e Fix 02b aggregate_cache_to_jsonl to parse HTML cache files`
147. `e34b2db Add step 16 - final validation checklist for DATA folder completion`
148. `1490455 Add data coverage report for partants_master.jsonl`
149. `08e151b Add DATA_DICTIONARY.md with all partants_master fields`
150. `99b430a Add PIPELINE_README.md with complete pipeline guide`
151. `27e2525 Add validate_data_final.py for one-command data validation`
152. `00f5e8d Add docs/FEATURE_CATALOG.md with complete 291-feature catalog`
153. `2f3d5fc Add SHA256 checksums for all master data, labels, and features files`
154. `90255e7 Update docs/SOURCES.md with current data for all 92+ sources`
155. `a3a2f33 Migrate 55_equidia_data_scraper from requests/cloudscraper to Playwright`
156. `b5631f3 Update 60_oddschecker_scraper to use Playwright instead of requests`
157. `e27dd8f Migrate scrapers 58, 62, 64, 65 from requests/cloudscraper to Playwright`
158. `aa15aed Migrate scrapers 52, 54, 80 from requests/cloudscraper to Playwright`
159. `c166261 Migrate scrapers 53, 59, 66, 69 from requests/cloudscraper to Playwright`
160. `9cf2ee3 Add shared utils/playwright.py module for Playwright scraper helpers`
161. `46bd62b Replace local load_json_safe/load_json_or_jsonl with imports from utils.loaders`
162. `3d8df79 Migrate 13 scrapers to use shared utils/playwright.py helpers`
163. `5b6db51 Add NOTE comments for incompatible local safe_float/safe_int/normalize_name`
164. `ff10144 Add shared create_session() to utils/scraping.py and migrate scrapers 20-39`
165. `d74f3b2 Migrate scrapers 51-99 and clean up 20-26 to use shared create_session()`
166. `ab7200b Deduplicate save_jsonl, sauver_json, sauver_csv into utils/output.py`
167. `2c5dc9b Migrate remaining new_session() calls to create_session() from utils.scraping`
168. `aa3a0df Deduplicate safe_mean, safe_rate, safe_stdev into utils/math.py`
169. `0dea80c Deduplicate utc_now_iso and normaliser_texte into shared utils modules`
170. `646e006 Deduplicate extract_embedded_json and extract_data_attributes into utils/html_parsing.py`
171. `378c628 Deduplicate sauver_parquet into utils/output.py`
172. `c251460 Remove dead imports from utils.* across 11 scraper/pipeline files`
173. `4cf3220 Deduplicate rotate_session and aggregate_cache_to_jsonl into utils/scraping.py`
174. `bdddb17 Add Elo rating feature builder for horses, jockeys and trainers`
175. `2991cca Add recovery and fatigue feature builders for horse racing analysis`
176. `9b68392 Fix sys.path in recovery and fatigue feature builders`
177. `cb3843e Deduplicate save helpers in 01_calendrier_reunions Sauvegarder class`
178. `a34ca13 Update SOURCES.md with latest scraper counts and add Feature Builders section`
179. `f896a92 Migrate scrapers 91, 94, 95, 100 from requests to Playwright`
180. `5c1e9f1 Add missing __init__.py to scrapers_playwright/ and scripts/ packages`
181. `33ca359 Update requirements.txt with missing packages and organize by category`
182. `17096f6 Improve .gitignore with broader backup, packaging, and IDE patterns`
183. `98ed599 Remove dead 'import logging' from 73 files that use setup_logging`
184. `2c5dfe2 Add elo_rating_builder, recovery_features, fatigue_features to pipeline DAG`
185. `25e3c39 Update FEATURE_CATALOG.md with 22 new features from Elo, Recovery, Fatigue builders`
186. `f9c6529 Regenerate CHECKSUMS.sha256 after pipeline re-run`
187. `f4a1715 Fix data format mismatches in scripts 42 and 49 that caused 0% feature output`