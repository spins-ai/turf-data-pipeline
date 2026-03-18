# Contexte projet — Phase 1 : Data Warehouse hippique

## Projet
Système de prédiction hippique : 68 modules, 24 modèles ML, 16 phases.
Phase 1 = collecte de données dans `~/models hybride/` (ce dossier).
Phase 2 = modèles ML dans un NOUVEAU dossier (pas encore créé).

## Machine : MacBook M1 16 GB RAM
CRITIQUE : la RAM est limitée. Les scripts doivent utiliser le format JSONL (append) et NON charger les gros JSON en mémoire.

## Données collectées (25 GB, dans output/)
- 41,477 réunions hippiques (2013-2026)
- 217,591 courses normalisées
- 2,589,758 partants normalisés (66 champs)
- 20 sources de données, 7 endpoints PMU
- ~258 millions de valeurs individuelles

## Scripts patchés JSONL (légers, ~15 MB RAM chacun)
Ces scripts ont TOUS été convertis pour utiliser JSONL au lieu de JSON :
- `02_liste_courses.py` — PATCHÉ session 2 — 4 fichiers JSONL (courses_brut/norm, partants_brut/norm) + JsonlWriter class + --rebuild flag
- `14_pedigree_scraper.py` — PATCHÉ session 2 — streaming partants (ijson ou fallback) + append JSONL
- `21_rapports_definitifs.py` — 240K records dans .jsonl
- `22_performances_detaillees.py` — 245K records dans .jsonl
- `27_citations_enjeux.py` — 1.5M records dans .jsonl (en cours ~15%)
- `28_combinaisons_marche.py` — 6M records dans .jsonl
- `37_rpscrape_racing_post.py` — PATCHÉ session 2 — checkpoint par course + append JSONL
- `38_rapports_internet.py` — 3.2M records dans .jsonl
- `39_reunions_enrichies.py` — 230K records dans .jsonl (en cours ~36%)

### Pattern JSONL appliqué :
1. `load_courses()` ne garde que les clés nécessaires + `del data` après chargement
2. Pas de `all_records = json.load(file)` au démarrage
3. Append JSONL : `open(file.jsonl, "a")` + `f.write(json.dumps(r) + "\n")`
4. Checkpoint ne stocke que `last_index` + `total_records`

## Scripts de calcul CRÉÉS (session 2 — 0 requête API, calcul local)
Tous ces scripts ont été créés et sont prêts à exécuter :
- `41_sequences_performances.py` — ~30 features séquences (trend, momentum, séries, repos, volatilité) pour LSTM/GRU/TFT
- `42_croisement_racing_post_pmu.py` — ~15 features RPR/TopSpeed/class_rating depuis Racing Post
- `43_croisement_meteo_courses.py` — ~20 features météo exacte + historique terrain par cheval
- `44_croisement_pedigree_partants.py` — ~25 features pedigree (sire stats progressives, inbreeding, stamina/speed index)
- `45_graphe_relations_gnn.py` — ~15 features graphe (duo cheval-jockey, cheval-entraineur, cheval-hippodrome) + edges JSONL
- `46_track_bias_speed_class.py` — ~25 features track bias + speed figures normalisées + class ratings + field_strength
- `48_parse_conditions_texte.py` — ~20 features regex (âge, sexe, poids, gains, groupe, handicap, apprentis)
- `49_ecart_cotes_internet_national.py` — ~20 features market (CLV, steam move, sharp money, overbet/underbet, market efficiency)

## Checkpoints et reprise
Tous les scripts ont des checkpoints (.checkpoint_XX.json). Ils reprennent automatiquement.
ATTENTION : les scripts 21, 22, 28, 38 ont leur checkpoint "au bout" (last_index = taille ancienne liste).
Si le script 02 a ajouté de nouvelles courses depuis, il faut RESET les checkpoints de ces 4 scripts
pour qu'ils re-scannent les nouvelles courses ajoutées.

## Endpoints PMU utilisés
1. `offline.turfinfo.api.pmu.fr/.../participants` — partants + résultats
2. `online.turfinfo.api.pmu.fr/.../rapports-definitifs` — dividendes nationaux
3. `offline.turfinfo.api.pmu.fr/.../rapports-definitifs?specialisation=INTERNET` — e-paris
4. `offline.turfinfo.api.pmu.fr/.../performances-detaillees/pretty` — 9 dernières courses
5. `offline.turfinfo.api.pmu.fr/.../citations` — enjeux par cheval
6. `offline.turfinfo.api.pmu.fr/.../combinaisons` — masse d'enjeux par combinaison
7. `offline.turfinfo.api.pmu.fr/.../R{r}` — météo, incidents, conditions réunion

## Champs PMU manquants (à ajouter via script 40, déjà fait)
gainsParticipant (5 sous-champs), dernierRapportDirect/Reference, idCheval,
nomPereMere, handicapValeur, eleveur, race, robe, nombrePlacesSecond/Troisieme,
paysEntrainement — script 40 a déjà enrichi 2.5M records.

## 24 modèles ML prévus (phase 2)
logistic_regression, random_forest, xgboost, lightgbm, catboost, mlp, lstm, gru,
tabnet, tft, gnn, bayesian_nn, survival_model, quantile_regressor, autogluon, tpot,
h2o, stacking_classifier, blending, meta_model, anomalie_detector, retour_forme_hidden,
gan_turf, value_hunter_rl

## Pipeline complet CRÉÉ (session 2 — 18 mars 2026)
Tous ces scripts sont prêts à exécuter dans l'ordre :
1. `audit_data_integrity.py` — audit JSON/JSONL, doublons, outliers, taux remplissage
2. `nettoyage_global.py` — fix UTF-8, normalisation noms/hippos/disciplines
3. `deduplication.py` — déduplique courses/partants/pedigrees/rapports
4. `comblage_trous.py` — remplissage depuis météo/SIRE/hippodromes + inférence
5. `merge_02_02b_courses_master.py` — fusionne 02+02b → courses_master.jsonl
6. `mega_merge_partants_master.py` — fusion toutes sources → partants_master.jsonl
7. Scripts 41-49 (calcul features)
8. 9 builders features dans feature_builders/
9. 10 scripts affinités croisées (feat_cheval_*, feat_jockey_*, etc.)

## 9 Builders features CRÉÉS (feature_builders/)
- perf_detaillees_builder.py, smarkets_builder.py, racing_post_builder.py
- reunions_builder.py, enrichissement_builder.py, pedigree_advanced_builder.py
- canalturf_builder.py, turfostats_builder.py, geny_builder.py

## 10 Scripts affinités croisées CRÉÉS
- feat_cheval_jockey_affinity.py, feat_cheval_hippodrome_affinity.py
- feat_cheval_distance_affinity.py, feat_cheval_terrain_affinity.py
- feat_jockey_entraineur_combo.py, feat_entraineur_hippodrome.py
- feat_value_betting.py, feat_meteo_terrain_interaction.py
- feat_pedigree_discipline_match.py, feat_field_strength.py

## Prochaines étapes (session 3+)
- EXÉCUTER tous les scripts ci-dessus dans l'ordre
- Étape 6.1 : Debugger les 177 features cassées (11 builders existants)
- Étape 6.4 : Reconstruire la matrice de features (400+ colonnes)
- Étape 7+ : Nouveaux scrapers (FR, UK, US, AU, etc.)
- Phase 2 : Modèles ML (dans un nouveau dossier)
