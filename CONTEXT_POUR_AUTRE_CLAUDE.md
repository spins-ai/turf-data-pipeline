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

## Prochaines étapes (session 3+)
- Exécuter les 8 scripts de calcul (41-49) sur la machine puissante
- Étape 2 du TODO : audit/vérification de tous les JSON
- Étape 3 : nettoyage global (encodage, normalisation, déduplication)
- Étape 4 : comblage de trous (croisements entre sources)
- Étape 5 : fusion/mega-merge → partants_master.json
- Étape 6 : feature engineering complet (400+ colonnes)
