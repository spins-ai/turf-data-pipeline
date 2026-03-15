# ════════════════════════════════════════════════════════════════
# 🗄️ TODO MASTER - BASE DE DONNÉES HIPPIQUES COMPLÈTE
# ════════════════════════════════════════════════════════════════
# Objectif : dossier DATA 100% terminé, propre, documenté,
# modulable, facile à naviguer, sauvegardé, maintenable.
# Les modèles (autre dossier) n'ont qu'à se brancher dessus.
# ════════════════════════════════════════════════════════════════

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 1 — FINIR LA COLLECTE EN COURS  │
# └─────────────────────────────────────────┘

## 1.1 Scripts terminés ✅
- [x] 01_calendrier_reunions (686 MB — calendrier 10 ans)
- [x] 02_liste_courses (14 GB — 221,570 courses + 2.7M partants)
- [x] 02b_scraper_letrot (15 GB — Le Trot HTML brut)
- [x] 02b_liste_courses_2013 (3.2 GB — données 2004-2013)
- [x] 05_historique_chevaux (324 MB — stats carrière par cheval)
- [x] 06_historique_jockeys (14 MB — stats jockeys)
- [x] 07_cotes_marche (286 MB — cotes PMU)
- [x] 08_pedigree (20 MB — stats mères/pères)
- [x] 09_equipements (319 MB — œillères/déferré)
- [x] 10_poids_handicaps (141 MB — poids portés/handicaps)
- [x] 11_sectionals (133 MB — temps sectionnels)
- [x] 13_meteo_historique (71 MB — météo par course)
- [x] 17_sire_ifce (1.1 GB — SIRE/IFCE données élevage)
- [x] 22_performances_detaillees (12 GB — 9 dernières courses)
- [x] 24_canalturf (41 MB — stats CanalTurf)
- [x] 25_turfostats (27 MB — stats TurfoStats)
- [x] 26_geny (44 MB — données Geny)
- [x] 30_smarkets_exchange (640 KB — cotes exchange)
- [x] 36_pedigree_query (39 MB — pedigree 4 générations)
- [x] 39_reunions_enrichies (2 GB — météo/incidents/paris)
- [x] 40_enrichissement_partants (655 MB — cotes tendance)

## 1.2 Scripts en cours 🔄 (attendre la fin)
- [ ] 04_resultats — rapports définitifs PMU (221K courses)
- [ ] 14_pedigree_scraper — pedigree 4 gen (15.6%, ETA ~2 jours)
- [ ] 21_rapports_definitifs — rapports officiels (74%)
- [ ] 23_pronostics_equidia — pronostics PMU (lancé)
- [ ] 27_citations_enjeux — citations/enjeux (37%)
- [ ] 28_combinaisons_marche — combinaisons (57%)
- [ ] 37_rpscrape_racing_post — Racing Post UK (5,780 pages)
- [ ] 38_rapports_internet — rapports internet (65%)
- [ ] fetch_openmeteo_missing — météo mondiale (12,754 cache)

## 1.3 Scripts à relancer / compléter
- [ ] Vérifier que le monitor auto-relance bien si crash
- [ ] Relancer 16_nanaelie si données incomplètes 2004-2013
- [ ] Relancer 30_smarkets pour plus de cotes exchange
- [ ] Relancer 35_meteo_france (données payantes Météo France)
- [ ] Vérifier 18_letrot_records (152 KB seulement, incomplet ?)
- [ ] Vérifier 19_boturfers_stats (632 KB seulement)
- [ ] Vérifier 20_ifce_stats (252 KB seulement)
- [ ] Lancer 12_pedigree_scraper consolidation (544 cache → fichier)

## 1.4 Backup intermédiaire #1
- [ ] Sauvegarder tout le dossier après fin de tous les scripts
- [ ] Vérifier intégrité backup (comparer tailles)
- [ ] Garder backup_complet_20260315 comme point de restauration

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 2 — VÉRIFICATION & INTÉGRITÉ    │
# └─────────────────────────────────────────┘

## 2.1 Audit de chaque fichier JSON
- [ ] Vérifier que chaque JSON est valide (pas tronqué)
- [ ] Compter les records par fichier vs attendu
- [ ] Identifier les fichiers de 0 bytes
- [ ] Identifier les JSON mal fermés (tronqués mid-object)
- [ ] Lister les fichiers cache corrompus
- [ ] Vérifier cohérence entre cache et fichiers consolidés

## 2.2 Audit des données
- [ ] Compter les doublons par source (course_uid, partant_uid)
- [ ] Vérifier les plages de dates (2014-2026 attendu)
- [ ] Vérifier couverture par année (pas de trous)
- [ ] Vérifier couverture par hippodrome
- [ ] Vérifier couverture par discipline (trot attelé, trot monté, galop plat, obstacle, steeple)
- [ ] Identifier les outliers évidents (cotes négatives, distances aberrantes, etc.)
- [ ] Vérifier les types de données (string vs int vs float)
- [ ] Vérifier les valeurs possibles pour chaque champ catégoriel

## 2.3 Rapport d'audit
- [ ] Générer un rapport HTML/MD avec stats par source
- [ ] Nombre de records, champs, taux de remplissage par champ
- [ ] Graphiques de couverture temporelle
- [ ] Liste des anomalies trouvées
- [ ] Sauvegarder le rapport dans docs/

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 3 — NETTOYAGE GLOBAL            │
# └─────────────────────────────────────────┘

## 3.1 Encodage
- [ ] Fix UTF-8 cassé sur tous les fichiers JSON
- [ ] Normaliser les accents (é, è, ê, ë → cohérent)
- [ ] Normaliser la casse des noms (chevaux, jockeys, hippodromes)
- [ ] Supprimer les caractères spéciaux parasites

## 3.2 Nettoyage des valeurs
- [ ] Uniformiser les formats de date (ISO 8601 partout)
- [ ] Uniformiser les formats numériques (pas de virgule/point mixte)
- [ ] Remplacer les "null", "None", "", "N/A" → null cohérent
- [ ] Supprimer les espaces en début/fin de chaîne
- [ ] Normaliser les noms d'hippodromes (vincennes vs VINCENNES vs Vincennes)
- [ ] Normaliser les noms de jockeys (accent, tirets, espaces)
- [ ] Normaliser les noms d'entraîneurs
- [ ] Normaliser les disciplines (TROT_ATTELE vs trot_attele vs Trot Attelé)

## 3.3 Déduplication
- [ ] Dédupliquer les courses (même course dans 02 et 02b)
- [ ] Dédupliquer les partants
- [ ] Dédupliquer les pedigrees (même cheval dans 08, 12, 14, 36)
- [ ] Dédupliquer les rapports (même rapport dans 21 et 38)
- [ ] Garder la version la plus complète en cas de doublon

## 3.4 Suppression des données inutiles
- [ ] Identifier et supprimer les champs toujours vides (100% null)
- [ ] Identifier et supprimer les champs redondants
- [ ] Supprimer les champs techniques internes (timestamps scraping, etc.)
- [ ] Supprimer les fichiers temporaires / logs de debug dans output/

## 3.5 Backup intermédiaire #2
- [ ] Sauvegarder après nettoyage
- [ ] Log des modifications effectuées

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 4 — COMBLAGE DE TROUS           │
# └─────────────────────────────────────────┘

## 4.1 Champs à remplir depuis sources existantes
- [ ] penetrometre (56% vide) → croiser avec réunions enrichies (39) + météo
- [ ] condition_age (51% vide) → regex depuis conditions_texte
- [ ] pays_cheval → croiser avec SIRE/IFCE (17)
- [ ] eleveur → croiser avec SIRE/IFCE (17)
- [ ] is_disqualifie (17% incohérent) → vérifier via rapports définitifs (04/21)
- [ ] type_piste manquant → croiser avec hippodromes_db.py
- [ ] corde manquante → croiser avec hippodromes_db.py
- [ ] altitude hippodrome → déjà fait dans hippodromes_db.py, propager
- [ ] GPS coordonnées → déjà fait, propager
- [ ] sexe_cheval manquant → croiser avec SIRE/IFCE (17)
- [ ] race_cheval manquant → croiser avec SIRE/IFCE (17)
- [ ] date_naissance_cheval → croiser avec SIRE/IFCE (17)
- [ ] nombre_partants si manquant → compter depuis partants
- [ ] allocation si manquant → croiser avec rapports (21/38)

## 4.2 Comblage par inférence
- [ ] Terrain probable si manquant (inférer depuis météo + historique hippo)
- [ ] Distance réelle si manquant (inférer depuis type course + hippo)
- [ ] Poids porté si manquant (handicap officiel + surcharge)
- [ ] Cote finale si manquant (dernière cote connue)
- [ ] Temps course si manquant (inférer depuis réduction km + distance)

## 4.3 Comblage par croisement de sources
- [ ] Croiser PMU (02) + Le Trot (02b) → compléter mutuellement
- [ ] Croiser résultats (04) + rapports (21) → positions confirmées
- [ ] Croiser météo France (35) + Open-Meteo + NASA → météo la plus complète
- [ ] Croiser pedigree (08+12+14+36) → pedigree le plus complet possible
- [ ] Croiser canalturf (24) + turfostats (25) + geny (26) → stats consensus
- [ ] Croiser rapports définitifs (21) + internet (38) → rapports complets

## 4.4 Vérification post-comblage
- [ ] Recalculer les taux de remplissage pour chaque champ
- [ ] Comparer avant/après pour chaque champ comblé
- [ ] Vérifier cohérence des valeurs inférées
- [ ] Log de tout ce qui a été comblé et comment

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 5 — FUSION / CONSOLIDATION      │
# └─────────────────────────────────────────┘

## 5.1 Fusions principales
- [ ] Merger 02 + 02b → courses_master.json (toutes les courses PMU+LeTrot)
- [ ] Merger 08 + 12 + 14 + 36 → pedigree_master.json (tout le pedigree)
- [ ] Merger 21 + 38 → rapports_master.json (tous les rapports)
- [ ] Merger 13 + 35 + Open-Meteo → meteo_master.json (toute la météo)
- [ ] Merger 24 + 25 + 26 → stats_externes_master.json (CanalTurf+TurfoStats+Geny)
- [ ] Merger 27 + 28 → marche_master.json (citations+combinaisons)

## 5.2 Mega-merge : partants enrichis
- [ ] Partir de partants_normalises (2.7M records)
- [ ] Joindre : historique cheval (05)
- [ ] Joindre : historique jockey (06)
- [ ] Joindre : cotes marché (07)
- [ ] Joindre : pedigree_master
- [ ] Joindre : équipements (09)
- [ ] Joindre : poids/handicaps (10)
- [ ] Joindre : sectionals (11)
- [ ] Joindre : meteo_master
- [ ] Joindre : SIRE/IFCE (17)
- [ ] Joindre : performances détaillées (22)
- [ ] Joindre : rapports_master
- [ ] Joindre : pronostics (23)
- [ ] Joindre : stats_externes_master
- [ ] Joindre : marche_master
- [ ] Joindre : Racing Post (37)
- [ ] Joindre : réunions enrichies (39)
- [ ] Joindre : enrichissement partants (40)
- [ ] Joindre : Smarkets exchange (30)
- [ ] Joindre : hippodromes_db.py (GPS, altitude, piste)
- [ ] Résultat → partants_master.json (LE fichier maître)

## 5.3 Vérification post-fusion
- [ ] Vérifier nombre records (doit être ≥ 2.7M)
- [ ] Compter nombre de colonnes (cible: 200+)
- [ ] Vérifier qu'aucun record n'a été perdu
- [ ] Vérifier les jointures (pas de décalage)
- [ ] Sample aléatoire de 100 records pour vérification manuelle

## 5.4 Backup intermédiaire #3
- [ ] Sauvegarder après fusion
- [ ] Versionner les fichiers maîtres

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 6 — FEATURE ENGINEERING         │
# └─────────────────────────────────────────┘

## 6.1 Fixer les 177 features cassées (builders existants)
- [ ] Debugger musique_features.py (22 features)
- [ ] Debugger temps_features.py (15 features)
- [ ] Debugger profil_cheval_features.py (24 features)
- [ ] Debugger equipement_features.py (16 features)
- [ ] Debugger poids_features.py (15 features)
- [ ] Debugger meteo_features.py (15 features)
- [ ] Debugger combo_features.py (13 features)
- [ ] Debugger class_change_features.py (11 features)
- [ ] Debugger interaction_features.py (10 features)
- [ ] Debugger precomputed_partant_joiner.py (14 features)
- [ ] Debugger precomputed_entity_joiner.py (22 features)
- [ ] Tester chaque builder individuellement
- [ ] Vérifier que les 177 features ne sont plus None

## 6.2 Créer 9 nouveaux builders (sources existantes non exploitées)
- [ ] perf_detaillees_builder.py (40-60 features depuis output/22)
       → rolling moyennes, volatilité, best/worst perf, patterns
- [ ] smarkets_builder.py (15-20 features exchange)
       → spread back/lay, volume, market efficiency
- [ ] racing_post_builder.py (10-15 features)
       → RPR, TopSpeed, class rating international
- [ ] reunions_builder.py (15-20 features)
       → météo PMU directe, incidents, types paris, audience
- [ ] enrichissement_builder.py (8 features)
       → gains décomposées, tendance cote, grosse prise
- [ ] pedigree_advanced_builder.py (15-20 features)
       → grands-parents, inbreeding, lignée, stamina/speed index
- [ ] canalturf_builder.py (10-15 features)
       → stats alternatives, cross-validation
- [ ] turfostats_builder.py (10-15 features)
       → keyrace index, style course, affinité distance
- [ ] geny_builder.py (10-15 features)
       → consensus pronostiqueurs, score commentaires

## 6.3 Créer features croisées (combinaisons entre sources)
- [ ] cheval_jockey_affinity.py (10 features)
       → duo historique, taux victoire ensemble, affinité
- [ ] cheval_hippodrome_affinity.py (8 features)
       → affinité piste, perf par track, première fois
- [ ] cheval_distance_affinity.py (8 features)
       → distance optimale, écart à l'optimale
- [ ] cheval_terrain_affinity.py (6 features)
       → perf par going, terrain optimal
- [ ] jockey_entraineur_combo.py (6 features)
       → combo gagnante, spécialité discipline
- [ ] entraineur_hippodrome.py (5 features)
       → spécialiste piste, déplacement
- [ ] value_betting_features.py (10 features)
       → CLV, steam moves, sharp money, overbet/underbet
- [ ] meteo_terrain_interaction.py (8 features)
       → pluie×going, sol gelé, vent face
- [ ] pedigree_discipline_match.py (10 features)
       → lignée adaptée, stamina index, precocity
- [ ] field_strength_features.py (10 features)
       → force du lot, concentration cotes, hétérogénéité

## 6.4 Reconstruire la matrice de features
- [ ] Exécuter master_feature_builder.py avec TOUS les builders
- [ ] Vérifier que la matrice contient 400+ colonnes
- [ ] Vérifier taux de remplissage par feature
- [ ] Supprimer features avec >90% de None
- [ ] Log du nombre de features et stats

## 6.5 Backup intermédiaire #4
- [ ] Sauvegarder après feature engineering

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 7 — COLLECTE NOUVELLES SOURCES  │
# │  (Machine puissante - lundi)            │
# └─────────────────────────────────────────┘

## 7A - Sources françaises
- [ ] Écrire scraper Zeturf
- [ ] Écrire scraper Turfomania
- [ ] Écrire scraper Paris-Turf
- [ ] Écrire scraper TurfInfo
- [ ] Écrire scraper Tiercé Magazine
- [ ] Écrire scraper Equidia data
- [ ] Écrire scraper Turf-France
- [ ] Écrire scraper TurfPronos
- [ ] Écrire scraper TurfActu
- [ ] Écrire scraper Turf-VIP
- [ ] Lancer tous les scrapers FR
- [ ] Vérifier les données collectées
- [ ] Intégrer dans le pipeline

## 7B - Sources UK
- [ ] Écrire scraper Timeform (ratings, speed figures)
- [ ] Écrire scraper GeeGeez Gold
- [ ] Écrire scraper Proform Racing
- [ ] Écrire scraper Smartform
- [ ] Écrire scraper HorseRaceBase
- [ ] Écrire scraper At The Races
- [ ] Écrire scraper Sporting Life
- [ ] Écrire scraper Racing TV
- [ ] Écrire scraper Racing Index
- [ ] Lancer tous les scrapers UK
- [ ] Vérifier et intégrer

## 7C - Sources US
- [ ] Écrire scraper Equibase
- [ ] Écrire scraper Horse Racing Nation
- [ ] Écrire scraper Daily Racing Form
- [ ] Écrire scraper Brisnet
- [ ] Écrire scraper TrackMaster
- [ ] Écrire scraper Horse Racing Radar
- [ ] Lancer tous les scrapers US
- [ ] Vérifier et intégrer

## 7D - Sources Australie/NZ/Asie
- [ ] Écrire scraper Punters.com.au
- [ ] Écrire scraper Racenet
- [ ] Écrire scraper Racing Australia
- [ ] Écrire scraper NZ Thoroughbred Racing
- [ ] Écrire scraper HKJC (sectionals + GPS)
- [ ] Écrire scraper JRA database
- [ ] Écrire scraper Korea Racing
- [ ] Écrire scraper Singapore Pools
- [ ] Lancer, vérifier, intégrer

## 7E - Cotes / Marchés
- [ ] Écrire scraper Oddschecker
- [ ] Écrire scraper OddsPortal
- [ ] Écrire scraper BetExplorer
- [ ] Configurer Betfair API
- [ ] Écrire scraper Matchbook
- [ ] Compléter Smarkets API
- [ ] Écrire scraper Bet365
- [ ] Écrire scraper William Hill
- [ ] Écrire scraper BestOdds / Betbrain
- [ ] Lancer, vérifier, intégrer

## 7F - Pedigree mondial
- [ ] Scraper AllBreedPedigree complet
- [ ] Scraper PedigreeQuery complet (toutes races)
- [ ] Scraper Equineline / Weatherbys
- [ ] Scraper American / Australian / Japan Stud Book
- [ ] Scraper WAHO (arabes)
- [ ] Scraper Sporthorse-Data / Hippomundo / HorseTelex
- [ ] Fusionner dans pedigree_master

## 7G - Ventes / Enchères
- [ ] Scraper Arqana complet (historique ventes FR)
- [ ] Scraper Tattersalls (ventes UK)
- [ ] Scraper Goffs (ventes IRE)
- [ ] Scraper Keeneland / Fasig-Tipton (US)
- [ ] Scraper Magic Millions / OBS / Inglis (AU)
- [ ] Scraper BloodHorse Stallion Register
- [ ] Créer table prix_vente_cheval (joinable par nom cheval)

## 7H - Trot international
- [ ] Scraper USTA (trot US complet)
- [ ] Scraper Harness Racing Australia
- [ ] Scraper Standardbred Canada
- [ ] Intégrer dans le pipeline trot

## 7I - Sectionals / GPS / Biomécanique
- [ ] Investiguer accès Total Performance Data (TPD)
- [ ] Scraper StrideMASTER données AU
- [ ] Scraper Trakus données US
- [ ] Scraper TurfTrax données UK
- [ ] Investiguer Equimetre France Galop
- [ ] Scraper HKJC sectional tracking
- [ ] Créer table sectionals_master

## 7J - Météo ultra précise
- [ ] Configurer NOAA API (historique mondial)
- [ ] Configurer Meteostat API
- [ ] Configurer Visual Crossing API
- [ ] Configurer Weatherbit API
- [ ] Récupérer données stations météo par hippodrome
- [ ] Fusionner dans meteo_master

## 7K - Terrain / Going
- [ ] Scraper GoingStick data UK
- [ ] Scraper TurfTrax going data
- [ ] Scraper Clerk of Course reports
- [ ] Scraper HKJC going reports
- [ ] Scraper Racing AU Track Conditions
- [ ] Créer table terrain_master

## 7L - Stats jockey/entraîneur avancées
- [ ] Scraper TrainerTrackStats
- [ ] Scraper JockeyStats Pro
- [ ] Scraper Stable Performance Index
- [ ] Scraper Jockey Club database
- [ ] Créer table jockey_stats_master + trainer_stats_master

## 7M - Organismes officiels
- [ ] Scraper BHA (British Horseracing Authority)
- [ ] Scraper IHRB (Irish)
- [ ] Scraper Emirates Racing Authority
- [ ] Scraper IFHA (International Federation)
- [ ] Scraper France Galop data complète
- [ ] Scraper LeTrot data complète

## 7N - Datasets open / Kaggle
- [ ] Télécharger TOUS les datasets Kaggle horse racing
- [ ] Télécharger UK Racing Data archive
- [ ] Télécharger Australian Racing Historical Data
- [ ] Télécharger JRA historical database
- [ ] Télécharger HKJC historical archive
- [ ] Parser et intégrer chaque dataset

## 7O - APIs professionnelles (payantes)
- [ ] Évaluer coût Timeform API
- [ ] Évaluer coût The Racing API / Podium Racing API
- [ ] Évaluer coût LSports Horse Racing API
- [ ] Évaluer coût OptixEQ / ThoroughGraph
- [ ] Souscrire aux APIs les plus utiles
- [ ] Intégrer les données

## 7P - Bloodstock & élevage
- [ ] Scraper BloodHorse
- [ ] Scraper Thoroughbred Daily News
- [ ] Scraper Bloodstock World
- [ ] Scraper European Bloodstock News
- [ ] Scraper Japan Bloodhorse Breeders Association
- [ ] Intégrer dans pedigree_master

## 7Q - Stats avancées / Ratings pro
- [ ] Scraper OptixEQ (speed figures avancés)
- [ ] Scraper ThoroughGraph (speed + pace)
- [ ] Scraper Equine Edge
- [ ] Scraper Horse Racing Analytics
- [ ] Scraper EquiRatings
- [ ] Créer table ratings_master

## 7R - Données par hippodrome
- [ ] Scraper données Churchill Downs
- [ ] Scraper données Ascot
- [ ] Scraper données Longchamp
- [ ] Scraper données Sha Tin / Happy Valley (HKJC)
- [ ] Scraper données Flemington
- [ ] Scraper données Meydan
- [ ] Enrichir hippodromes_db.py avec tout

## 7S - Backup après collecte massive
- [ ] Sauvegarder tout le dossier
- [ ] Versionner les fichiers maîtres
- [ ] Comparer tailles avant/après

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 8 — INTÉGRATION NOUVELLES       │
# │  SOURCES DANS LE PIPELINE              │
# └─────────────────────────────────────────┘

## 8.0 🔴 Audit HTML : re-vérifier TOUTES les sources pour valeurs cachées
- [ ] 🔴 Pour chaque source déjà scrapée : comparer champs collectés vs champs dispo dans HTML brut
- [ ] 🔴 Re-scraper en HTML brut les sources où on soupçonne des champs manquants
- [ ] 🔴 Lister tous les champs HTML non exploités par source (tableau source → champs_ignorés)
- [ ] 🔴 Parser les HTML bruts sauvegardés (geny, canalturf, turfostats, etc.) pour extraire valeurs manquantes
- [ ] 🔴 Comparer les champs API PMU vs champs HTML TurfInfo (souvent plus de données en HTML)
- [ ] 🔴 Vérifier les pages détail cheval sur chaque site (souvent plus riche que la page course)
- [ ] 🔴 Vérifier les onglets/sections masquées (stats détaillées, historique, commentaires d'experts)
- [ ] 🟠 Créer script audit_html_vs_json.py : pour chaque source, compare nb champs HTML vs nb champs collectés
- [ ] 🟠 Mapper les champs HTML non exploités vers des features potentielles
- [ ] 🟠 Prioriser par valeur ajoutée : quels champs HTML manquants ont le plus d'impact prédictif
- [ ] 🟠 Transformer TOUS les HTML bruts récupérés (output/*/html_raw/) en JSON structuré

## 8.1 Pour chaque nouvelle source collectée :
- [ ] Parser les données brutes → JSON normalisé
- [ ] Nettoyer (même process qu'étape 3)
- [ ] Dédupliquer vs données existantes
- [ ] Créer le builder de features correspondant
- [ ] Ajouter les jointures dans master_feature_builder
- [ ] Ajouter le symlink dans pipeline/
- [ ] Documenter la source dans docs/

## 8.2 Re-merger tout
- [ ] Mettre à jour partants_master.json avec nouvelles sources
- [ ] Mettre à jour la matrice de features
- [ ] Vérifier nombre total de features (cible: 468+)

## 8.3 Backup intermédiaire #5
- [ ] Sauvegarder après intégration

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 9 — ORGANISATION DES DOSSIERS   │
# └─────────────────────────────────────────┘

## 9.1 Structure finale du dossier
- [ ] Créer arborescence claire et modulable :
      models_hybride/
      ├── output/                    ← données brutes par source
      │   ├── 01_calendrier/
      │   ├── 02_courses/
      │   ├── ...
      │   └── 40_enrichissement/
      ├── data_master/               ← fichiers maîtres fusionnés
      │   ├── partants_master.json   ← LE fichier maître
      │   ├── courses_master.json
      │   ├── pedigree_master.json
      │   ├── meteo_master.json
      │   ├── rapports_master.json
      │   ├── marche_master.json
      │   └── stats_externes_master.json
      ├── features/                  ← matrice de features
      │   ├── features_matrix.json
      │   ├── features_matrix.csv
      │   ├── features_matrix.parquet
      │   └── FEATURE_CATALOG.md
      ├── labels/                    ← labels/targets
      ├── pipeline/                  ← symlinks par module/modèle
      │   ├── phase_01_.../
      │   ├── ...
      │   └── phase_16_.../
      ├── feature_builders/          ← scripts de calcul features
      ├── scripts/                   ← scripts de collecte
      ├── hippodromes_db.py          ← base hippodromes
      ├── docs/                      ← documentation complète
      ├── backups/                   ← sauvegardes
      ├── logs/                      ← logs de tous les scripts
      └── quality/                   ← rapports qualité

## 9.2 Réorganisation des fichiers
- [ ] Déplacer tous les scripts XX_*.py dans scripts/
- [ ] Déplacer les feature builders dans feature_builders/
- [ ] Créer data_master/ avec les fichiers fusionnés
- [ ] Mettre à jour tous les chemins dans les scripts
- [ ] Mettre à jour tous les symlinks dans pipeline/
- [ ] Vérifier que rien n'est cassé après réorg

## 9.3 Symlinks pipeline/
- [ ] Vérifier que chaque module a ses symlinks
- [ ] Ajouter les symlinks pour les nouvelles sources
- [ ] Tester que tous les symlinks pointent au bon endroit
- [ ] Supprimer les symlinks cassés

## 9.4 Export triple format
- [ ] Exporter partants_master en JSON + CSV + Parquet
- [ ] Exporter courses_master en JSON + CSV + Parquet
- [ ] Exporter pedigree_master en JSON + CSV + Parquet
- [ ] Exporter meteo_master en JSON + CSV + Parquet
- [ ] Exporter features_matrix en JSON + CSV + Parquet
- [ ] Exporter labels en JSON + CSV + Parquet
- [ ] Exporter chaque master en triple format

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 10 — DOCUMENTATION              │
# └─────────────────────────────────────────┘

## 10.1 Documentation des données
- [ ] Créer docs/README.md — vue d'ensemble du projet data
- [ ] Créer docs/SOURCES.md — liste de toutes les sources avec :
      → URL, type (API/scraping), fréquence MAJ, volume, date dernier scrape
- [ ] Créer docs/SCHEMA.md — schéma de chaque table/fichier :
      → nom champ, type, description, taux remplissage, valeurs possibles
- [ ] Créer docs/FEATURES.md — catalogue complet des features :
      → nom, description, builder source, type, stats
- [ ] Créer docs/PIPELINE.md — description du pipeline complet :
      → flux de données, dépendances, ordre d'exécution
- [ ] Créer docs/HIPPODROMES.md — documentation hippodromes_db.py
- [ ] Créer docs/PEDIGREE.md — documentation pedigree (sources, couverture)
- [ ] Créer docs/METEO.md — documentation météo (sources, couverture)

## 10.2 Documentation technique
- [ ] Créer docs/INSTALL.md — comment installer les dépendances
- [ ] Créer docs/SCRIPTS.md — comment lancer chaque script
- [ ] Créer docs/TROUBLESHOOTING.md — problèmes courants et solutions
- [ ] Créer docs/BACKUP.md — procédure de backup/restore
- [ ] Créer docs/MAINTENANCE.md — comment mettre à jour les données
- [ ] Créer docs/CHANGELOG.md — historique des modifications

## 10.3 Documentation pour la maintenance
- [ ] Documenter le process de relance après crash
- [ ] Documenter le process d'ajout d'une nouvelle source
- [ ] Documenter le process d'ajout d'un nouveau builder
- [ ] Documenter le process de rebuild de la matrice
- [ ] Documenter les clés de jointure entre tables
- [ ] Documenter les alias d'hippodromes

## 10.4 Schémas visuels
- [ ] Diagramme du flux de données (mermaid ou draw.io)
- [ ] Diagramme des dépendances entre scripts
- [ ] Tableau de couverture par source × année
- [ ] Matrice de jointure (quelle clé relie quoi)

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 11 — QUALITÉ FINALE             │
# └─────────────────────────────────────────┘

## 11.1 Tests d'intégrité
- [ ] Script de test automatique : tous les JSON sont valides
- [ ] Script de test : tous les symlinks pointent correctement
- [ ] Script de test : aucun fichier de 0 bytes
- [ ] Script de test : nombre de records cohérent entre sources
- [ ] Script de test : pas de NaN/Inf dans les features numériques
- [ ] Script de test : toutes les dates sont valides
- [ ] Script de test : toutes les cotes sont > 0
- [ ] Script de test : toutes les distances sont > 0

## 11.2 Statistiques finales
- [ ] Nombre total de courses
- [ ] Nombre total de partants
- [ ] Nombre total de chevaux uniques
- [ ] Nombre total de jockeys uniques
- [ ] Nombre total d'hippodromes
- [ ] Plage de dates couverte
- [ ] Nombre total de features
- [ ] Taux de remplissage moyen
- [ ] Taille totale des données
- [ ] Sauvegarder ces stats dans docs/STATS.md

## 11.3 Validation croisée entre sources
- [ ] Vérifier que les résultats PMU = résultats Le Trot (même course)
- [ ] Vérifier que les cotes PMU ≈ cotes exchange (même course)
- [ ] Vérifier que les pedigrees sont cohérents entre sources
- [ ] Vérifier que les sectionals sont cohérents
- [ ] Identifier et résoudre les conflits entre sources

## 11.4 Backup FINAL
- [ ] Sauvegarder la version finale complète
- [ ] Créer un README dans le backup expliquant son contenu
- [ ] Versionner avec date et stats (nb records, nb features, taille)
- [ ] Copie sur disque externe si possible

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 12 — PRÊT POUR LES MODÈLES     │
# └─────────────────────────────────────────┘

## 12.1 Vérification finale avant passage aux modèles
- [ ] Confirmer que partants_master.json est complet
- [ ] Confirmer que features_matrix contient 400+ features
- [ ] Confirmer que labels.json est aligné avec features
- [ ] Confirmer que tous les symlinks pipeline/ fonctionnent
- [ ] Confirmer que la documentation est à jour
- [ ] Confirmer que le backup final est fait
- [ ] Confirmer 0 fichier corrompu
- [ ] Confirmer 0 doublon
- [ ] Confirmer taux remplissage acceptable par feature

## 12.2 Livrable final
- [ ] data_master/ complet avec tous les masters
- [ ] features/ complet avec matrice 400+ features
- [ ] pipeline/ avec symlinks fonctionnels
- [ ] docs/ avec documentation complète
- [ ] quality/ avec rapports de qualité
- [ ] Tout en triple format (JSON + CSV + Parquet)
- [ ] Prêt à être branché sur le dossier modèles

# ════════════════════════════════════════════════════════════════
# COMPTEURS FINAUX
# ════════════════════════════════════════════════════════════════
# Scripts de collecte existants: 41
# Nouvelles sources à scraper: ~120+
# Features actuelles: 80
# Features cible: 468+
# Records partants: ~2.7M+
# Courses: ~221K+
# Années couvertes: 2004-2026
# Taille données brutes: ~80+ GB
# Taille données nettoyées: ~50+ GB
# Zéro trou, zéro corruption, zéro doublon
# Documentation complète
# Backup versionné
# ════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════
# 🏛️ PILIERS QUALITÉ — À RESPECTER SUR TOUT LE PIPELINE DATA
# ════════════════════════════════════════════════════════════════

# ┌─────────────────────────────────────────┐
# │  PILIER 1 — PERFORMANCE                │
# └─────────────────────────────────────────┘
# Les fichiers de 8 GB en JSON c'est trop lent.
# Chaque requête doit être rapide.

- [ ] Convertir les fichiers maîtres en Parquet (lecture 10x plus rapide)
- [ ] Créer une base DuckDB locale (requêtes SQL sur les data sans charger en RAM)
- [ ] Indexer par course_uid, partant_uid, date, hippodrome
- [ ] Partitionner les gros fichiers par année (2014/, 2015/, ..., 2026/)
- [ ] Compresser les archives anciennes (gzip/zstd pour <2020)
- [ ] Lazy loading : ne charger que les colonnes nécessaires
- [ ] Benchmark : mesurer temps de chargement de chaque fichier maître
- [ ] Cache mémoire pour les lookups fréquents (hippodromes, chevaux)
- [ ] Profiler les scripts les plus lents et optimiser
# --- AUDIT PILIER 1 : tâches ajoutées ---
- [ ] 🔴 Cache LRU en mémoire avec TTL pour lookups répétitifs + métriques hit/miss
- [ ] 🔴 Vues matérialisées DuckDB pour jointures fréquentes (partant+course+météo, partant+pedigree)
- [ ] 🔴 Pré-calcul features stables (pedigree, hippodromes) dans fichier séparé rechargeable
- [ ] 🔴 Bloom filter DuckDB sur course_uid et partant_uid
- [ ] 🔴 Warmup cache au démarrage du pipeline (pré-charger tables de référence)
- [ ] 🔴 Budget mémoire par étape (merge max 16GB, features max 32GB) dans config/pipeline.yaml
- [ ] 🟠 Parallel read Parquet multi-thread par partition année lors du feature building
- [ ] 🟠 benchmark_results.json : temps bout en bout par étape documenté
- [ ] 🟠 Dictionary-encoded Parquet pour champs catégoriques (hippodrome, discipline, jockey)
- [ ] 🟠 Pré-agrégation stats rolling (moyennes, écarts-types) en cache intermédiaire

# ┌─────────────────────────────────────────┐
# │  PILIER 2 — SÉCURITÉ                   │
# └─────────────────────────────────────────┘
# Protéger les données contre perte, corruption, accès non voulu.

- [ ] Backups automatiques programmés (quotidien incrémental)
- [ ] Backup sur disque externe + cloud si possible
- [ ] Checksums SHA256 pour chaque fichier maître (détecter corruption silencieuse)
- [ ] Fichier .env pour les clés API (jamais en dur dans le code)
- [ ] .gitignore pour exclure données sensibles et fichiers lourds
- [ ] Permissions fichiers : read-only sur les fichiers maîtres finaux
- [ ] Pas de données personnelles dans les exports (RGPD)
- [ ] Script de vérification d'intégrité (compare checksums)
# --- AUDIT PILIER 2 : tâches ajoutées ---
- [ ] 🔴 Chiffrer le fichier .env avec sops ou age (clés API en clair = risque)
- [ ] 🔴 Rotation auto des tokens/clés API (alerter X jours avant expiration)
- [ ] 🔴 audit_secrets.py : scanner tout le code pour détecter clés API en dur
- [ ] 🟠 Vérification intégrité backups APRÈS écriture (lire + vérifier checksum)
- [ ] 🟠 Lock file (.lock) empêcher 2 instances du pipeline d'écrire en même temps
- [ ] 🟠 Politique de rétention backups (garder N jours, supprimer anciens auto)
- [ ] 🟡 Logger accès lecture/écriture aux fichiers maîtres
- [ ] 🟡 Anti-tampering : signer fichiers maîtres (HMAC) pour détecter modification non autorisée

# ┌─────────────────────────────────────────┐
# │  PILIER 3 — STABILITÉ                  │
# └─────────────────────────────────────────┘
# Le pipeline ne doit JAMAIS perdre de données, même en cas de crash.

- [ ] Checkpoint/resume sur TOUS les scripts (déjà fait sur la plupart)
- [ ] Écriture atomique : écrire dans .tmp puis rename (pas de fichier tronqué)
- [ ] Validation JSON avant et après chaque écriture
- [ ] Retry automatique avec backoff exponentiel sur les appels API
- [ ] Timeout sur toutes les requêtes réseau
- [ ] Gestion mémoire : streaming JSON pour les gros fichiers (pas tout charger)
- [ ] Watchdog : script qui surveille les scripts et relance si crash (monitor.sh)
- [ ] Graceful shutdown : sauvegarder l'état en cours si SIGTERM/SIGINT
- [ ] Limiter la RAM par script (ulimit ou checks internes)
# --- AUDIT PILIER 3 : tâches ajoutées ---
- [ ] 🔴 Write-ahead log (WAL) pour merges : journaliser avant d'appliquer → rollback si crash mid-merge
- [ ] 🔴 Circuit-breaker par source (closed/open/half-open) avec seuils dans config/sources.yaml
- [ ] 🟠 Heartbeat par script long (écrire timestamp toutes les N sec → distinguer "bloqué" de "lent")
- [ ] 🟠 Quarantaine auto : fichier échoue 3x validation → quarantine/ + alerte
- [ ] 🟠 Pré-validation données AVANT écriture dans masters (reject gate)
- [ ] 🟠 Test santé disque avant écritures lourdes (espace dispo, vitesse I/O)
- [ ] 🟠 Mode "safe merge" : ancien master intact jusqu'à validation complète du nouveau

# ┌─────────────────────────────────────────┐
# │  PILIER 4 — REDONDANCE                 │
# └─────────────────────────────────────────┘
# Chaque donnée critique existe en au moins 2 copies/formats.

- [ ] Triple format pour tous les masters : JSON + CSV + Parquet
- [ ] Cache fichier par fichier (un crash ne perd qu'un record)
- [ ] Backup versionné avec date (backup_20260315, backup_20260316, ...)
- [ ] Fichiers maîtres + fichiers cache source = double source de vérité
- [ ] Pouvoir reconstruire n'importe quel master depuis les caches
- [ ] Script rebuild_from_cache.py pour chaque source
- [ ] Garder les données brutes (ne jamais supprimer les raw)
# --- AUDIT PILIER 4 : tâches ajoutées ---
- [ ] 🟠 Test auto rebuild mensuel : rebuild from cache + comparaison avec master actuel
- [ ] 🟠 verify_rebuild_coverage.py : vérifier 100% records reconstructibles depuis caches
- [ ] 🟠 versions_registry.json : hash, date, nb records par version de chaque master
- [ ] 🟠 master_diff.py : diff records ajoutés/modifiés/supprimés entre 2 versions
- [ ] 🟡 Checksums des caches individuels (détecter corruption sans rebuild complet)
- [ ] 🟡 Stratégie rétention formats : quand supprimer vieux CSV/JSON si Parquet = source de vérité

# ┌─────────────────────────────────────────┐
# │  PILIER 5 — AUDITABILITÉ               │
# └─────────────────────────────────────────┘
# Savoir exactement ce qui s'est passé, quand, pourquoi.

- [ ] Log structuré (JSON) pour chaque script avec timestamp + action + résultat
- [ ] CHANGELOG.md : historique de TOUTES les modifications de données
- [ ] Chaque comblage de trou loggé : quel champ, quelle valeur, quelle source
- [ ] Chaque fusion loggée : combien de records avant/après, doublons supprimés
- [ ] Rapport d'audit automatique après chaque étape majeure
- [ ] Git pour versionner les scripts (pas les données, trop lourdes)
- [ ] Fichier MANIFEST.json : liste tous les fichiers avec taille, date, checksum
- [ ] Tracer l'origine de chaque record (source_tag sur chaque ligne)
# --- AUDIT PILIER 5 : tâches ajoutées ---
- [ ] 🟠 Audit trail immutable : append-only audit_trail.jsonl (jamais modifié, seulement append)
- [ ] 🟠 Outil requête audit : "toutes les modifs du record partant_uid=X depuis sa création"
- [ ] 🟠 Rapport audit automatique par run complet du pipeline (pas seulement par étape)
- [ ] 🟡 Dashboard audit visuel HTML : historique modifications par source et type opération
- [ ] 🟡 Métriques audit : nb modifs/jour, ratio ajout vs modification vs suppression
- [ ] 🟡 Signature temporelle des logs (timestamp signé pour prouver non-altération)

# ┌─────────────────────────────────────────┐
# │  PILIER 6 — STRATÉGIE                  │
# └─────────────────────────────────────────┘
# Savoir pourquoi on collecte chaque donnée et à quoi elle sert.

- [ ] Mapping source → module → modèle (quelle donnée nourrit quel modèle)
- [ ] Prioriser les sources par impact sur la prédiction
- [ ] Matrice d'utilité : chaque feature a un score d'importance estimé
- [ ] Roadmap de collecte : quoi d'abord, quoi ensuite, quoi si budget
- [ ] Identifier les sources à fort ROI (gratuit + haute valeur prédictive)
- [ ] Plan B pour chaque source (si le site tombe, alternative ?)
- [ ] Coût/bénéfice des APIs payantes vs scraping gratuit
# --- AUDIT PILIER 6 : tâches ajoutées ---
- [ ] 🔴 Scoring ROI quantitatif par source : coût (temps+stockage+maintenance) vs valeur → sources_roi.json
- [ ] 🔴 Risk assessment par source : risque ban, juridique (CGU), disparition + plan mitigation
- [ ] 🟠 Critères GO/NO-GO par nouvelle source (seuil couverture, fraîcheur, unicité)
- [ ] 🟠 Tableau décision sources payantes (seuil rentabilité vs coût annuel)
- [ ] 🟠 Mécanisme dépréciation source : si gratuite → payante ou instable → processus remplacement
- [ ] 🟠 Collecte différentielle : critique=quotidien, secondaire=hebdo, tertiaire=mensuel

# ┌─────────────────────────────────────────┐
# │  PILIER 7 — INTELLIGENCE               │
# └─────────────────────────────────────────┘
# Le pipeline doit être "intelligent" dans sa gestion des données.

- [ ] Imputation intelligente : KNN ou MICE pour les valeurs manquantes
- [ ] Détection automatique d'anomalies dans les données (outliers)
- [ ] Auto-détection du format de date (DDMMYYYY vs YYYY-MM-DD vs ISO)
- [ ] Auto-détection de l'encodage (UTF-8 vs Latin-1 vs ASCII)
- [ ] Matching fuzzy pour les noms (VINCENNES ≈ vincennes ≈ Vincennès)
- [ ] Déduplication intelligente (même cheval avec noms légèrement différents)
- [ ] Inférence de champs manquants depuis d'autres champs
- [ ] Scoring automatique de la qualité de chaque record (0-100)
- [ ] Alertes si un pattern inhabituel apparaît dans les données
# --- AUDIT PILIER 7 : tâches ajoutées ---
- [ ] 🔴 Score de confiance par valeur : _confidence (1.0=officiel, 0.7=inféré, 0.3=imputé)
- [ ] 🔴 Réconciliation multi-sources : vote pondéré par fiabilité quand 3 sources divergent
- [ ] 🟠 Détection data drift temporel : alerte si distribution d'un champ change d'une année à l'autre
- [ ] 🟠 Apprentissage patterns manquants : trot vs galop = patterns de complétude différents
- [ ] 🟠 Détection cohortes : groupes de records avec même pattern de complétude
- [ ] 🟠 Validation sémantique : cote 1.01 pour dernier au classement = suspect, 4800m galop plat = suspect
- [ ] 🟠 Moteur règles métier : cheval 2 ans pas en steeple, trotteur pas en galop, etc.

# ┌─────────────────────────────────────────┐
# │  PILIER 8 — ORCHESTRATION              │
# └─────────────────────────────────────────┘
# Les scripts doivent s'exécuter dans le bon ordre avec les bonnes dépendances.

- [ ] Créer un DAG (Directed Acyclic Graph) des dépendances entre scripts
- [ ] Fichier pipeline_config.yaml : ordre d'exécution, dépendances, paramètres
- [ ] Script orchestrator.py : lance les scripts dans l'ordre avec gestion erreurs
- [ ] Parallélisation automatique des scripts indépendants
- [ ] File d'attente avec priorité (collecte > nettoyage > features)
- [ ] Détection automatique : "ce script a besoin de X qui n'est pas encore prêt"
- [ ] Mode dry-run : simuler l'exécution sans rien faire
- [ ] Mode reprise : reprendre à l'étape qui a planté
- [ ] Notifications (mail/telegram/discord) quand un script finit ou plante
# --- AUDIT PILIER 8 : tâches ajoutées ---
- [ ] 🔴 Lock distribué pour fichiers maîtres partagés (pas 2 scripts qui écrivent en même temps)
- [ ] 🔴 Pipeline partiel : si seule la météo a été MAJ → ne recalculer que les features météo
- [ ] 🟠 Exécution conditionnelle : check ETag/Last-Modified → skip si source inchangée
- [ ] 🟠 Planificateur ressources : max 5 scrapers concurrents (éviter épuisement réseau/RAM)
- [ ] 🟠 Priorité dynamique : course dans 2h = scraping prioritaire vs backfill historique
- [ ] 🟠 Graphe dépendances visuel auto-généré depuis pipeline_config.yaml (mermaid/graphviz)
- [ ] 🟠 Dead letter queue : records qui échouent → mis de côté pour retraitement ultérieur

# ┌─────────────────────────────────────────┐
# │  PILIER 9 — COMPATIBILITÉ SYSTÈME      │
# └─────────────────────────────────────────┘
# Doit fonctionner sur Mac (actuel) ET PC (lundi) sans problème.

- [ ] requirements.txt avec toutes les dépendances Python exactes
- [ ] Pas de chemins absolus en dur (utiliser os.path, pathlib)
- [ ] Script setup.sh / setup.py pour installer l'environnement
- [ ] Compatible Python 3.9+ (Mac) et 3.12+ (PC)
- [ ] Pas de dépendance à grep -P ou commandes Mac-only
- [ ] Tester sur Windows (WSL si besoin)
- [ ] Docker optionnel pour environnement reproductible
- [ ] Variables d'environnement pour les chemins racine
- [ ] config.py centralisé avec tous les paramètres (chemins, URLs, clés)
# --- AUDIT PILIER 9 : tâches ajoutées ---
- [ ] 🟠 test_install.py : smoke test post-installation (vérifie tous les imports)
- [ ] 🟠 Check espace disque avant lancement (150 GB minimum requis)
- [ ] 🟡 Doc différences performances Mac ARM vs PC x86/CUDA par étape
- [ ] 🟡 pyenv ou conda pour gérer versions Python auto
- [ ] 🟡 Fichier .python-version pour fixer la version
- [ ] 🟡 Tester comportement sur NFS/SMB si données sur NAS

# ┌─────────────────────────────────────────┐
# │  PILIER 10 — AUTO-ADAPTATIVITÉ         │
# └─────────────────────────────────────────┘
# Le pipeline s'adapte automatiquement aux changements.

- [ ] Détection auto de nouvelles courses (scraping incrémental quotidien)
- [ ] Détection auto de nouveaux chevaux → ajout dans pedigree_master
- [ ] Détection auto de nouveaux hippodromes → ajout dans hippodromes_db
- [ ] Détection auto de changement de format API (alerte si le parsing casse)
- [ ] Schema evolution : gérer l'ajout de nouveaux champs sans casser l'existant
- [ ] Auto-discovery de nouvelles features depuis les données brutes
- [ ] Gestion des sources qui changent d'URL ou de structure HTML
- [ ] Mise à jour automatique des taux de remplissage après chaque run
# --- AUDIT PILIER 10 : tâches ajoutées ---
- [ ] 🔴 Moniteur structure HTML par source : hasher DOM → alerter si refactoring détecté
- [ ] 🔴 Fallback en cascade configurable : source A tombe → B → C (dans config/sources.yaml)
- [ ] 🟠 Auto-réparation scrapers : champ disparaît → deprecated auto, pas crash
- [ ] 🟠 Détection auto nouvelles colonnes dans APIs → incorporation auto dans schéma
- [ ] 🟠 Feature deprecation auto : 100% None depuis 30j → retirer de la matrice
- [ ] 🟠 Détection throttling/rate limiting (429, Captcha, ralentissements) → adapter rythme auto

# ┌─────────────────────────────────────────┐
# │  PILIER 11 — SYNCHRONISATION INTER-BLOCS│
# └─────────────────────────────────────────┘
# Toutes les sources doivent être cohérentes entre elles.

- [ ] Clés de jointure standardisées (course_uid, partant_uid format uniforme)
- [ ] Vérification croisée : même course = même nb partants dans toutes les sources
- [ ] Vérification croisée : même cheval = même pedigree dans toutes les sources
- [ ] Timestamp de dernière MAJ par source (savoir quelle source est à jour)
- [ ] Détection de conflit : si 2 sources donnent des infos contradictoires → log
- [ ] Résolution de conflit : règle de priorité entre sources
- [ ] Fichier sync_status.json : état de synchro de chaque source
- [ ] Cohérence temporelle : toutes les sources couvrent les mêmes dates
# --- AUDIT PILIER 11 : tâches ajoutées ---
- [ ] 🔴 Golden record par entité : source of truth par champ pour chaque cheval/course/jockey
- [ ] 🔴 Score de concordance par entité ("ce cheval a 95% concordance entre 4 sources pedigree")
- [ ] 🟠 Rapport couverture croisée auto : matrice sources × champs (% couvert par chaque source)
- [ ] 🟠 Versioning clés jointure : si course_uid change de format → mapping ancien→nouveau
- [ ] 🟠 Test cohérence temporelle : dates concordent entre sources pour même course (timezone)
- [ ] 🟠 Réconciliation batch post-import : vérif croisée complète après gros import
- [ ] 🟠 Rapport conflits non résolus : lister cas où sources se contredisent sans règle de priorité

# ┌─────────────────────────────────────────┐
# │  PILIER 12 — MODULARITÉ                │
# └─────────────────────────────────────────┘
# Chaque source est indépendante. On peut ajouter/retirer sans tout casser.

- [ ] 1 script = 1 source = 1 dossier output = 1 builder features
- [ ] Chaque module a : input/, output/, cache/, config.json, README.md
- [ ] Ajouter une source = créer un script + un builder, rien d'autre
- [ ] Retirer une source = supprimer le symlink, rien ne casse
- [ ] Pas de couplage fort entre les scripts (pas d'import croisé)
- [ ] Interface standardisée : chaque script produit JSON avec les mêmes clés de base
- [ ] Template de script pour créer rapidement un nouveau scraper
- [ ] Template de builder pour créer rapidement un nouveau feature builder
# --- AUDIT PILIER 12 : tâches ajoutées ---
- [ ] 🔴 Classe abstraite BaseScraper (scrape/validate/export) que tous les scrapers implémentent
- [ ] 🔴 Classe abstraite BaseFeatureBuilder (build/validate/get_feature_names)
- [ ] 🟠 Système plugin/registry : nouveau scraper/builder s'enregistre auto sans modifier code existant
- [ ] 🟠 Découverte auto modules : scanner scripts/ et feature_builders/ sans liste hardcodée
- [ ] 🟠 Test conformité interface : vérifier que chaque module respecte le standard (config.json, bon format)
- [ ] 🟡 Versioning interfaces : si BaseBuilder change, anciens builders continuent de fonctionner

# ┌─────────────────────────────────────────┐
# │  PILIER 13 — TÉLÉMÉTRIE                │
# └─────────────────────────────────────────┘
# Voir en temps réel ce qui se passe dans le pipeline.

- [ ] Dashboard HTML (ou Streamlit) : état de chaque script en temps réel
- [ ] Métriques : nb records collectés/heure, taux d'erreur, RAM, CPU
- [ ] Historique des runs : quand chaque script a tourné, combien de temps
- [ ] Graphiques de progression (courbes de collecte au fil du temps)
- [ ] Alertes si un script est bloqué depuis > 30 min sans progrès
- [ ] Monitoring taille des fichiers (croissance attendue vs réelle)
- [ ] Tableau de bord couverture : % de courses avec météo, pedigree, etc.
- [ ] Export des métriques en CSV pour analyse
# --- AUDIT PILIER 13 : tâches ajoutées ---
- [ ] 🔴 Métriques qualité données temps réel : taux remplissage par champ 24h trending up/down
- [ ] 🔴 SLA monitoring par source : "données attendues dans les 2h après chaque course, sinon alerte"
- [ ] 🟠 Métriques performance scrapers : temps/page, taux succès, latence réseau par domaine
- [ ] 🟠 Alerting multi-canal configurable : email + Telegram + Discord + webhook, niveaux sévérité
- [ ] 🟠 Métriques drift : comparer distributions features entre dernier batch et historique
- [ ] 🟡 Rapport santé hebdomadaire auto par email : résumé métriques clés, anomalies, tendances

# ┌─────────────────────────────────────────┐
# │  PILIER 14 — DEBUGGING                 │
# └─────────────────────────────────────────┘
# Trouver et corriger les problèmes rapidement.

- [ ] Logs avec niveaux (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- [ ] Chaque erreur logguée avec : fichier, ligne, traceback complet
- [ ] Mode verbose activable par flag (--debug ou --verbose)
- [ ] Script diagnostic.py : vérifie tout le pipeline et liste les problèmes
- [ ] Fichier KNOWN_ISSUES.md : bugs connus et workarounds
- [ ] Tracer chaque record problématique (quel fichier, quelle ligne)
- [ ] Tests unitaires pour les fonctions critiques (parsing, jointure)
- [ ] Assertions dans le code (assert nb_records > 0, "Fichier vide!")
# --- AUDIT PILIER 14 : tâches ajoutées ---
- [ ] 🔴 investigate_record.py <partant_uid> : affiche TOUTES données brutes+transformées+features avec source de chaque valeur
- [ ] 🔴 Mode "replay" : rejouer traitement 1 record spécifique avec logs DEBUG complet
- [ ] 🟠 Sampling debug : sauvegarder état intermédiaire de N records aléatoires à chaque étape
- [ ] 🟠 Outil comparaison records : comparer même record entre 2 versions du master
- [ ] 🟡 Tags debug : marquer certains records "à surveiller", notification si traitement change
- [ ] 🟡 Couverture tests : mesurer % des fonctions critiques couvertes par les tests

# ┌─────────────────────────────────────────┐
# │  PILIER 15 — STRESS-TEST               │
# └─────────────────────────────────────────┘
# Vérifier que le pipeline tient sous charge.

- [ ] Tester avec 10M records (simuler croissance future)
- [ ] Tester avec des champs manquants aléatoires (30%, 50%, 70%)
- [ ] Tester avec des données corrompues (JSON malformé, UTF-8 cassé)
- [ ] Tester avec des valeurs extrêmes (cotes de 999, distances de 100km)
- [ ] Tester le rebuild complet from scratch
- [ ] Tester la reprise après crash à chaque étape
- [ ] Tester sur la machine puissante (64 GB RAM, est-ce qu'on tient ?)
- [ ] Mesurer temps de reconstruction complète du pipeline
- [ ] Tester l'ajout de 10 nouvelles sources en même temps
# --- AUDIT PILIER 15 : tâches ajoutées ---
- [ ] 🔴 Test concurrence : lancer 2 instances pipeline simultanément → vérifier 0 corruption
- [ ] 🔴 Test disque plein 95% : le pipeline doit s'arrêter proprement, pas corrompre
- [ ] 🟠 Générateur données synthétiques réalistes (courses/partants crédibles, bonnes distributions)
- [ ] 🟠 Test coupure réseau mid-scraping : caches sauvegardés, 0 perte
- [ ] 🟠 Test régression performance : comparer temps d'exécution entre versions du pipeline
- [ ] 🟡 Test fuseaux horaires (courses internationales, serveur UTC vs données heure locale)
- [ ] 🟡 Test noms fichiers avec accents/espaces (hippodromes à accents)

# ┌─────────────────────────────────────────┐
# │  PILIER 16 — RENTABILITÉ TURF          │
# └─────────────────────────────────────────┘
# Chaque donnée collectée doit servir la prédiction.

- [ ] Scoring de chaque source par impact prédictif estimé
- [ ] Supprimer les features à 0 corrélation avec le résultat
- [ ] Prioriser les features avec forte importance SHAP/permutation
- [ ] Identifier les features redondantes (corrélation > 0.95)
- [ ] Feature importance ranking automatique
- [ ] A/B testing de features : ajouter/retirer et mesurer l'impact
- [ ] Couvrir les 10 facteurs clés des hedge funds turf :
      → odds, résultats, pedigree, météo, terrain, sectionals,
      → biomécanique, GPS, jockey, entraîneur
- [ ] Données de closing line value (CLV) pour value betting
- [ ] Données de volume de paris pour détecter le smart money
# --- AUDIT PILIER 16 : tâches ajoutées ---
- [ ] 🔴 Backtest automatisé rentabilité par source : ROI marginal de chaque source de données
- [ ] 🔴 Matrice couverture feature × discipline × pays (identifier zones aveugles)
- [ ] 🟠 Calcul break-even source payante : nb courses pour amortir le coût vs gain précision
- [ ] 🟠 Alpha par feature : valeur ajoutée unique non capturée par les autres features
- [ ] 🟠 Benchmark features par discipline : trot attelé ≠ galop plat ≠ obstacle
- [ ] 🟠 Tracking feature decay : pouvoir prédictif qui baisse avec le temps
- [ ] 🟠 Score fraîcheur informationnelle : rolling 5 courses = plus frais que carrière entière

# ┌─────────────────────────────────────────┐
# │  PILIER 17 — RÉSILIENCE ALGORITHMIQUE  │
# └─────────────────────────────────────────┘
# Le pipeline gère gracieusement les données pourries.

- [ ] Fallback si une source est vide (utiliser une source alternative)
- [ ] Fallback si un champ est manquant (valeur par défaut intelligente)
- [ ] Gestion des NaN, Inf, None dans les calculs de features
- [ ] Gestion des divisions par zéro (taux_victoire avec 0 courses)
- [ ] Clamp des valeurs extrêmes (pas de cote > 1000, pas de poids < 0)
- [ ] Gestion des courses annulées, reportées, abandonnées
- [ ] Gestion des chevaux disqualifiés après course
- [ ] Gestion des ex-aequo (2 chevaux même position)
- [ ] Gestion des non-partants de dernière minute
# --- AUDIT PILIER 17 : tâches ajoutées ---
- [ ] 🔴 Dégradation gracieuse par feature : rolling_5 → rolling_3 → moyenne carrière si pas assez de courses
- [ ] 🔴 Gestion courses multi-pays (Arc = chevaux FR+UK+IRE+JP+US avec formats différents)
- [ ] 🟠 Matrice fallback documentée par feature : "si X manque → Y, sinon Z, sinon default"
- [ ] 🟠 Gestion courses groupe international (US: furlongs/dirt, UK: going/furlongs, FR: mètres/going)
- [ ] 🟠 Gestion changements nom cheval à l'international (cheval renommé quand exporté)
- [ ] 🟠 Gestion homonymes entre pays (2 chevaux différents même nom dans 2 pays)
- [ ] 🟠 Confidence-weighted features : donnée incomplète → réduire poids dans matrice finale

# ┌─────────────────────────────────────────┐
# │  PILIER 18 — EXPLAINABILITY            │
# └─────────────────────────────────────────┘
# Chaque valeur, chaque feature doit pouvoir être expliquée.

- [ ] Chaque feature a une description humaine dans FEATURE_CATALOG.md
- [ ] Chaque feature a sa formule de calcul documentée
- [ ] Chaque valeur a son champ source_tag (d'où vient cette donnée)
- [ ] Lineage tracking : de la donnée brute à la feature finale
- [ ] Dictionnaire de données : nom_champ → description → type → exemple
- [ ] Glossaire turf : expliquer les termes métier (going, corde, déferré, etc.)
- [ ] Pour chaque feature : distribution, min, max, moyenne, médiane
# --- AUDIT PILIER 18 : tâches ajoutées ---
- [ ] 🟠 Feature cards : fiche récap par feature (nom, source, formule, distribution, corrélation target, SHAP)
- [ ] 🟠 Glossaire auto valeurs catégoriques : pour chaque champ catégoriel, lister toutes valeurs + signification
- [ ] 🟡 Visualisation lineage interactif : diagramme chemin complet donnée brute → feature (data lineage graph)
- [ ] 🟡 Exemples concrets dans dictionnaire : 3 exemples réels avec explication par feature
- [ ] 🟡 Rapport distribution par feature par discipline : distributions différentes trot vs galop
- [ ] 🟡 Doc cas limites par feature : quand la feature est non fiable (ex: rolling stats 2 courses seulement)

# ┌─────────────────────────────────────────┐
# │  PILIER 19 — CYCLE AUTO-APPRENANT      │
# └─────────────────────────────────────────┘
# Le pipeline s'améliore automatiquement au fil du temps.

- [ ] Feedback loop : les résultats des modèles alimentent la qualité data
- [ ] Si un modèle dit "feature X inutile" → la marquer dans le catalogue
- [ ] Si un modèle dit "feature Y manque" → l'ajouter à la TODO auto
- [ ] Monitoring de la fraîcheur : alerter si une source n'a pas été MAJ > 7j
- [ ] Auto-détection de concept drift dans les données (distribution change)
- [ ] Réentraînement automatique des imputations (KNN/MICE) périodiquement
- [ ] Log des erreurs de prédiction → identifier les données manquantes responsables
- [ ] Scoring qualité par record qui s'améliore à chaque pass
# --- AUDIT PILIER 19 : tâches ajoutées ---
- [ ] 🔴 Correction rétroactive : PMU corrige un résultat → détecter + propager dans tout le pipeline
- [ ] 🔴 Auto-détection features obsolètes : rolling window pouvoir prédictif → flagger auto si baisse
- [ ] 🟠 Pipeline auto feature generation : combinaisons auto (produits, ratios, différences) + test prédictif
- [ ] 🟠 A/B testing imputations : comparer KNN vs MICE vs median vs mode par champ sur sample
- [ ] 🟠 Suggestions auto : "source X améliorerait champ Y de 60% → 90%"
- [ ] 🟠 Benchmark qualité données par trimestre : comparer complétude, cohérence, fraîcheur

# ┌─────────────────────────────────────────┐
# │  PILIER 20 — ALIGNEMENT TURF/MARCHÉ    │
# └─────────────────────────────────────────┘
# Les données reflètent la réalité du marché des paris.

- [ ] Cotes PMU vs cotes exchange vs cotes bookmakers → triangulation
- [ ] Historique des mouvements de cotes (pas juste la cote finale)
- [ ] Volume de paris par course et par type de pari
- [ ] Profiling des parieurs sharp vs public
- [ ] Données de liquidité par marché (Betfair, PMU, Smarkets)
- [ ] Taux de retour par type de pari (simple, couplé, trio, etc.)
- [ ] Historique des dividendes PMU
- [ ] Comparaison proba implicite cotes vs proba réelle historique
# --- AUDIT PILIER 20 : tâches ajoutées ---
- [ ] 🔴 True price méthode de Shin : supprimer marge bookmaker → estimer vraie proba
- [ ] 🔴 Détection steam moves automatisée : cote chute brutale (seuil configurable) + log timing
- [ ] 🟠 Calcul overround par course et par bookmaker (mesurer efficience marché)
- [ ] 🟠 Index liquidité normalisé par course (comparer handicap Province vs Groupe 1 Longchamp)
- [ ] 🟠 Cotes historiques fractionnelles/décimales bookmakers UK pour courses UK
- [ ] 🟠 Indicateur "market surprise" : écart résultat vs attentes marché par course
- [ ] 🟠 Suivi market movers : top 3-5 chevaux dont cote change le plus dans dernières heures
- [ ] 🟡 Collecte limites paris par bookmaker (estimer confiance du bookmaker dans sa cote)

# ┌─────────────────────────────────────────┐
# │  PILIER 21 — TRAÇABILITÉ               │
# └─────────────────────────────────────────┘
# Pouvoir remonter de n'importe quelle valeur à sa source.

- [ ] Champ _source sur chaque record (ex: "pmu_api", "letrot_scrape", "openmeteo")
- [ ] Champ _collected_at sur chaque record (date de collecte)
- [ ] Champ _version sur chaque fichier maître
- [ ] Champ _modified_by sur chaque comblage (ex: "fill_penetrometre_from_meteo")
- [ ] Historique des transformations par record (pipeline de transformations)
- [ ] Pouvoir répondre : "d'où vient la cote 3.5 de ce cheval dans cette course ?"
- [ ] Index inversé : pour chaque source, lister tous les records qu'elle a produit
# --- AUDIT PILIER 21 : tâches ajoutées ---
- [ ] 🔴 Provenance complète par valeur : chaîne source_brute → nettoyage → imputation → merge → feature avec timestamps
- [ ] 🔴 _confidence_score par valeur (pas seulement par record) : cote officielle=1.0, imputée=0.5
- [ ] 🟠 Outil requête provenance : "toutes les étapes qui ont produit feature X pour partant Y dans course Z"
- [ ] 🟠 Tracking suppressions : quand record supprimé (dédup, nettoyage) → logger pourquoi et où il était
- [ ] 🟠 Rapport traçabilité par feature : sources brutes qui y contribuent + % contribution

# ┌─────────────────────────────────────────┐
# │  PILIER 22 — META-CONFIGURATION        │
# └─────────────────────────────────────────┘
# Tout est configurable, rien n'est hardcodé.

- [ ] config/global.yaml : chemins, URLs base, paramètres globaux
- [ ] config/sources.yaml : liste des sources avec URL, fréquence, priorité
- [ ] config/features.yaml : liste des features avec builder, paramètres
- [ ] config/pipeline.yaml : ordre d'exécution, dépendances, timeouts
- [ ] config/quality.yaml : seuils de qualité (min remplissage, max outliers)
- [ ] config/alerts.yaml : configuration des alertes (seuils, destinataires)
- [ ] Chaque script lit sa config depuis un fichier, pas de valeur en dur
- [ ] Possibilité de changer de source/paramètre sans modifier le code
- [ ] Fichier .env pour les secrets (clés API, tokens)
# --- AUDIT PILIER 22 : tâches ajoutées ---
- [ ] 🔴 Validation auto configs au démarrage : champs requis présents, URLs valides, seuils cohérents
- [ ] 🔴 Config par environnement : dev (petit sample), staging (1 an), prod (tout) avec un switch
- [ ] 🟠 Diff de config : comparer config actuelle vs config d'un run précédent
- [ ] 🟠 Overrides via variables d'environnement (ex: PIPELINE_MAX_RAM=32G override le yaml)
- [ ] 🟠 Schema validation pour chaque fichier config (JSON Schema ou pydantic)
- [ ] 🟡 Générateur config : outil qui génère config de base pour première installation
- [ ] 🟡 CONFIG_REFERENCE.md : toutes variables avec valeurs défaut, description, exemples

# ┌─────────────────────────────────────────┐
# │  PILIER 23 — GPU-AWARENESS,            │
# │  MONITORING, HAUTE DISPONIBILITÉ       │
# └─────────────────────────────────────────┘
# Exploiter le hardware au maximum et ne jamais s'arrêter.

- [ ] Détecter automatiquement GPU (CUDA) et l'utiliser pour le preprocessing lourd
- [ ] cuDF (GPU DataFrame) pour les opérations sur gros DataFrames si GPU dispo
- [ ] Monitoring RAM/CPU/GPU en temps réel pendant les scripts
- [ ] Alerte si RAM > 80% → réduire la charge automatiquement
- [ ] Alerte si disque > 90% → nettoyer les caches anciens
- [ ] Mode dégradé : si GPU pas dispo, fallback CPU transparent
- [ ] Multiprocessing pour les scripts CPU-bound (feature building)
- [ ] Async I/O pour les scripts I/O-bound (scraping)
- [ ] Process manager (supervisor/systemd) pour garantir que les scripts tournent 24/7
- [ ] Healthcheck endpoint : script qui vérifie que tout tourne bien
- [ ] Auto-restart si un script consomme trop de RAM (kill + relaunch)
- [ ] Rotation des logs (pas de fichier log de 10 GB)
# --- AUDIT PILIER 23 : tâches ajoutées ---
- [ ] 🔴 Profilage hardware auto au démarrage : détecter cores, RAM, GPU VRAM, vitesse disque → adapter batch size, nb workers
- [ ] 🔴 Quotas ressources par étape : scraping ≤25% RAM, merge ≤75% RAM (configurable)
- [ ] 🟠 RAPIDS (cuML, cuDF) pour imputation KNN/MICE sur GPU si disponible
- [ ] 🟠 Scheduling intelligent : tâches GPU-bound et CPU-bound ne se chevauchent pas
- [ ] 🟠 Failover : script secours reprend auto si principal crashe avec données sauvegardées
- [ ] 🟠 Monitoring température GPU/CPU : ralentir auto si thermal throttling détecté
- [ ] 🟠 Rapport capacité : estimer temps pour X courses supplémentaires selon hardware
- [ ] 🟡 Calcul distribué (Dask/Ray) si multi-machines disponibles
- [ ] 🟡 Checkpoint GPU : sauvegarder état calcul périodiquement → reprendre après crash GPU/OOM


# ════════════════════════════════════════════════════════════════
# 🔍 RÉSULTATS AUDIT — TÂCHES MANQUANTES AJOUTÉES
# ════════════════════════════════════════════════════════════════

# ┌─────────────────────────────────────────┐
# │  ÉTAPE 0 — FONDATIONS TECHNIQUES       │
# │  (À FAIRE EN PREMIER)                  │
# └─────────────────────────────────────────┘

## 0.1 Fichiers projet critiques manquants
- [ ] Créer .gitignore (exclure output/, backups/, __pycache__, logs/, *.pyc)
- [ ] Créer .env pour les clés API (Betfair, Smarkets, NOAA, Météo France, etc.)
- [ ] Créer requirements.txt COMPLET (ajouter ijson, pandas, numpy, requests, etc.)
- [ ] Créer requirements.lock (pip freeze exact pour reproductibilité)
- [ ] Créer pyproject.toml ou setup.py (projet installable)
- [ ] Créer Makefile : commandes make scrape, make merge, make features, make test, etc.
- [ ] Supprimer TOUS les chemins absolus hardcodés (/Users/quentinherve/...)
      → utiliser pathlib.Path(__file__).parent ou variable d'environnement
- [ ] Créer config/global.py avec BASE_DIR, OUTPUT_DIR, etc. centralisés

## 0.2 Structure dossiers manquants
- [ ] Créer tests/ avec structure pytest (tests/test_parsing.py, test_merging.py, etc.)
- [ ] Créer config/ (global.yaml, sources.yaml, features.yaml, pipeline.yaml)
- [ ] Créer schemas/ (JSON Schema pour valider chaque type de fichier)
- [ ] Créer reference/ (hippodromes_db.py, alias, constantes métier)
- [ ] Créer scripts/scrapers/ (tous les scripts XX_*.py)
- [ ] Créer scripts/mergers/ (merge_02_02b.py, mega_merge, etc.)
- [ ] Créer scripts/patches/ (patch_brutes_*.py, fill_empty_fields.py)
- [ ] Créer scripts/utils/ (utilitaires divers)
- [ ] Créer migrations/ (scripts de migration quand le schéma change)

## 0.3 Manifest et catalogues
- [ ] Créer data_catalog.json : chaque source avec ses champs, clés jointure, dépendances
- [ ] Créer MANIFEST.json : tous les fichiers avec taille, date, checksum SHA256
- [ ] Créer sources_status.json : dernière MAJ, nb records, taux erreur par source
- [ ] Créer sync_status.json : état de synchronisation entre sources

# ┌─────────────────────────────────────────┐
# │  FEATURES OUBLIÉES                     │
# └─────────────────────────────────────────┘

## Features temporelles (manquaient totalement)
- [ ] jour_semaine (lundi=peu de courses, samedi/dimanche=gros meetings)
- [ ] heure_course (première/dernière course, effet fatigue jockey)
- [ ] mois / saison (saisonnalité des formes, plat vs obstacle)
- [ ] numero_course_dans_reunion (R1C1 vs R1C8)
- [ ] jours_depuis_debut_saison (flat season, jump season)
- [ ] est_jour_ferie (plus de parieurs = déformation des cotes)
- [ ] position_course_reunion (première, dernière, course phare)

## Features de contexte course
- [ ] type_paris_disponibles (quinté, tiercé, simple → influe volumes)
- [ ] prestige_course (Groupe 1/2/3, Listed, handicap, claimer, réclamer)
- [ ] est_course_support vs est_course_phare
- [ ] ecart_cotes_pmu_vs_exchange (inefficience marché)
- [ ] concentration_paris_favori (% volume sur le favori)

## Features avancées
- [ ] momentum_3_5_10 (dérivée de la forme récente)
- [ ] regression_moyenne_score (va-t-il régresser ?)
- [ ] elo_rating (rating adaptatif basé sur adversaires battus)
- [ ] bayesian_rating (TrueSkill, prenant en compte incertitude)
- [ ] entropy_marche (course ouverte vs fermée)
- [ ] expected_value_brute (cote × proba implicite)
- [ ] closing_line_value (CLV : écart ouverture vs fermeture)
- [ ] speed_figures_normalises (comme Beyer Speed Figures US)
- [ ] classe_relative_peloton (niveau cheval vs ce peloton spécifique)
- [ ] fatigue_cumulee_30_60_90j (nb courses pondérées par distance)
- [ ] pattern_retour_repos (perf après repos long)
- [ ] first_time_events (1er départ PSF, 1ères œillères, 1ère distance)
- [ ] jockey_booking_signal (jockey top-10 monte cheval inconnu = signal)
- [ ] changement_entraineur_recent (signal potentiel)
- [ ] entraineur_forme_recente (rolling win rate 30j de l'entraîneur)

## Features pedigree avancées
- [ ] inbreeding_coefficient (coefficient de consanguinité)
- [ ] dosage_index (dosage de Rasmussen, Center of Distribution)
- [ ] aptitude_genetique_surface (sire stats gazon vs PSF vs dirt)
- [ ] aptitude_genetique_distance (sire average winning distance)
- [ ] precocity_index (fils de certains étalons performent jeunes)
- [ ] broodmare_sire_influence (impact père de la mère)

## Features odds movement
- [ ] steam_move_detection (baisse brutale de cote)
- [ ] drift_detection (hausse brutale de cote)
- [ ] vwap_cotes (Volume-Weighted Average Price)
- [ ] market_consensus_vs_pmu_divergence
- [ ] overround_evolution (marge bookmaker dans le temps)

## Croisements oubliés
- [ ] cheval × météo (perf quand il pleut, quand il fait chaud)
- [ ] jockey × hippodrome (spécialiste de certains hippos)
- [ ] entraîneur × type_course (domine handicaps vs Groupes)
- [ ] sire × distance × terrain (triple croisement pedigree)
- [ ] age_cheval × mois (jeunes progressent en début de saison)
- [ ] performances_meme_course (même hippo + même distance + même discipline dans l'historique)

## Champs importants non mentionnés
- [ ] handicap_rating_officiel (France Galop / BHA, distinct du poids porté)
- [ ] nb_departs_carriere (expérience globale)
- [ ] gains_totaux_carriere (indicateur de classe)
- [ ] gains_par_course_moyen (normalisé)
- [ ] surcharge_decharge_jockey (poids réel vs poids handicap)
- [ ] pays_naissance_cheval (pays d'origine, élevage spécifique)
- [ ] statut_castration (entier/hongre/jument — impact par âge)
- [ ] stall_draw / position_depart (numéro de stalle en galop)

## Labels supplémentaires
- [ ] y_roi_combine (ROI sur paris combinés)
- [ ] y_place_top2 (pour le couplé)
- [ ] y_exacta / y_tierce / y_quarte / y_quinte (paires ordonnées)
- [ ] y_ecart_temps (écart en secondes avec le gagnant — régression)
- [ ] y_vitesse_normalisee (speed figure comme target)
- [ ] y_value_bet (le cheval a-t-il été un value bet rétrospectif ?)

# ┌─────────────────────────────────────────┐
# │  QUALITÉ DONNÉES APPROFONDIE           │
# └─────────────────────────────────────────┘

## Validation de schéma
- [ ] Créer JSON Schema pour partants (types, min/max, enums, required)
- [ ] Créer JSON Schema pour courses
- [ ] Créer JSON Schema pour pedigree
- [ ] Créer JSON Schema pour météo
- [ ] Script validate_schema.py : valide tous les fichiers contre les schémas
- [ ] Exécuter la validation après chaque merge/scrape

## Intégrité référentielle
- [ ] Chaque partant_uid → course_uid existant
- [ ] Chaque course_uid → reunion_uid existant
- [ ] Chaque hippodrome_normalise → entrée dans hippodromes_db
- [ ] Chaque jockey → entrée dans historique jockeys
- [ ] Script check_referential_integrity.py

## Tests de non-régression
- [ ] Après re-scrape : nb records ne diminue JAMAIS
- [ ] Après merge : nb records ≥ max(source_A, source_B)
- [ ] Après feature building : nb features ≥ précédent run
- [ ] Tests automatiques dans tests/ avec pytest

## Feature selection (après les 468+ features)
- [ ] Calculer corrélation inter-features → supprimer si >0.95
- [ ] Calculer VIF (Variance Inflation Factor) → multicolinéarité
- [ ] Feature importance (permutation, SHAP) → ranking
- [ ] Supprimer features à 0 importance
- [ ] PCA/UMAP pour exploration dimensionnelle
- [ ] Documenter les features retenues et pourquoi

# ┌─────────────────────────────────────────┐
# │  MAINTENANCE QUOTIDIENNE               │
# └─────────────────────────────────────────┘

## Pipeline incrémental quotidien
- [ ] Script daily_update.sh : scrape les courses du jour
- [ ] Mode incrémental : ne traiter que les nouveaux records (pas tout re-scraper)
- [ ] Système de delta/diff (ne merger que le nouveau)
- [ ] Cron job pour lancer automatiquement chaque soir
- [ ] Notification si le daily_update échoue

## Gestion d'une nouvelle année
- [ ] Procédure documentée : quels scripts relancer, dans quel ordre
- [ ] Étendre calendrier automatiquement
- [ ] Vérifier que l'année est complète (365 jours couverts)

## Rebuild from scratch
- [ ] Script rebuild_all.sh : enchaîne tout dans le bon ordre
- [ ] Estimation temps de rebuild documentée
- [ ] Dépendances entre scripts (DAG) documentées

## Rollback
- [ ] Procédure de rollback documentée
- [ ] Versioning des fichiers master (v1, v2, v3...)
- [ ] Checkpoints automatiques avant chaque opération destructive

## Gestion erreurs scraping avancée
- [ ] Circuit-breaker : si un site est down, ne pas boucler
- [ ] Rate-limiting configurable par source (dans config/sources.yaml)
- [ ] Gestion des bans IP (rotation proxy, user-agent, backoff)
- [ ] Diagnostic automatique avant relance (pas relancer aveuglément)

# ┌─────────────────────────────────────────┐
# │  PERFORMANCE BASE DE DONNÉES           │
# └─────────────────────────────────────────┘

## Conversion DuckDB (CRITIQUE pour les gros fichiers)
- [ ] Installer DuckDB
- [ ] Convertir partants_master.json → partants.duckdb
- [ ] Convertir courses_master.json → courses.duckdb
- [ ] Indexer par course_uid, partant_uid, date, hippodrome
- [ ] Requêtes SQL au lieu de json.load() pour les jointures
- [ ] Benchmark : comparer temps requête JSON vs DuckDB

## Partitionnement
- [ ] Partitionner par année : partants_2014.parquet, ..., partants_2026.parquet
- [ ] Consolider les milliers de petits cache JSON en fichiers annuels
- [ ] output/22_performances_detaillees/cache/ (97K fichiers → 12 fichiers annuels)

## Compression
- [ ] Compresser archives anciennes (<2020) en zstd ou lz4
- [ ] Estimation gain : 80 GB → ~20 GB compressé

## Streaming / batch processing
- [ ] master_feature_builder.py : passer en mode batch/chunk (pas tout en RAM)
- [ ] mega_merge : streaming JSON (ijson) pour les fichiers >1 GB
- [ ] Limiter RAM par script (monitoring interne)

# ┌─────────────────────────────────────────┐
# │  HIPPODROMES_DB AMÉLIORATIONS          │
# └─────────────────────────────────────────┘

- [ ] Corriger doublons (aby / aby goteborg / aby suede = même hippo)
- [ ] Normaliser type_piste : 'herbe' et 'gazon' → un seul terme
- [ ] Normaliser pays : 'france' vs 'suède' vs 'suede' vs 'états-unis' → ISO
- [ ] Ajouter longueur_ligne_droite_arrivee
- [ ] Ajouter denivele_parcours
- [ ] Ajouter largeur_piste
- [ ] Ajouter rayon_virages
- [ ] Fonctions utilitaires : recherche fuzzy, liste par pays, par discipline
- [ ] Fonction distance_from(lat, lon) → distance au plus proche hippo
- [ ] Compléter type_piste pour les 291 hippodromes sans
- [ ] Compléter corde pour les 340 hippodromes sans
- [ ] Compléter disciplines pour les 291 hippodromes sans

# ┌─────────────────────────────────────────┐
# │  PIPELINE D'INFÉRENCE (TEMPS RÉEL)     │
# └─────────────────────────────────────────┘
# Préparer la donnée pour la prédiction en direct (autre dossier mais la data doit le supporter)

- [ ] Script scrape_partants_du_jour.py : récupérer le programme du jour
- [ ] Script features_temps_reel.py : calculer les features pour les courses du jour
- [ ] Format de sortie standardisé pour les prédictions
- [ ] Données de cotes en temps réel (Betfair, PMU)
- [ ] Structure data compatible avec le streaming (pas besoin de tout recharger)
- [ ] API FastAPI pour servir les données aux modèles

# ┌─────────────────────────────────────────┐
# │  REPRODUCTIBILITÉ & CI/CD              │
# └─────────────────────────────────────────┘

- [ ] Dockerfile pour environnement reproductible
- [ ] docker-compose.yml si nécessaire (DuckDB + API)
- [ ] Linting : configurer ruff ou black (formatage code cohérent)
- [ ] Type checking : configurer mypy (types Python)
- [ ] Pre-commit hooks : lint + format avant chaque commit
- [ ] GitHub Actions : tests automatiques sur push
- [ ] Rotation des logs (logrotate ou script custom, pas de log de 10 GB)
- [ ] Logging structuré JSON pour agrégation

# ┌─────────────────────────────────────────┐
# │  DATA LEAKAGE PREVENTION               │
# └─────────────────────────────────────────┘

- [ ] Exécuter quality/leakage_detector.py systématiquement avant export
- [ ] Vérifier que train/test split respecte la temporalité
- [ ] Aucune donnée future dans le train set
- [ ] Documenter quels champs sont "post-course" (à exclure pour prédiction)
- [ ] Marquer chaque champ : pre_course / post_course / metadata

# ┌─────────────────────────────────────────┐
# │  POINT-IN-TIME CORRECTNESS             │
# │  🔴 CRITIQUE — DATA LEAKAGE           │
# └─────────────────────────────────────────┘
- [ ] 🔴 Garantir que CHAQUE feature rolling est calculée avec date < date_course (jamais ≤)
- [ ] 🔴 Point-in-time join : rejoindre la bonne version de chaque feature au bon moment
- [ ] 🔴 Script validate_point_in_time.py : vérifier qu'aucune feature ne contient d'info future
- [ ] 🔴 Marquage chaque champ : available_at = "J-1", "J-0 10h", "post-course"
- [ ] 🔴 Tester sur sample : recalculer 1000 features avec date stricte → comparer avec actuel

# ┌─────────────────────────────────────────┐
# │  ENTITY RESOLUTION / ID MATCHING       │
# │  🔴 CRITIQUE — JOINTURES              │
# └─────────────────────────────────────────┘
- [ ] 🔴 Table résolution d'entités : entity_id unique par cheval, jockey, entraîneur
- [ ] 🔴 Mapping multi-source : {entity_id: 42, pmu_id: "P123", sire_id: "12345678Z", rp_id: "UK-567"}
- [ ] 🔴 Algorithme matching fuzzy + validation manuelle pour cas ambigus
- [ ] 🔴 Gestion changements de nom de cheval (fréquent à l'international)
- [ ] 🔴 Gestion homonymes (2 chevaux différents même nom dans 2 pays)
- [ ] 🔴 Script build_entity_registry.py

# ┌─────────────────────────────────────────┐
# │  NORMALISATION UNITÉS INTERNATIONALES  │
# │  🔴 CRITIQUE — DONNÉES INCOMPARABLES  │
# └─────────────────────────────────────────┘
- [ ] 🔴 Distances : mètres (FR) vs furlongs (UK/US) vs yards → tout en mètres
- [ ] 🔴 Poids : kg (FR) vs stones+pounds (UK) vs pounds (US) → tout en kg
- [ ] 🔴 Going/terrain : FR (bon/souple/lourd) vs UK (good/soft/heavy) vs US (fast/muddy) → table mapping universelle
- [ ] 🔴 Gains : EUR, GBP, USD, AUD, HKD, JPY → normaliser en EUR avec taux change historique par date
- [ ] 🟠 Âge : Nord (1er janvier) vs Sud (1er août) → norme unique
- [ ] 🟠 Fuseaux horaires pour heures de course internationales
- [ ] 🟠 Script normalize_units.py centralisé pour toutes les conversions

# ┌─────────────────────────────────────────┐
# │  FEATURE STORE & BACKFILL             │
# │  🔴 CRITIQUE — TRAINING-SERVING SKEW  │
# └─────────────────────────────────────────┘
- [ ] 🔴 Feature store centralisé (Feast ou système maison clé-valeur daté)
- [ ] 🔴 Même code calcul pour batch (historique) et online (temps réel)
- [ ] 🟠 Versioning features : si formule change, anciennes valeurs restent cohérentes
- [ ] 🟠 Feature freshness tracking : quand chaque feature a été calculée
- [ ] 🟠 Script backfill_feature.py <feature_name> : recalcule sur tout l'historique
- [ ] 🟠 Parallélisation backfill (chunked par année)
- [ ] 🟠 Validation post-backfill (pas de NaN, distribution cohérente)

# ┌─────────────────────────────────────────┐
# │  CLASS IMBALANCE & SPLITS              │
# │  🟠 IMPORTANT — MODÈLES               │
# └─────────────────────────────────────────┘
- [ ] 🟠 Documenter distribution labels (% victoire, % top 3, % rentable)
- [ ] 🟠 Fournir poids de classe pré-calculés dans labels/
- [ ] 🟠 Stratégie sampling documentée (oversampling, undersampling, SMOTE)
- [ ] 🟠 Labels par course groupés (ne JAMAIS séparer partants même course entre train/test)
- [ ] 🟠 Split par course_uid (GroupKFold)
- [ ] 🟠 Walk-forward validation (train 2014-2022, test 2023, glisser)
- [ ] 🟠 Purging : gap temporel entre train et test
- [ ] 🟠 Fournir splits pré-calculés dans labels/splits/
- [ ] 🟠 docs/VALIDATION.md : stratégie de validation documentée

# ┌─────────────────────────────────────────┐
# │  FEATURE TYPE METADATA                 │
# │  🟠 IMPORTANT — POUR LES MODÈLES      │
# └─────────────────────────────────────────┘
- [ ] 🟠 feature_types.json : {type: numeric|categorical|binary|ordinal, cardinality, encoding_suggestion}
- [ ] 🟠 Identifier features haute cardinalité (nom_cheval 50K+ → target encoding obligatoire)
- [ ] 🟠 Identifier features ordonnées (position_finale = ordinal, pas nominal)
- [ ] 🟠 Identifier features circulaires (mois, jour_semaine → sin/cos encoding)
- [ ] 🟠 missing_indicator features : has_sectionals, has_pedigree_4gen, has_weather

# ┌─────────────────────────────────────────┐
# │  SANITY CHECKS MÉTIER                  │
# │  🟠 IMPORTANT — DÉTECTION ERREURS     │
# └─────────────────────────────────────────┘
- [ ] 🟠 Vérifier : un cheval ne peut PAS courir 2 courses le même jour à 2 hippodromes différents
- [ ] 🟠 Vérifier : un jockey ne monte pas 2 chevaux dans la même course
- [ ] 🟠 Vérifier : date naissance cheval AVANT première course
- [ ] 🟠 Vérifier : poids porté plage réaliste (45-80 kg galop, 60-90 kg obstacle)
- [ ] 🟠 Vérifier : cote > 1.0
- [ ] 🟠 Vérifier : nombre partants entre 3 et 24
- [ ] 🟠 Vérifier : gagnant DANS la liste des partants de cette course
- [ ] 🟠 Script sanity_checks.py avec toutes ces règles

# ┌─────────────────────────────────────────┐
# │  ANTI-SCRAPING / LÉGALITÉ             │
# │  🟠 IMPORTANT — 120+ SCRAPERS         │
# └─────────────────────────────────────────┘
- [ ] 🟠 Rotation proxies résidentiels pour scrapers lourds
- [ ] 🟠 Pool User-Agents rotatifs
- [ ] 🟠 Respect robots.txt par source
- [ ] 🟠 Rate limiting implémenté et configurable par source (dans config/sources.yaml)
- [ ] 🟠 Gestion CAPTCHA (2captcha, hCaptcha solver ou skip)
- [ ] 🟠 legal_compliance.md : quels sites autorisent le scraping
- [ ] 🟠 Headless browser pool (Playwright/Selenium) pour sites JS-heavy

# ┌─────────────────────────────────────────┐
# │  MULTI-DISCIPLINE & NON-ÉVÉNEMENTS     │
# │  🟡 SECONDAIRE                         │
# └─────────────────────────────────────────┘
- [ ] 🟡 Séparer feature matrices par discipline (ou flag discipline)
- [ ] 🟡 Features discipline-spécifiques (déferré=trot only, stall_draw=galop only)
- [ ] 🟡 Documenter quelles features s'appliquent à quelles disciplines
- [ ] 🟡 Courses annulées/reportées → données contexte
- [ ] 🟡 Non-partants dernière minute → feature nb_non_partants (signal terrain/météo)
- [ ] 🟡 Chevaux déclarés puis retirés → feature withdrawal_rate_per_stable

# ┌─────────────────────────────────────────┐
# │  VERSIONING MATRICE DE FEATURES        │
# │  🟡 SECONDAIRE                         │
# └─────────────────────────────────────────┘
- [ ] 🟡 Chaque version features_matrix versionnée : v1.0, v1.1, v2.0
- [ ] 🟡 Changelog features ajoutées/supprimées par version
- [ ] 🟡 Modèles référencent UNE version précise de la matrice
- [ ] 🟡 DVC (Data Version Control) ou système maison pour versionner les données

# ════════════════════════════════════════════════════════════════
# COMPTEURS FINAUX MIS À JOUR (après audit expert 15/03/2026)
# ════════════════════════════════════════════════════════════════
# TÂCHES TOTALES: ~1010+ (769 initiales + 90 audit #1 + 148 audit piliers)
# dont 🔴 critiques: ~60  🟠 importantes: ~120  🟡 nice-to-have: ~60
#
# Scripts de collecte existants: 41
# Nouvelles sources à scraper: ~120+
# Features actuelles: 80
# Features builders cassés: +177
# Features nouvelles sources: +130
# Features croisées: +81
# Features temporelles/contexte/avancées: +60
# TOTAL CIBLE: 528+ features (avant selection)
# Labels: 15+ (victoire, place, ROI, temps, value bet...)
# Records partants: ~2.7M+
# Courses: ~221K+
# Années couvertes: 2004-2026
# Hippodromes: 673 (monde entier)
# Taille données brutes: ~80+ GB
# Format cible: Parquet + DuckDB
# Zéro trou, zéro corruption, zéro doublon
# Documentation complète (docs/, schemas/, tests/)
# Backup versionné
# Pipeline incrémental quotidien
# Prêt pour inférence temps réel
#
# 23 piliers qualité couverts + audités
# Entity resolution multi-source
# Normalisation unités internationales
# Point-in-time correctness garanti
# Feature store + backfill strategy
# Group-aware train/test splits
# Anti-scraping / légalité
# ════════════════════════════════════════════════════════════════
