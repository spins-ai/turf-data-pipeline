# TODOLIST DEFINITIVE V2 - Pipeline Donnees Turf
# Sauvegarde le 2026-04-11
# ================================================
# Cette liste contient TOUT ce qu'il reste a faire.
# Rien n'est oublie. Quand tout est coche, le pipeline est parfait.

## LEGENDE
- [x] Termine
- [ ] A faire
- [SKIP] Inutile / reporte

---

## A. DEJA FAIT (pour memoire)

- [x] A1. Suppression 7 colonnes post-course (leakage round 1)
- [x] A2. Suppression leakage round 2 (wmf_position_margin, log_rapport, etc.)
- [x] A3. Suppression leakage round 3 (rtm_is_fastest, wmf_top_quarter)
- [x] A4. Selection 500 features par LightGBM (features_selected.parquet)
- [x] A5. Validation post-pipeline OK (zero leakage, target 8.5%)
- [x] A6. Git: 378 fichiers sauvegardes, 10 commits
- [x] A7. Nettoyage 301 builder_outputs (295 Go liberes)
- [x] A8. Nettoyage 5 JSONL partants_master (109 Go liberes)
- [x] A9. Suppression scripts obsoletes (DuckDB, wave, v2)
- [x] A10. DuckDB reduit (11.2 -> 6.3 Go)
- [x] A11. Rapports couvrent 2013-2026 (OK)

---

## B. DONNEES A RECUPERER / FUSIONNER

### B1. [ ] Performances detaillees -> performances_master.parquet
- Source: 02_DONNEES_BRUTES/22_performances_detaillees/ (4 Go, ~600K records)
- Contient: temps par course, position, terrain, distance, jockey
- A faire: convertir JSONL en Parquet, joindre avec partant_uid
- Impact: ELEVE - donne les temps historiques de chaque cheval

### B2. [ ] Sectionnels -> integrer dans performances_master
- Source: 02_DONNEES_BRUTES/11_sectionals/sectionals.parquet (243K rows)
- Contient: temps intermediaires, vitesse km/h, ecart vs gagnant
- A faire: fusionner avec performances_master par partant_uid
- Impact: MOYEN - donne la vitesse par portion de course

### B3. [ ] Cotes historiques -> relancer script 07
- Source: 02_DONNEES_BRUTES/07_cotes_marche/ (286 Mo)
- Probleme: script 07 pas relance depuis 2020 pour la periode 2020-2026
- A faire: relancer scripts/collection/07_cotes_marche.py
- Impact: ELEVE - cotes = signal de marche tres predictif

### B4. [ ] Citations enjeux -> exporter 177K fichiers cache
- Source: 02_DONNEES_BRUTES/27_citations_enjeux/ (13 Go, 178K fichiers)
- Probleme: cache JSON jamais exporte en master
- A faire: lancer scripts/export_citations_full.py
- Impact: MOYEN - info sur les enjeux financiers par course

### B5. [ ] Pedigrees profonds -> exporter 15K cache + scraper 14
- Source: 02_DONNEES_BRUTES/14_pedigree/ (45K fichiers)
- Probleme: 15K pedigrees en cache jamais fusionnes
- A faire: exporter cache, relancer scraper 14 pour profondeur 3+ generations
- Impact: MOYEN - lignee du cheval (pere, grand-pere, etc.)

### B6. [ ] Historique chevaux -> horse_stats_master
- Source: 02_DONNEES_BRUTES/05_historique_chevaux/ (324 Mo)
- horse_stats_master.json ne fait que 2 octets (vide!)
- A faire: lancer le merge pour creer le vrai fichier
- Impact: MOYEN - stats de carriere aggregees

### B7. [ ] Donnees LeTrot -> integrer trot
- Source: 02_DONNEES_BRUTES/83_letrot/ (982 Mo) + 02b_scraper_letrot/
- Probleme: donnees trot separees, pas dans partants_master
- A faire: parser et fusionner dans partants_master
- Impact: FAIBLE pour le galop, ELEVE pour le trot

### B8. [ ] Pronostics experts -> parser
- Source: 02_DONNEES_BRUTES/23_pronostics/ (498 Mo, 207K fichiers)
- Contient: avis d'experts et pronostiqueurs
- A faire: parser et creer pronostics_master.parquet
- Impact: MOYEN - consensus des experts = signal utile

### B9. [ ] Racing Post data -> parser
- Source: 02_DONNEES_BRUTES/37_racing_post/ (4.5 Go)
- racing_post_master.json ne fait que 2 octets (vide!)
- A faire: parser les donnees Racing Post
- Impact: MOYEN - source internationale de qualite

### B10. [ ] Donnees PMU API -> verifier couverture
- Source: 02_DONNEES_BRUTES/101_pmu_api/ (5 Go, 225K fichiers)
- A faire: verifier quelles donnees sont deja dans partants_master
- Impact: FAIBLE si deja integre, ELEVE sinon

---

## C. NOUVEAUX CALCULS (Feature Builders)

### Builders prioritaires (impact eleve sur prediction)

### C1. [ ] musique_lag_features
- Quoi: Position individuelle des 5 dernieres courses (1er, 3e, 7e, etc.)
- Pourquoi: La "forme recente" est le signal #1 en courses hippiques
- Necessite: performances_master (B1)

### C2. [ ] handicap_deep
- Quoi: Poids du handicap vs moyenne du peloton, ecart au poids ideal
- Pourquoi: Le poids est crucial en plat/obstacle
- Source: deja dans partants_master (colonnes poids/handicap)

### C3. [ ] speed_form_composite
- Quoi: Meilleur chrono a chaque distance, vitesse relative au terrain
- Pourquoi: La vitesse pure est tres predictive
- Necessite: performances_master (B1)

### C4. [ ] ecart_repos_features
- Quoi: Jours depuis derniere course, duree optimale de repos par cheval
- Pourquoi: Un cheval trop/pas assez repose performe mal
- Source: deja dans partants_master (dates)

### C5. [ ] poids_impact_deep
- Quoi: Interaction poids x distance x terrain x age
- Pourquoi: L'impact du poids varie selon les conditions
- Source: deja dans partants_master

### Builders secondaires (impact moyen)

### C6. [ ] proprietaire_eleveur_deep
- Quoi: Taux victoire par proprietaire et eleveur
- Source: partants_master + pedigree_master

### C7. [ ] inedit_debutant
- Quoi: Signaux pour chevaux en premiere course (sans historique)
- Source: partants_master (nb_courses_carriere == 0)

### C8. [ ] incident_history
- Quoi: Historique disqualifications, chutes, fautes de parcours
- Source: partants_master (colonne statut)

### C9. [ ] pays_origin_features
- Quoi: Pays de naissance du cheval, stats par pays
- Source: pedigree_master

### C10. [ ] robe_phenotype
- Quoi: Couleur robe x discipline x terrain (certaines robes correlent avec endurance)
- Source: partants_master (colonne robe)

### C11. [ ] market_exotic_features
- Quoi: Paris exotiques (couple, trio, quinte) - detection argent intelligent
- Necessite: citations (B4)

### C12. [ ] combinaison_marche_deep
- Quoi: Analyse fine des mouvements de cotes (argent intelligent)
- Source: marche_master.parquet

---

## D. QUALITE ET NETTOYAGE

### D1. [ ] Supprimer 50 dossiers vides dans 02_DONNEES_BRUTES
- 18 completement vides + ~30 avec juste un placeholder
- Gain: clarte, pas d'espace disque significatif

### D2. [ ] Archiver les gros JSONL restants
- training_labels.jsonl (941 Mo, corrompu/vide)
- Verifier s'il reste d'autres JSONL inutiles
- Gain potentiel: quelques Go

### D3. [ ] Auditer 02_DONNEES_BRUTES en detail (restant ~150 Go)
- builder_outputs restant: 11.7 Go (17 suspects)
- 02_liste_courses + raw_pmu + merged_intermediate: 82 Go
- Verifier si les merged_intermediate sont deja dans master
- Gain potentiel: 50-80 Go

### D4. [ ] Recalculer stats de normalisation
- Les features_consolidated ont change (leakage supprime)
- Les stats min/max/mean doivent etre recalculees
- Impact: necessaire avant entrainement modele

### D5. [ ] Colonnes >50% NaN: evaluer et traiter
- 77 colonnes avec beaucoup de valeurs manquantes
- Options: supprimer, imputer, ou garder (LightGBM gere bien les NaN)
- A decider avant entrainement

---

## E. CODE ET AUTOMATISATION

### E1. [ ] Centraliser les chemins (333 fichiers avec D:/turf en dur)
- config.py existe deja avec les bons chemins
- A faire: remplacer les chemins en dur par import config
- Impact: le code marchera sur n'importe quel PC

### E2. [ ] Mettre a jour run_full_pipeline.sh
- Le script pipeline existe mais n'est pas a jour
- Doit inclure: collecte -> builders -> consolidation -> selection -> validation
- Impact: pouvoir tout relancer en 1 commande

### E3. [ ] Reparer les 4 tests existants
- tests/test_temporal_ordering.py
- tests/test_builder_output_completeness.py
- tests/test_fill_rate_regression.py
- tests/test_utils.py
- Probablement casses apres les nettoyages

### E4. [ ] Setup collecte automatique quotidienne PMU
- Script de collecte journaliere des nouvelles courses
- Cron/planificateur Windows pour lancer chaque jour
- Impact: le dataset se met a jour tout seul

### E5. [ ] Ajouter validation automatique post-pipeline
- validate_pipeline_output.py existe deja
- L'integrer dans run_full_pipeline.sh
- Impact: detection automatique des problemes

---

## F. ESPACE DISQUE SUPPLEMENTAIRE

### F1. [ ] 02_liste_courses (36 Go) - verifier si redondant avec master
### F2. [ ] 02_merged_intermediate (33 Go) - probablement supprimable
### F3. [ ] 02_liste_courses_raw_pmu (14 Go) - brut PMU, garder?
### F4. [ ] 22_performances_detaillees cache/ - nettoyer apres B1
### F5. [ ] 27_citations_enjeux cache/ - nettoyer apres B4

---

## ORDRE RECOMMANDE D'EXECUTION

### Phase 1 - Donnees manquantes (le plus impactant)
B1 -> B2 -> C1 -> C3 -> C4 (performances + builders vitesse/forme)
B3 (cotes historiques)
B4 (citations)

### Phase 2 - Nouveaux calculs
C2, C5 (handicap, poids - deja faisable)
C6-C12 (builders secondaires)

### Phase 3 - Qualite
D4, D5 (normalisation, NaN)
Relancer apply_feature_selection.py avec les nouveaux builders

### Phase 4 - Code
E1-E5 (centraliser, pipeline, tests, cron)

### Phase 5 - Nettoyage final
D1-D3, F1-F5 (espace disque)

---

## COMPTEUR
- Taches restantes: 35
- Taches terminees: 11
- Total: 46
- Priorite absolue: B1 (performances), B3 (cotes), C1-C5 (builders clefs)
