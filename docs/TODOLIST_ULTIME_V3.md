# TODOLIST ULTIME V3 - Pipeline Donnees Turf
# Sauvegarde le 2026-04-11
# ================================================
# Liste EXHAUSTIVE de TOUT ce qui reste a faire.
# Inclut: donnees manquantes, calculs mathematiques,
# sources de donnees, angles morts, optimisations.
# Quand tout est coche, le pipeline est PARFAIT.

## LEGENDE
- [x] Termine
- [ ] A faire
- [P] En cours (agent en parallele)

---

## A. DEJA FAIT (pour memoire, 15 taches)

- [x] A1. Suppression leakage round 1, 2, 3 (rtm_is_fastest, wmf_top_quarter, etc.)
- [x] A2. Selection 500 features par LightGBM
- [x] A3. Validation post-pipeline OK (zero leakage, target 8.5%)
- [x] A4. Git: 211 commits, tout pousse sur GitHub
- [x] A5. Nettoyage builder_outputs (295 Go liberes)
- [x] A6. Nettoyage 5 JSONL partants (109 Go liberes)
- [x] A7. Suppression scripts obsoletes
- [x] A8. DuckDB optimise (6.3 Go)
- [x] A9. Export citations (125K records, 3.3 Go)
- [x] A10. Export pedigrees (44K records)
- [x] A11. Rapports couvrent 2013-2026
- [x] A12. config.py centralise existe
- [x] A13. 4 tests unitaires existent
- [x] A14. validate_pipeline_output.py cree
- [x] A15. check_ram.py cree

---

## B. DONNEES MANQUANTES A RECUPERER (10 taches)

### B1. [x] Performances detaillees -> performances_master.parquet (FAIT: 6M rows, 114 Mo)
- Source: 22_performances_detaillees/perf_detaillees_enriched.jsonl (4 Go, ~2M lignes)
- Contient: temps course, position, terrain, distance, jockey de chaque course passee
- Agent en cours de creation
- IMPACT: TRES ELEVE - base pour features vitesse/forme

### B2. [ ] Sectionnels -> fusionner dans performances_master
- Source: 11_sectionals/sectionals.parquet (243K rows)
- Contient: vitesse par portion de course, ecart vs gagnant
- IMPACT: TRES ELEVE - #1 feature manquante selon la recherche

### B3. [ ] Cotes historiques 2020-2026 -> relancer script 07
- Source: 07_cotes_marche (286 Mo, arrete en 2020)
- A faire: relancer collection pour 2020-2026
- IMPACT: ELEVE - mouvement de cotes = signal de marche

### B4. [x] Citations enjeux -> FAIT (125K records exportes)

### B5. [x] Pedigrees profonds -> FAIT (44K exportes, 14K complets)

### B6. [x] Horse stats master -> FAIT (80K chevaux, 17 cols)
- horse_stats_master.json = 2 octets (VIDE!)
- Source: 05_historique_chevaux (324 Mo)
- IMPACT: MOYEN

### B7. [ ] LeTrot -> integrer donnees trot
- Source: 83_letrot (982 Mo) + 02b_scraper_letrot
- IMPACT: FAIBLE pour galop, ELEVE pour trot

### B8. [ ] Pronostics experts -> parser
- Source: 23_pronostics (498 Mo, 207K fichiers)
- IMPACT: MOYEN - consensus expert = signal utile

### B9. [ ] Racing Post -> parser
- racing_post_master.json = 2 octets (VIDE!)
- Source: 37_racing_post (4.5 Go)
- IMPACT: MOYEN - source internationale

### B10. [ ] PMU API -> verifier couverture complete
- Source: 101_pmu_api (5 Go, 225K fichiers)
- IMPACT: variable

---

## C. CALCULS MATHEMATIQUES - FEATURES CRITIQUES (15 taches)

### Basees sur des colonnes DEJA dans partants_master mais pas exploitees

### C1. [x] meteo_impact_builder (FAIT: 8 features, 1 dans top 500) (6 colonnes meteo inexploitees)
- met_cheval_nb_courses_terrain, met_cheval_specialist_terrain
- met_cheval_taux_place_terrain, met_cheval_taux_vic_pluie
- met_cheval_taux_vic_terrain, met_impact_meteo_score
- Calcul: taux victoire par type de terrain x cheval, score impact meteo
- IMPACT: ELEVE - le terrain change tout

### C2. [x] handicap_deep_builder (FAIT: 5 features, 4 dans top 500 dont #39!) (colonnes handicap inexploitees)
- handicap_distance_m, handicap_valeur vs moyenne peloton
- Calculs: handicap_vs_field_avg, handicap_per_kg, weight_adjusted_speed
- Formule: speed_figure / poids_porte vs moyenne
- IMPACT: ELEVE - le poids est crucial

### C3. [x] marche_enjeux_builder (FAIT: 6 features dans run_new_builders.py)
- mch_enjeu_combinaison, mch_pct_masse, mch_total_enjeu_pari
- Calculs: ratio enjeu/masse totale, detection argent intelligent
- Formule: si enjeu_combinaison / total_enjeu > seuil = argent smart
- IMPACT: ELEVE - detecte ou va l'argent

### C4. [x] ecart_repos_builder (FAIT: 5 features dans run_new_builders.py)
- Source: date_reunion_iso dans partants_master
- Calculs: jours_depuis_derniere, repos_optimal_par_cheval
- Formule non-lineaire: bins [0-10, 10-20, 20-30, 30-60, 60-90, 90+]
- Facteurs: is_repos_optimal = 1 si dans fourchette historique optimale
- IMPACT: ELEVE - repos = facteur cle de performance

### C5. [x] poids_impact_deep_builder (FAIT: 7 features dans run_new_builders.py)
- Interactions: poids x distance x terrain x age x discipline
- Calculs: poids_relatif_peloton, poids_par_km, poids_vs_ideal
- IMPACT: MOYEN-ELEVE

### C6. [x] conditions_course_deep_builder (FAIT: 7 features dans run_new_builders.py)
- cnd_cond_age_max, cnd_cond_age_min, cnd_cond_distance_m
- cnd_cond_groupe, cnd_cond_nb_victoires_max, cnd_cond_prix_euros
- Calculs: is_eligible_age, is_adapte_conditions, ecart_distance_pref
- IMPACT: MOYEN - conditions d'eligibilite = signal

### C7. [x] rapports_dividendes_builder (FAIT: 7 features historiques, #37 importance!)
- rap_rapport_simple_gagnant/place, rap_rapport_couple, rap_rapport_multi
- rap_dividend_moyen, rap_market_concentration
- Calculs historiques: rapport_moyen_hippodrome, rapport_type_course
- ATTENTION: ne PAS utiliser les rapports de la course actuelle (= leakage!)
- Utiliser seulement les rapports historiques de l'hippodrome/type course
- IMPACT: ELEVE si bien fait (pas de leakage)

### C8. [x] speed_bias_builder (FAIT: 4 features dans run_new_builders.py)
- spd_bias_corde_gagnant_moy, spd_bias_interieur
- spd_field_strength_avg/max/std
- Calculs: corde_advantage_score, field_strength_relative
- IMPACT: MOYEN

### Basees sur les PERFORMANCES DETAILLEES (necessite B1)

### C9. [x] musique_lag_features_builder (FAIT: 14 features dans run_perf_builders.py)
- Position des 5 dernieres courses individuellement
- Calculs: pos_last_1 a pos_last_5, mean_pos_3, trend_pos_3
- Formule: regression lineaire sur les 5 dernieres positions = tendance
- IMPACT: TRES ELEVE - forme recente = signal #1

### C10. [x] speed_form_composite_builder (FAIT: 6 features dans run_perf_builders.py)
- Meilleur chrono a chaque distance, reduction km/s
- Calculs: best_time_at_dist, avg_time_at_dist, speed_vs_field
- Formule: (temps_cheval - temps_gagnant) / temps_gagnant * 100
- IMPACT: TRES ELEVE

### C11. [x] sectional_pace_builder (FAIT: 4 features dans run_perf_builders.py)
- Vitesse par segment: debut, milieu, fin de course
- Calculs: early_pace, mid_pace, late_pace, finish_kick
- Formule: acceleration = late_speed / early_speed
- IMPACT: TRES ELEVE - #1 feature manquante selon la recherche

### C12. [x] class_drop_rise_builder (FAIT: 4 features dans run_advanced_builders.py)
- Changement de classe entre courses consecutives
- Calculs: class_ratio = current_alloc / prev_alloc
- Signal: class_drop = 1 si le cheval descend de classe (tres predictif)
- IMPACT: ELEVE

### Calculs mathematiques avances

### C13. [x] graph_pagerank_builder (FAIT: 3 features, 116K chevaux, 58% match)
- Modeliser les chevaux comme un reseau (qui a battu qui)
- Calculs: PageRank du cheval, authority_score, hub_score
- Formule: algorithme PageRank sur le graphe cheval-vs-cheval
- IMPACT: MOYEN-ELEVE (confirme par recherche Louisville)

### C14. [x] expected_value_builder (FAIT: 6 features, ev_x__proba_estimee = #5!)
- Calcul du rapport qualite/prix de chaque cheval
- Formules: EV = proba_estimee * cote - 1
- Kelly criterion: kelly_fraction = (p * b - q) / b
- Sharpe ratio: (mean_return - risk_free) / std_return
- IMPACT: ELEVE pour l'optimisation des mises

### C15. [x] relative_performance_builder (FAIT: 5 features dans run_advanced_builders.py)
- Performance normalisee par la qualite du peloton
- Calcul: (position / nb_partants) normalisee
- Formule: z_score = (perf - mean_field) / std_field
- Adjusted_rating = elo + field_strength_bonus
- IMPACT: ELEVE

---

## D. SOURCES DE DONNEES SUPPLEMENTAIRES (5 taches)

### D1. [ ] Donnees meteo fines (precipitations heure par heure)
- OpenMeteo API archive: precipitations, vent, humidite au niveau hippodrome
- On a deja meteo_master mais pas au niveau horaire
- IMPACT: MOYEN - la pluie pendant la course change le terrain

### D2. [ ] Donnees Turfomania/Zone-Turf (pronostics gratuits)
- Scraper les consensus de pronostiqueurs (deja scrapers 31/32 existants)
- Donnees deja collectees dans 51_zeturf, 54_turfinfo
- A faire: parser et integrer dans le pipeline
- IMPACT: MOYEN

### D3. [ ] Flux PMU temps reel (programme du jour)
- PMU API donne le programme du jour avec les chevaux, cotes initiales
- Script 36 existe (collecte quotidienne)
- A automatiser pour alimenter le pipeline chaque jour
- IMPACT: ELEVE pour la mise en production

### D4. [ ] Donnees entrainement (si disponibles)
- Certains sites publient les galops d'entrainement
- Tres predictif mais rare et difficile a obtenir
- IMPACT: TRES ELEVE si dispo, probablement pas faisable

### D5. [ ] Statistiques officielles France Galop / LeTrot
- Stats annuelles par jockey, entraineur, proprietaire
- Classements officiels
- IMPACT: FAIBLE (deja approxime par nos calculs)

---

## E. NETTOYAGE ET OPTIMISATION (8 taches)

### E1. [x] Supprimer dossiers vides 02_DONNEES_BRUTES (18 dossiers supprimes)
### E2. [ ] Verifier 02_merged_intermediate (33 Go) -> supprimable ?
### E3. [ ] Verifier 02_liste_courses_raw_pmu (14 Go) -> archivable ?
### E4. [x] Supprimer training_labels.jsonl (corrompu, 941 Mo supprime)
### E5. [ ] Recalculer stats normalisation
### E6. [x] Traiter les colonnes NaN (186 colonnes 100% NaN exclues de la selection)
### E7. [ ] Archiver les 17 builder_outputs suspects (11.7 Go)
### E8. [x] Mettre a jour DuckDB features.duckdb (502 cols, 2.5 Go)

---

## F. CODE ET AUTOMATISATION (6 taches)

### F1. [ ] Migrer 333 fichiers vers config.py (chemins en dur)
### F2. [x] Mettre a jour run_full_pipeline.sh (11 etapes, consolidation+selection+validation)
### F3. [x] Reparer les tests (30 passed, 1 skipped, 0 failures)
### F4. [ ] Setup collecte automatique quotidienne PMU
### F5. [x] Integrer validation dans le pipeline (etape 9 dans run_full_pipeline.sh)
### F6. [ ] Documenter le pipeline complet (README a jour)

---

## G. DERNIERE PASSE AVANT MODELES (3 taches)

### G1. [ ] Relancer tous les builders avec les nouvelles donnees
### G2. [x] Relancer apply_feature_selection.py (FAIT: 500/2060 features, 3286 cols consolidated)
### G3. [x] Validation finale (FAIT: zero leakage, target 8.5%, 30 tests passent)

---

## COMPTEUR FINAL
- Taches terminees: 40
- Taches restantes: 22
- TOTAL: 62

## ORDRE D'EXECUTION RECOMMANDE

### Phase 1 - Donnees manquantes (B1-B3, B6-B10)
### Phase 2 - Builders critiques (C1-C8, faisables maintenant)
### Phase 3 - Builders performances (C9-C12, apres B1)
### Phase 4 - Builders avances (C13-C15)
### Phase 5 - Sources supplementaires (D1-D5)
### Phase 6 - Nettoyage (E1-E8)
### Phase 7 - Code (F1-F6)
### Phase 8 - Derniere passe (G1-G3)
