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

### B2. [x] Sectionnels -> deja exploites via C11 (sectional_x, 4 features par partant_uid)
- Source: 11_sectionals/sectionals.parquet (243K rows)
- Contient: vitesse par portion de course, ecart vs gagnant
- IMPACT: TRES ELEVE - #1 feature manquante selon la recherche

### B3. [x] Cotes historiques -> DEJA DANS partants_master (77-95% couverture 2013-2025)
- cote_finale presente dans 77-95% des partants (RG0 a RG27)
- Trou: RG28-29 (donnees recentes sept 2025-mars 2026) = 0% cotes
- IMPACT: les features mch_* et ev_x exploitent deja ces cotes

### B4. [x] Citations enjeux -> FAIT (125K records exportes)

### B5. [x] Pedigrees profonds -> FAIT (44K exportes, 14K complets)

### B6. [x] Horse stats master -> FAIT (80K chevaux, 17 cols)
- horse_stats_master.json = 2 octets (VIDE!)
- Source: 05_historique_chevaux (324 Mo)
- IMPACT: MOYEN

### B7. [x] LeTrot -> integre (1.35M partants, 263K matches = 9%, 5 features letrot_x)
- Source: 83_letrot (600 Mo), champs: temps, reduction_km, rapport_prob
- Matching: nom_cheval normalise + date (401K index, 263K matched)
- IMPACT: FAIBLE pour galop, ELEVE pour trot

### B8. [x] Pronostics experts (FAIT: 5 features, 8.8K matches sur 204K pronos - couverture faible)
- Source: 23_pronostics (498 Mo, 207K fichiers)
- IMPACT: MOYEN - consensus expert = signal utile

### B9. [x] Racing Post -> ABANDONNE (donnees UK/HK seulement, 0 course francaise)
- racing_post_fr.jsonl = 3.6M lignes mais aucune course FR
- Source: 37_racing_post (4.5 Go) = raw HTML mal structure
- IMPACT: NUL pour notre pipeline FR

### B10. [x] PMU API -> verifie (2020-2026, 1.2Go participants, deja dans master)
- Source: 101_pmu_api (2 Go, 9 fichiers principaux)
- Couverture: 2020-01-01 a 2026-03-19
- IMPACT: deja integre dans partants_master via consolidation

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

### D1. [x] Donnees meteo fines (FAIT: 8 features meteofine_x, 56% couverture, 10K appels API)
- OpenMeteo API archive: precipitations 3h/12h/24h/48h, temp, humidity, vent, rafales
- 140K courses avec GPS, 1.64M partants matched
- Cache: 8622 fichiers JSON dans meteo_fine_cache/
- IMPACT: MOYEN - integre dans consolidated (3307 cols)

### D2. [ ] Donnees Turfomania/Zone-Turf (pronostics gratuits)
- Donnees collectees (51_zeturf, 54_turfinfo) = metadata reunions seulement
- PAS de pronostics par cheval dans ces fichiers (scraping incomplet)
- Necessite nouveau scraping avec parsing plus fin
- IMPACT: MOYEN - BASSE PRIORITE (B8 couvre deja partiellement)

### D3. [x] Flux PMU temps reel -> COUVERT par F4 (daily_collect_pmu.py)
- Scripts 01+02+04 lancables quotidiennement via daily_collect_pmu.py
- Instructions schtasks fournies pour automatisation Windows

### D4. [ ] Donnees entrainement (si disponibles)
- Certains sites publient les galops d'entrainement
- Tres predictif mais rare et difficile a obtenir
- IMPACT: TRES ELEVE si dispo, PAS FAISABLE actuellement

### D5. [x] Statistiques officielles France Galop / LeTrot -> COUVERT
- Stats approximees par nos calculs (elo, pagerank, relative_performance)
- LeTrot integre (B7: 263K matches)
- IMPACT: FAIBLE (deja couvert)

---

## E. NETTOYAGE ET OPTIMISATION (8 taches)

### E1. [x] Supprimer dossiers vides 02_DONNEES_BRUTES (18 dossiers supprimes)
### E2. [x] Verifier 02_merged_intermediate (33 Go) -> GARDER (tracabilite enrichissements)
### E3. [x] 02_liste_courses_raw_pmu (14 Go) -> a nettoyer manuellement quand pret
### E4. [x] Supprimer training_labels.jsonl (corrompu, 941 Mo supprime)
### E5. [x] Recalculer stats normalisation (fait via apply_feature_selection + LightGBM)
### E6. [x] Traiter les colonnes NaN (186 colonnes 100% NaN exclues de la selection)
### E7. [x] Archiver builder_outputs suspects (15 dossiers vides supprimes, 6.6 Go liberes)
### E8. [x] Mettre a jour DuckDB features.duckdb (502 cols, 2.4 Go, rebuild 2026-04-11)

---

## F. CODE ET AUTOMATISATION (6 taches)

### F1. [x] Migrer config.py vers vrais chemins D: (DATA_DIR, RAW_DIR, CONSOLIDATED, SELECTED, etc.)
### F2. [x] Mettre a jour run_full_pipeline.sh (11 etapes, consolidation+selection+validation)
### F3. [x] Reparer les tests (30 passed, 1 skipped, 0 failures)
### F4. [x] Setup collecte automatique quotidienne PMU (daily_collect_pmu.py + instructions schtasks)
### F5. [x] Integrer validation dans le pipeline (etape 9 dans run_full_pipeline.sh)
### F6. [x] Documenter le pipeline complet (docs/README.md reecrit avec chiffres a jour)

---

## G. DERNIERE PASSE AVANT MODELES (3 taches)

### G1. [x] Relancer tous les builders avec les nouvelles donnees (FAIT: commit 22aa740, 467K PMU records)
### G2. [x] Relancer apply_feature_selection.py (FAIT: 500/2270 features, 3307 cols consolidated, top1=letrot_x__rang)
### G3. [x] Validation finale (FAIT: zero leakage, target 8.5%, 30 tests passent)

---

## COMPTEUR FINAL
- Taches terminees: 56
- Taches restantes: 6 (D2, D4 = basse priorite/pas faisable)
- TOTAL: 62
- PIPELINE PRET POUR ML: OUI (toutes taches critiques terminees)
- Derniere mise a jour: 2026-04-11 (integration meteofine_x + renr_x, 3307 cols total)

## ORDRE D'EXECUTION RECOMMANDE

### Phase 1 - Donnees manquantes (B1-B3, B6-B10)
### Phase 2 - Builders critiques (C1-C8, faisables maintenant)
### Phase 3 - Builders performances (C9-C12, apres B1)
### Phase 4 - Builders avances (C13-C15)
### Phase 5 - Sources supplementaires (D1-D5)
### Phase 6 - Nettoyage (E1-E8)
### Phase 7 - Code (F1-F6)
### Phase 8 - Derniere passe (G1-G3)
