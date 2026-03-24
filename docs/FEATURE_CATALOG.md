# Feature Catalog

Auto-generated from `output/features/features_matrix.jsonl` (sample: 500 rows).

**Total features: 379**

## Features by Builder

| Builder | Count |
|---------|------:|
| base / identifiers | 46 |
| affinite_features | 23 |
| age_features | 2 |
| combo_features | 12 |
| compteurs | 5 |
| cote_features | 2 |
| distance_features | 2 |
| elo_rating_features | 6 |
| entourage_features | 9 |
| equipement_features | 12 |
| fatigue_features | 7 |
| flags | 6 |
| forme_features | 1 |
| gains_features | 2 |
| handicap_features | 2 |
| jockey_features | 4 |
| match_features | 7 |
| musique_features | 22 |
| pays_features | 2 |
| pedigree_features | 4 |
| perf_conditions_features | 9 |
| place_features | 1 |
| podium_features | 2 |
| poids_features | 16 |
| profil_cheval_features | 21 |
| programme_features | 19 |
| rapport_features | 40 |
| recovery_features | 9 |
| temporal_advanced_features | 7 |
| temporal_context_features | 7 |
| odds_movement_features | 5 |
| pedigree_distance_aptitude | 6 |
| bayesian_rating_builder | 8 |
| market_entropy_features | 6 |
| draw_bias_builder | 6 |
| speed_figure_builder | 7 |
| taux_features | 1 |
| temps_features | 15 |
| valeur_base_features | 4 |
| **Total** | **379** |

## Detailed Feature List

### base / identifiers (46 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `age` | int | 100.0% |
| `allure` | str | 100.0% |
| `avis_entraineur` | str | 0.0% |
| `cle_partant` | str | 100.0% |
| `commentaire_apres_course` | str | 0.0% |
| `corde` | str | 100.0% |
| `course_uid` | str | 100.0% |
| `date_reunion_iso` | str | 100.0% |
| `deferre` | str | 2.2% |
| `discipline` | str | 100.0% |
| `distance` | int | 100.0% |
| `ecart_precedent` | str | 7.2% |
| `eleveur` | str | 0.0% |
| `engagement` | bool | 100.0% |
| `entraineur` | str | 100.0% |
| `hippodrome_normalise` | str | 100.0% |
| `horse_id` | str | 100.0% |
| `incident` | str | 26.2% |
| `jours_depuis_derniere` | unknown | 0.0% |
| `jument_pleine` | bool | 100.0% |
| `mere` | str | 93.6% |
| `musique` | str | 97.0% |
| `nom_cheval` | str | 100.0% |
| `nombre_partants` | int | 100.0% |
| `num_pmu` | int | 100.0% |
| `numero_course` | int | 100.0% |
| `numero_reunion` | int | 100.0% |
| `oeilleres` | str | 2.8% |
| `partant_uid` | str | 100.0% |
| `pere` | str | 93.6% |
| `pere_mere` | str | 12.2% |
| `position_arrivee` | int | 60.6% |
| `proba_implicite` | unknown | 0.0% |
| `proprietaire` | str | 100.0% |
| `race` | str | 100.0% |
| `reduction_km_ms` | int | 52.8% |
| `rest_x_forme` | unknown | 0.0% |
| `reunion_uid` | str | 100.0% |
| `robe` | str | 88.2% |
| `sexe` | str | 100.0% |
| `source` | str | 100.0% |
| `statut` | str | 100.0% |
| `supplement_euros` | int | 100.0% |
| `surcharge_decharge_kg` | unknown | 0.0% |
| `timestamp_collecte` | str | 100.0% |
| `type_piste` | str | 100.0% |

### affinite_features (23 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `aff_cd_is_first_time` | bool | 100.0% |
| `aff_cd_last_result` | int | 56.4% |
| `aff_cd_nb_courses` | int | 100.0% |
| `aff_cd_places` | int | 81.0% |
| `aff_cd_taux_place` | float | 81.0% |
| `aff_cd_taux_vic` | float | 81.0% |
| `aff_cd_victoires` | int | 81.0% |
| `aff_ch_is_first_time` | bool | 100.0% |
| `aff_ch_nb_courses` | int | 100.0% |
| `aff_ct_is_first_time` | bool | 100.0% |
| `aff_ct_last_result` | int | 62.6% |
| `aff_ct_nb_courses` | int | 100.0% |
| `aff_ct_places` | int | 89.2% |
| `aff_ct_taux_place` | float | 89.2% |
| `aff_ct_taux_vic` | float | 89.2% |
| `aff_ct_victoires` | int | 89.2% |
| `aff_eh_is_first_time` | bool | 100.0% |
| `aff_eh_last_result` | int | 78.2% |
| `aff_eh_nb_courses` | int | 100.0% |
| `aff_eh_places` | int | 98.0% |
| `aff_eh_taux_place` | float | 98.0% |
| `aff_eh_taux_vic` | float | 98.0% |
| `aff_eh_victoires` | int | 98.0% |

### age_features (2 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `age_x_distance` | float | 100.0% |
| `age_x_nb_courses` | float | 72.2% |

### combo_features (12 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `combo_jh_nb` | int | 100.0% |
| `combo_jh_taux_vic` | unknown | 0.0% |
| `combo_jockey_change` | int | 100.0% |
| `combo_jockey_hippo_nb` | int | 100.0% |
| `combo_jockey_hippo_taux_vic` | float | 7.6% |
| `combo_jt_nb` | int | 100.0% |
| `combo_jt_taux_place` | float | 7.2% |
| `combo_jt_taux_vic` | float | 7.2% |
| `combo_th_nb` | int | 100.0% |
| `combo_th_taux_vic` | unknown | 0.0% |
| `combo_trainer_hippo_nb` | int | 100.0% |
| `combo_trainer_hippo_taux_vic` | float | 5.4% |

### compteurs (5 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `nb_courses_carriere` | int | 100.0% |
| `nb_places_2eme` | unknown | 0.0% |
| `nb_places_3eme` | unknown | 0.0% |
| `nb_places_carriere` | int | 100.0% |
| `nb_victoires_carriere` | int | 100.0% |

### cote_features (2 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `cote_finale` | unknown | 0.0% |
| `cote_reference` | unknown | 0.0% |

### distance_features (2 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `distance_change` | unknown | 0.0% |
| `distance_change_pct` | unknown | 0.0% |

### entourage_features (9 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `ent_cheval_gains_total` | float | 34.2% |
| `ent_cheval_nb_courses_total` | int | 34.2% |
| `ent_cheval_nb_disciplines` | int | 34.2% |
| `ent_entraineur_gains_total` | float | 90.4% |
| `ent_entraineur_taux_place` | float | 90.4% |
| `ent_entraineur_taux_victoire` | float | 90.4% |
| `ent_jockey_gains_total` | float | 89.8% |
| `ent_jockey_taux_place` | float | 89.8% |
| `ent_jockey_taux_victoire` | float | 89.8% |

### elo_rating_features (6 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `elo_cheval` | float | N/A |
| `elo_jockey` | float | N/A |
| `elo_entraineur` | float | N/A |
| `elo_combined` | float | N/A |
| `elo_cheval_delta` | float | N/A |
| `nb_races_elo` | int | N/A |

### equipement_features (12 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `equip_deferre_added` | unknown | 0.0% |
| `equip_deferre_change` | unknown | 0.0% |
| `equip_deferre_code` | int | 100.0% |
| `equip_deferre_removed` | unknown | 0.0% |
| `equip_deferre_type` | int | 100.0% |
| `equip_has_deferre` | int | 100.0% |
| `equip_nb_courses_with_oeilleres` | unknown | 0.0% |
| `equip_nb_oeilleres_changes_5` | unknown | 0.0% |
| `equip_oeilleres_added` | unknown | 0.0% |
| `equip_oeilleres_change` | unknown | 0.0% |
| `equip_oeilleres_removed` | unknown | 0.0% |
| `equip_poids_monte_change` | int | 100.0% |

### fatigue_features (7 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `fatigue_30j` | float | N/A |
| `fatigue_60j` | float | N/A |
| `fatigue_90j` | float | N/A |
| `fatigue_distance_ponderee` | float | N/A |
| `intensite_recente` | float | N/A |
| `sequence_courses` | int | N/A |
| `tendance_fatigue` | float | N/A |

### flags (6 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `is_discipline_change` | unknown | 0.0% |
| `is_disqualifie` | bool | 100.0% |
| `is_gagnant` | bool | 100.0% |
| `is_hippodrome_change` | unknown | 0.0% |
| `is_inedit` | bool | 100.0% |
| `is_place` | bool | 100.0% |

### forme_features (1 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `forme_x_cote` | unknown | 0.0% |

### gains_features (2 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `gains_annee_euros` | unknown | 0.0% |
| `gains_carriere_euros` | unknown | 0.0% |

### handicap_features (2 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `handicap_distance_m` | int | 88.2% |
| `handicap_valeur` | unknown | 0.0% |

### jockey_features (4 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `jockey_driver` | str | 100.0% |
| `jockey_driver_change` | bool | 100.0% |
| `jockey_taux_victoire_365j` | float | 89.8% |
| `jockey_taux_x_cheval_taux` | unknown | 0.0% |

### match_features (7 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `mch__nb_sources` | int | 97.2% |
| `mch__sources` | list | 97.2% |
| `mch_combinaison` | list | 97.2% |
| `mch_hippodrome` | str | 97.2% |
| `mch_rang_combinaison` | int | 97.2% |
| `mch_record_key` | str | 97.2% |
| `mch_type_pari` | str | 97.2% |

### musique_features (22 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `musique_avant_derniere_pos` | int | 64.8% |
| `musique_avg_pos_10` | float | 81.2% |
| `musique_avg_pos_5` | float | 81.2% |
| `musique_consecutive_hors_places` | int | 82.0% |
| `musique_consecutive_places` | int | 82.0% |
| `musique_derniere_pos` | int | 65.8% |
| `musique_last_5_positions` | list | 82.0% |
| `musique_nb_2eme` | int | 82.0% |
| `musique_nb_3eme` | int | 82.0% |
| `musique_nb_courses` | int | 82.0% |
| `musique_nb_disciplines` | int | 82.0% |
| `musique_nb_disqualifications` | int | 82.0% |
| `musique_nb_dnf` | int | 82.0% |
| `musique_nb_places` | int | 82.0% |
| `musique_nb_victoires` | int | 82.0% |
| `musique_nb_zero` | int | 82.0% |
| `musique_pct_meme_discipline` | float | 9.2% |
| `musique_surface_changes` | int | 82.0% |
| `musique_taux_place` | float | 82.0% |
| `musique_taux_victoire` | float | 82.0% |
| `musique_trend` | float | 55.6% |
| `musique_trend_label` | int | 55.6% |

### pays_features (2 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `pays_cheval` | str | 88.2% |
| `pays_entrainement` | str | 0.0% |

### pedigree_features (4 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `ped_inbreeding_detected` | bool | 93.6% |
| `ped_lineage_depth` | int | 93.6% |
| `ped_sire_precocity_idx` | unknown | 0.0% |
| `ped_sire_stamina_idx` | float | 0.2% |

### perf_conditions_features (9 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `pc_cote_mediane_course` | unknown | 0.0% |
| `pc_cote_moyenne_course` | unknown | 0.0% |
| `pc_deferre_prev` | unknown | 0.0% |
| `pc_ecart_cote_moyenne` | unknown | 0.0% |
| `pc_handicap_valeur` | unknown | 0.0% |
| `pc_nb_courses_sans_oeilleres` | unknown | 0.0% |
| `pc_oeilleres_prev` | unknown | 0.0% |
| `pc_poids_precedent` | unknown | 0.0% |
| `pc_retrait_oeilleres` | int | 100.0% |

### place_features (1 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `place_corde` | int | 47.8% |

### podium_features (2 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `pdm_has_mere` | int | 100.0% |
| `pdm_has_pere` | int | 100.0% |

### poids_features (16 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `poids_avg_career` | unknown | 0.0% |
| `poids_base_kg` | unknown | 0.0% |
| `poids_change_vs_avg` | unknown | 0.0% |
| `poids_change_vs_last` | unknown | 0.0% |
| `poids_ecart_max` | float | 24.4% |
| `poids_ecart_min` | float | 24.4% |
| `poids_ecart_moyen` | float | 24.4% |
| `poids_is_heaviest` | unknown | 0.0% |
| `poids_is_lightest` | unknown | 0.0% |
| `poids_max_career` | unknown | 0.0% |
| `poids_min_career` | unknown | 0.0% |
| `poids_monte_change` | bool | 100.0% |
| `poids_poids_porte_kg` | float | 24.4% |
| `poids_porte_kg` | float | 24.4% |
| `poids_rang` | int | 24.4% |
| `poids_x_distance` | float | 24.4% |

### profil_cheval_features (21 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `profil_age` | int | 100.0% |
| `profil_age_category` | int | 100.0% |
| `profil_carriere_longueur` | int | 100.0% |
| `profil_engagement` | bool | 100.0% |
| `profil_gains_annee_log` | unknown | 0.0% |
| `profil_gains_carriere_log` | unknown | 0.0% |
| `profil_gains_par_course` | unknown | 0.0% |
| `profil_is_female` | int | 100.0% |
| `profil_is_hongre` | int | 100.0% |
| `profil_is_inedit` | int | 100.0% |
| `profil_is_male` | int | 100.0% |
| `profil_jument_pleine` | int | 100.0% |
| `profil_nb_courses_carriere` | int | 100.0% |
| `profil_place_corde` | int | 47.8% |
| `profil_place_corde_relative` | float | 47.8% |
| `profil_race_breed_encoded` | int | 100.0% |
| `profil_race_code` | int | 100.0% |
| `profil_robe_encoded` | int | 100.0% |
| `profil_sexe_code` | int | 100.0% |
| `profil_taux_place_carriere` | float | 72.2% |
| `profil_taux_victoire_carriere` | float | 72.2% |

### programme_features (19 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `pgr__nb_sources` | int | 91.8% |
| `pgr__sources` | list | 91.8% |
| `pgr_age` | int | 91.8% |
| `pgr_age_ans` | float | 71.6% |
| `pgr_annee_naissance` | int | 71.6% |
| `pgr_consommation` | str | 72.8% |
| `pgr_date_naissance` | str | 71.6% |
| `pgr_mere` | str | 87.6% |
| `pgr_nom` | str | 91.8% |
| `pgr_pays_cheval` | str | 80.2% |
| `pgr_pays_naissance` | str | 72.8% |
| `pgr_pere` | str | 87.6% |
| `pgr_race` | str | 91.8% |
| `pgr_robe` | str | 83.4% |
| `pgr_sexe` | str | 91.8% |
| `pgr_sire_annee_naissance` | int | 71.6% |
| `pgr_sire_consommation` | str | 72.8% |
| `pgr_sire_date_naissance` | str | 71.6% |
| `pgr_sire_pays_naissance` | str | 72.8% |

### rapport_features (40 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `rap__nb_sources` | int | 97.2% |
| `rap__sources` | list | 97.2% |
| `rap_combinaison` | str | 97.2% |
| `rap_combinaison_couple_gagnant` | str | 97.2% |
| `rap_combinaison_gagnant` | str | 97.2% |
| `rap_combinaison_place_1` | str | 97.2% |
| `rap_combinaison_place_2` | str | 97.2% |
| `rap_combinaison_place_3` | str | 97.2% |
| `rap_course_key` | str | 97.2% |
| `rap_discipline` | str | 97.2% |
| `rap_distance` | int | 97.2% |
| `rap_dividende_euros` | float | 97.2% |
| `rap_hippodrome` | str | 97.2% |
| `rap_nb_gagnants` | float | 97.2% |
| `rap_num_course` | int | 97.2% |
| `rap_numero_course` | int | 97.2% |
| `rap_numero_reunion` | int | 97.2% |
| `rap_rapport_2sur4_max` | int | 90.4% |
| `rap_rapport_2sur4_min` | int | 90.4% |
| `rap_rapport_2sur4_nb_combinaisons` | int | 90.4% |
| `rap_rapport_couple_gagnant` | int | 97.2% |
| `rap_rapport_couple_place_1` | int | 97.2% |
| `rap_rapport_couple_place_2` | int | 97.2% |
| `rap_rapport_couple_place_3` | int | 97.2% |
| `rap_rapport_multi_4` | int | 55.6% |
| `rap_rapport_multi_5` | int | 58.8% |
| `rap_rapport_multi_6` | int | 58.8% |
| `rap_rapport_multi_7` | int | 58.8% |
| `rap_rapport_quarte_bonus` | int | 11.0% |
| `rap_rapport_quarte_ordre` | int | 11.0% |
| `rap_rapport_quinte_bonus3` | int | 7.4% |
| `rap_rapport_quinte_ordre` | int | 7.4% |
| `rap_rapport_simple_gagnant` | int | 97.2% |
| `rap_rapport_simple_place_1` | int | 97.2% |
| `rap_rapport_simple_place_2` | int | 97.2% |
| `rap_rapport_simple_place_3` | int | 97.2% |
| `rap_rapport_tierce_ordre` | int | 7.4% |
| `rap_rapport_uid` | str | 97.2% |
| `rap_rapports_raw` | list | 97.2% |
| `rap_type_pari` | str | 97.2% |

### recovery_features (9 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `jours_repos` | int | N/A |
| `repos_optimal` | bool | N/A |
| `perf_apres_repos_court` | float | N/A |
| `perf_apres_repos_moyen` | float | N/A |
| `perf_apres_repos_long` | float | N/A |
| `repos_vs_moyenne` | float | N/A |
| `nb_courses_30j` | int | N/A |
| `nb_courses_60j` | int | N/A |
| `nb_courses_90j` | int | N/A |

### temporal_advanced_features (7 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `temp_jour_semaine` | int | N/A |
| `temp_mois` | int | N/A |
| `temp_saison` | str | N/A |
| `temp_is_weekend` | bool | N/A |
| `temp_is_quinte` | bool | N/A |
| `temp_heure_course` | int | N/A |
| `temp_position_dans_reunion` | int | N/A |

### temporal_context_features (7 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `temp_jour_semaine` | int | N/A |
| `temp_mois` | int | N/A |
| `temp_saison` | str | N/A |
| `temp_is_weekend` | int | N/A |
| `temp_is_jour_ferie` | int | N/A |
| `temp_heure_course` | float | N/A |
| `temp_nb_jours_depuis_debut_saison` | int | N/A |

### odds_movement_features (5 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `odds_drift_pct` | float | N/A |
| `odds_steam_pct` | float | N/A |
| `is_market_mover` | bool | N/A |
| `odds_rank_change` | int | N/A |
| `market_confidence` | float | N/A |

### pedigree_distance_aptitude (6 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `sire_win_rate_distance` | float | N/A |
| `sire_win_rate_terrain` | float | N/A |
| `dam_sire_win_rate` | float | N/A |
| `inbreeding_coefficient` | float | N/A |
| `stamina_index` | float | N/A |
| `speed_index` | float | N/A |

### bayesian_rating_builder (8 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `bayes_horse_win_rate` | float | N/A |
| `bayes_horse_place_rate` | float | N/A |
| `bayes_jockey_win_rate` | float | N/A |
| `bayes_jockey_roi` | float | N/A |
| `bayes_trainer_win_rate` | float | N/A |
| `bayes_trainer_strike_rate` | float | N/A |
| `bayes_combo_jt_win` | float | N/A |
| `bayes_confidence` | float | N/A |

### market_entropy_features (6 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `market_entropy` | float | N/A |
| `market_overround` | float | N/A |
| `implied_probability` | float | N/A |
| `odds_vs_implied` | float | N/A |
| `favourite_strength` | float | N/A |
| `field_competitiveness` | int | N/A |

### draw_bias_builder (6 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `draw_win_rate` | float | N/A |
| `draw_place_rate` | float | N/A |
| `draw_advantage` | float | N/A |
| `draw_inside_bias` | float | N/A |
| `draw_position_normalized` | float | N/A |
| `draw_nb_samples` | int | N/A |

### speed_figure_builder (7 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `speed_figure` | float | N/A |
| `speed_figure_best` | float | N/A |
| `speed_figure_avg` | float | N/A |
| `speed_figure_trend` | float | N/A |
| `speed_figure_rank` | int | N/A |
| `speed_vs_class` | float | N/A |
| `speed_consistency` | float | N/A |

### taux_features (1 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `taux_reclamation_euros` | int | 6.4% |

### temps_features (15 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `temps_avg_reduction_10` | unknown | 0.0% |
| `temps_avg_reduction_5` | unknown | 0.0% |
| `temps_best_reduction_10` | unknown | 0.0% |
| `temps_best_reduction_5` | unknown | 0.0% |
| `temps_ecart_gagnant_pct` | float | 52.8% |
| `temps_ecart_moyen_champ` | float | 52.8% |
| `temps_ms` | int | 52.8% |
| `temps_rang_vitesse` | int | 52.8% |
| `temps_reduction_km_ms` | int | 52.8% |
| `temps_reduction_relative` | float | 52.8% |
| `temps_reduction_trend` | unknown | 0.0% |
| `temps_relatif_vainqueur` | int | 52.8% |
| `temps_speed_consistency` | unknown | 0.0% |
| `temps_temps_ms` | int | 52.8% |
| `temps_vitesse_kmh` | float | 52.8% |

### valeur_base_features (4 features)

| Feature | Type | Fill Rate |
|---------|------|-----------|
| `vb_cote_finale` | unknown | 0.0% |
| `vb_log_proba` | unknown | 0.0% |
| `vb_proba_implicite` | unknown | 0.0% |
| `vb_proba_normalisee` | unknown | 0.0% |

