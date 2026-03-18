# Catalogue des features

481 features cataloguees, reparties en 29 categories. 193 features deja implementees, 288 nouvelles a construire.

Cible finale : 528+ features.

---

## Resume par categorie

| # | Categorie | Features | Existantes | Nouvelles | Builder(s) |
|---|-----------|----------|------------|-----------|------------|
| 1 | Horse Form (Rolling Windows) | 41 | 14 | 27 | feat_historique.py, musique_features.py |
| 2 | Horse Profile (Static) | 24 | 14 | 10 | profil_cheval_features.py, cheval_features.py |
| 3 | Jockey Stats | 26 | 13 | 13 | jockey_features.py, feat_jockey.py |
| 4 | Trainer Stats | 22 | 13 | 9 | jockey_features.py, feat_jockey.py |
| 5 | Jockey-Trainer Combo | 6 | 0 | 6 | combo_features.py, feat_jockey_entraineur_combo.py |
| 6 | Jockey-Horse Combo | 6 | 2 | 4 | feat_cheval_jockey_affinity.py |
| 7 | Horse-Hippodrome Affinity | 7 | 1 | 6 | feat_cheval_hippodrome_affinity.py |
| 8 | Horse-Distance Affinity | 11 | 1 | 10 | feat_cheval_distance_affinity.py |
| 9 | Horse-Discipline Affinity | 8 | 1 | 7 | feat_croisements.py |
| 10 | Field Strength | 32 | 8 | 24 | feat_field_strength.py, field_strength_builder.py |
| 11 | Odds / Market | 19 | 9 | 10 | marche_features.py, feat_value_betting.py |
| 12 | Pedigree | 27 | 13 | 14 | pedigree_features.py, pedigree_advanced_builder.py, feat_pedigree.py |
| 13 | Meteo | 25 | 15 | 10 | meteo_features.py, feat_meteo_terrain_interaction.py |
| 14 | Track / Hippodrome | 20 | 6 | 14 | track_bias_detector.py, course_features.py |
| 15 | Pace / Tempo | 14 | 11 | 3 | pace_profile_builder.py |
| 16 | Equipment | 15 | 11 | 4 | equipement_features.py, enrichissement_builder.py |
| 17 | Weight / Handicap | 17 | 11 | 6 | poids_features.py |
| 18 | Time / Performance | 20 | 14 | 6 | temps_features.py |
| 19 | Musique (Form String) | 20 | 15 | 5 | musique_features.py |
| 20 | Race Conditions | 20 | 1 | 19 | course_features.py, 48_parse_conditions_texte.py |
| 21 | Recency / Rest / Fatigue | 13 | 1 | 12 | feat_temporel.py |
| 22 | Consistency | 8 | 0 | 8 | feat_historique.py |
| 23 | Class Changes | 12 | 0 | 12 | class_change_features.py |
| 24 | Rapports / Betting Historical | 8 | 0 | 8 | reunions_builder.py |
| 25 | Precomputed Data | 19 | 19 | 0 | precomputed_partant_joiner.py, precomputed_entity_joiner.py |
| 26 | Performances Detaillees | 15 | 0 | 15 | perf_detaillees_builder.py |
| 27 | Interaction / Cross Features | 10 | 0 | 10 | interaction_features.py, feat_interactions.py |
| 28 | Calendar / Temporal | 9 | 0 | 9 | feat_temporel.py |
| 29 | Proprietaire / Eleveur | 7 | 0 | 7 | feat_croisements.py |
| **TOTAL** | | **481** | **193** | **288** | |

---

## Category 1 : Horse Form (Rolling Windows) -- 41 features

Source : partants (position_arrivee, is_gagnant, is_place, gains). Fenetres : 3, 5, 10, 20 courses.

| Feature | Type | Description |
|---------|------|-------------|
| forme_victoire_3 | numeric | Taux de victoire sur 3 dernieres courses |
| forme_victoire_5 | numeric | Taux de victoire sur 5 dernieres courses |
| forme_victoire_10 | numeric | Taux de victoire sur 10 dernieres courses |
| forme_victoire_20 | numeric | Taux de victoire sur 20 dernieres courses |
| forme_place_3 | numeric | Taux de place (top 3) sur 3 dernieres |
| forme_place_5 | numeric | Taux de place sur 5 dernieres |
| forme_place_10 | numeric | Taux de place sur 10 dernieres |
| forme_place_20 | numeric | Taux de place sur 20 dernieres |
| avg_position_3 | numeric | Position moyenne sur 3 dernieres |
| avg_position_5 | numeric | Position moyenne sur 5 dernieres |
| avg_position_10 | numeric | Position moyenne sur 10 dernieres |
| avg_position_20 | numeric | Position moyenne sur 20 dernieres |
| median_position_5 | numeric | Position mediane sur 5 dernieres |
| median_position_10 | numeric | Position mediane sur 10 dernieres |
| best_position_5 | numeric | Meilleure position sur 5 dernieres |
| best_position_10 | numeric | Meilleure position sur 10 dernieres |
| worst_position_5 | numeric | Pire position sur 5 dernieres |
| worst_position_10 | numeric | Pire position sur 10 dernieres |
| gains_cumules | numeric | Gains cumules en carriere avant la course |
| gains_5 | numeric | Somme des gains sur 5 dernieres courses |
| gains_10 | numeric | Somme des gains sur 10 dernieres courses |
| gains_moyen_par_course | numeric | Gains carriere / nb courses |
| nb_courses_avant | numeric | Nombre de courses avant celle-ci |
| nb_victoires_avant | numeric | Nombre de victoires avant |
| nb_places_avant | numeric | Nombre de places avant |
| taux_victoire_carriere | numeric | Taux de victoire en carriere |
| taux_place_carriere | numeric | Taux de place en carriere |
| derniere_position | numeric | Position dans la derniere course |
| avant_derniere_position | numeric | Position dans l'avant-derniere course |
| progression | categorical | Trend : improving/declining/stable |
| progression_score | numeric | avg(pos last 3) - avg(pos prev 3) |
| nb_dnf_5 | numeric | DNF/DQ dans les 5 dernieres |
| nb_dnf_10 | numeric | DNF/DQ dans les 10 dernieres |
| taux_dnf_carriere | numeric | Taux de DNF en carriere |
| consecutive_wins | numeric | Serie de victoires en cours |
| consecutive_places | numeric | Serie de places en cours |
| consecutive_hors_places | numeric | Serie hors-place en cours |
| derniere_victoire_jours | numeric | Jours depuis derniere victoire |
| derniere_place_jours | numeric | Jours depuis dernier top-3 |
| forme_top5_5 | numeric | Taux top-5 sur 5 dernieres |
| forme_top5_10 | numeric | Taux top-5 sur 10 dernieres |

---

## Category 2 : Horse Profile (Static) -- 24 features

| Feature | Type | Description |
|---------|------|-------------|
| profil_age | numeric | Age du cheval le jour de la course |
| profil_sexe_code | categorical | Sexe encode (M=0, F=1, H=2) |
| profil_is_male | binary | Est male |
| profil_is_female | binary | Est femelle |
| profil_is_hongre | binary | Est hongre |
| profil_race_code | categorical | Race encodee (PS/AQPS/TF/other) |
| profil_is_inedit | binary | Premier depart (debutant) |
| profil_gains_carriere_log | numeric | log1p(gains carriere) |
| profil_gains_annee_log | numeric | log1p(gains annee) |
| profil_nb_courses_carriere | numeric | Nombre de courses en carriere |
| profil_jument_pleine | binary | Jument gestante |
| profil_engagement | numeric | Montant engagement |
| profil_place_corde | numeric | Position a la corde |
| profil_place_corde_relative | numeric | Corde / nb_partants |
| profil_pays_cheval | categorical | Pays d'origine encode |
| profil_pays_entrainement | categorical | Pays d'entrainement encode |
| profil_robe_code | categorical | Couleur de robe encodee |
| profil_age_squared | numeric | age^2 (effet non-lineaire) |
| profil_age_bucket | categorical | Tranche d'age (2-3, 4-5, 6-7, 8+) |
| profil_experience_ratio | numeric | nb_victoires / nb_courses |
| profil_gains_par_course | numeric | gains_carriere / nb_courses |
| profil_gains_annee_ratio | numeric | gains_annee / gains_carriere |
| profil_is_reclamation | binary | Cheval reclamable |
| profil_supplement_paid | binary | Supplement paye |

---

## Category 3 : Jockey Stats -- 26 features

Fenetres temporelles : 30j, 90j, 365j.

| Feature | Type | Description |
|---------|------|-------------|
| jockey_nb_montes_30j | numeric | Montes dans les 30 derniers jours |
| jockey_nb_montes_90j | numeric | Montes dans les 90 derniers jours |
| jockey_nb_montes_365j | numeric | Montes dans les 365 derniers jours |
| jockey_taux_victoire_30j | numeric | Taux victoire 30 jours |
| jockey_taux_victoire_90j | numeric | Taux victoire 90 jours |
| jockey_taux_victoire_365j | numeric | Taux victoire 365 jours |
| jockey_taux_place_30j | numeric | Taux place 30 jours |
| jockey_taux_place_90j | numeric | Taux place 90 jours |
| jockey_taux_place_365j | numeric | Taux place 365 jours |
| jockey_taux_victoire_hippo | numeric | Taux victoire a cet hippodrome |
| jockey_taux_victoire_distance | numeric | Taux victoire a cette distance |
| jockey_nb_montes_cheval | numeric | Fois monte ce cheval |
| jockey_taux_victoire_cheval | numeric | Taux victoire sur ce cheval |
| jockey_taux_place_hippo | numeric | Taux place a cet hippodrome |
| jockey_taux_place_distance | numeric | Taux place a cette distance |
| jockey_taux_place_cheval | numeric | Taux place sur ce cheval |
| jockey_taux_victoire_discipline | numeric | Taux victoire dans cette discipline |
| jockey_taux_place_discipline | numeric | Taux place dans cette discipline |
| jockey_nb_montes_hippo | numeric | Montes a cet hippodrome (tout temps) |
| jockey_nb_montes_discipline | numeric | Montes dans cette discipline |
| jockey_avg_position_30j | numeric | Position moyenne 30 jours |
| jockey_avg_position_90j | numeric | Position moyenne 90 jours |
| jockey_roi_30j | numeric | ROI 30 jours |
| jockey_hot_streak | numeric | Serie de victoires/places en cours |
| jockey_change | binary | Changement de jockey vs derniere course |
| jockey_nb_victoires_jour | numeric | Victoires du jockey le meme jour |

---

## Category 4 : Trainer Stats -- 22 features

Meme structure que jockey, prefixe `entraineur_`.

| Feature | Type | Description |
|---------|------|-------------|
| entraineur_nb_montes_30j | numeric | Partants 30 jours |
| entraineur_nb_montes_90j | numeric | Partants 90 jours |
| entraineur_nb_montes_365j | numeric | Partants 365 jours |
| entraineur_taux_victoire_30j | numeric | Taux victoire 30 jours |
| entraineur_taux_victoire_90j | numeric | Taux victoire 90 jours |
| entraineur_taux_victoire_365j | numeric | Taux victoire 365 jours |
| entraineur_taux_place_30j | numeric | Taux place 30 jours |
| entraineur_taux_place_90j | numeric | Taux place 90 jours |
| entraineur_taux_place_365j | numeric | Taux place 365 jours |
| entraineur_taux_victoire_hippo | numeric | Taux victoire a cet hippodrome |
| entraineur_taux_victoire_distance | numeric | Taux victoire a cette distance |
| entraineur_nb_montes_cheval | numeric | Fois entraine ce cheval |
| entraineur_taux_victoire_cheval | numeric | Taux victoire avec ce cheval |
| entraineur_taux_place_hippo | numeric | Taux place a cet hippodrome |
| entraineur_taux_place_distance | numeric | Taux place a cette distance |
| entraineur_taux_victoire_discipline | numeric | Taux victoire dans cette discipline |
| entraineur_taux_place_discipline | numeric | Taux place dans cette discipline |
| entraineur_nb_montes_hippo | numeric | Partants a cet hippodrome |
| entraineur_nb_montes_discipline | numeric | Partants dans cette discipline |
| entraineur_avg_position_90j | numeric | Position moyenne 90 jours |
| entraineur_hot_streak | numeric | Serie en cours |
| entraineur_nb_partants_jour | numeric | Partants le meme jour (charge ecurie) |

---

## Category 5 : Jockey-Trainer Combo -- 6 features

| Feature | Type | Description |
|---------|------|-------------|
| combo_jt_nb_courses | numeric | Courses du duo jockey+entraineur |
| combo_jt_taux_victoire | numeric | Taux victoire du duo |
| combo_jt_taux_place | numeric | Taux place du duo |
| combo_jt_avg_position | numeric | Position moyenne du duo |
| combo_jt_derniere_victoire_jours | numeric | Jours depuis derniere victoire du duo |
| combo_jt_is_regular | binary | Duo avec 5+ courses ensemble |

---

## Category 6 : Jockey-Horse Combo -- 6 features

| Feature | Type | Description |
|---------|------|-------------|
| combo_jh_nb_courses | numeric | Courses jockey+cheval |
| combo_jh_taux_victoire | numeric | Taux victoire jockey+cheval |
| combo_jh_taux_place | numeric | Taux place jockey+cheval |
| combo_jh_avg_position | numeric | Position moyenne jockey+cheval |
| combo_jh_is_first_time | binary | Premiere monte de ce jockey sur ce cheval |
| combo_jh_jours_depuis_dernier | numeric | Jours depuis derniere monte ensemble |

---

## Category 7 : Horse-Hippodrome Affinity -- 7 features

| Feature | Type | Description |
|---------|------|-------------|
| affin_hippo_nb_courses | numeric | Courses a cet hippodrome |
| affin_hippo_taux_victoire | numeric | Taux victoire a cet hippodrome |
| affin_hippo_taux_place | numeric | Taux place a cet hippodrome |
| affin_hippo_avg_position | numeric | Position moyenne a cet hippodrome |
| affin_hippo_best_position | numeric | Meilleure position a cet hippodrome |
| affin_hippo_gains | numeric | Gains a cet hippodrome |
| affin_hippo_is_new | binary | Premier depart a cet hippodrome |

---

## Category 8 : Horse-Distance Affinity -- 11 features

| Feature | Type | Description |
|---------|------|-------------|
| affin_dist_nb_courses | numeric | Courses a distance similaire (+-200m) |
| affin_dist_taux_victoire | numeric | Taux victoire distance similaire |
| affin_dist_taux_place | numeric | Taux place distance similaire |
| affin_dist_avg_position | numeric | Position moyenne distance similaire |
| affin_dist_exact_nb | numeric | Courses a distance exacte (+-50m) |
| affin_dist_exact_taux_victoire | numeric | Taux victoire distance exacte |
| affin_dist_category_nb | numeric | Courses dans meme categorie de distance |
| affin_dist_category_taux_victoire | numeric | Taux victoire meme categorie |
| affin_dist_ecart_optimal | numeric | Ecart avec la distance optimale |
| affin_dist_is_shortening | binary | Court plus court que d'habitude |
| affin_dist_is_lengthening | binary | Court plus long que d'habitude |

---

## Category 9 : Horse-Discipline Affinity -- 8 features

| Feature | Type | Description |
|---------|------|-------------|
| affin_disc_nb_courses | numeric | Courses dans cette discipline |
| affin_disc_taux_victoire | numeric | Taux victoire dans cette discipline |
| affin_disc_taux_place | numeric | Taux place dans cette discipline |
| affin_disc_avg_position | numeric | Position moyenne dans cette discipline |
| affin_disc_pct_courses | numeric | % de carriere dans cette discipline |
| affin_disc_is_specialist | binary | >80% des courses dans cette discipline |
| affin_disc_switching | binary | Changement de discipline vs derniere course |
| affin_disc_nb_disciplines_tried | numeric | Nombre de disciplines essayees |

---

## Category 10 : Field Strength -- 32 features

| Feature | Type | Description |
|---------|------|-------------|
| nb_partants | numeric | Nombre de partants |
| allocation_relative | numeric | Allocation / mediane discipline |
| force_champ | numeric | Taux victoire moyen du champ |
| dispersion_champ | numeric | Ecart-type des taux victoire |
| nb_favoris | numeric | Partants avec cote < 5 |
| nb_outsiders | numeric | Partants avec cote > 20 |
| cote_favori | numeric | Cote du favori |
| rating_moyen | numeric | Taux victoire moyen du champ |
| gains_moyen | numeric | Gains moyens du champ |
| handicap_moyen | numeric | Handicap moyen du champ |
| rating_std | numeric | Ecart-type taux victoire |
| gains_std | numeric | Ecart-type gains |
| rating_range | numeric | Max - min taux victoire |
| hhi_marche | numeric | Indice Herfindahl-Hirschman des probas |
| proba_top1 | numeric | Probabilite du favori |
| proba_top3_sum | numeric | Somme des 3 plus grosses probas |
| nb_competitifs | numeric | Partants avec proba > 1/(2*N) |
| ratio_competitifs | numeric | nb_competitifs / N |
| ecart_favori_2eme | numeric | Ecart proba entre 1er et 2eme favori |
| ecart_1er_dernier | numeric | Ecart proba entre favori et outsider |
| is_open_race | binary | Proba top1 < 0.20 |
| experience_moyenne | numeric | Nb courses moyen du champ |
| nb_inedits | numeric | Nombre de debutants |
| pct_inedits | numeric | % de debutants |
| rang_proba | numeric | Rang du partant par proba implicite |
| rang_gains | numeric | Rang par gains carriere |
| rang_experience | numeric | Rang par nb courses carriere |
| field_age_moyen | numeric | Age moyen du champ |
| field_pct_hongres | numeric | % de hongres dans le champ |
| field_pct_femelles | numeric | % de femelles dans le champ |
| nb_partants_meme_entraineur | numeric | Partants du meme entraineur |
| nb_partants_meme_pere | numeric | Partants du meme pere |

---

## Category 11 : Odds / Market -- 19 features

| Feature | Type | Description |
|---------|------|-------------|
| proba_implicite | numeric | 1/cote (probabilite implicite) |
| rang_cote | numeric | Rang par cote (1=favori) |
| is_favori | binary | Favori (rang 1) |
| is_deuxieme_favori | binary | Deuxieme favori (rang 2) |
| is_outsider | binary | Outsider (cote > 20) |
| cote_relative | numeric | Cote / mediane des cotes |
| ecart_favori | numeric | Cote - cote favori |
| somme_probas | numeric | Overround (somme des 1/cote) |
| proba_normalisee | numeric | Proba normalisee (/ overround) |
| cote_log | numeric | log(cote) |
| rang_cote_pct | numeric | rang_cote / nb_partants |
| is_top3_cote | binary | Dans le top 3 par cote |
| cote_ecart_mediane | numeric | Ecart absolu a la mediane |
| ratio_cote_vs_forme | numeric | Cote vs taux victoire historique |
| cote_vs_avg_position | numeric | Correlation cote / position moyenne |
| cote_mouvement | numeric | Cote finale - cote reference |
| cote_mouvement_pct | numeric | Mouvement en % |
| is_steam | binary | Cote en forte baisse (confiance marche) |
| is_drift | binary | Cote en forte hausse (doute marche) |

---

## Category 12 : Pedigree -- 27 features

| Feature | Type | Description |
|---------|------|-------------|
| pere_taux_victoire | numeric | Taux victoire progeny du pere (temporel) |
| pere_nb_descendants_courses | numeric | Nb courses progeny du pere |
| pere_taux_victoire_distance | numeric | Taux victoire pere a cette distance |
| pere_taux_victoire_discipline | numeric | Taux victoire pere dans cette discipline |
| mere_taux_victoire | numeric | Taux victoire progeny de la mere |
| mere_nb_descendants_courses | numeric | Nb courses progeny mere |
| pere_mere_taux_victoire | numeric | Taux victoire pere de la mere |
| pere_taux_place | numeric | Taux place progeny pere |
| pere_taux_place_distance | numeric | Taux place pere a distance |
| pere_taux_place_discipline | numeric | Taux place pere en discipline |
| pere_avg_position | numeric | Position moyenne progeny pere |
| mere_taux_place | numeric | Taux place progeny mere |
| mere_avg_position | numeric | Position moyenne progeny mere |
| pere_mere_taux_place | numeric | Taux place pere de la mere |
| pere_mere_nb_descendants | numeric | Taille echantillon pere de mere |
| ent_pere_nb_descendants | numeric | Total progeny pere (script 08) |
| ent_pere_taux_victoire | numeric | Taux victoire pere (script 08) |
| ent_pere_nb_disciplines | numeric | Diversite disciplines pere |
| ent_mere_nb_descendants | numeric | Total progeny mere |
| ent_mere_taux_victoire | numeric | Taux victoire mere (script 08) |
| ent_mere_nb_disciplines | numeric | Diversite disciplines mere |
| ped_grand_pere_maternel | categorical | Grand-pere maternel encode |
| ped_grand_pere_paternel | categorical | Grand-pere paternel encode |
| ped_inbreeding_coefficient | numeric | Coefficient de consanguinite |
| ped_pere_is_top_sire | binary | Pere dans le top 20 stallions |
| ped_depth_known | numeric | Nombre de generations connues |
| ped_pere_surface_specialist | binary | Pere specialiste d'une surface |

---

## Category 13 : Meteo -- 25 features

| Feature | Type | Description |
|---------|------|-------------|
| meteo_temperature_c | numeric | Temperature au moment de la course |
| meteo_temp_range | numeric | Ecart temp journalier (max-min) |
| meteo_humidity_pct | numeric | Humidite relative |
| meteo_precipitation_mm | numeric | Precipitations horaires |
| meteo_precip_total_mm | numeric | Precipitations journalieres totales |
| meteo_wind_speed_kmh | numeric | Vitesse du vent |
| meteo_wind_gusts_kmh | numeric | Rafales |
| meteo_is_rainy | binary | Flag pluie |
| meteo_is_windy | binary | Vent > 30 km/h |
| meteo_is_hot | binary | Temperature > 30C |
| meteo_is_cold | binary | Temperature < 5C |
| meteo_weather_code | categorical | Code meteo WMO |
| meteo_comfort_index | numeric | Index confort (temp+vent+pluie) |
| meteo_wind_impact | numeric | Score perturbation vent |
| meteo_ground_moisture | numeric | Humidite sol estimee |
| meteo_temp_bucket | categorical | Categorie temperature |
| meteo_wind_direction_encoded | categorical | Direction vent encodee |
| meteo_season | categorical | Saison |
| meteo_month | numeric | Mois (1-12) |
| meteo_is_afternoon | binary | Course l'apres-midi |
| meteo_daylight_hours | numeric | Heures de jour estimees |
| horse_perf_rain | numeric | Taux victoire cheval sous la pluie |
| horse_perf_cold | numeric | Taux victoire cheval par temps froid |
| horse_perf_hot | numeric | Taux victoire cheval par temps chaud |
| horse_perf_wind | numeric | Performance cheval par vent fort |

---

## Category 14 : Track / Hippodrome -- 20 features

| Feature | Type | Description |
|---------|------|-------------|
| track_altitude | numeric | Altitude de l'hippodrome (m) |
| track_latitude | numeric | Latitude (proxy climat) |
| track_longitude | numeric | Longitude |
| track_type_piste | categorical | Surface (gazon/sable/PSF) |
| track_corde | categorical | Direction corde (gauche/droite) |
| track_is_france | binary | Hippodrome francais |
| track_region | categorical | Region encodee |
| track_nb_courses_historique | numeric | Nb courses historiques |
| track_distance_range | numeric | Amplitude des distances |
| track_is_grande_piste | binary | Grande piste (Vincennes, Longchamp...) |
| biais_stalle | numeric | Biais de stall a ce track |
| biais_corde_position | categorical | Position corde (interieur/milieu/exterieur) |
| biais_corde_winrate | numeric | Avantage corde cette position |
| biais_frontrunner | numeric | Biais front-runner a ce track |
| biais_terrain_hippodrome | numeric | Biais surface vs moyenne globale |
| biais_favori_distance | numeric | Taux victoire favori a cette distance |
| track_penetrometre_code | categorical | Etat terrain encode |
| track_mode_depart | categorical | Mode de depart |
| track_paris_types_count | numeric | Nb types de paris disponibles |
| horse_track_surface_match | numeric | Match cheval/surface |

---

## Categories 15-29 : Resume compact

### Category 15 : Pace / Tempo -- 14 features
Front-runner score, closer score, style de course, reduction km moyennes et tendances, nombre de front-runners/closers dans le champ, pression de pace, scenario de pace, probabilite leader.

### Category 16 : Equipment -- 15 features
Oeilleres (code, bool, changement, premier port), deferre (code, bool, changement), poids_monte_change, retrait oeilleres, tout changement equipement.

### Category 17 : Weight / Handicap -- 17 features
Poids porte, handicap valeur, handicap distance, poids relatif au champ, ecart top/min weight, rang poids, supplement, poids precedent, evolution poids, poids/km, base vs porte, surcharge/decharge, percentile poids, ecart handicap moyen.

### Category 18 : Time / Performance -- 20 features
Temps brut, reduction km, vitesse km/h, temps relatif au vainqueur, rang vitesse, reduction relative, moyennes et tendances vitesse, consistance reduction, vitesse a cette distance, duree course.

### Category 19 : Musique (Form String) -- 20 features
Nb courses/victoires/places/DNF dans la musique, taux victoire/place, positions recentes, moyenne/mediane, tendance, diversite disciplines, serie places/hors-places, best/worst position, variance positions.

### Category 20 : Race Conditions -- 20 features
Penetrometre encode, terrain lourd/souple/bon, condition age/sexe, allocation totale et 1er, log allocation, ratio allocation, is groupe/listed, categorie course, distance category, discipline, numero course, premier/derniere course, perf historique cheval sur ce terrain.

### Category 21 : Recency / Rest / Fatigue -- 13 features
Jours depuis derniere course, log repos, repos court/moyen/long, is rentree, nb courses 7/14/30 jours, surcharge courses, jours moyen entre courses, regularite, back-to-back.

### Category 22 : Consistency -- 8 features
Ecart-type positions sur 5/10/20 courses, consistency score, IQR positions, is consistent, is erratic, pct top-3 dans top-10.

### Category 23 : Class Changes -- 12 features
Allocation precedente, changement allocation (absolu et %), montee/descente de classe, allocation moyenne 5 courses, changement nb partants, changement distance, changement hippodrome, changement discipline.

### Category 24 : Rapports / Betting Historical -- 8 features
Simple gagnant, simple place moyen, couple gagnant, has tierce/quinte, surprise index, surprise moyenne hippodrome, taux victoire favori hippodrome.

### Category 25 : Precomputed Data -- 19 features
Cote moyenne/mediane course, ecart cote moyenne, stats carriere cheval (courses, gains, disciplines, hippodromes, anciennete, distances), stats jockey (montes, taux victoire/place, chevaux, gains), stats entraineur (partants, taux victoire/place, chevaux, gains).

### Category 26 : Performances Detaillees -- 15 features
Nb performances connues, allocation moyenne/max passee, pct places, changements jockey, diversite hippo/distance, nb partants moyen, meilleure/moyenne reduction km, courses au meme hippo/distance/jockey, intervalle moyen, tendance allocation.

### Category 27 : Interaction / Cross Features -- 10 features
forme x cote, jockey x hippo, entraineur x jockey, age x distance, poids x distance, forme x repos, cote x field strength, meteo x surface, consistency x cote, class change x forme.

### Category 28 : Calendar / Temporal -- 9 features
Jour semaine, is weekend, mois, trimestre, vacances scolaires, jour ouvre, fraction annee, sin/cos mois (encodage cyclique).

### Category 29 : Proprietaire / Eleveur -- 7 features
Nb partants/taux victoire/taux place/nb chevaux proprietaire, is ecurie, nb chevaux/taux victoire eleveur.

---

## Scripts de calcul additionnels (41-49)

Ces scripts calculateurs ne font aucun appel API et produisent des features a partir des donnees locales.

| Script | Features | Description |
|--------|----------|-------------|
| 41_sequences_performances.py | ~30 | Trend, momentum, series, repos, volatilite (pour LSTM/GRU/TFT) |
| 42_croisement_racing_post_pmu.py | ~15 | RPR, TopSpeed, class_rating depuis Racing Post |
| 43_croisement_meteo_courses.py | ~20 | Meteo exacte + historique terrain par cheval |
| 44_croisement_pedigree_partants.py | ~25 | Sire stats progressives, inbreeding, stamina/speed index |
| 45_graphe_relations_gnn.py | ~15 | Features graphe (duo cheval-jockey, cheval-entraineur, cheval-hippodrome) + edges JSONL |
| 46_track_bias_speed_class.py | ~25 | Track bias, speed figures normalisees, class ratings, field_strength |
| 48_parse_conditions_texte.py | ~20 | Features regex (age, sexe, poids, gains, groupe, handicap, apprentis) |
| 49_ecart_cotes_internet_national.py | ~20 | Market (CLV, steam move, sharp money, overbet/underbet, market efficiency) |

---

## Feature Builders dans feature_builders/

### Builders existants (11)

| Builder | Features | Statut |
|---------|----------|--------|
| musique_features.py | 22 | A debugger |
| temps_features.py | 15 | A debugger |
| profil_cheval_features.py | 24 | A debugger |
| equipement_features.py | 16 | A debugger |
| poids_features.py | 15 | A debugger |
| meteo_features.py | 15 | A debugger |
| combo_features.py | 13 | A debugger |
| class_change_features.py | 11 | A debugger |
| interaction_features.py | 10 | A debugger |
| precomputed_partant_joiner.py | 14 | A debugger |
| precomputed_entity_joiner.py | 22 | A debugger |

### Nouveaux builders (9)

| Builder | Features | Statut |
|---------|----------|--------|
| perf_detaillees_builder.py | 40-60 | Ecrit, a executer |
| smarkets_builder.py | 15-20 | Ecrit, a executer |
| racing_post_builder.py | 10-15 | Ecrit, a executer |
| reunions_builder.py | 15-20 | Ecrit, a executer |
| enrichissement_builder.py | 8 | Ecrit, a executer |
| pedigree_advanced_builder.py | 15-20 | Ecrit, a executer |
| canalturf_builder.py | 10-15 | Ecrit, a executer |
| turfostats_builder.py | 10-15 | Ecrit, a executer |
| geny_builder.py | 10-15 | Ecrit, a executer |

### Feature engineering avance (7)

| Script | Features | Statut |
|--------|----------|--------|
| feat_historique.py | ~80 | Ecrit, a executer |
| feat_croisements.py | ~60 | Ecrit, a executer |
| feat_jockey.py | ~50 | Ecrit, a executer |
| feat_interactions.py | ~60 | Ecrit, a executer |
| feat_pedigree.py | ~40 | Ecrit, a executer |
| feat_temporel.py | ~40 | Ecrit, a executer |
| feat_sequences.py | ~30 | Ecrit, a executer |
