# FEATURE CATALOG - Horse Racing Prediction Model

> Updated 2026-03-25
> Cataloged features: 853 (across 95 feature builders)
> Estimated total output columns: ~1,200 (including windowed variants)
> Target: 350-500 features -- EXCEEDED (680+ target also exceeded)
> This catalog lists ALL features buildable from available data sources.

---

## DATA SOURCES INVENTORY

| Source | Path | Records | Status |
|--------|------|---------|--------|
| Partants normalises | output/02_merged/partants_normalises.json | 2,930,290 | Available |
| Courses normalisees | output/02_merged/courses_normalisees.json | 257,806 | Available |
| Meteo historique | output/13_meteo_historique/meteo_historique.json | 31,778 | Available |
| Pedigrees complets | output/14_pedigree/pedigrees_pq.json | 24,484 | Available |
| Historique chevaux | output/05_historique_chevaux/historique_chevaux.json | 80,656 | Available |
| Historique jockeys | output/06_historique_jockeys/historique_jockeys.json | 12,319 | Available |
| Historique entraineurs | output/06_historique_jockeys/historique_entraineurs.json | ~5,000 | Available |
| Pedigree peres | output/08_pedigree/pedigree_peres.json | Available | Available |
| Pedigree meres | output/08_pedigree/pedigree_meres.json | Available | Available |
| Cotes marche | output/07_cotes_marche/cotes_marche.json | Available | Available |
| Equipements | output/09_equipements/equipements_historique.json | Available | Available |
| Poids handicaps | output/10_poids_handicaps/poids_handicaps.json | Available | Available |
| Sectionals | output/11_sectionals/sectionals.json | Available | Available |
| Rapports complets | output/rapports_merged/rapports_complets.json | 124,287 | Available |
| Performances detaillees | output/22_performances_detaillees/performances_detaillees.json | 917,805 | Available |
| Hippodromes DB | hippodromes_db.py | 673 | Available |

### Partant fields (66 fields available):
age, allure, avis_entraineur, cle_partant, commentaire_apres_course, cote_finale, cote_reference,
course_uid, date_reunion_iso, deferre, discipline, distance, ecart_precedent, eleveur, engagement,
entraineur, gains_annee_euros, gains_carriere_euros, handicap_distance_m, handicap_valeur,
hippodrome_normalise, horse_id, incident, is_disqualifie, is_gagnant, is_inedit, is_place,
jockey_driver, jockey_driver_change, jument_pleine, mere, musique, nb_courses_carriere,
nb_places_2eme, nb_places_3eme, nb_places_carriere, nb_victoires_carriere, nom_cheval, num_pmu,
numero_course, numero_reunion, oeilleres, partant_uid, pays_cheval, pays_entrainement, pere,
pere_mere, place_corde, poids_base_kg, poids_monte_change, poids_porte_kg, position_arrivee,
proba_implicite, proprietaire, race, reduction_km_ms, reunion_uid, robe, sexe, source, statut,
supplement_euros, surcharge_decharge_kg, taux_reclamation_euros, temps_ms, timestamp_collecte

### Course fields (36 fields available):
allocation_1er, allocation_totale, categorie, cle_course, condition_age, condition_sexe,
conditions_texte, corde, course_trackee, course_uid, date_reunion_iso, discipline, distance,
duree_course_ms, heure_depart, hippodrome, hippodrome_normalise, incidents, libelle,
mode_depart, nombre_partants, numero_course, numero_reunion, ordre_arrivee, parcours,
paris_types, pays, penetrometre, replay_disponible, reunion_uid, source, specialite,
statut, timestamp_collecte, type_piste, url_source

---

## LEGEND

- **NOW** = Can be built with currently available data
- **PARTIAL** = Available for a subset of records
- **NEED** = Requires additional data collection
- **(E)** = Existing feature (already in the 67-feature matrix)

---

## CATEGORY 1: HORSE FORM (Rolling Windows)
> Source: partants (position_arrivee, is_gagnant, is_place, gains)
> Windows: last 3, 5, 10, 20 courses

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 1 | forme_victoire_3 | Win rate last 3 races | partants | NOW |
| 2 | forme_victoire_5 | Win rate last 5 races | partants | NOW (E) |
| 3 | forme_victoire_10 | Win rate last 10 races | partants | NOW (E) |
| 4 | forme_victoire_20 | Win rate last 20 races | partants | NOW (E) |
| 5 | forme_place_3 | Place rate (top 3) last 3 races | partants | NOW |
| 6 | forme_place_5 | Place rate last 5 races | partants | NOW (E) |
| 7 | forme_place_10 | Place rate last 10 races | partants | NOW (E) |
| 8 | forme_place_20 | Place rate last 20 races | partants | NOW (E) |
| 9 | avg_position_3 | Mean position last 3 races | partants | NOW |
| 10 | avg_position_5 | Mean position last 5 races | partants | NOW (E) |
| 11 | avg_position_10 | Mean position last 10 races | partants | NOW |
| 12 | avg_position_20 | Mean position last 20 races | partants | NOW |
| 13 | median_position_5 | Median position last 5 races | partants | NOW |
| 14 | median_position_10 | Median position last 10 races | partants | NOW |
| 15 | best_position_5 | Best (min) position last 5 races | partants | NOW |
| 16 | best_position_10 | Best position last 10 races | partants | NOW |
| 17 | worst_position_5 | Worst (max) position last 5 races | partants | NOW |
| 18 | worst_position_10 | Worst position last 10 races | partants | NOW |
| 19 | gains_cumules | Total career gains before this race | partants | NOW (E) |
| 20 | gains_5 | Sum of gains last 5 races | partants | NOW |
| 21 | gains_10 | Sum of gains last 10 races | partants | NOW |
| 22 | gains_moyen_par_course | Career gains / nb courses | partants | NOW |
| 23 | nb_courses_avant | Number of prior races | partants | NOW (E) |
| 24 | nb_victoires_avant | Number of prior wins | partants | NOW (E) |
| 25 | nb_places_avant | Number of prior places | partants | NOW (E) |
| 26 | taux_victoire_carriere | Career win rate (nb_victoires / nb_courses) | partants | NOW |
| 27 | taux_place_carriere | Career place rate | partants | NOW |
| 28 | derniere_position | Position in last race | partants | NOW (E) |
| 29 | avant_derniere_position | Position in 2nd-to-last race | partants | NOW |
| 30 | progression | Trend: improving/declining/stable | partants | NOW (E) |
| 31 | progression_score | Numeric: avg(pos last 3) - avg(pos prev 3) | partants | NOW |
| 32 | nb_dnf_5 | Count of DNF/DQ in last 5 races | partants | NOW |
| 33 | nb_dnf_10 | Count of DNF/DQ in last 10 races | partants | NOW |
| 34 | taux_dnf_carriere | DNF rate across career | partants | NOW |
| 35 | consecutive_wins | Current winning streak | partants | NOW |
| 36 | consecutive_places | Current placing streak | partants | NOW |
| 37 | consecutive_hors_places | Current non-placing streak | partants | NOW |
| 38 | derniere_victoire_jours | Days since last win | partants | NOW |
| 39 | derniere_place_jours | Days since last top-3 finish | partants | NOW |
| 40 | forme_top5_5 | Rate of top-5 finishes in last 5 | partants | NOW |
| 41 | forme_top5_10 | Rate of top-5 finishes in last 10 | partants | NOW |

**Subtotal: 41 features (14 existing + 27 new)**

---

## CATEGORY 2: HORSE PROFILE (Static Attributes)
> Source: partants (age, sexe, race, etc.)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 42 | profil_age | Horse age at race time | partants | NOW (E via profil) |
| 43 | profil_sexe_code | Encoded sex (M=0, F=1, H=2) | partants | NOW (E) |
| 44 | profil_is_male | Binary: is male | partants | NOW (E) |
| 45 | profil_is_female | Binary: is female | partants | NOW (E) |
| 46 | profil_is_hongre | Binary: is gelding | partants | NOW (E) |
| 47 | profil_race_code | Encoded race (PS/AQPS/TF/other) | partants | NOW (E) |
| 48 | profil_is_inedit | First race ever (debut) | partants | NOW (E) |
| 49 | profil_gains_carriere_log | log1p(career gains) | partants | NOW (E) |
| 50 | profil_gains_annee_log | log1p(current year gains) | partants | NOW (E) |
| 51 | profil_nb_courses_carriere | Career race count | partants | NOW (E) |
| 52 | profil_jument_pleine | Pregnant mare flag | partants | NOW (E) |
| 53 | profil_engagement | Engagement amount | partants | NOW (E) |
| 54 | profil_place_corde | Stall/rope position | partants | NOW (E) |
| 55 | profil_place_corde_relative | Corde / nb_partants | partants | NOW (E) |
| 56 | profil_pays_cheval | Country of horse (encoded) | partants | NOW |
| 57 | profil_pays_entrainement | Training country (encoded) | partants | NOW |
| 58 | profil_robe_code | Encoded coat color | partants | NOW |
| 59 | profil_age_squared | age^2 (nonlinear age effect) | partants | NOW |
| 60 | profil_age_bucket | Age bucket (2-3, 4-5, 6-7, 8+) | partants | NOW |
| 61 | profil_experience_ratio | nb_victoires / nb_courses (career quality) | partants | NOW |
| 62 | profil_gains_par_course | gains_carriere / nb_courses | partants | NOW |
| 63 | profil_gains_annee_ratio | gains_annee / gains_carriere | partants | NOW |
| 64 | profil_is_reclamation | Whether horse can be claimed | partants | NOW |
| 65 | profil_supplement_paid | Whether supplement was paid | partants | NOW |

**Subtotal: 24 features (14 existing + 10 new)**

---

## CATEGORY 3: JOCKEY STATS
> Source: partants (rolling temporal windows)
> Windows: 30j, 90j, 365j

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 66 | jockey_nb_montes_30j | Rides in last 30 days | partants | NOW (E) |
| 67 | jockey_nb_montes_90j | Rides in last 90 days | partants | NOW (E) |
| 68 | jockey_nb_montes_365j | Rides in last 365 days | partants | NOW (E) |
| 69 | jockey_taux_victoire_30j | Win rate last 30 days | partants | NOW (E) |
| 70 | jockey_taux_victoire_90j | Win rate last 90 days | partants | NOW (E) |
| 71 | jockey_taux_victoire_365j | Win rate last 365 days | partants | NOW (E) |
| 72 | jockey_taux_place_30j | Place rate last 30 days | partants | NOW (E) |
| 73 | jockey_taux_place_90j | Place rate last 90 days | partants | NOW (E) |
| 74 | jockey_taux_place_365j | Place rate last 365 days | partants | NOW (E) |
| 75 | jockey_taux_victoire_hippo | Win rate at this track | partants | NOW (E) |
| 76 | jockey_taux_victoire_distance | Win rate at this distance | partants | NOW (E) |
| 77 | jockey_nb_montes_cheval | Times ridden this horse | partants | NOW (E) |
| 78 | jockey_taux_victoire_cheval | Win rate on this horse | partants | NOW (E) |
| 79 | jockey_taux_place_hippo | Place rate at this track | partants | NOW |
| 80 | jockey_taux_place_distance | Place rate at this distance | partants | NOW |
| 81 | jockey_taux_place_cheval | Place rate on this horse | partants | NOW |
| 82 | jockey_taux_victoire_discipline | Win rate in this discipline | partants | NOW |
| 83 | jockey_taux_place_discipline | Place rate in this discipline | partants | NOW |
| 84 | jockey_nb_montes_hippo | Rides at this track (all time) | partants | NOW |
| 85 | jockey_nb_montes_discipline | Rides in this discipline | partants | NOW |
| 86 | jockey_avg_position_30j | Average finish position last 30 days | partants | NOW |
| 87 | jockey_avg_position_90j | Average finish position last 90 days | partants | NOW |
| 88 | jockey_roi_30j | Return on investment last 30 days | partants+rapports | NOW |
| 89 | jockey_hot_streak | Current consecutive wins or places | partants | NOW |
| 90 | jockey_change | Boolean: jockey changed vs last race | partants | NOW |
| 91 | jockey_nb_victoires_jour | Wins by jockey on same day (before this race) | partants | NOW |

**Subtotal: 26 features (13 existing + 13 new)**

---

## CATEGORY 4: TRAINER STATS
> Source: partants (rolling temporal windows)
> Same structure as jockey

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 92 | entraineur_nb_montes_30j | Starters last 30 days | partants | NOW (E) |
| 93 | entraineur_nb_montes_90j | Starters last 90 days | partants | NOW (E) |
| 94 | entraineur_nb_montes_365j | Starters last 365 days | partants | NOW (E) |
| 95 | entraineur_taux_victoire_30j | Win rate last 30 days | partants | NOW (E) |
| 96 | entraineur_taux_victoire_90j | Win rate last 90 days | partants | NOW (E) |
| 97 | entraineur_taux_victoire_365j | Win rate last 365 days | partants | NOW (E) |
| 98 | entraineur_taux_place_30j | Place rate last 30 days | partants | NOW (E) |
| 99 | entraineur_taux_place_90j | Place rate last 90 days | partants | NOW (E) |
| 100 | entraineur_taux_place_365j | Place rate last 365 days | partants | NOW (E) |
| 101 | entraineur_taux_victoire_hippo | Win rate at this track | partants | NOW (E) |
| 102 | entraineur_taux_victoire_distance | Win rate at this distance | partants | NOW (E) |
| 103 | entraineur_nb_montes_cheval | Times trained this horse | partants | NOW (E) |
| 104 | entraineur_taux_victoire_cheval | Win rate with this horse | partants | NOW (E) |
| 105 | entraineur_taux_place_hippo | Place rate at this track | partants | NOW |
| 106 | entraineur_taux_place_distance | Place rate at this distance | partants | NOW |
| 107 | entraineur_taux_victoire_discipline | Win rate in this discipline | partants | NOW |
| 108 | entraineur_taux_place_discipline | Place rate in this discipline | partants | NOW |
| 109 | entraineur_nb_montes_hippo | Starters at this track (all time) | partants | NOW |
| 110 | entraineur_nb_montes_discipline | Starters in this discipline | partants | NOW |
| 111 | entraineur_avg_position_90j | Average finish position last 90 days | partants | NOW |
| 112 | entraineur_hot_streak | Current consecutive wins | partants | NOW |
| 113 | entraineur_nb_partants_jour | Starters on same day (stable load) | partants | NOW |

**Subtotal: 22 features (13 existing + 9 new)**

---

## CATEGORY 5: JOCKEY-TRAINER COMBO
> Source: partants (cross-reference jockey x trainer)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 114 | combo_jt_nb_courses | Times this jockey+trainer combo raced | partants | NOW |
| 115 | combo_jt_taux_victoire | Win rate of jockey+trainer combo | partants | NOW |
| 116 | combo_jt_taux_place | Place rate of jockey+trainer combo | partants | NOW |
| 117 | combo_jt_avg_position | Average position of combo | partants | NOW |
| 118 | combo_jt_derniere_victoire_jours | Days since combo last won | partants | NOW |
| 119 | combo_jt_is_regular | Boolean: combo has 5+ past races together | partants | NOW |

**Subtotal: 6 features (0 existing + 6 new)**

---

## CATEGORY 6: JOCKEY-HORSE COMBO
> Source: partants

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 120 | combo_jh_nb_courses | Times jockey rode this horse | partants | NOW (partial via jockey_nb_montes_cheval) |
| 121 | combo_jh_taux_victoire | Win rate jockey+horse | partants | NOW (partial via jockey_taux_victoire_cheval) |
| 122 | combo_jh_taux_place | Place rate jockey+horse | partants | NOW |
| 123 | combo_jh_avg_position | Average position jockey+horse | partants | NOW |
| 124 | combo_jh_is_first_time | First time this jockey rides this horse | partants | NOW |
| 125 | combo_jh_jours_depuis_dernier | Days since jockey last rode this horse | partants | NOW |

**Subtotal: 6 features (2 partial existing + 4 new)**

---

## CATEGORY 7: HORSE-HIPPODROME AFFINITY
> Source: partants

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 126 | affin_hippo_nb_courses | Races at this track | partants | NOW |
| 127 | affin_hippo_taux_victoire | Win rate at this track | partants | NOW (E) |
| 128 | affin_hippo_taux_place | Place rate at this track | partants | NOW |
| 129 | affin_hippo_avg_position | Average position at this track | partants | NOW |
| 130 | affin_hippo_best_position | Best position at this track | partants | NOW |
| 131 | affin_hippo_gains | Total gains at this track | partants | NOW |
| 132 | affin_hippo_is_new | First time at this track | partants | NOW |

**Subtotal: 7 features (1 existing + 6 new)**

---

## CATEGORY 8: HORSE-DISTANCE AFFINITY
> Source: partants

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 133 | affin_dist_nb_courses | Races at similar distance (+-200m) | partants | NOW |
| 134 | affin_dist_taux_victoire | Win rate at similar distance | partants | NOW (E) |
| 135 | affin_dist_taux_place | Place rate at similar distance | partants | NOW |
| 136 | affin_dist_avg_position | Avg position at similar distance | partants | NOW |
| 137 | affin_dist_exact_nb | Races at exact distance (+-50m) | partants | NOW |
| 138 | affin_dist_exact_taux_victoire | Win rate at exact distance | partants | NOW |
| 139 | affin_dist_category_nb | Races in same distance category | partants | NOW |
| 140 | affin_dist_category_taux_victoire | Win rate in same distance category | partants | NOW |
| 141 | affin_dist_ecart_optimal | Distance delta from best-performing distance | partants | NOW |
| 142 | affin_dist_is_shortening | Running shorter than usual | partants | NOW |
| 143 | affin_dist_is_lengthening | Running longer than usual | partants | NOW |

**Subtotal: 11 features (1 existing + 10 new)**

---

## CATEGORY 9: HORSE-DISCIPLINE AFFINITY
> Source: partants

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 144 | affin_disc_nb_courses | Races in this discipline | partants | NOW |
| 145 | affin_disc_taux_victoire | Win rate in this discipline | partants | NOW (E) |
| 146 | affin_disc_taux_place | Place rate in this discipline | partants | NOW |
| 147 | affin_disc_avg_position | Avg position in this discipline | partants | NOW |
| 148 | affin_disc_pct_courses | % of career in this discipline | partants | NOW |
| 149 | affin_disc_is_specialist | >80% of races in this discipline | partants | NOW |
| 150 | affin_disc_switching | Changing discipline vs last race | partants | NOW |
| 151 | affin_disc_nb_disciplines_tried | Number of distinct disciplines tried | partants | NOW |

**Subtotal: 8 features (1 existing + 7 new)**

---

## CATEGORY 10: FIELD STRENGTH & COMPOSITION
> Source: partants (grouped by course_uid)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 152 | nb_partants | Number of runners in race | partants | NOW (E) |
| 153 | allocation_relative | Allocation / median allocation for discipline | courses | NOW (E) |
| 154 | force_champ | Mean win rate of field | partants | NOW (E) |
| 155 | dispersion_champ | StDev of win rates | partants | NOW (E) |
| 156 | nb_favoris | Count of runners with odds < 5 | partants | NOW (E) |
| 157 | nb_outsiders | Count of runners with odds > 20 | partants | NOW (E) |
| 158 | cote_favori | Lowest odds in race | partants | NOW (E) |
| 159 | rating_moyen | Mean career win rate of field | partants | NOW (via field_strength) |
| 160 | gains_moyen | Mean career gains of field | partants | NOW |
| 161 | handicap_moyen | Mean handicap value of field | partants | NOW |
| 162 | rating_std | StDev of career win rates | partants | NOW |
| 163 | gains_std | StDev of career gains | partants | NOW |
| 164 | rating_range | Max - min win rate in field | partants | NOW |
| 165 | hhi_marche | Herfindahl-Hirschman index of probas | partants | NOW |
| 166 | proba_top1 | Probability of favorite | partants | NOW |
| 167 | proba_top3_sum | Sum of top 3 probabilities | partants | NOW |
| 168 | nb_competitifs | Runners with proba > 1/(2*N) | partants | NOW |
| 169 | ratio_competitifs | nb_competitifs / N | partants | NOW |
| 170 | ecart_favori_2eme | Proba gap between 1st and 2nd favorite | partants | NOW |
| 171 | ecart_1er_dernier | Proba gap between favorite and outsider | partants | NOW |
| 172 | is_open_race | Boolean: proba_top1 < 0.20 | partants | NOW |
| 173 | experience_moyenne | Mean career race count of field | partants | NOW |
| 174 | nb_inedits | Count of debutants in field | partants | NOW |
| 175 | pct_inedits | % of debutants | partants | NOW |
| 176 | rang_proba | Runner's rank by implied proba | partants | NOW |
| 177 | rang_gains | Runner's rank by career gains | partants | NOW |
| 178 | rang_experience | Runner's rank by career race count | partants | NOW |
| 179 | field_age_moyen | Mean age of field | partants | NOW |
| 180 | field_pct_hongres | % of geldings in field | partants | NOW |
| 181 | field_pct_femelles | % of females in field | partants | NOW |
| 182 | nb_partants_meme_entraineur | Runners from same trainer in race | partants | NOW |
| 183 | nb_partants_meme_pere | Runners from same sire in race | partants | NOW |

**Subtotal: 32 features (8 existing + 24 new)**

---

## CATEGORY 11: ODDS / MARKET FEATURES
> Source: partants (cote_finale, cote_reference, proba_implicite)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 184 | proba_implicite | 1/odds implied probability | partants | NOW (E) |
| 185 | rang_cote | Rank by odds (1=favorite) | partants | NOW (E) |
| 186 | is_favori | Boolean: rank 1 by odds | partants | NOW (E) |
| 187 | is_deuxieme_favori | Boolean: rank 2 | partants | NOW (E) |
| 188 | is_outsider | Boolean: odds > 20 | partants | NOW (E) |
| 189 | cote_relative | odds / median odds of race | partants | NOW (E) |
| 190 | ecart_favori | odds - favorite odds | partants | NOW (E) |
| 191 | somme_probas | Overround: sum of 1/odds | partants | NOW (E) |
| 192 | proba_normalisee | Normalized proba (/ overround) | partants | NOW (E) |
| 193 | cote_log | log(odds) | partants | NOW |
| 194 | rang_cote_pct | rang_cote / nb_partants (percentile rank) | partants | NOW |
| 195 | is_top3_cote | Boolean: in top 3 by odds | partants | NOW |
| 196 | cote_ecart_mediane | |cote - mediane_cote| / mediane | partants | NOW |
| 197 | ratio_cote_vs_forme | cote vs historical win rate (value detection) | partants | NOW |
| 198 | cote_vs_avg_position | correlation between odds and actual average position | partants | NOW |
| 199 | cote_mouvement | cote_finale - cote_reference (drift) | partants | PARTIAL |
| 200 | cote_mouvement_pct | (finale-ref)/ref as % | partants | PARTIAL |
| 201 | is_steam | Boolean: odds dropped significantly (market confidence) | partants | PARTIAL |
| 202 | is_drift | Boolean: odds increased significantly (market doubt) | partants | PARTIAL |

**Subtotal: 19 features (9 existing + 10 new)**

---

## CATEGORY 12: PEDIGREE FEATURES
> Source: partants (pere, mere, pere_mere), pedigrees_pq.json, pedigree_peres/meres

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 203 | pere_taux_victoire | Sire offspring win rate (temporal) | partants | NOW (E) |
| 204 | pere_nb_descendants_courses | Sire offspring race count | partants | NOW (E) |
| 205 | pere_taux_victoire_distance | Sire win rate at this distance | partants | NOW (E) |
| 206 | pere_taux_victoire_discipline | Sire win rate in this discipline | partants | NOW (E) |
| 207 | mere_taux_victoire | Dam offspring win rate | partants | NOW (E) |
| 208 | mere_nb_descendants_courses | Dam offspring race count | partants | NOW (E) |
| 209 | pere_mere_taux_victoire | Broodmare sire win rate | partants | NOW (E) |
| 210 | pere_taux_place | Sire offspring place rate | partants | NOW |
| 211 | pere_taux_place_distance | Sire place rate at distance | partants | NOW |
| 212 | pere_taux_place_discipline | Sire place rate in discipline | partants | NOW |
| 213 | pere_avg_position | Sire offspring avg position | partants | NOW |
| 214 | mere_taux_place | Dam offspring place rate | partants | NOW |
| 215 | mere_avg_position | Dam offspring avg position | partants | NOW |
| 216 | pere_mere_taux_place | Broodmare sire place rate | partants | NOW |
| 217 | pere_mere_nb_descendants | Broodmare sire sample size | partants | NOW |
| 218 | ent_pere_nb_descendants | Sire total offspring (from script 08) | 08_pedigree | NOW (E via entity joiner) |
| 219 | ent_pere_taux_victoire | Sire win rate (from script 08) | 08_pedigree | NOW (E) |
| 220 | ent_pere_nb_disciplines | Sire discipline diversity | 08_pedigree | NOW (E) |
| 221 | ent_mere_nb_descendants | Dam total offspring | 08_pedigree | NOW (E) |
| 222 | ent_mere_taux_victoire | Dam win rate (from script 08) | 08_pedigree | NOW (E) |
| 223 | ent_mere_nb_disciplines | Dam discipline diversity | 08_pedigree | NOW (E) |
| 224 | ped_grand_pere_maternel | Grand-sire maternal name (encoded) | pedigrees_pq | NOW |
| 225 | ped_grand_pere_paternel | Grand-sire paternal name (encoded) | pedigrees_pq | NOW |
| 226 | ped_inbreeding_coefficient | Simple inbreeding check (same ancestor appearing twice) | pedigrees_pq | NOW |
| 227 | ped_pere_is_top_sire | Boolean: sire in top 20 by offspring wins | partants | NOW |
| 228 | ped_depth_known | How many generations of pedigree are known | pedigrees_pq | NOW |
| 229 | ped_pere_surface_specialist | Boolean: sire offspring perform better on specific surface | partants+courses | NOW |

**Subtotal: 27 features (13 existing + 14 new)**

---

## CATEGORY 13: METEO FEATURES
> Source: meteo_historique.json (31,778 course-level records)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 230 | meteo_temperature_c | Temperature at race time | meteo | NOW (E via meteo builder) |
| 231 | meteo_temp_range | Daily temp range (max-min) | meteo | NOW (E) |
| 232 | meteo_humidity_pct | Relative humidity | meteo | NOW (E) |
| 233 | meteo_precipitation_mm | Hourly precipitation | meteo | NOW (E) |
| 234 | meteo_precip_total_mm | Daily total precipitation | meteo | NOW (E) |
| 235 | meteo_wind_speed_kmh | Wind speed | meteo | NOW (E) |
| 236 | meteo_wind_gusts_kmh | Wind gusts | meteo | NOW (E) |
| 237 | meteo_is_rainy | Boolean: rain flag | meteo | NOW (E) |
| 238 | meteo_is_windy | Boolean: wind > 30 km/h | meteo | NOW (E) |
| 239 | meteo_is_hot | Boolean: temp > 30C | meteo | NOW (E) |
| 240 | meteo_is_cold | Boolean: temp < 5C | meteo | NOW (E) |
| 241 | meteo_weather_code | WMO weather code | meteo | NOW (E) |
| 242 | meteo_comfort_index | Combined comfort (temp+wind+rain) | meteo | NOW (E) |
| 243 | meteo_wind_impact | Wind disruption score | meteo | NOW (E) |
| 244 | meteo_ground_moisture | Estimated ground moisture | meteo | NOW (E) |
| 245 | meteo_temp_bucket | Temperature category (cold/cool/warm/hot) | meteo | NOW |
| 246 | meteo_wind_direction_encoded | Wind direction encoded (if available) | meteo | PARTIAL |
| 247 | meteo_season | Season derived from date (spring/summer/autumn/winter) | date | NOW |
| 248 | meteo_month | Month of year (1-12) | date | NOW |
| 249 | meteo_is_afternoon | Boolean: race in afternoon vs morning | courses | NOW |
| 250 | meteo_daylight_hours | Estimated daylight based on lat+date | hippo_db+date | NOW |
| 251 | horse_perf_rain | Horse's historical win rate in rainy conditions | partants+meteo | NOW |
| 252 | horse_perf_cold | Horse's historical win rate in cold conditions | partants+meteo | NOW |
| 253 | horse_perf_hot | Horse's historical win rate in hot conditions | partants+meteo | NOW |
| 254 | horse_perf_wind | Horse's historical perf in windy conditions | partants+meteo | NOW |

**Subtotal: 25 features (15 existing + 10 new)**

---

## CATEGORY 14: TRACK / HIPPODROME FEATURES
> Source: hippodromes_db.py (673 entries), courses

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 255 | track_altitude | Altitude of hippodrome (m) | hippo_db | NOW |
| 256 | track_latitude | Latitude (proxy for climate) | hippo_db | NOW |
| 257 | track_longitude | Longitude | hippo_db | NOW |
| 258 | track_type_piste | Surface type (gazon/cendree/sable/PSF) | hippo_db/courses | NOW |
| 259 | track_corde | Corde direction (gauche/droite) | hippo_db/courses | NOW |
| 260 | track_is_france | Boolean: French track | hippo_db | NOW |
| 261 | track_region | Region encoded | hippo_db | NOW |
| 262 | track_nb_courses_historique | Historical number of races at track | hippo_db | NOW |
| 263 | track_distance_range | Max - min distance at this track | hippo_db | NOW |
| 264 | track_is_grande_piste | Boolean: major track (Vincennes/Longchamp/etc) | hippo_db | NOW |
| 265 | biais_stalle | Stall bias at this track (galop) | partants+courses | NOW (E via track_bias) |
| 266 | biais_corde_position | Corde position category (inner/middle/outer) | partants+courses | NOW (E) |
| 267 | biais_corde_winrate | Win rate advantage of current corde position | partants+courses | NOW (E) |
| 268 | biais_frontrunner | Front-runner bias at this track | partants+courses | NOW (E) |
| 269 | biais_terrain_hippodrome | Surface bias vs global average | partants+courses | NOW (E) |
| 270 | biais_favori_distance | Favorite win rate at distance category | partants+courses | NOW (E) |
| 271 | track_penetrometre_code | Encoded going (bon/souple/lourd/etc) | courses | NOW |
| 272 | track_mode_depart | Mode de depart (autostart/volte/etc) | courses | NOW |
| 273 | track_paris_types_count | Number of betting types available | courses | NOW |
| 274 | horse_track_surface_match | Does horse prefer this surface type | partants+courses | NOW |

**Subtotal: 20 features (6 existing + 14 new)**

---

## CATEGORY 15: PACE / TEMPO FEATURES
> Source: partants (place_corde, reduction_km_ms)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 275 | front_runner_score | Score of front-running style | partants | NOW (E via pace) |
| 276 | closer_score | Score of closing style | partants | NOW (E) |
| 277 | style_course | Dominant style (front/mid/closer) | partants | NOW (E) |
| 278 | avg_reduction_km_5 | Avg reduction km last 5 races | partants | NOW (E) |
| 279 | best_reduction_km_10 | Best reduction km last 10 | partants | NOW (E) |
| 280 | reduction_km_trend | Trend in reduction km | partants | NOW (E) |
| 281 | nb_front_runners | Count of front-runners in field | partants | NOW (E) |
| 282 | nb_closers | Count of closers in field | partants | NOW (E) |
| 283 | pace_pressure | Front-runner density in field | partants | NOW (E) |
| 284 | pace_scenario | Expected pace (fast/moderate/slow) | partants | NOW (E) |
| 285 | is_probable_leader | Boolean: likely to lead | partants | NOW (E) |
| 286 | pace_style_vs_scenario | Match between horse style and expected pace (advantage) | partants | NOW |
| 287 | pace_diversity | Diversity of running styles in field | partants | NOW |
| 288 | pace_front_vs_closer_ratio | Ratio front-runners to closers | partants | NOW |

**Subtotal: 14 features (11 existing + 3 new)**

---

## CATEGORY 16: EQUIPMENT FEATURES
> Source: partants (oeilleres, deferre)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 289 | equip_oeilleres_code | Encoded oeilleres (0/1/2) | partants | NOW (E via equip) |
| 290 | equip_has_oeilleres | Boolean: wearing oeilleres | partants | NOW (E) |
| 291 | equip_deferre_code | Encoded deferre (0-3) | partants | NOW (E) |
| 292 | equip_has_deferre | Boolean: unshod | partants | NOW (E) |
| 293 | equip_oeilleres_change | Oeilleres changed vs last race | partants | NOW (E) |
| 294 | equip_deferre_change | Deferre changed vs last race | partants | NOW (E) |
| 295 | equip_premier_oeilleres | First time with oeilleres | partants | NOW (E) |
| 296 | equip_premier_deferre | First time unshod | partants | NOW (E) |
| 297 | equip_nb_courses_avec_oeilleres | Prior races with oeilleres | partants | NOW (E) |
| 298 | equip_poids_monte_change | Weight/rider change flag | partants | NOW (E) |
| 299 | pc_retrait_oeilleres | Oeilleres removed from last race | 09_equipements | NOW (E via pc_joiner) |
| 300 | equip_any_change | Boolean: any equipment change | partants | NOW |
| 301 | equip_ajout_oeilleres | Oeilleres added (not first time) | partants | NOW |
| 302 | equip_deferre_anterieurs | Boolean: deferre_anterieurs specifically | partants | NOW |
| 303 | equip_deferre_4_pieds | Boolean: deferre_4_pieds specifically | partants | NOW |

**Subtotal: 15 features (11 existing + 4 new)**

---

## CATEGORY 17: WEIGHT / HANDICAP FEATURES
> Source: partants (poids_porte_kg, handicap_valeur, etc.)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 304 | poids_porte_kg | Absolute weight carried | partants | NOW (E via poids) |
| 305 | poids_handicap_valeur | Handicap rating value | partants | NOW (E) |
| 306 | poids_handicap_distance_m | Distance handicap (trot) | partants | NOW (E) |
| 307 | poids_relatif_champ | Weight vs field average | partants | NOW (E) |
| 308 | poids_ecart_top_weight | Weight vs top weight | partants | NOW (E) |
| 309 | poids_ecart_min_weight | Weight vs minimum weight | partants | NOW (E) |
| 310 | poids_rang_poids | Rank by weight (1=heaviest) | partants | NOW (E) |
| 311 | poids_supplement | Supplement paid | partants | NOW (E) |
| 312 | pc_poids_precedent | Weight in previous race | 10_poids | NOW (E via pc_joiner) |
| 313 | pc_evolution_poids | Weight change from previous | 10_poids | NOW (E) |
| 314 | pc_poids_par_km | Weight per km | 10_poids | NOW (E) |
| 315 | poids_base_kg | Base weight before adjustments | partants | NOW |
| 316 | surcharge_decharge_kg | Surcharge or decharge | partants | NOW |
| 317 | poids_ecart_base_porte | Difference between base and carried weight | partants | NOW |
| 318 | poids_rang_poids_pct | poids_rang / nb_partants (percentile) | partants | NOW |
| 319 | handicap_ecart_moyen | handicap_valeur - field average handicap | partants | NOW |
| 320 | handicap_distance_ecart | handicap_distance_m - base_distance | partants | NOW |

**Subtotal: 17 features (11 existing + 6 new)**

---

## CATEGORY 18: TIME / PERFORMANCE FEATURES
> Source: partants (temps_ms, reduction_km_ms)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 321 | temps_temps_ms | Raw finish time | partants | NOW (E via temps) |
| 322 | temps_reduction_km_ms | Reduction km in ms | partants | NOW (E) |
| 323 | temps_vitesse_kmh | Average speed km/h | partants | NOW (E) |
| 324 | temps_relatif_vainqueur | Time gap to winner | partants | NOW (E) |
| 325 | temps_rang_vitesse | Rank by speed in race | partants | NOW (E) |
| 326 | temps_reduction_relative | Reduction km vs race average | partants | NOW (E) |
| 327 | temps_avg_reduction_5 | Avg reduction km last 5 | partants | NOW (E) |
| 328 | temps_best_reduction_10 | Best reduction km last 10 | partants | NOW (E) |
| 329 | temps_reduction_trend | Trend in reduction km | partants | NOW (E) |
| 330 | temps_ecart_moyen_champ | Time gap to field average | partants | NOW (E) |
| 331 | pc_reduction_km_sec | Reduction km in seconds | 11_sectionals | NOW (E via pc_joiner) |
| 332 | pc_vitesse_relative | Speed relative to race average | 11_sectionals | NOW (E) |
| 333 | pc_ecart_redkm_gagnant | Reduction km gap to winner | 11_sectionals | NOW (E) |
| 334 | pc_ecart_temps_gagnant | Time gap to winner (ms) | 11_sectionals | NOW (E) |
| 335 | temps_avg_vitesse_5 | Average speed last 5 races | partants | NOW |
| 336 | temps_best_vitesse_10 | Best speed last 10 races | partants | NOW |
| 337 | temps_vitesse_trend | Speed trend (last 3 vs prev 3) | partants | NOW |
| 338 | temps_consistency_reduction | StDev of last 10 reduction_km values | partants | NOW |
| 339 | temps_speed_at_distance | Horse's average speed at this distance category | partants | NOW |
| 340 | duree_course_ms | Official race duration | courses | NOW |

**Subtotal: 20 features (14 existing + 6 new)**

---

## CATEGORY 19: MUSIQUE (Form String) FEATURES
> Source: partants (musique field)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 341 | musique_nb_courses | Total races in musique | partants | NOW (E via musique) |
| 342 | musique_nb_victoires | Wins in musique | partants | NOW (E) |
| 343 | musique_nb_places | Places in musique | partants | NOW (E) |
| 344 | musique_nb_dnf | DNFs in musique | partants | NOW (E) |
| 345 | musique_taux_victoire | Win rate from musique | partants | NOW (E) |
| 346 | musique_taux_place | Place rate from musique | partants | NOW (E) |
| 347 | musique_derniere_pos | Most recent position | partants | NOW (E) |
| 348 | musique_avant_derniere_pos | 2nd most recent position | partants | NOW (E) |
| 349 | musique_avg_pos_5 | Avg position last 5 (musique) | partants | NOW (E) |
| 350 | musique_avg_pos_10 | Avg position last 10 (musique) | partants | NOW (E) |
| 351 | musique_trend | Trend from musique | partants | NOW (E) |
| 352 | musique_nb_disciplines | Discipline diversity in musique | partants | NOW (E) |
| 353 | musique_pct_meme_discipline | % in current discipline | partants | NOW (E) |
| 354 | musique_consecutive_places | Consecutive top-3 streak | partants | NOW (E) |
| 355 | musique_consecutive_hors_places | Consecutive non-placed streak | partants | NOW (E) |
| 356 | musique_pct_dnf | % of DNF in musique | partants | NOW |
| 357 | musique_best_pos | Best position in musique | partants | NOW |
| 358 | musique_worst_pos | Worst position in musique | partants | NOW |
| 359 | musique_median_pos | Median position in musique | partants | NOW |
| 360 | musique_pos_variance | Variance of positions (consistency) | partants | NOW |

**Subtotal: 20 features (15 existing + 5 new)**

---

## CATEGORY 20: RACE CONDITIONS
> Source: courses (penetrometre, condition_age, condition_sexe, allocation)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 361 | cond_penetrometre_code | Encoded going condition | courses | NOW |
| 362 | cond_is_terrain_lourd | Boolean: heavy going | courses | NOW |
| 363 | cond_is_terrain_souple | Boolean: soft going | courses | NOW |
| 364 | cond_is_terrain_bon | Boolean: good going | courses | NOW |
| 365 | cond_condition_age_code | Encoded age condition (2yo only, 3yo only, etc) | courses | NOW |
| 366 | cond_is_age_restricted | Boolean: age-restricted race | courses | NOW |
| 367 | cond_condition_sexe_code | Encoded sex condition | courses | NOW |
| 368 | cond_is_sexe_restricted | Boolean: sex-restricted race | courses | NOW |
| 369 | cond_allocation_totale | Total prize money | courses | NOW |
| 370 | cond_allocation_1er | Prize for winner | courses | NOW |
| 371 | cond_allocation_log | log(allocation_totale) | courses | NOW |
| 372 | cond_allocation_1er_ratio | allocation_1er / allocation_totale | courses | NOW |
| 373 | cond_is_groupe | Boolean: Group/Listed race (from allocation level) | courses | NOW |
| 374 | cond_categorie_code | Encoded race category | courses | NOW |
| 375 | cond_distance_category | Sprint/mile/intermediaire/long/marathon | courses | NOW (E) |
| 376 | cond_discipline_code | Encoded discipline (numeric) | courses | NOW |
| 377 | cond_numero_course | Race number on card (1-8+) | courses | NOW |
| 378 | cond_is_premier_course | Boolean: first race of the day | courses | NOW |
| 379 | cond_is_derniere_course | Boolean: last race of the day | courses | NOW |
| 380 | horse_perf_terrain | Horse win rate on this going | partants+courses | NOW |

**Subtotal: 20 features (1 existing + 19 new)**

---

## CATEGORY 21: RECENCY / REST / FATIGUE
> Source: partants (dates)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 381 | jours_depuis_derniere | Days since last race | partants | NOW (E) |
| 382 | jours_depuis_derniere_log | log(days since last race) | partants | NOW |
| 383 | is_repos_court | Boolean: < 14 days rest | partants | NOW |
| 384 | is_repos_moyen | Boolean: 14-45 days rest | partants | NOW |
| 385 | is_repos_long | Boolean: > 45 days rest | partants | NOW |
| 386 | is_rentree | Boolean: first run after > 90 days off | partants | NOW |
| 387 | nb_courses_30j | Number of races in last 30 days | partants | NOW |
| 388 | nb_courses_14j | Number of races in last 14 days | partants | NOW |
| 389 | nb_courses_7j | Number of races in last 7 days | partants | NOW |
| 390 | is_surcharge_courses | Boolean: > 3 races in last 14 days (fatigue risk) | partants | NOW |
| 391 | jours_moyen_entre_courses | Average gap between races (from history) | partants | NOW |
| 392 | regularity_score | StDev of intervals between races (regularity) | partants | NOW |
| 393 | is_back_to_back | Boolean: raced within 3 days | partants | NOW |

**Subtotal: 13 features (1 existing + 12 new)**

---

## CATEGORY 22: CONSISTENCY FEATURES
> Source: partants (position_arrivee history)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 394 | consistency_stdev_5 | StDev of positions last 5 races | partants | NOW |
| 395 | consistency_stdev_10 | StDev of positions last 10 races | partants | NOW |
| 396 | consistency_stdev_20 | StDev of positions last 20 races | partants | NOW |
| 397 | consistency_score | 1 / (1 + stdev) -- higher = more consistent | partants | NOW |
| 398 | consistency_iqr_10 | IQR of positions last 10 races | partants | NOW |
| 399 | is_consistent | Boolean: stdev < 2.0 over last 10 | partants | NOW |
| 400 | is_erratic | Boolean: stdev > 4.0 over last 10 | partants | NOW |
| 401 | pct_top3_over_top10 | % of top-3 finishes in last 10 vs outside top 10 | partants | NOW |

**Subtotal: 8 features (0 existing + 8 new)**

---

## CATEGORY 23: CLASS CHANGES
> Source: partants + courses (allocation_totale comparison)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 402 | class_allocation_prev | Allocation of previous race | partants+courses | NOW |
| 403 | class_allocation_change | Current allocation - previous allocation | partants+courses | NOW |
| 404 | class_allocation_change_pct | % change in allocation | partants+courses | NOW |
| 405 | class_is_moving_up | Boolean: higher allocation than last race | partants+courses | NOW |
| 406 | class_is_moving_down | Boolean: lower allocation than last race | partants+courses | NOW |
| 407 | class_avg_allocation_5 | Average allocation of last 5 races | partants+courses | NOW |
| 408 | class_nb_partants_prev | Field size of previous race | partants | NOW |
| 409 | class_nb_partants_change | Field size change vs previous | partants | NOW |
| 410 | class_distance_change | Distance change vs previous race | partants | NOW |
| 411 | class_distance_change_pct | % distance change vs previous | partants | NOW |
| 412 | class_hippo_change | Boolean: different track vs last race | partants | NOW |
| 413 | class_discipline_change | Boolean: different discipline vs last race | partants | NOW |

**Subtotal: 12 features (0 existing + 12 new)**

---

## CATEGORY 24: RAPPORTS / BETTING MARKET HISTORICAL FEATURES
> Source: rapports_complets.json (124,287 courses)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 414 | rap_simple_gagnant | Simple gagnant payout (centimes) | rapports | NOW |
| 415 | rap_simple_place_avg | Average simple place payout | rapports | NOW |
| 416 | rap_couple_gagnant | Couple gagnant payout | rapports | NOW |
| 417 | rap_has_tierce | Boolean: tierce bet available | rapports | NOW |
| 418 | rap_has_quinte | Boolean: quinte bet available | rapports | NOW |
| 419 | rap_surprise_index | Actual payout vs expected (measure of upset) | rapports | NOW |
| 420 | rap_avg_surprise_hippo | Historical surprise rate at this track | rapports | NOW |
| 421 | rap_avg_favori_wins_hippo | Historical favorite win rate at this track | rapports | NOW |

**Subtotal: 8 features (0 existing + 8 new)**

---

## CATEGORY 25: PRECOMPUTED DATA (from scripts 05-11)
> Already partially joined, ensuring all are captured

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 422 | pc_cote_moyenne_course | Average odds in race | 07_cotes | NOW (E via pc_joiner) |
| 423 | pc_cote_mediane_course | Median odds in race | 07_cotes | NOW (E) |
| 424 | pc_ecart_cote_moyenne | Diff from race avg odds | 07_cotes | NOW (E) |
| 425 | ent_cheval_nb_courses_total | Total career races (aggregated) | 05_historique | NOW (E via entity_joiner) |
| 426 | ent_cheval_gains_total | Total career earnings (aggregated) | 05_historique | NOW (E) |
| 427 | ent_cheval_nb_disciplines | Discipline diversity | 05_historique | NOW (E) |
| 428 | ent_cheval_nb_hippodromes | Track diversity | 05_historique | NOW (E) |
| 429 | ent_cheval_anciennete_jours | Days since first race | 05_historique | NOW (E) |
| 430 | ent_cheval_nb_distances | Distance diversity | 05_historique | NOW (E) |
| 431 | ent_jockey_nb_montes_total | Jockey career rides | 06_historique | NOW (E) |
| 432 | ent_jockey_taux_victoire_global | Jockey career win rate | 06_historique | NOW (E) |
| 433 | ent_jockey_taux_place_global | Jockey career place rate | 06_historique | NOW (E) |
| 434 | ent_jockey_nb_chevaux_montes | Distinct horses ridden | 06_historique | NOW (E) |
| 435 | ent_jockey_gains_total | Jockey career earnings | 06_historique | NOW (E) |
| 436 | ent_entraineur_nb_partants_total | Trainer career starters | 06_historique | NOW (E) |
| 437 | ent_entraineur_taux_victoire_global | Trainer career win rate | 06_historique | NOW (E) |
| 438 | ent_entraineur_taux_place_global | Trainer career place rate | 06_historique | NOW (E) |
| 439 | ent_entraineur_nb_chevaux | Distinct horses trained | 06_historique | NOW (E) |
| 440 | ent_entraineur_gains_total | Trainer career earnings | 06_historique | NOW (E) |

**Subtotal: 19 features (19 existing)**

---

## CATEGORY 26: PERFORMANCES DETAILLEES (Rich Historical Data)
> Source: performances_detaillees.json (917,805 records with up to 5 past performances each)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 441 | perf_det_nb_known | Number of detailed past performances available | perf_det | NOW |
| 442 | perf_det_avg_allocation | Average allocation of past detailed races | perf_det | NOW |
| 443 | perf_det_max_allocation | Max allocation in past performances | perf_det | NOW |
| 444 | perf_det_pct_places | % placed in detailed performances | perf_det | NOW |
| 445 | perf_det_jockey_change_count | How many times jockey changed in last 5 | perf_det | NOW |
| 446 | perf_det_hippo_diversity | Number of distinct tracks in last 5 | perf_det | NOW |
| 447 | perf_det_dist_diversity | Number of distinct distances in last 5 | perf_det | NOW |
| 448 | perf_det_avg_nb_partants | Average field size in past 5 performances | perf_det | NOW |
| 449 | perf_det_best_reduction_km | Best reduction km from detailed perfs | perf_det | NOW |
| 450 | perf_det_avg_reduction_km | Avg reduction km from detailed perfs | perf_det | NOW |
| 451 | perf_det_same_hippo_count | How many of last 5 were at same track | perf_det | NOW |
| 452 | perf_det_same_distance_count | How many of last 5 at same distance | perf_det | NOW |
| 453 | perf_det_same_jockey_count | How many of last 5 with same jockey | perf_det | NOW |
| 454 | perf_det_interval_avg | Average days between past performances | perf_det | NOW |
| 455 | perf_det_allocation_trend | Trend in allocation (moving up/down class) | perf_det | NOW |

**Subtotal: 15 features (0 existing + 15 new)**

---

## CATEGORY 27: INTERACTION / CROSS FEATURES
> Derived from combining categories above

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 456 | inter_forme_x_cote | forme_victoire_5 * proba_implicite (value) | computed | NOW |
| 457 | inter_jockey_x_hippo | jockey_tv_365 * horse_tv_hippo | computed | NOW |
| 458 | inter_entraineur_x_jockey | entraineur_tv_90 * jockey_tv_90 | computed | NOW |
| 459 | inter_age_x_distance | age * distance (stamina interaction) | computed | NOW |
| 460 | inter_poids_x_distance | poids_porte * distance | computed | NOW |
| 461 | inter_forme_x_repos | forme_victoire_5 * (1/log(jours_repos)) | computed | NOW |
| 462 | inter_cote_x_field_strength | cote_relative * force_champ | computed | NOW |
| 463 | inter_meteo_x_surface | meteo_ground_moisture * track_type_piste | computed | NOW |
| 464 | inter_consistency_x_cote | consistency_score * rang_cote_pct | computed | NOW |
| 465 | inter_class_change_x_forme | class_allocation_change * forme_place_5 | computed | NOW |

**Subtotal: 10 features (0 existing + 10 new)**

---

## CATEGORY 28: TEMPORAL / CALENDAR FEATURES
> Source: date_reunion_iso

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 466 | cal_day_of_week | Day of week (0-6) | date | NOW |
| 467 | cal_is_weekend | Boolean: Saturday or Sunday | date | NOW |
| 468 | cal_month | Month (1-12) | date | NOW |
| 469 | cal_quarter | Quarter (1-4) | date | NOW |
| 470 | cal_is_holiday_period | Boolean: school holidays / major holiday | date | NOW |
| 471 | cal_jour_ouvre | Boolean: business day | date | NOW |
| 472 | cal_year_fraction | Day of year / 365 (cyclical) | date | NOW |
| 473 | cal_sin_month | sin(2*pi*month/12) for cyclical encoding | date | NOW |
| 474 | cal_cos_month | cos(2*pi*month/12) | date | NOW |

**Subtotal: 9 features (0 existing + 9 new)**

---

## CATEGORY 29: PROPRIETAIRE / ELEVEUR FEATURES
> Source: partants (proprietaire, eleveur)

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 475 | proprio_nb_partants | Owner total runners (rolling) | partants | NOW |
| 476 | proprio_taux_victoire | Owner win rate (rolling) | partants | NOW |
| 477 | proprio_taux_place | Owner place rate (rolling) | partants | NOW |
| 478 | proprio_nb_chevaux | Distinct horses owned (in dataset) | partants | NOW |
| 479 | proprio_is_ecurie | Boolean: owner name starts with "Ecurie" | partants | NOW |
| 480 | eleveur_nb_chevaux | Distinct horses bred by this breeder | partants | NOW |
| 481 | eleveur_taux_victoire | Breeder's horses win rate | partants | NOW |

**Subtotal: 7 features (0 existing + 7 new)**

---

## CATEGORY 30: RATING & ELO FEATURES
> Source: partants (computed)
> Builders: elo_rating_builder, bayesian_rating_builder, speed_figure_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 482 | elo_cheval | horse Elo at race time | partants | NOW |
| 483 | elo_jockey | jockey Elo at race time | partants | NOW |
| 484 | elo_entraineur | trainer Elo at race time | partants | NOW |
| 485 | elo_combined | weighted combination (60/25/15) | partants | NOW |
| 486 | elo_cheval_delta | change since horse's last race | partants | NOW |
| 487 | nb_races_elo | number of past races for horse (experience) | partants | NOW |
| 488 | bayes_horse_win_rate | Bayesian win rate for the horse (shrunk toward global avg) | partants | NOW |
| 489 | bayes_horse_place_rate | Bayesian place rate (top 3) | partants | NOW |
| 490 | bayes_jockey_win_rate | Bayesian win rate for jockey | partants | NOW |
| 491 | bayes_jockey_roi | Bayesian ROI for jockey (shrunk toward -15% global avg) | partants | NOW |
| 492 | bayes_trainer_win_rate | Bayesian win rate for trainer | partants | NOW |
| 493 | bayes_combo_jt_win | Jockey-trainer combination win rate | partants | NOW |
| 494 | bayes_confidence | 1 - (prior_weight / (prior_weight + nb_courses)) | partants | NOW |
| 495 | speed_figure | standardized speed rating for this run (0-200 scale) | partants | NOW |
| 496 | speed_figure_best | best speed figure in horse's career (before this race) | partants | NOW |
| 497 | speed_figure_avg | average of last 5 speed figures | partants | NOW |
| 498 | speed_figure_trend | linear regression slope of last 5 speed figures | partants | NOW |
| 499 | speed_figure_rank | rank of this horse's best figure among the field | partants | NOW |
| 500 | speed_vs_class | speed_figure_avg / average speed at this allocation level | partants | NOW |
| 501 | speed_consistency | standard deviation of last 5 speed figures | partants | NOW |

**Subtotal: 20 features (0 existing + 20 new)**

---

## CATEGORY 31: STREAK & MOMENTUM FEATURES
> Source: partants (historique)
> Builders: streak_builder, momentum_builder, recency_bias_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 502 | current_win_streak | consecutive wins heading into this race (0 if last was a loss) | partants | NOW |
| 503 | current_loss_streak | consecutive non-wins heading into this race (0 if last was a win) | partants | NOW |
| 504 | best_streak_career | longest win streak in the horse's career so far | partants | NOW |
| 505 | streak_vs_field_avg | horse's current win streak minus avg win streak of the field | partants | NOW |
| 506 | streak_at_hippodrome | current win streak at this specific hippodrome | partants | NOW |
| 507 | momentum_3 | average of last 3 position ranks (lower = better) | partants | NOW |
| 508 | momentum_5 | average of last 5 position ranks | partants | NOW |
| 509 | momentum_trend | linear regression slope of last 5 positions (negative = improving) | partants | NOW |
| 510 | regression_to_mean | how far current form is from career average (positive = above avg) | partants | NOW |
| 511 | form_volatility | standard deviation of last 5 positions | partants | NOW |
| 512 | weight_recent_3x | weighted avg position (last 3 races weighted 3x vs older) | partants | NOW |
| 513 | weight_recent_5x | weighted avg position (last 5 races weighted 5x vs older) | partants | NOW |
| 514 | exponential_decay_form | exponentially decayed avg position (lambda=0.3) | partants | NOW |
| 515 | time_weighted_elo | Elo rating with time-decayed K-factor | partants | NOW |
| 516 | recency_adjusted_speed | recent speed figure weighted higher than older ones | partants | NOW |

**Subtotal: 15 features (0 existing + 15 new)**

---

## CATEGORY 32: CAREER MILESTONE & LIFECYCLE
> Source: partants (historique)
> Builders: career_milestone_builder, career_stats_builder, age_lifecycle_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 517 | is_first_10_races | 1 if horse has fewer than 10 career starts, else 0 | partants | NOW |
| 518 | is_maiden | 1 if horse has never won before this race, else 0 | partants | NOW |
| 519 | days_since_first_race | calendar days since the horse's first recorded race | partants | NOW |
| 520 | is_career_best_class | 1 if this race's allocation is the highest the horse | partants | NOW |
| 521 | nb_courses_carriere | total career race count before this race | partants | NOW |
| 522 | gains_carriere_total | total career earnings before this race | partants | NOW |
| 523 | gains_par_course_moyen | gains_carriere_total / nb_courses_carriere | partants | NOW |
| 524 | win_rate_carriere | career win rate (wins / races) | partants | NOW |
| 525 | place_rate_carriere | career place rate (top 3 / races) | partants | NOW |
| 526 | best_allocation_won | highest allocation in a race the horse won | partants | NOW |
| 527 | peak_age_for_discipline | is horse at peak age for its discipline? | partants | NOW |
| 528 | races_since_peak | nb races since horse's best position | partants | NOW |
| 529 | career_phase | early(0-10 races)/mid(10-30)/veteran(30+) | partants | NOW |
| 530 | optimal_distance_age | does horse's current distance match | partants | NOW |

**Subtotal: 14 features (0 existing + 14 new)**

---

## CATEGORY 33: PREFERENCE & AFFINITY (Advanced)
> Source: partants (historique)
> Builders: distance_preference_builder, going_preference_builder, draw_bias_builder, hippodrome_expertise_builder, horse_profile_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 531 | dist_pref_win_rate | horse win rate at current distance category | partants | NOW |
| 532 | dist_pref_place_rate | horse place rate at current distance category | partants | NOW |
| 533 | dist_pref_advantage | dist_pref_win_rate / overall win rate (>1 = prefers this distance) | partants | NOW |
| 534 | dist_pref_nb_runs | number of past runs at this distance category | partants | NOW |
| 535 | dist_pref_best_category | distance category where horse has best win rate | partants | NOW |
| 536 | dist_match_score | 1.0 if current = best distance, 0.5 if adjacent, 0.0 otherwise | partants | NOW |
| 537 | going_pref_win_rate | horse's win rate on current terrain type | partants | NOW |
| 538 | going_pref_place_rate | horse's place rate on current terrain type | partants | NOW |
| 539 | going_pref_advantage | going_pref_win_rate / overall win rate (>1 = prefers this going) | partants | NOW |
| 540 | going_pref_nb_runs | number of past runs on this terrain type (confidence) | partants | NOW |
| 541 | going_pref_best_terrain | terrain with best win rate (encoded as int) | partants | NOW |
| 542 | going_match_score | 1.0 if current = best, 0.5 if adjacent, 0.0 if opposite | partants | NOW |
| 543 | draw_win_rate | historical win rate from this draw at hippo+distance | partants | NOW |
| 544 | draw_place_rate | historical place rate (top 3) from this draw | partants | NOW |
| 545 | draw_advantage | draw_win_rate / avg_win_rate for the hippo+distance (>1 = advantaged) | partants | NOW |
| 546 | draw_inside_bias | win rate of draws 1-4 vs 5+ at this hippo+distance | partants | NOW |
| 547 | draw_position_normalized | numPmu / nb_partants (0-1 scale, 0=inside, 1=outside) | partants | NOW |
| 548 | draw_nb_samples | number of historical races this draw stat is based on | partants | NOW |
| 549 | horse_hippo_win_rate | horse's win rate at this hippodrome | partants | NOW |
| 550 | horse_hippo_nb_runs | times horse has raced here | partants | NOW |
| 551 | jockey_hippo_win_rate | jockey's win rate at this hippodrome | partants | NOW |
| 552 | jockey_hippo_nb_runs | times jockey has ridden here | partants | NOW |
| 553 | hippo_specialist_score | (horse_hippo_win_rate + jockey_hippo_win_rate) / 2 | partants | NOW |
| 554 | hippo_first_time | 1 if horse has never raced at this hippodrome | partants | NOW |
| 555 | is_front_runner_by_history | float, % of past races where horse finished top-3 from low corde (<=4) | partants | NOW |
| 556 | preferred_distance_match | 1.0 if today's distance category == horse's best win-rate category, else 0 | partants | NOW |
| 557 | preferred_terrain_match | 1.0 if today's type_piste == horse's best win-rate terrain, else 0 | partants | NOW |
| 558 | career_roi | (total gains - total mise) / total mise  (mise = nb_courses * 1) | partants | NOW |
| 559 | career_avg_beaten_length | average beaten-length across career | partants | NOW |
| 560 | versatility_score | count of unique (distance_cat, type_piste, hippodrome) combos raced | partants | NOW |

**Subtotal: 30 features (0 existing + 30 new)**

---

## CATEGORY 34: JOCKEY & TRAINER FORM
> Source: partants (historique)
> Builders: jockey_form_builder, trainer_form_builder, jockey_horse_affinity_builder, jockey_trainer_deep_builder, trainer_horse_compatibility_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 561 | jockey_win_rate_30j | jockey win rate in last 30 days | partants | NOW |
| 562 | jockey_win_rate_90j | jockey win rate in last 90 days | partants | NOW |
| 563 | jockey_rides_30j | number of rides in last 30 days | partants | NOW |
| 564 | jockey_form_trend | win_rate_30j / win_rate_90j (>1 = improving) | partants | NOW |
| 565 | trainer_win_rate_30j | trainer win rate in last 30 days | partants | NOW |
| 566 | trainer_win_rate_90j | trainer win rate in last 90 days | partants | NOW |
| 567 | trainer_runners_30j | number of runners trained in last 30 days | partants | NOW |
| 568 | trainer_hot_streak | consecutive wins by trainer's horses (0 if last lost) | partants | NOW |
| 569 | trainer_roi_30j | ROI of backing all trainer's horses in last 30 days | partants | NOW |
| 570 | trainer_form_trend | win_rate_30j / win_rate_90j (>1 = improving form) | partants | NOW |
| 571 | jh_combo_win_rate | win rate of this jockey on this specific horse | partants | NOW |
| 572 | jh_combo_nb_rides | number of times this jockey has ridden this horse before | partants | NOW |
| 573 | jh_combo_place_rate | place rate (top 3) of this jockey-horse combo | partants | NOW |
| 574 | jh_is_regular | 1 if jockey has ridden this horse 3+ times, 0 otherwise | partants | NOW |
| 575 | jh_first_time | 1 if this is the first time this jockey rides this horse | partants | NOW |
| 576 | jt_combo_roi | ROI of betting on this jockey-trainer combo | partants | NOW |
| 577 | jt_combo_avg_position | average finish position of combo | partants | NOW |
| 578 | jockey_claiming_expert | jockey win rate in claiming races | partants | NOW |
| 579 | trainer_2yo_specialist | trainer win rate with 2-year-old horses | partants | NOW |
| 580 | trainer_horse_win_rate | win rate of this trainer with this specific horse | partants | NOW |
| 581 | trainer_horse_nb_races | number of times trainer has trained this horse before | partants | NOW |
| 582 | trainer_horse_roi | ROI backing this trainer-horse combo | partants | NOW |
| 583 | trainer_new_horse | 1 if trainer has this horse for first time | partants | NOW |
| 584 | trainer_speciality_match | 1 if trainer's best discipline = this race's discipline | partants | NOW |

**Subtotal: 24 features (0 existing + 24 new)**

---

## CATEGORY 35: MARKET & VALUE SIGNALS
> Source: partants (cotes, rapports)
> Builders: closing_line_value_builder, value_signal_builder, betting_edge_features_builder, public_money_builder, market_inefficiency_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 585 | closing_line_value | (1/cote_ref - 1/cote_fin) * cote_fin | partants | NOW |
| 586 | expected_value_brute | (estimated_prob * cote_fin) - 1 | partants | NOW |
| 587 | cote_movement_pct | (cote_fin - cote_ref) / cote_ref * 100 | partants | NOW |
| 588 | is_value_bet | 1 if expected_value_brute > 0 | partants | NOW |
| 589 | expected_value | model_implied_prob * cote_finale - 1 | partants | NOW |
| 590 | edge_vs_market | bayes_win_rate - (1 / cote_finale) | partants | NOW |
| 591 | is_value_bet | 1 if expected_value > 0, else 0 | partants | NOW |
| 592 | cote_vs_elo_gap | normalised gap between market-odds rank and Elo rank | partants | NOW |
| 593 | smart_money_signal | 1 if odds shortened > 15% AND Elo rank <= 3 | partants | NOW |
| 594 | kelly_fraction | (p*b - q) / b  where p = Bayesian win rate, | partants | NOW |
| 595 | edge_percentage | (p_model - p_market) / p_market * 100 | partants | NOW |
| 596 | edge_consistency | rolling hit-rate of positive-edge bets over | partants | NOW |
| 597 | kelly_bankroll_pct | kelly_fraction * edge_consistency | partants | NOW |
| 598 | market_prob | 1 / cote_finale  (implied market probability). | partants | NOW |
| 599 | model_prob | Bayesian shrinkage win-rate used as proxy. | partants | NOW |
| 600 | is_public_favorite | 1 if this horse has the lowest cote in the | partants | NOW |
| 601 | favorite_vs_form_gap | rank_by_cote minus rank_by_recent_winrate | partants | NOW |
| 602 | longshot_form_signal | 1 if horse has above-median recent win rate | partants | NOW |
| 603 | market_vs_elo_divergence | z-scored difference between implied | partants | NOW |
| 604 | mkt_odds_calibration_edge | actual_wr - implied_wr for this odds bucket | partants | NOW |
| 605 | mkt_odds_bucket | categorical odds bucket (0-8) | partants | NOW |
| 606 | mkt_hippo_fav_winrate | historical favourite win rate at this hippodrome | partants | NOW |
| 607 | mkt_hippo_predictability | how predictable this hippodrome is (0-1 scale) | partants | NOW |
| 608 | mkt_steam_odds_interaction | win rate for this drift_direction x odds_level combo | partants | NOW |
| 609 | mkt_drift_direction | -1 (steamer), 0 (stable), +1 (drifter) | partants | NOW |
| 610 | mkt_overbet_score | cumulative overbetting signal for this odds range | partants | NOW |
| 611 | mkt_field_adj_implied_prob | field-size-adjusted implied probability | partants | NOW |
| 612 | mkt_is_value_zone | 1 if odds are in a historically underbet range | partants | NOW |
| 613 | mkt_fav_in_field | 1 if horse is the favourite in its race | partants | NOW |
| 614 | mkt_fav_edge_vs_field_size | fav_win_rate_for_field_size - implied_probability | partants | NOW |
| 615 | mkt_longshot_bias_score | degree to which this horse is affected by longshot bias | partants | NOW |

**Subtotal: 31 features (0 existing + 31 new)**

---

## CATEGORY 36: FIELD & CLASS ANALYSIS
> Source: partants (computed)
> Builders: field_quality_builder, class_consistency_builder, course_context_builder, consistency_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 616 | field_elo_mean | Elo moyen du peloton | partants | NOW |
| 617 | field_elo_std | ecart-type Elo du peloton | partants | NOW |
| 618 | field_nb_outsiders | nb chevaux avec Elo < 1400 dans le peloton | partants | NOW |
| 619 | field_nb_class_horses | nb chevaux avec Elo > 1600 | partants | NOW |
| 620 | horse_elo_rank_in_field | rang du cheval par Elo dans ce peloton (1 = best) | partants | NOW |
| 621 | class_level | current race class level (1-6) | partants | NOW |
| 622 | class_win_rate_at_level | horse win rate at this specific class level | partants | NOW |
| 623 | class_drop | 1 if running at lower class than avg career class | partants | NOW |
| 624 | class_rise | 1 if running at higher class than avg career class | partants | NOW |
| 625 | class_consistency_score | CV of finish positions across all classes | partants | NOW |
| 626 | course_prestige | 1-5 scale (Handicap/other=1, Listed=2, Gr3=3, Gr2=4, Gr1=5) | partants | NOW |
| 627 | is_course_phare | 1 if highest-allocation course in its reunion | partants | NOW |
| 628 | type_paris_level | 1-5 based on bet types (simple=1 .. quinte=5) | partants | NOW |
| 629 | nb_partants_normalized | nombre_partants / 20 | partants | NOW |
| 630 | allocation_per_partant | allocation_totale / nb_partants | partants | NOW |
| 631 | is_handicap | 1 if handicap race | partants | NOW |
| 632 | position_std_5 | standard deviation of last 5 finishing positions | partants | NOW |
| 633 | position_cv | coefficient of variation of all career finishing | partants | NOW |
| 634 | best_worst_gap | best position - worst position over last 10 races | partants | NOW |
| 635 | dnf_rate | fraction of career races that resulted in DNF | partants | NOW |
| 636 | improvement_trend | OLS slope of last 10 finishing positions over time | partants | NOW |

**Subtotal: 21 features (0 existing + 21 new)**

---

## CATEGORY 37: DELTA & LAG FEATURES
> Source: partants (historique)
> Builders: delta_features_builder, lag_features_builder, ranking_features_builder, derived_features_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 637 | delta_cote | cote_finale - previous cote_finale | partants | NOW |
| 638 | delta_poids | poids_porte_kg - previous poids_porte_kg | partants | NOW |
| 639 | delta_distance | distance - previous distance | partants | NOW |
| 640 | delta_reduction_km | reduction_km_ms - previous reduction_km_ms | partants | NOW |
| 641 | same_hippodrome | 1 if same hippodrome as last race, else 0 | partants | NOW |
| 642 | same_jockey | 1 if same jockey as last race, else 0 | partants | NOW |
| 643 | same_discipline | 1 if same discipline as last race, else 0 | partants | NOW |
| 644 | days_between | days since previous race | partants | NOW |
| 645 | lag_position_1 | position course N-1 du meme cheval | partants | NOW |
| 646 | lag_position_2 | position course N-2 | partants | NOW |
| 647 | lag_position_3 | position course N-3 | partants | NOW |
| 648 | lag_cote_1 | cote finale course N-1 | partants | NOW |
| 649 | lag_days_since_last | jours depuis derniere course | partants | NOW |
| 650 | rank_age | rank of age within this race (1 = oldest) | partants | NOW |
| 651 | rank_gains | rank of gains_carriere within race (1 = highest) | partants | NOW |
| 652 | rank_nb_courses | rank of experience within race (1 = most experienced) | partants | NOW |
| 653 | rank_poids | rank of weight within race (1 = heaviest) | partants | NOW |
| 654 | percentile_cote | percentile of cote within race (0-1, lower = shorter price) | partants | NOW |
| 655 | field_homogeneity | std(cotes) / mean(cotes) in race (coefficient of variation) | partants | NOW |
| 656 | is_most_experienced | 1 if highest nb_courses in field, else 0 | partants | NOW |
| 657 | is_youngest | 1 if lowest age in field, else 0 | partants | NOW |
| 658 | class_drop_x_gains | spd_is_class_drop * gains_carriere_euros. | partants | NOW |
| 659 | cote_vs_form | cote_finale / seq_position_moy_5. | partants | NOW |
| 660 | inedit_x_experience | is_inedit * nb_courses_carriere. | partants | NOW |
| 661 | places_2_3_rate | (nb_places_2eme + nb_places_3eme) | partants | NOW |
| 662 | gains_per_race_rank | rank of (gains_carriere_euros | partants | NOW |

**Subtotal: 26 features (0 existing + 26 new)**

---

## CATEGORY 38: PATTERN DISCOVERY & CROSS
> Source: partants (computed)
> Builders: pattern_discovery_builder, cross_features_builder, combo_triple_builder, interaction_advanced_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 663 | pat_dow_winrate | historical win rate on same day-of-week (all runners) | partants | NOW |
| 664 | pat_career_stage | categorical 0-4 (debut/early/developing/mature/veteran) | partants | NOW |
| 665 | pat_career_stage_winrate | historical win rate for this career stage bucket | partants | NOW |
| 666 | pat_age_sex_dist_winrate | historical win rate for this age x sex x distance combo | partants | NOW |
| 667 | pat_field_fav_interaction | field-size-adjusted expected win rate based on odds | partants | NOW |
| 668 | pat_trainer_month_winrate | trainer's historical win rate in this calendar month | partants | NOW |
| 669 | pat_trainer_month_delta | trainer_month_wr - trainer_overall_wr (seasonal edge) | partants | NOW |
| 670 | pat_jockey_dist_terrain_wr | jockey win rate for this distance x terrain combo | partants | NOW |
| 671 | pat_jockey_dist_terrain_n | number of past races in this triple combo | partants | NOW |
| 672 | pat_career_wr_bucket | horse career win rate bucket (0-5 scale) | partants | NOW |
| 673 | pat_career_wr_next_signal | historical next-win rate for this career wr bucket | partants | NOW |
| 674 | pat_field_size_upset_rate | historical upset rate (fav loses) for this field size | partants | NOW |
| 675 | horse_meteo_win_rate | horse's win rate in similar weather (rain/dry) | partants | NOW |
| 676 | trainer_type_win_rate | trainer's win rate in this race type | partants | NOW |
| 677 | age_month_factor | performance factor for horse's age in this month | partants | NOW |
| 678 | sire_distance_terrain_score | sire's offspring win rate at distance+terrain | partants | NOW |
| 679 | same_course_history | horse's win rate at hippo+distance+discipline | partants | NOW |
| 680 | jockey_discipline_win_rate | jockey's win rate in this discipline | partants | NOW |
| 681 | jockey_distance_terrain_wr | jockey x distance_bucket x terrain win rate | partants | NOW |
| 682 | trainer_hippo_discipline_wr | trainer x hippodrome x discipline win rate | partants | NOW |
| 683 | age_sex_distance_wr | age x sex x distance_bucket win rate | partants | NOW |
| 684 | horse_season_wr | horse x season (quarter) win rate | partants | NOW |
| 685 | jockey_corde_wr | jockey x corde (rope/rail) win rate | partants | NOW |
| 686 | elo_x_cote | elo_cheval * (1 / cote_finale) | partants | NOW |
| 687 | forme_x_distance_pref | momentum_3 * dist_pref_advantage | partants | NOW |
| 688 | jockey_x_hippo_specialist | jockey_hippo_win_rate * horse_hippo_win_rate | partants | NOW |
| 689 | age_x_distance | age * distance_category_encoded | partants | NOW |
| 690 | fatigue_x_repos | fatigue_30j * jours_repos | partants | NOW |
| 691 | field_size_x_draw | nombre_partants * draw_position_normalized | partants | NOW |

**Subtotal: 29 features (0 existing + 29 new)**

---

## CATEGORY 39: ENCODING & ML-READY
> Source: partants (computed)
> Builders: target_encoding_builder, advanced_encoding_builder, ml_features_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 692 | te_hippodrome_win_rate | win rate historique par hippodrome (smoothed) | partants | NOW |
| 693 | te_jockey_win_rate | win rate par jockey (smoothed) | partants | NOW |
| 694 | te_trainer_win_rate | win rate par entraineur (smoothed) | partants | NOW |
| 695 | te_discipline_win_rate | win rate par discipline (smoothed) | partants | NOW |
| 696 | te_distance_cat_win_rate | win rate par categorie distance (smoothed) | partants | NOW |
| 697 | te_month_win_rate | win rate par mois (smoothed) | partants | NOW |
| 698 | freq_enc_hippodrome | nb past races at this hippodrome (all horses) | partants | NOW |
| 699 | freq_enc_jockey_global | total career races of this jockey | partants | NOW |
| 700 | freq_enc_trainer_global | total career races of this trainer | partants | NOW |
| 701 | woe_hippodrome | Weight of Evidence for hippodrome (log odds) | partants | NOW |
| 702 | woe_discipline | Weight of Evidence for discipline | partants | NOW |
| 703 | sin_month | sin(2*pi*month/12) cyclical month encoding | partants | NOW |
| 704 | cos_month | cos(2*pi*month/12) | partants | NOW |
| 705 | sin_dow | sin(2*pi*day_of_week/7) cyclical DOW | partants | NOW |
| 706 | cos_dow | cos(2*pi*day_of_week/7) | partants | NOW |
| 707 | sin_hour | sin(2*pi*hour/24) cyclical hour | partants | NOW |
| 708 | cos_hour | cos(2*pi*hour/24) | partants | NOW |
| 709 | position_encoding_seq | sinusoidal position encoding for sequence index | partants | NOW |
| 710 | advanced_combo_poly | polynomial combo of top features (cote * nb_partants) | partants | NOW |
| 711 | win_probability_implied | 1/cote normalised by field sum | partants | NOW |
| 712 | trainer_jockey_combo_roi | historical ROI of the trainer-jockey pair | partants | NOW |
| 713 | trainer_jockey_combo_wins | historical win count of the pair | partants | NOW |
| 714 | trainer_jockey_combo_runs | historical run count of the pair | partants | NOW |
| 715 | horse_improvement_rate | linear Elo slope over last 5 races | partants | NOW |
| 716 | distance_change_impact | current distance - last race distance (metres) | partants | NOW |
| 717 | weight_change_impact | current weight - last race weight (kg) | partants | NOW |
| 718 | days_since_win | calendar days since last win | partants | NOW |
| 719 | surface_switch_flag | 1 if surface differs from last race | partants | NOW |
| 720 | race_type_encoding_plat | 1 if galop plat | partants | NOW |
| 721 | race_type_encoding_trot | 1 if trot (attele or monte) | partants | NOW |
| 722 | field_size_bucket | 0=small(<=8), 1=medium(9-14), 2=large(>=15) | partants | NOW |
| 723 | upset_frequency_cond | historical upset rate for this hippodrome+discipline | partants | NOW |
| 724 | variance_historical | variance of finishing positions (last 10) | partants | NOW |
| 725 | entropy_field | Shannon entropy of implied probabilities in field | partants | NOW |
| 726 | frequency_enc_hippodrome | nb past races at this hippodrome (horse) | partants | NOW |
| 727 | frequency_enc_jockey | nb past races for this jockey (career) | partants | NOW |
| 728 | frequency_enc_trainer | nb past races for this trainer (career) | partants | NOW |
| 729 | discipline_is_trot | 1 if trot discipline | partants | NOW |
| 730 | discipline_is_galop | 1 if galop discipline | partants | NOW |

**Subtotal: 39 features (0 existing + 39 new)**

---

## CATEGORY 40: DEEP LEARNING FEATURES
> Source: partants (computed)
> Builders: deep_learning_features_builder, sequence_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 731 | attention_cheval_norm | normalised horse ID hash (0-1) | partants | NOW |
| 732 | attention_jockey_norm | normalised jockey ID hash (0-1) | partants | NOW |
| 733 | attention_course_norm | normalised course context hash (0-1) | partants | NOW |
| 734 | tft_is_static_pedigree | 1 if horse has pedigree data (static feature flag) | partants | NOW |
| 735 | tft_is_static_hippo | 1 if hippodrome is known (static feature flag) | partants | NOW |
| 736 | tft_is_dynamic_form | 1 (always; form is dynamic by nature) | partants | NOW |
| 737 | tft_is_dynamic_odds | 1 if odds data available | partants | NOW |
| 738 | tabnet_group_form | feature group ID for form features (0) | partants | NOW |
| 739 | tabnet_group_pedigree | feature group ID for pedigree features (1) | partants | NOW |
| 740 | tabnet_group_odds | feature group ID for odds features (2) | partants | NOW |
| 741 | tabnet_group_context | feature group ID for context features (3) | partants | NOW |
| 742 | has_full_sequence | 1 if horse has >=5 prior races for sequence models | partants | NOW |
| 743 | seq_positions_10 | list of last 10 positions (padded with -1) | partants | NOW |
| 744 | seq_cotes_10 | list of last 10 cotes finales (padded with -1) | partants | NOW |
| 745 | seq_distances_10 | list of last 10 distances (padded with -1) | partants | NOW |
| 746 | seq_jours_entre_10 | list of last 10 inter-race gaps in days (padded with -1) | partants | NOW |
| 747 | seq_is_winner_10 | list of last 10 win flags 0/1 (padded with -1) | partants | NOW |
| 748 | seq_length | actual number of past races (0-10) | partants | NOW |

**Subtotal: 18 features (0 existing + 18 new)**

---

## CATEGORY 41: TEMPORAL & CALENDAR (Advanced)
> Source: partants (dates)
> Builders: temporal_advanced_features, seasonality_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 749 | jour_semaine | day of week (0=Mon, 6=Sun) | partants | NOW |
| 750 | mois | month (1-12) | partants | NOW |
| 751 | saison | season string (printemps, ete, automne, hiver) | partants | NOW |
| 752 | is_weekend | bool (samedi/dimanche) | partants | NOW |
| 753 | is_quinte | bool (course quinte du jour) | partants | NOW |
| 754 | heure_course | hour of race (int, 0-23) | partants | NOW |
| 755 | position_dans_reunion | which race number in the meeting (1, 2, ...) | partants | NOW |
| 756 | horse_season_win_rate | horse's win rate in current season (spring/summer/autumn/winter) | partants | NOW |
| 757 | horse_best_season | which season horse performs best in (1=spring..4=winter) | partants | NOW |
| 758 | season_match_score | 1.0 if current=best, 0.5 adjacent, 0.0 opposite | partants | NOW |
| 759 | hippo_season_bias | this hippodrome's win-rate deviation in current season | partants | NOW |
| 760 | discipline_seasonal_trend | avg field size growth rate for this discipline in current season | partants | NOW |

**Subtotal: 12 features (0 existing + 12 new)**

---

## CATEGORY 42: EQUIPMENT & CONDITION CHANGES
> Source: partants (equipement, conditions)
> Builders: equipment_impact_builder, first_time_events_builder, signal_features_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 761 | oeilleres_change_impact | win rate WITH oeilleres minus win rate WITHOUT | partants | NOW |
| 762 | deferre_change_impact | win rate with current deferre config minus | partants | NOW |
| 763 | equipment_stability_score | fraction of recent races (last 5) where the | partants | NOW |
| 764 | first_time_psf | 1 if horse has never raced on PSF before | partants | NOW |
| 765 | first_time_distance_cat | 1 if horse has never raced at this distance category | partants | NOW |
| 766 | first_time_hippodrome | 1 if horse has never raced at this hippodrome | partants | NOW |
| 767 | first_time_oeilleres | 1 if horse has equipment change (oeilleres or deferre) | partants | NOW |
| 768 | nb_firsts_count | count of how many "firsts" this run has (0-4) | partants | NOW |
| 769 | jockey_upgrade | 1 if current jockey has higher win rate than previous jockey | partants | NOW |
| 770 | trainer_change_recent | 1 if trainer changed in last 90 days | partants | NOW |
| 771 | class_drop_after_win | 1 if horse won last race and is now in lower class | partants | NOW |
| 772 | returning_from_break | 1 if >90 days since last race | partants | NOW |
| 773 | equipment_change | 1 if any equipment field differs from last run | partants | NOW |

**Subtotal: 13 features (0 existing + 13 new)**

---

## CATEGORY 43: TRACK BIAS & WEATHER (Deep)
> Source: partants (hippo, meteo)
> Builders: track_bias_deep_builder, weather_interaction_builder, race_rhythm_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 774 | corde_advantage_today | win-rate of this corde position at this hippo vs expected (obs - exp) | partants | NOW |
| 775 | inside_vs_outside_winrate | win-rate(inner cordes 1-4) - win-rate(outer cordes 9+) at this hippo | partants | NOW |
| 776 | rail_position_bias | correlation proxy -- % of inner-corde winners at this hippo, past 365d | partants | NOW |
| 777 | track_speed_vs_average | hippo avg speed / global avg speed (ratio), past 365d | partants | NOW |
| 778 | hippodrome_unpredictability | 1 - (favourite win-rate at this hippo), past 365d | partants | NOW |
| 779 | horse_rain_win_rate | horse's win rate when met_impact_meteo_score > 0.5 | partants | NOW |
| 780 | horse_dry_win_rate | horse's win rate when met_impact_meteo_score <= 0.5 | partants | NOW |
| 781 | rain_advantage | rain_win_rate - dry_win_rate (positive = prefers rain) | partants | NOW |
| 782 | terrain_lourd_specialist | horse's win rate on 'lourd' terrain | partants | NOW |
| 783 | wind_sensitivity | std of positions vs wind speed (high = sensitive) | partants | NOW |
| 784 | temperature_optimum | distance from horse's best-performing temperature range | partants | NOW |
| 785 | nb_favoris_battus_hippo | how often favorites (lowest odds) lose at this hippodrome | partants | NOW |
| 786 | avg_winning_cote_hippo | average winning odds at hippodrome (high = unpredictable) | partants | NOW |
| 787 | discipline_predictability | 1/entropy of winner distribution in discipline | partants | NOW |
| 788 | course_surprise_index | how often the winner was >10.0 odds at hippo+distance | partants | NOW |
| 789 | repeat_winner_rate | how often same horse wins back-to-back at same hippodrome | partants | NOW |

**Subtotal: 16 features (0 existing + 16 new)**

---

## CATEGORY 44: EXPERIENCE & DATA QUALITY
> Source: partants (computed)
> Builders: experience_depth_builder, freshness_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 790 | hippo_experience | nb past races at this hippodrome | partants | NOW |
| 791 | distance_experience | nb past races at this distance category | partants | NOW |
| 792 | terrain_experience | nb past races on this terrain type | partants | NOW |
| 793 | discipline_experience | nb past races in this discipline | partants | NOW |
| 794 | total_variety_score | nb unique (hippo, distance, terrain, discipline) combos in career | partants | NOW |
| 795 | data_freshness_score | days since last record for this horse (before current race) | partants | NOW |
| 796 | form_sample_size | nb races in last 90 days (confidence measure) | partants | NOW |
| 797 | odds_available | 1 if cote_finale is present, 0 if missing | partants | NOW |
| 798 | pedigree_available | 1 if pere+mere known, 0 if missing | partants | NOW |
| 799 | data_completeness | fraction of key fields filled for this record (0.0-1.0) | partants | NOW |

**Subtotal: 10 features (0 existing + 10 new)**

---

## CATEGORY 45: STATISTICAL & QUANTILE
> Source: partants (computed)
> Builders: quantile_features_builder, survival_features_builder, uncertainty_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 800 | position_q10 | 10th percentile of historical positions (best case) | partants | NOW |
| 801 | position_q50 | median historical position | partants | NOW |
| 802 | position_q90 | 90th percentile (worst case) | partants | NOW |
| 803 | earnings_q75 | 75th percentile of earnings per race | partants | NOW |
| 804 | cote_q25 | 25th percentile of cotes (when horse is well-backed) | partants | NOW |
| 805 | hazard_rate | historical probability of DNF (did not finish) | partants | NOW |
| 806 | top3_survival_rate | cumulative % of races finishing in top 3 | partants | NOW |
| 807 | career_longevity_days | days between first and last observed race | partants | NOW |
| 808 | races_per_year | average races per year over career | partants | NOW |
| 809 | career_trend | career-wide improving/declining trend (OLS slope | partants | NOW |
| 810 | prediction_variance | variance of horse's recent positions (last 10) | partants | NOW |
| 811 | result_entropy | Shannon entropy of position distribution | partants | NOW |
| 812 | upset_potential | how often this horse beats the favorite | partants | NOW |
| 813 | consistency_vs_class | std_positions / mean_position (CV) | partants | NOW |
| 814 | form_uncertainty | max - min position in last 5 (range) | partants | NOW |

**Subtotal: 15 features (0 existing + 15 new)**

---

## CATEGORY 46: ROLLING ADVANCED & OUTSIDER
> Source: partants (computed)
> Builders: rolling_advanced_builder, outsider_profile_builder, pace_scenario_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 815 | rolling_earnings_5 | sum of gains from last 5 races | partants | NOW |
| 816 | rolling_win_rate_10 | win rate over last 10 races | partants | NOW |
| 817 | rolling_place_rate_10 | place rate (top 3) over last 10 races | partants | NOW |
| 818 | rolling_avg_cote_5 | average cote_finale over last 5 races | partants | NOW |
| 819 | rolling_avg_field_size_5 | average nb_partants over last 5 races | partants | NOW |
| 820 | rolling_distance_variety | nb unique distance categories in last 10 races | partants | NOW |
| 821 | anomaly_score | composite z-score deviation across cote, Elo, | partants | NOW |
| 822 | upset_freq_hippodrome | historical upset rate at this hippodrome | partants | NOW |
| 823 | upset_freq_discipline | historical upset rate for this discipline. | partants | NOW |
| 824 | upset_freq_distance | historical upset rate at this distance bucket. | partants | NOW |
| 825 | is_profile_outlier | 1 if anomaly_score > 2.0 (2 sigma), else 0. | partants | NOW |
| 826 | longshot_upset_score | anomaly_score * upset_freq for the conditions. | partants | NOW |
| 827 | pace_early_leader_prob | probability this horse leads early | partants | NOW |
| 828 | pace_finisher_type | 1=front-runner, 2=stalker, 3=closer | partants | NOW |
| 829 | pace_collapse_risk | fraction of field that are front-runners (type=1); | partants | NOW |
| 830 | pace_advantage | 1 if horse's type matches the favourable scenario | partants | NOW |

**Subtotal: 16 features (0 existing + 16 new)**

---

## CATEGORY 47: RAPPORT & GRAPH FEATURES
> Source: rapports, partants
> Builders: rapport_features_builder, graph_features_builder, unused_fields_builder

| # | Feature Name | Formula/Logic | Source | Status |
|---|-------------|---------------|--------|--------|
| 831 | avg_gagnant_dividend_hippo | running avg of rap_rapport_simple_gagnant | partants | NOW |
| 832 | std_gagnant_dividend_hippo | running stdev of gagnant dividends at hippo. | partants | NOW |
| 833 | avg_couple_dividend_hippo | running avg of rap_rapport_couple_gagnant | partants | NOW |
| 834 | horse_avg_winning_dividend | running avg of gagnant dividends when this | partants | NOW |
| 835 | horse_upset_dividend_avg | running avg of gagnant dividends when this | partants | NOW |
| 836 | dividend_vs_cote_ratio | actual gagnant dividend / cote_finale | partants | NOW |
| 837 | market_overround_actual | sum of implied probs from place dividends | partants | NOW |
| 838 | is_historically_undervalued | 1 if horse's historical avg winning dividend | partants | NOW |
| 839 | graph_jockey_centrality | nb unique horses this jockey has ridden (PageRank proxy) | partants | NOW |
| 840 | graph_trainer_centrality | nb unique horses this trainer trains | partants | NOW |
| 841 | graph_horse_connectivity | nb unique jockeys who have ridden this horse | partants | NOW |
| 842 | graph_jt_combo_strength | jockey-trainer pair strength (courses together / total) | partants | NOW |
| 843 | graph_hippo_diversity | nb different hippodromes this horse has raced at | partants | NOW |
| 844 | network_strength | gnn_cheval_degree * gnn_jockey_nb_chevaux | partants | NOW |
| 845 | history_coverage_rate | seq_nb_courses_historique / nb_courses_carriere | partants | NOW |
| 846 | minor_place_rate | (nb_places_2eme + nb_places_3eme) / nb_courses_carriere | partants | NOW |
| 847 | genetic_fitness | ped_inbreeding_score * ped_stamina_index | partants | NOW |
| 848 | field_compression | spd_field_strength_avg / spd_field_strength_max | partants | NOW |
| 849 | track_inside_bias | spd_bias_interieur (direct passthrough, never used) | partants | NOW |
| 850 | weight_advantage | poids_base_kg - poids_monte_kg (positive = lighter ride) | partants | NOW |
| 851 | implied_market_prob | 1 / (rap_rapport_simple_gagnant / 100) | partants | NOW |
| 852 | terrain_weather_combo | cnd_cond_type_terrain * met_impact_meteo_score | partants | NOW |
| 853 | age_distance_interaction | pgr_age_ans * distance / 1000 | partants | NOW |

**Subtotal: 23 features (0 existing + 23 new)**

---

## GRAND TOTAL SUMMARY

| Category | # Features | Existing | New |
|----------|-----------|----------|-----|
| 1. Horse Form (Rolling Windows) | 41 | 14 | 27 |
| 2. Horse Profile | 24 | 14 | 10 |
| 3. Jockey Stats | 26 | 13 | 13 |
| 4. Trainer Stats | 22 | 13 | 9 |
| 5. Jockey-Trainer Combo | 6 | 0 | 6 |
| 6. Jockey-Horse Combo | 6 | 2 | 4 |
| 7. Horse-Hippodrome Affinity | 7 | 1 | 6 |
| 8. Horse-Distance Affinity | 11 | 1 | 10 |
| 9. Horse-Discipline Affinity | 8 | 1 | 7 |
| 10. Field Strength | 32 | 8 | 24 |
| 11. Odds/Market | 19 | 9 | 10 |
| 12. Pedigree | 27 | 13 | 14 |
| 13. Meteo | 25 | 15 | 10 |
| 14. Track/Hippodrome | 20 | 6 | 14 |
| 15. Pace/Tempo | 14 | 11 | 3 |
| 16. Equipment | 15 | 11 | 4 |
| 17. Weight/Handicap | 17 | 11 | 6 |
| 18. Time/Performance | 20 | 14 | 6 |
| 19. Musique | 20 | 15 | 5 |
| 20. Race Conditions | 20 | 1 | 19 |
| 21. Recency/Rest | 13 | 1 | 12 |
| 22. Consistency | 8 | 0 | 8 |
| 23. Class Changes | 12 | 0 | 12 |
| 24. Rapports/Betting | 8 | 0 | 8 |
| 25. Precomputed Data | 19 | 19 | 0 |
| 26. Performances Detaillees | 15 | 0 | 15 |
| 27. Interaction Features | 10 | 0 | 10 |
| 28. Calendar/Temporal | 9 | 0 | 9 |
| 29. Proprietaire/Eleveur | 7 | 0 | 7 |
| 30. Rating & Elo | 20 | 0 | 20 |
| 31. Streak & Momentum | 15 | 0 | 15 |
| 32. Career Milestone & Lifecycle | 14 | 0 | 14 |
| 33. Preference & Affinity (Adv) | 30 | 0 | 30 |
| 34. Jockey & Trainer Form | 24 | 0 | 24 |
| 35. Market & Value Signals | 31 | 0 | 31 |
| 36. Field & Class Analysis | 21 | 0 | 21 |
| 37. Delta & Lag Features | 26 | 0 | 26 |
| 38. Pattern Discovery & Cross | 29 | 0 | 29 |
| 39. Encoding & ML-Ready | 39 | 0 | 39 |
| 40. Deep Learning Features | 18 | 0 | 18 |
| 41. Temporal & Calendar (Adv) | 12 | 0 | 12 |
| 42. Equipment & Condition Changes | 13 | 0 | 13 |
| 43. Track Bias & Weather (Deep) | 16 | 0 | 16 |
| 44. Experience & Data Quality | 10 | 0 | 10 |
| 45. Statistical & Quantile | 15 | 0 | 15 |
| 46. Rolling Advanced & Outsider | 16 | 0 | 16 |
| 47. Rapport & Graph Features | 23 | 0 | 23 |
| **TOTAL** | **853** | **193** | **660** |

---

## BUILDABILITY ASSESSMENT

### Can be built NOW with available data: **~820 features**
Almost all features can be built from existing data sources. The main limitations are:
- `cote_mouvement` / `cote_reference` fields have partial fill rate
- Meteo data covers ~31,778 courses out of ~257,806 (12%)
- Performances detaillees covers 917,805 partants out of 2,930,290 (31%)
- Pedigree depth (pedigrees_pq) covers 24,484 horses
- Precomputed entity files may not cover all entities

### Features that need more data: ~31
- Weather conditions for older races (meteo gaps)
- Sectional timing data (only for tracked courses)
- Video/visual features (not available)
- Real-time odds movement (only final + reference available)
- Detailed race fractions / splits (not available)

---

## IMPLEMENTATION PRIORITY

All 95 feature builders are implemented and registered in run_pipeline.py Phase 7.
853 cataloged features across 47 categories, produced by 95 builders.
All builders run in parallel after mega_merge (Phase 6) completes.
