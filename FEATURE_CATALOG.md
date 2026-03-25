# FEATURE CATALOG - Horse Racing Prediction Model

> Updated 2026-03-25
> Cataloged features: 510 (across 73 feature builders)
> Estimated total output columns: ~836 (including windowed variants)
> Target: 350-500 features -- EXCEEDED
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
| **TOTAL** | **481** | **193** | **288** |

---

## BUILDABILITY ASSESSMENT

### Can be built NOW with available data: **~450 features**
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

### Phase 1 - Quick Wins (estimated +100 features, ~2h work)
Add rolling windows (3, 10, 20) to existing builders, add missing rate calculations,
calendar features, consistency features, class changes, recency features.

### Phase 2 - New Builders (estimated +100 features, ~4h work)
Jockey-trainer combo, proprietaire/eleveur, race conditions, rapports features,
performances detaillees builder, interaction features.

### Phase 3 - Enrichments (estimated +88 features, ~4h work)
Horse-hippodrome/distance/discipline affinities (expanded), pedigree depth,
track features from hippodromes_db, meteo cross-features, market value features.
