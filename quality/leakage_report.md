# Leakage Prevention Report

Generated: 2026-03-24 20:24:39  
Runtime: 498.8s

**Status: FAIL** -- 51 issue(s) found.

## 1. Temporal Leakage

**48 violation(s) detected.**

| Field | Partant UID | Race Date | Value | Reason |
|-------|-------------|-----------|-------|--------|
| gnn_duo_jockey_entraineur_nb | N/A | N/A | N/A | Column name contains future-data keyword |
| aff_ct_last_result | N/A | N/A | N/A | Column name contains future-data keyword |
| ent_jockey_taux_place | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_temps_moy_20 | N/A | N/A | N/A | Column name contains future-data keyword |
| jockey_driver | N/A | N/A | N/A | Column name contains future-data keyword |
| gnn_duo_cheval_jockey_place_rate | N/A | N/A | N/A | Column name contains future-data keyword |
| commentaire_apres_course | N/A | N/A | N/A | Column name contains future-data keyword |
| jockey_taux_x_cheval_taux | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_red_moy_10 | N/A | N/A | N/A | Column name contains future-data keyword |
| ped_sire_precocity_idx | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_temps_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| combo_jockey_hippo_nb | N/A | N/A | N/A | Column name contains future-data keyword |
| gnn_duo_cheval_jockey_nb | N/A | N/A | N/A | Column name contains future-data keyword |
| seq_position_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| seq_position_moy_10 | N/A | N/A | N/A | Column name contains future-data keyword |
| combo_jockey_change | N/A | N/A | N/A | Column name contains future-data keyword |
| gnn_duo_jockey_entraineur_win_rate | N/A | N/A | N/A | Column name contains future-data keyword |
| seq_red_km_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| ent_jockey_taux_victoire | N/A | N/A | N/A | Column name contains future-data keyword |
| ent_jockey_gains_total | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_gains_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| jockey_taux_victoire_365j | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_pos_moy_10 | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_red_moy_20 | N/A | N/A | N/A | Column name contains future-data keyword |
| combo_jockey_hippo_taux_vic | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_pos_moy_20 | N/A | N/A | N/A | Column name contains future-data keyword |
| jockey_driver_change | N/A | N/A | N/A | Column name contains future-data keyword |
| gnn_duo_cheval_jockey_win_rate | N/A | N/A | N/A | Column name contains future-data keyword |
| mkt_sharp_money_indicator | N/A | N/A | N/A | Column name contains future-data keyword |
| aff_eh_last_result | N/A | N/A | N/A | Column name contains future-data keyword |
| spd_class_rating_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_gains_moy_20 | N/A | N/A | N/A | Column name contains future-data keyword |
| aff_ch_last_result | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_pos_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| aff_cd_last_result | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_red_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| gnn_jockey_nb_chevaux | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_gains_moy_10 | N/A | N/A | N/A | Column name contains future-data keyword |
| perf_temps_moy_10 | N/A | N/A | N/A | Column name contains future-data keyword |
| spd_speed_figure_moy_5 | N/A | N/A | N/A | Column name contains future-data keyword |
| pgr_date_deces | fd8af14e51c7c829 | 2013-05-23 | 2021-09-09 | Feature date > race date (future data) |
| pgr_date_deces | 78a31bbd2a8ce9db | 2019-12-09 | 2021-10-25 | Feature date > race date (future data) |
| pgr_sire_date_naissance | 947e2586f0404268 | 2015-04-04 | 2017-03-11 | Feature date > race date (future data) |
| pgr_date_naissance | 947e2586f0404268 | 2015-04-04 | 2017-03-11 | Feature date > race date (future data) |
| pgr_sire_date_naissance | ff69a0bf389bc44c | 2014-11-05 | 2020-03-06 | Feature date > race date (future data) |
| pgr_date_naissance | ff69a0bf389bc44c | 2014-11-05 | 2020-03-06 | Feature date > race date (future data) |
| pgr_date_deces | a354e29114bc7f68 | 2015-08-15 | 2019-09-05 | Feature date > race date (future data) |
| pgr_date_deces | d6b09d97e7536e12 | 2013-12-08 | 2019-11-07 | Feature date > race date (future data) |

## 2. Target Leakage

**3 forbidden field(s) found in feature outputs.**

| File | Field | Reason |
|------|-------|--------|
| output\features\features_matrix.jsonl | position_arrivee | Direct result column present in feature output |
| output\features\features_matrix.jsonl | is_gagnant | Direct result column present in feature output |
| output\features\features_matrix.jsonl | cote_finale | Direct result column present in feature output |

## 3. Train/Test Date Contamination

No date overlap detected between train and test splits.

- Train date range: 2013-08-17 .. 2023-12-31
- Test date range:   .. 2026-03-12

## 4. Feature-Label Correlation

Analysed 0 numeric features over 0 sampled records.

No suspiciously high correlations detected.
