# Architecture du Pipeline

Schemas visuels du pipeline de donnees hippiques.

---

## 1. Flux de donnees principal

```
 COLLECTE (40+ scrapers)          NETTOYAGE            MERGE              FEATURES           SORTIE
 ========================     ===============     ===============     ===============     ===========

 +------------------+
 | PMU API          |--+
 | (01-11, 21-23,   |  |
 |  27-28, 38-40)   |  |
 +------------------+  |
                       |      +-----------+      +---------------+
 +------------------+  +----->|           |      |  Domain       |
 | Pedigree         |--+----->| Audit     |      |  Merges (x10) |
 | (12, 14, 36)     |  |     | Nettoyage |----->|  Phase 5      |
 +------------------+  |     | Dedup     |      |               |
                       |     | Comblage  |      | courses_m     |
 +------------------+  |     |           |      | pedigree_m    |     +-----------+
 | Meteo            |--+     | Phase 1-4 |      | rapports_m    |     | Feature   |     +----------+
 | (00, 13, 35)     |  |     +-----------+      | meteo_m       |---->| Builders  |---->| features |
 +------------------+  |                        | equipements_m |     | (53 scripts|    | _matrix  |
                       |                        | marche_m      |     |  Phase 7)  |    | .jsonl   |
 +------------------+  |                        | performances_m|     +-----------+     +----------+
 | External Turf    |--+                        | stats_ext_m   |           |                |
 | (24-26, 31-34)   |  |                        +-------+-------+           |                |
 +------------------+  |                                |                    v                v
                       |                                v             +-----------+     +----------+
 +------------------+  |                        +-------+-------+     | master_   |     | training |
 | Racing Post (37) |--+                        |  Mega Merge   |     | feature_  |     | _labels  |
 +------------------+  |                        |  Phase 6      |     | builder   |     | .jsonl   |
                       |                        |               |     | Phase 8   |     +----------+
 +------------------+  |                        | partants_     |     +-----------+
 | Institutional    |--+                        | master.jsonl  |           |
 | (15-20, SIRE)    |                           | (~17 GB)      |           v
 +------------------+                           +---------------+     +-----------+
                                                                      | Quality   |
 +------------------+                                                 | Tests     |
 | Exchanges        |                                                 | Phase 9   |
 | (30 Smarkets,    |                                                 +-----------+
 |  34 Unibet)      |
 +------------------+
```

---

## 2. Dependances entre phases (DAG simplifie)

```
Phase 1         Phase 2         Phase 3         Phase 4
+-------+       +---------+     +-------+       +---------+
| Audit |------>|Nettoyage|---->| Dedup |----->>| Comblage|
+-------+       +---------+     +-------+       +---------+
                                                     |
                     +-------------------------------+
                     |
                     v
Phase 5: Merges (parallel)
+-------------------+  +-------------------+  +---------------------+
| merge_courses_m   |  | merge_pedigree_m  |  | merge_rapports_21_38|
+-------------------+  +-------------------+  +---------+-----------+
         |                      |                       |
         |                      |                       v
         |                      |             +-------------------+
         |                      |             | merge_rapports_m  |
         |                      |             +-------------------+
         |                      |                       |
+-------------------+  +-------------------+            |
| merge_equipements |  | merge_meteo       |            |
+-------------------+  +--------+----------+            |
         |                      |                       |
         |                      v                       |
         |             +-------------------+            |
         |             | merge_meteo_m     |            |
         |             +-------------------+            |
         |                      |                       |
+-------------------+  +-------------------+            |
| merge_marche_m    |  | merge_perfs_m     |            |
+-------------------+  +-------------------+            |
         |                      |                       |
+-------------------+           |                       |
| merge_stats_ext_m |           |                       |
+-------------------+           |                       |
         |                      |                       |
         +----------+-----------+-----------+-----------+
                    |
                    v
Phase 6:    +---------------+
            |  Mega Merge   |
            | (all 8 masters|
            |  -> 1 file)   |
            +-------+-------+
                    |
    +---------------+---------------+
    |               |               |
    v               v               v
Phase 7: Feature Builders (53 scripts, parallel)
+-------------+ +-------------+ +-------------+ +-------------+
| fb_cheval   | | fb_jockey   | | fb_pedigree | | fb_meteo    |  ...
| _features   | | _features   | | _features   | | _features   |
+-------------+ +-------------+ +-------------+ +-------------+
| fb_course   | | fb_marche   | | fb_musique  | | fb_equipement|
| _features   | | _features   | | _features   | | _features   |
+-------------+ +-------------+ +-------------+ +-------------+
| feat_histo  | | feat_jockey | | feat_pedigree| | feat_tempo  |
| rique       | |             | |              | | rel         |
+-------------+ +-------------+ +-------------+ +-------------+
| calc_41_seq | | calc_42_rp  | | calc_43_meteo| | calc_44_ped |
+-------------+ +-------------+ +-------------+ +-------------+
    |               |               |               |
    +---------------+---------------+---------------+
                    |
                    v
Phase 8:    +-------------------+
            | master_feature_   |
            | builder           |
            | (consolidate all) |
            +--------+----------+
                     |
                     v
Phase 9:    +-------------------+
            | Quality Tests     |
            | (7 test suites)   |
            +-------------------+
```

---

## 3. Couverture par source et annee

Tableau montrant quelles sources couvrent quelles annees.
Legende : `####` = couverture complete, `....` = couverture partielle, `    ` = pas de donnees.

```
Source                    2004  2008  2013  2014  2015  2016  2017  2018  2019  2020  2021  2022  2023  2024  2025  2026
--------------------------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
PMU Courses/Partants (02) |     |     |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
PMU Resultats (04)        |     |     |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
PMU Hist. Chevaux (05)    |     |     |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
PMU Cotes Marche (07)     |     |     |.... |.... |.... |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
PMU Equipements (09)      |     |     |.... |.... |.... |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
PMU Sectionals (11)       |     |     |.... |.... |.... |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
PMU Rapports (21)         |     |     |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
PMU Perfs Detail. (22)    |     |     |.... |.... |.... |.... |.... |.... |#### |#### |#### |#### |#### |#### |#### |### |
PMU Rapports Internet(38) |     |     |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
Nanaelie/Le Trot (16)     |#### |#### |#### |     |     |     |     |     |     |     |     |     |     |     |     |     |
SIRE/IFCE (17)            |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |
Meteo (00,13,35)          |     |     |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
Racing Post (37)          |     |     |.... |.... |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |.... |
CanalTurf (24)            |     |     |     |     |     |     |     |     |#### |#### |#### |#### |#### |#### |#### |.... |
TurfoStats (25)           |     |     |     |     |     |     |     |     |#### |#### |#### |#### |#### |#### |#### |.... |
Geny (26)                 |     |     |     |     |     |     |     |     |#### |#### |#### |#### |#### |#### |#### |.... |
Smarkets (30)             |     |     |     |     |     |     |     |     |     |     |     |     |.... |.... |.... |     |
Pedigree (08,12,14,36)    |     |     |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |#### |### |
```

Notes :
- 2026 est partiel (donnees jusqu'a mars).
- Les sources externes (24-26, 37) dependent des sites tiers et peuvent avoir des trous.
- Nanaelie/Le Trot (16) couvre uniquement 2004-2013 (donnees historiques).

---

## 4. Inventaire des feature builders

### feature_builders/ (27 inscrits au DAG + 27 additionnels)

| Builder | Categorie | Features | Description |
|---------|-----------|----------|-------------|
| cheval_features.py | Cheval | ~21 | Profil statique du cheval (age, sexe, race, nb courses) |
| course_features.py | Course | ~19 | Conditions de course (distance, discipline, terrain) |
| jockey_features.py | Jockey | ~4 | Stats jockey/entraineur (victoires, places, gains) |
| pedigree_features.py | Pedigree | ~4 | Stats pere/mere (taux victoire, gains moyens) |
| marche_features.py | Marche | ~7 | Features marche (cote, proba implicite, mouvement) |
| musique_features.py | Forme | ~22 | Parsing musique (series, tendances, dnf) |
| temps_features.py | Temps | ~15 | Reduction km, vitesse, comparaison record piste |
| meteo_features.py | Meteo | ~6 | Temperature, precipitation, vent, penetrometre |
| equipement_features.py | Equipement | ~12 | Oeilleres, deferre, changements recents |
| poids_features.py | Poids | ~16 | Poids porte, surcharge, handicap, evolution |
| combo_features.py | Combo | ~12 | Combinaisons jockey-entraineur, jockey-hippodrome |
| interaction_features.py | Interaction | ~10 | Cross-features (age x distance, poids x cote) |
| profil_cheval_features.py | Profil | ~21 | Profil avance (specialisation distance, terrain) |
| class_change_features.py | Classe | ~6 | Montee/descente de classe, ecart valeur handicap |
| field_strength_builder.py | Peloton | ~6 | Force du peloton, rang relatif, cote moyenne |
| pace_profile_builder.py | Rythme | ~6 | Profil de course (leader, finisseur, regulier) |
| track_bias_detector.py | Piste | ~6 | Biais de piste (corde, position depart) |
| perf_detaillees_builder.py | Perf | ~6 | Performances detaillees (9 dernieres courses) |
| smarkets_builder.py | Exchange | ~5 | Cotes exchange Smarkets (back/lay) |
| racing_post_builder.py | Racing Post | ~5 | Stats Racing Post (RPR, TS) |
| reunions_builder.py | Reunions | ~8 | Infos reunion (type, conditions, prize money) |
| enrichissement_builder.py | Enrichissement | ~9 | Champs enrichis PMU (engagement, avis) |
| pedigree_advanced_builder.py | Pedigree+ | ~6 | Pedigree avance (inbreeding, aptitude distance) |
| canalturf_builder.py | CanalTurf | ~5 | Stats CanalTurf (pronostics, classement) |
| turfostats_builder.py | TurfoStats | ~5 | Stats TurfoStats (indices, classement) |
| geny_builder.py | Geny | ~5 | Stats Geny.com (pronostics PMU Group) |
| precomputed_partant_joiner.py | Precomputed | ~10 | Jointure donnees pre-calculees par partant |
| precomputed_entity_joiner.py | Precomputed | ~9 | Jointure donnees pre-calculees par entite |

### feat_*.py (16 scripts standalone)

| Builder | Categorie | Description |
|---------|-----------|-------------|
| feat_historique.py | Forme | Rolling windows (victoires, places, gains sur 3/5/10/20 courses) |
| feat_jockey.py | Jockey | Stats detaillees jockey et entraineur |
| feat_pedigree.py | Pedigree | Stats pere/mere par discipline et distance |
| feat_croisements.py | Croisements | Proprietaire, eleveur, discipline affinity |
| feat_sequences.py | Sequences | Patterns dans les sequences de resultats |
| feat_temporel.py | Temporel | Repos, fatigue, saisonnalite, recence |
| feat_interactions.py | Interactions | Features croisees multi-domaines |
| feat_cheval_jockey_affinity.py | Affinite | Taux victoire cheval+jockey ensemble |
| feat_cheval_hippodrome_affinity.py | Affinite | Performance cheval par hippodrome |
| feat_cheval_distance_affinity.py | Affinite | Performance cheval par tranche distance |
| feat_cheval_terrain_affinity.py | Affinite | Performance cheval par type terrain |
| feat_jockey_entraineur_combo.py | Combo | Stats duo jockey-entraineur |
| feat_entraineur_hippodrome.py | Affinite | Performance entraineur par hippodrome |
| feat_value_betting.py | Value | Ecart cote vs probabilite estimee |
| feat_meteo_terrain_interaction.py | Meteo | Interaction meteo x type terrain |
| feat_pedigree_discipline_match.py | Pedigree | Adequation pedigree x discipline course |
| feat_field_strength.py | Peloton | Force du peloton et rang relatif |

### Calculation scripts (41-49)

| Script | Description |
|--------|-------------|
| 41_sequences_performances.py | Sequences et patterns de performances |
| 42_croisement_racing_post_pmu.py | Croisement Racing Post x PMU |
| 43_croisement_meteo_courses.py | Jointure meteo x courses |
| 44_croisement_pedigree_partants.py | Jointure pedigree x partants |
| 45_graphe_relations_gnn.py | Graphe de relations (GNN-ready) |
| 46_track_bias_speed_class.py | Biais de piste et classes de vitesse |
| 48_parse_conditions_texte.py | Parsing texte conditions de course |
| 49_ecart_cotes_internet_national.py | Ecart cotes internet vs national |

### Total

| Groupe | Scripts | Features estimees |
|--------|---------|-------------------|
| feature_builders/ (DAG) | 27 | ~250 |
| feat_*.py (standalone) | 16 | ~100 |
| calc_*.py (41-49) | 8 | ~50 |
| **Total Phase 7** | **51** | **~400+** |
| master_feature_builder (Phase 8) | 1 | Consolidation |
| **Grand total features** | | **420 (mesure reelle)** |
