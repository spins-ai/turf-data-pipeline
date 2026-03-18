# Pipeline d'execution

Ordre d'execution complet du pipeline de donnees hippiques, de la collecte brute au livrable final.

---

## Diagramme global

```mermaid
graph TD
    subgraph "ETAPE 1 - Collecte"
        A1[01_calendrier_reunions] --> RAW[output/ brut]
        A2[02_liste_courses] --> RAW
        A3[02b_scraper_letrot] --> RAW
        A4[04_resultats] --> RAW
        A5[05-11 historiques] --> RAW
        A6[13_meteo_historique] --> RAW
        A7[14_pedigree_scraper] --> RAW
        A8[17_process_sire] --> RAW
        A9[21-28 rapports/enjeux] --> RAW
        A10[30-39 externes] --> RAW
        A11[40_enrichissement] --> RAW
    end

    subgraph "ETAPE 2 - Audit"
        RAW --> B1[audit_data_integrity.py]
        B1 --> B2[Rapport audit]
    end

    subgraph "ETAPE 3 - Nettoyage"
        B1 --> C1[nettoyage_global.py]
        C1 --> CLEAN[Donnees nettoyees]
    end

    subgraph "ETAPE 4 - Deduplication + Comblage"
        CLEAN --> D1[deduplication.py]
        D1 --> D2[comblage_trous.py]
    end

    subgraph "ETAPE 5 - Fusion Masters"
        D2 --> E1[merge_02_02b_courses_master.py]
        D2 --> E2[merge_pedigree_master.py]
        D2 --> E3[merge_meteo_master.py]
        D2 --> E4[merge_rapports_master.py]
        D2 --> E5[merge_marche_master.py]
        D2 --> E6[merge_equipements_master.py]
        D2 --> E7[merge_performances_master.py]
        E1 --> MASTERS[data_master/]
        E2 --> MASTERS
        E3 --> MASTERS
        E4 --> MASTERS
        E5 --> MASTERS
        E6 --> MASTERS
        E7 --> MASTERS
        MASTERS --> E8[mega_merge_partants_master.py]
        E8 --> PM[partants_master.jsonl]
    end

    subgraph "ETAPE 5b - Post-processing"
        MASTERS --> F1[postprocess_meteo.py]
        MASTERS --> F2[postprocess_rapports.py]
        MASTERS --> F3[postprocess_marche.py]
        MASTERS --> F4[postprocess_equipements.py]
        MASTERS --> F5[postprocess_horse_stats.py]
    end

    subgraph "ETAPE 6 - Feature Engineering"
        PM --> G1[Scripts 41-49 calcul]
        PM --> G2[feature_builders/ 30 builders]
        PM --> G3[feat_*.py 10 affinites croisees]
        G1 --> FEAT[features_matrix.parquet]
        G2 --> FEAT
        G3 --> FEAT
    end

    subgraph "ETAPE 11 - Qualite"
        FEAT --> H1[quality/ tests]
        H1 --> H2[Validation]
    end

    subgraph "ETAPE 12 - Livrable"
        H2 --> I1[Export JSON + CSV + Parquet]
        I1 --> I2[Pret pour ML Phase 2]
    end
```

---

## Ordre d'execution detaille

### Phase 1 : Collecte (scripts 00-40)

Les scripts de collecte sont independants et peuvent tourner en parallele. Chacun ecrit dans son sous-dossier `output/XX_*/`.

```
# Lancer les collecteurs (en parallele si possible)
python 01_calendrier_reunions.py --date-debut 2013-01-01 --date-fin 2026-03-18
python 02_liste_courses.py
python 02b_scraper_letrot.py
python 04_resultats.py
python 05_historique_chevaux.py
python 06_historique_jockeys.py
python 07_cotes_marche.py
python 08_pedigree.py
python 09_equipements.py
python 10_poids_handicaps.py
python 11_sectionals.py
python 13_meteo_historique.py
python 14_pedigree_scraper.py
python 17_process_sire.py
python 21_rapports_definitifs.py
python 22_performances_detaillees.py
python 23_pronostics_equidia.py
python 24_canalturf_scraper.py
python 25_turfostats_scraper.py
python 26_geny_scraper.py
python 27_citations_enjeux.py
python 28_combinaisons_marche.py
python 30_smarkets_exchange.py
python 37_rpscrape_racing_post.py
python 38_rapports_internet.py
python 39_reunions_enrichies.py
python 40_enrichissement_partants.py
```

Tous les scripts ont des checkpoints et reprennent automatiquement apres un crash.

### Phase 2 : Audit

```
python audit_data_integrity.py
```

Verifie : JSON valides, 0-bytes, doublons, plages de dates, outliers, taux de remplissage.

### Phase 3 : Nettoyage

```
python nettoyage_global.py
```

Corrige : UTF-8, normalisation noms, formats date ISO 8601, valeurs null coherentes.

### Phase 4 : Deduplication + Comblage

```
python deduplication.py
python comblage_trous.py
```

Deduplication entre sources (02/02b, 08/12/14/36, 21/38). Comblage depuis sources croisees.

### Phase 5 : Fusion en Masters

Ordre de fusion (les merges par domaine sont independants) :

```
# Merges par domaine (en parallele)
python merge_02_02b_courses_master.py      # -> courses_master.jsonl
python merge_pedigree_master.py            # -> pedigree_master.json
python merge_meteo_master.py               # -> meteo_master.json
python merge_rapports_21_38.py             # -> rapports_master.json
python merge_marche_master.py              # -> marche_master.json
python merge_equipements_master.py         # -> equipements_master.json
python merge_performances_master.py        # -> performances_master.json
python merge_stats_externes_master.py      # -> stats_externes_master.json

# Post-processing des masters (en parallele)
python postprocess_meteo.py
python postprocess_rapports.py
python postprocess_marche.py
python postprocess_equipements.py
python postprocess_horse_stats.py

# Mega-merge final (depend de tous les masters)
python mega_merge_partants_master.py       # -> partants_master.jsonl
```

### Phase 6 : Feature Engineering

```
# Scripts de calcul (en parallele, chacun lit partants_master)
python 41_sequences_performances.py
python 42_croisement_racing_post_pmu.py
python 43_croisement_meteo_courses.py
python 44_croisement_pedigree_partants.py
python 45_graphe_relations_gnn.py
python 46_track_bias_speed_class.py
python 48_parse_conditions_texte.py
python 49_ecart_cotes_internet_national.py

# Feature builders avances (en parallele)
python feat_historique.py
python feat_croisements.py
python feat_jockey.py
python feat_interactions.py
python feat_pedigree.py
python feat_temporel.py
python feat_sequences.py

# Affinites croisees (en parallele)
python feat_cheval_jockey_affinity.py
python feat_cheval_hippodrome_affinity.py
python feat_cheval_distance_affinity.py
python feat_cheval_terrain_affinity.py
python feat_jockey_entraineur_combo.py
python feat_entraineur_hippodrome.py
python feat_value_betting.py
python feat_meteo_terrain_interaction.py
python feat_pedigree_discipline_match.py
python feat_field_strength.py

# Assemblage final de la matrice
python feature_builders/master_feature_builder.py
```

### Phase 7 : Qualite

```
python quality/test_json_integrity.py
python quality/test_zero_bytes.py
python quality/test_record_counts.py
python quality/test_features_quality.py
python quality/leakage_detector.py
```

### Phase 8 : Export

```
# Triple format : JSON + CSV + Parquet
# Export final de features_matrix, labels, et tous les masters
```

---

## Dependances entre scripts

```mermaid
graph LR
    subgraph Collecte
        S02[02_courses]
        S02b[02b_letrot]
        S05[05_chevaux]
        S06[06_jockeys]
        S07[07_cotes]
        S08[08_pedigree]
        S09[09_equipements]
        S10[10_poids]
        S11[11_sectionals]
        S13[13_meteo]
        S14[14_pedigree]
        S17[17_sire]
        S21[21_rapports]
        S22[22_perfs]
        S28[28_combinaisons]
        S38[38_rapports_inet]
    end

    subgraph Masters
        S02 --> CM[courses_master]
        S02b --> CM
        S08 --> PedM[pedigree_master]
        S14 --> PedM
        S13 --> MetM[meteo_master]
        S21 --> RapM[rapports_master]
        S38 --> RapM
        S28 --> MarM[marche_master]
        S09 --> EqM[equipements_master]
        S10 --> EqM
        S05 --> HSM[horse_stats_master]
        S22 --> PerfM[performances_master]
    end

    subgraph Mega-merge
        CM --> PM[partants_master]
        PedM --> PM
        MetM --> PM
        RapM --> PM
        MarM --> PM
        EqM --> PM
        HSM --> PM
        PerfM --> PM
        S06 --> PM
        S07 --> PM
        S11 --> PM
        S17 --> PM
    end

    subgraph Features
        PM --> FE[feature_builders]
        FE --> FM[features_matrix]
    end
```

---

## Temps d'execution estimes

| Phase | Duree estimee | RAM requise | Notes |
|-------|--------------|-------------|-------|
| Collecte complete | ~50h (parallele ~15h) | ~15 MB/script | Checkpoint/resume |
| Audit | ~30 min | ~4 GB | Lecture sequentielle |
| Nettoyage | ~1h | ~8 GB | Streaming JSONL |
| Deduplication | ~2h | ~16 GB | Index en memoire |
| Comblage | ~1h | ~8 GB | Lookups croises |
| Fusion masters | ~2h | ~16-32 GB | Merges par domaine |
| Mega-merge | ~3h | ~32-64 GB | Jointure de tous les masters |
| Feature engineering | ~4h | ~32 GB | Calcul rolling windows |
| Qualite | ~30 min | ~4 GB | Tests automatiques |
| Export | ~1h | ~16 GB | Triple format |
| **TOTAL** | **~12-15h** | **64 GB recommande** | |

---

## 16 Phases du systeme complet (modules 1-68)

Le systeme complet (data + modeles) est organise en 16 phases dans `pipeline/` :

| Phase | Nom | Modules | Description |
|-------|-----|---------|-------------|
| 01 | Infrastructure | 1-8 | Ingestion, schema, dataset builder, qualite, missing values, outliers, normalizer, cache |
| 02 | Feature Engineering | 9-18 | Features avancees, rolling stats, temporal, odds, synergy, pedigree, track bias, pace, sectional, field strength |
| 03 | Feature Selection | 19-20 | Selection automatique, optimisation subsets |
| 04 | ML Core | 21-25 | Logistic Regression, Random Forest, XGBoost, LightGBM, CatBoost |
| 05 | Deep Learning | 26-30 | MLP, LSTM, GRU, TabNet, TFT |
| 06 | Advanced | 31-34 | GNN, Bayesian NN, Survival Model, Quantile Regressor |
| 07 | AutoML | 35-37 | AutoGluon, TPOT, H2O |
| 08 | Fusion | 38-40 | Stacking, Blending, Meta-model |
| 09 | Calibration | 41-43 | Calibration des probabilites |
| 10 | Outsiders | 44-46 | Anomaly Detector, Retour Forme Hidden, GAN Turf |
| 11 | Betting | 47-50 | ROI Predictor, Value Hunter RL, Meta Selector, ZURI |
| 12 | Simulation | 51-52 | Monte Carlo, Race Simulation Engine |
| 13 | Bet Sizing | 53-57 | Kelly, Sizing, Tickets |
| 14 | Adaptation | 58-60 | Recalibration, Decay Detector, Drift Detector |
| 15 | Monitoring | 61-63 | Monitoring, Dashboard, Alerts |
| 16 | Orchestration | 64-68 | Pipeline, Scheduler, Controller |
