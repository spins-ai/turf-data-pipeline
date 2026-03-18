# Turf Data Pipeline

Systeme de prediction hippique : 68 modules, 24 modeles ML, 16 phases.

**Phase 1** (ce dossier) = collecte, nettoyage, fusion et feature engineering des donnees.
**Phase 2** (dossier separe, a venir) = entrainement et inference des modeles ML.

---

## Chiffres cles

| Metrique | Valeur |
|----------|--------|
| Reunions hippiques | 41,477 (2004-2026) |
| Courses normalisees | ~257,806 |
| Partants normalises | ~2,930,290 (66 champs) |
| Sources de donnees | 40+ (API, scraping, open data) |
| Features cataloguees | 481 (193 existantes + 288 nouvelles) |
| Features cible | 528+ |
| Scripts de collecte | 41 |
| Feature builders | 30 |
| Taille donnees brutes | ~52 GB |
| Masters fusionnes | 9 fichiers, ~2.5 GB |
| Modeles ML prevus | 24 |
| Modules totaux | 68 |

---

## Les 12 Etapes du pipeline

Le pipeline est structure en 12 etapes sequentielles, de la collecte brute au livrable final pret pour les modeles ML.

### Etape 1 -- Collecte des donnees
41 scripts de collecte (01 a 40 + fetch_openmeteo) interrogent les API PMU, scrappent les sites hippiques (Le Trot, CanalTurf, TurfoStats, Geny, Racing Post, etc.), et telecharges les datasets ouverts (SIRE/IFCE, Kaggle). Les scripts utilisent le format JSONL en streaming pour limiter l'usage memoire a ~15 MB chacun, avec checkpoint automatique pour reprise apres crash.

**Statut** : 21 scripts termines, ~10 en cours ou a relancer.

### Etape 2 -- Verification et integrite
`audit_data_integrity.py` verifie la validite des JSON/JSONL, compte les records, detecte les doublons, outliers, fichiers tronques, taux de remplissage.

**Statut** : Script ecrit, a executer.

### Etape 3 -- Nettoyage global
`nettoyage_global.py` corrige l'encodage UTF-8, normalise les noms (chevaux, jockeys, hippodromes), uniformise les formats de date (ISO 8601), supprime les valeurs parasites.

**Statut** : Script ecrit, a executer.

### Etape 4 -- Comblage de trous
`comblage_trous.py` remplit les champs manquants en croisant les sources existantes (penetrometre depuis reunions enrichies, pays_cheval depuis SIRE/IFCE, terrain infere depuis meteo + historique hippodrome, etc.).

**Statut** : Script ecrit, a executer.

### Etape 5 -- Fusion / Consolidation
Plusieurs scripts `merge_*.py` fusionnent les sources par domaine en fichiers masters :

| Master | Sources fusionnees | Records | Statut |
|--------|--------------------|---------|--------|
| courses_master | 02 + 02b | ~257K | A faire |
| partants_master | 02 + 05-11 + 17 + 22-40 | ~2.7M | A faire |
| pedigree_master | 08 + 12 + 14 + 36 | 1,413,913 | Fait |
| meteo_master | 13 + 35 + Open-Meteo | 479,377 | Fait + enrichi |
| rapports_master | 21 + 38 | 217,569 | Fait + enrichi |
| marche_master | 27 + 28 | 151,258 | Fait + enrichi |
| equipements_master | 09 + 10 | 573,111 | Fait + enrichi |
| horse_stats_master | 05 | 80,656 | Fait + enrichi |

### Etape 6 -- Feature Engineering
8 scripts de calcul (41-49), 9 feature builders dans `feature_builders/`, 10 scripts d'affinites croisees, 5 scripts de post-processing, 7 builders existants (musique, temps, profil, etc.) et 7 feature builders supplementaires (`feat_historique.py`, `feat_croisements.py`, etc.).

Objectif : passer de 67 features initiales a 481+ features couvrant forme, profil, jockey, entraineur, pedigree, meteo, terrain, marche, pace, equipement, poids, temps, conditions, repos, consistance, classe, rapports, calendrier, proprietaire et interactions croisees.

**Statut** : Tous les scripts sont ecrits. Execution en attente sur machine puissante (64 GB RAM).

### Etape 7 -- Collecte de nouvelles sources
Extension vers des sources internationales : UK (Timeform, Racing Post), US (Equibase, DRF), AU (Punters.com.au, Racenet), cotes exchange (Betfair, Oddschecker), pedigree mondial, ventes/encheres, sectionals/GPS.

**Statut** : Planifie.

### Etape 8 -- Integration nouvelles sources dans le pipeline
Pour chaque nouvelle source : parser, nettoyer, dedupliquer, creer le builder de features, ajouter les jointures, documenter.

**Statut** : Planifie.

### Etape 9 -- Organisation des dossiers
Structure finale avec `output/` (brut), `data_master/` (fusionne), `features/` (matrice), `labels/`, `pipeline/` (symlinks par phase), `feature_builders/`, `docs/`, `quality/`, `logs/`, `backups/`. Export triple format JSON + CSV + Parquet.

**Statut** : Partiellement fait (7 masters en Parquet).

### Etape 10 -- Documentation
Ce dossier `docs/` : README, SOURCES, SCHEMA, FEATURES, PIPELINE, INSTALL, plus AUDIT_MASTERS.md et CONTEXT.md.

**Statut** : En cours.

### Etape 11 -- Qualite finale
Tests automatiques (JSON valides, symlinks, 0-bytes, NaN/Inf, dates, cotes, distances), statistiques finales, validation croisee entre sources, backup final.

**Statut** : Scripts de test ecrits dans `quality/`.

### Etape 12 -- Pret pour les modeles
Verification que `partants_master.json` est complet, `features_matrix` contient 400+ features alignee avec les labels, tous les symlinks `pipeline/` fonctionnent, documentation a jour, backup fait.

**Statut** : A venir.

---

## 24 Modeles ML prevus (Phase 2)

| Categorie | Modeles |
|-----------|---------|
| ML classique | Logistic Regression, Random Forest, XGBoost, LightGBM, CatBoost |
| Deep Learning | MLP, LSTM, GRU, TabNet, TFT (Temporal Fusion Transformer) |
| Avance | GNN, Bayesian NN, Survival Model, Quantile Regressor |
| AutoML | AutoGluon, TPOT, H2O |
| Fusion | Stacking, Blending, Meta-model |
| Outsiders | Anomaly Detector, Retour Forme Hidden, GAN Turf |
| RL | Value Hunter RL |

---

## Structure du projet

```
turf-data-pipeline/
  output/                    Donnees brutes collectees (~52 GB)
  data_master/               Fichiers masters fusionnes (JSON + Parquet)
  feature_builders/          30 builders de features
  pipeline/                  Symlinks par phase (phase_01 a phase_16)
  quality/                   Tests et monitoring qualite
  labels/                    Labels pour les modeles
  docs/                      Documentation (ce dossier)
  XX_*.py                    Scripts de collecte (00-49)
  merge_*.py                 Scripts de fusion
  postprocess_*.py           Scripts d'enrichissement
  feat_*.py                  Scripts de features croisees
  hippodromes_db.py          Base de 673 hippodromes
  requirements.txt           Dependances Python
```

---

## Liens utiles

- [SOURCES.md](SOURCES.md) -- Liste complete des sources de donnees
- [SCHEMA.md](SCHEMA.md) -- Schemas des tables principales
- [FEATURES.md](FEATURES.md) -- Catalogue des 481 features
- [PIPELINE.md](PIPELINE.md) -- Flux d'execution et diagramme
- [INSTALL.md](INSTALL.md) -- Guide d'installation
