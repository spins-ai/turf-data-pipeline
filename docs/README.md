# Turf Data Pipeline

Pipeline de donnees pour la prediction hippique francaise (galop + trot).
Collecte multi-sources, feature engineering avance, preparation pour modeles ML/DL.

---

## Chiffres cles (avril 2026)

| Metrique | Valeur |
|----------|--------|
| Partants normalises | 2,930,290 |
| Courses normalisees | ~257,806 |
| Performances detaillees | 6,000,000+ |
| Chevaux uniques | 116,000+ |
| Periode couverte | 2013-2026 |
| Sources de donnees | 40+ (API PMU, scraping, open data) |
| Feature builders | 60+ |
| Features consolidees | 3,297 |
| Features selectionnees (top 500) | 502 |
| Taille donnees brutes | ~250 Go (D:/turf-data-pipeline/02_DONNEES_BRUTES) |
| Features consolidated.parquet | 6.79 Go |
| Features selected.parquet | ~1.7 Go |
| DuckDB | 2.5 Go |
| Tests automatises | 30+ (pytest) |
| Commits | 215+ |

---

## Architecture

```
D:/turf-data-pipeline/
  02_DONNEES_BRUTES/           Donnees brutes collectees (~250 Go)
    00_enrichissement_meteo/   ... 70+ sous-dossiers par source
    builder_outputs/           Sorties intermediaires des feature builders
  03_DONNEES_MASTER/           Fichiers masters fusionnes (Parquet)
    partants_master.parquet    2.93M rows, principal fichier de reference
    courses_master.parquet     257K courses normalisees
    performances_master.parquet 6M rows, base pour features vitesse
    meteo_master.parquet       Meteo par reunion
    horse_stats_master.parquet 80K chevaux, 17 cols
  04_FEATURES/                 Features finales
    features_consolidated.parquet  3297 cols, toutes les features
    features_selected.parquet      502 cols (top 500 par LightGBM)
    features.duckdb               Base DuckDB pour requetes rapides

turf-data-pipeline/ (repo git)
  config.py                    Configuration centralisee (chemins D:, URLs, RAM)
  feature_builders/            60+ builders Python (1 par famille de features)
  scripts/
    collection/                Scripts de collecte (01-16)
    run_full_pipeline.sh       Pipeline reproductible complet (11 etapes)
    daily_collect_pmu.py       Collecte automatique quotidienne
    daily_maintenance.py       Maintenance quotidienne
    consolidate_features.py    Consolidation JSONL -> Parquet
    apply_feature_selection.py Selection top 500 features (LightGBM)
    validate_pipeline_output.py Validation finale (leakage, target, schema)
  tests/                       Tests pytest (30+)
  docs/                        Documentation
  pipeline/                    Modules structures (phase_01 a phase_16, futur)
```

---

## Pipeline d'execution (11 etapes)

```bash
bash scripts/run_full_pipeline.sh [--collect] [--builders-only]
```

| Etape | Description | Duree approx |
|-------|-------------|--------------|
| 1 | Collecte PMU (optionnel, --collect) | ~30 min |
| 2 | Feature builders (60+ builders sequentiels) | ~4h |
| 3 | Audits qualite (fill rates, leakage, correlations) | ~10 min |
| 4 | Targets + splits temporels | ~5 min |
| 5 | Tests unitaires | ~2 min |
| 6 | Builders specialises (C1-C15) | ~1h |
| 7 | Consolidation + integration | ~30 min |
| 8 | Feature selection (LightGBM top 500) | ~20 min |
| 9 | Validation finale | ~5 min |
| 10 | Catalogue features (Markdown) | ~2 min |
| 11 | Tests pytest complets | ~3 min |

---

## Collecte automatique quotidienne

```bash
# Collecte du programme du jour a 8h
python scripts/daily_collect_pmu.py

# Collecte des resultats de la veille a 22h
python scripts/daily_collect_pmu.py --days-back 1

# Windows Task Scheduler:
schtasks /create /tn "TurfPMU_Daily" /tr "python scripts/daily_collect_pmu.py" /sc daily /st 08:00
schtasks /create /tn "TurfPMU_Results" /tr "python scripts/daily_collect_pmu.py --days-back 1" /sc daily /st 22:00
```

---

## Features principales (top 20 par importance LightGBM)

| Rang | Feature | Description |
|------|---------|-------------|
| 1 | cote_finale | Cote finale PMU du partant |
| 2 | elo_x__elo_rating | Rating Elo du cheval |
| 3 | spd_x__speed_figure | Figure de vitesse normalisee |
| 4 | mch_x__mch_cote_value | Signal valeur cote/marche |
| 5 | ev_x__proba_estimee | Probabilite Bayesienne de victoire |
| 6 | mus_x__pos_last_1 | Position derniere course |
| 7 | hdp_x__handicap_vs_field | Poids vs moyenne peloton |
| 8 | cls_x__class_drop | Signal descente de classe |
| 9 | pagerank_x__score | PageRank dans graphe chevaux |
| 10 | rapphist_x__avg_simple_gagnant | Rapport moyen historique |

---

## Contraintes techniques

- **RAM max**: 57 Go utilises sur 64 Go (check_ram avant chaque builder)
- **1 builder a la fois**: ne jamais paralleliser (crash constate)
- **Parquet partout**: lecture row-group par row-group pour gros fichiers
- **Zero leakage**: validation automatique (pas de donnees post-course)
- **Target**: 8.5% de taux positif (victoire)
- **Schemas**: gestion null-type columns via scan multi-row-groups

---

## Prerequis

- Python 3.12+
- Windows 11 (64 Go RAM)
- Disque D: (SSD, 500+ Go libres)
- Packages: pyarrow, pandas, numpy, lightgbm, duckdb, requests, pytest

```bash
pip install pyarrow pandas numpy lightgbm duckdb requests pytest
```

---

## Prochaines etapes

1. Integrer donnees LeTrot (1.35M partants trot, 2024-2026)
2. Meteorologie fine (precipitations horaires par hippodrome)
3. Migration chemins en dur vers config.py (progressif)
4. Modeles ML/DL (apres que toutes les donnees soient pretes)
