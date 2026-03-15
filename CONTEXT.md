# CONTEXT — Dossier Data Courses Hippiques

## ⚡ LIRE EN PREMIER — pour tout nouveau Claude

Ce dossier est la partie DATA d'un projet de prédiction de courses hippiques.
Les modèles sont dans un dossier séparé. Ici = collecte, nettoyage, merge, features.

## Architecture

```
models hybride/
├── output/           ← Données brutes collectées (40+ sources, ~30 GB total)
│   ├── 02_liste_courses/
│   ├── 04_resultats/
│   ├── 05_historique_chevaux/
│   ├── 07_cotes_marche/
│   ├── 08_pedigree/
│   ├── 09_equipements/
│   ├── 10_poids_handicaps/
│   ├── 11_sectionals/
│   ├── 13_meteo_historique/
│   ├── 14_pedigree_scraper/
│   ├── 17_sire_ifce/
│   ├── 21_rapports_definitifs/
│   ├── 22_performances_detaillees/  (12 GB !)
│   ├── 23_pronostics/
│   ├── 24_canalturf/
│   ├── 25_turfostats/
│   ├── 26_geny/
│   ├── 27_citations_enjeux/
│   ├── 28_combinaisons_marche/
│   ├── 30_smarkets_exchange/
│   ├── 35_meteo_france/
│   ├── 36_pedigree_query/
│   ├── 37_racing_post/
│   ├── 38_rapports_internet/
│   ├── 39_reunions_enrichies/
│   └── 40_enrichissement_partants/
├── data_master/      ← Fichiers fusionnés (JSON + Parquet)
├── logs/             ← Logs de tous les scripts
├── merge_*.py        ← Scripts de fusion par domaine
├── XX_*.py           ← Scripts de collecte (XX = numéro source)
├── TODO_MACHINE_PUISSANTE.md  ← TODO master (~1000 tâches)
└── CONTEXT.md        ← CE FICHIER
```

## Data Masters (dans data_master/)

| Fichier | Records | Contenu | Status |
|---------|---------|---------|--------|
| pedigree_master | 1,413,913 chevaux | père/mère/robe/sexe/race, 7 sources | ✅ FAIT |
| meteo_master | 479,377 courses | température/vent/pluie/pénétromètre | ✅ FAIT |
| rapports_master | 217,569 courses | rapports définitifs/arrivée/dividendes | ✅ FAIT |
| marche_master | 151,258 records | cotes/combinaisons/exchange | ✅ FAIT |
| equipements_master | 573,111 partants | œillères/déferré/poids/handicap | ✅ FAIT |
| horse_stats_master | 80,656 chevaux | stats agrégées (victoires/places/gains) | ✅ FAIT |
| horse_profiles_externes | 9,159 chevaux | profils canalturf | ✅ FAIT |
| courses_externes | 8,332 entries | turfostats + geny | ✅ FAIT |
| performances_master | ~487K partants | perfs détaillées + sectionals | 🔴 À FAIRE (besoin 64GB RAM) |

## Règles CRITIQUES

1. **NE JAMAIS SUPPRIMER de données sources** (output/) — lecture seule !
2. **Écriture atomique** : toujours écrire en .tmp puis os.replace()
3. **Triple format** : JSON + Parquet (zstd) + CSV quand possible
4. **Streaming** : fichiers > 4GB → utiliser ijson, pas json.load()
5. **Graceful shutdown** : signal handlers pour Ctrl+C / kill

## Clés de jointure entre masters

- `course_uid` = identifiant unique d'une course (format: YYYY-MM-DD|RX|CY)
- `partant_uid` = identifiant unique d'un partant (format: course_uid + |PZ)
- `nom_cheval` (normalisé en MAJUSCULES sans accents) pour les données par cheval

## Scripts de collecte encore en cours (au 15 mars 2026)

- 04_resultats (~28%), 14_pedigree (~28%), 23_pronostics, 27_citations (~57%)
- 28_combinaisons (~bientôt fini), 38_rapports (~88%)

## Prochaines étapes

1. Lundi: transférer sur PC 64GB RAM, finir performances_master
2. Nettoyage & normalisation (noms, hippodromes, dates, unités)
3. Entity Resolution (relier tous les masters entre eux)
4. Feature Engineering (objectif: 528+ features)
5. Point-in-Time correctness (pas de fuite du futur)
6. Export matrice finale: features_matrix.parquet

## Priorité merge sources (moins prioritaire → plus prioritaire)

pedigree_query < canalturf < scraper_14 < PMU_08 < partants_enrichis < SIRE_IFCE

## Contact

Projet personnel de Quentin. Expliquer les choses simplement en français.
