# CONTEXT — Dossier Data Courses Hippiques

## ⚡ LIRE EN PREMIER — pour tout nouveau Claude

Ce dossier est la partie DATA d'un projet de prédiction de courses hippiques.
Les modèles sont dans un dossier séparé. Ici = collecte, nettoyage, merge, features.

## Architecture

```
models hybride/
├── output/           ← Données brutes collectées (40+ sources, ~52 GB total)
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
│   ├── 37_racing_post/
│   ├── 38_rapports_internet/
│   ├── 39_reunions_enrichies/
│   └── 40_enrichissement_partants/
├── data_master/      ← Fichiers fusionnés + enrichis (JSON + Parquet)
├── logs/             ← Logs de tous les scripts
├── merge_*.py        ← Scripts de fusion par domaine
├── postprocess_*.py  ← Scripts d'enrichissement (features calculées)
├── XX_*.py           ← Scripts de collecte (XX = numéro source)
├── TODO_MACHINE_PUISSANTE.md  ← TODO master (~1000 tâches)
├── QUESTIONS_COUSIN.md        ← Questions pour expert hippique
├── CONTEXT.md        ← CE FICHIER
└── AUDIT_MASTERS.md  ← Audit qualité des masters
```

## Data Masters (dans data_master/) — MIS À JOUR 16 MARS 2026

| Fichier | Records | Champs | Taille | Post-traité | Status |
|---------|---------|--------|--------|-------------|--------|
| pedigree_master | 1,413,913 chevaux | 56 | 465 MB | ⏳ Lundi (lourd en RAM) | ✅ FAIT |
| equipements_master | 573,111 partants | 36 | 277 MB | ✅ poids_category, equipment_score, oeilleres_bool | ✅ ENRICHI |
| meteo_master | 479,377 courses | 33 | 797 MB | ✅ terrain_category, penetrometre_numeric, meteo_score | ✅ ENRICHI |
| rapports_master | 217,569 courses | 329 | 421 MB | ✅ jour_semaine, saison, is_quinte, is_surprise, distance_category | ✅ ENRICHI |
| marche_master | 151,258 records | 31 | 67 MB | ✅ cote_category, popularite, value_indicator | ✅ ENRICHI |
| horse_stats_master | 80,656 chevaux | 39 | 162 MB | ✅ class_category, distance_pref, experience, is_en_forme | ✅ ENRICHI |
| horse_profiles_externes | 9,159 chevaux | 44 | 5 MB | — | ✅ FAIT |
| courses_externes | 8,332 entries | 7 | 19 MB | — | ✅ FAIT |
| performances_master | ~487K partants | ~50 | — | — | 🔴 À FAIRE (besoin 64GB RAM) |

**TOTAL : 2,933,375 records, 2.5 GB, ~50 nouvelles features calculées**

## Scripts de collecte (au 16 mars 2026)

| Script | Statut | Données | Estimation fin |
|--------|--------|---------|----------------|
| 04_resultats | 🔄 Tourne | 2.1 GB | Bientôt |
| 14_pedigree | 🔄 Tourne | 89K fichiers, 377 MB | ~Jeudi |
| 23_pronostics | 🔄 Tourne | 110K fichiers, 431 MB | Ce weekend |
| 27_citations | 🔄 Tourne | 144K fichiers, 5.9 GB | ~Mercredi |
| 28_combinaisons | ⏹️ Fini ou crashé | 177K fichiers, 2.3 GB | — |
| 37_racing_post | ⏹️ Crashé (JSON 12GB) | 10K cache files | Relancer lundi |
| 38_rapports | ⏹️ Fini ou crashé | 160K fichiers, 1.9 GB | — |
| 36_pedigree_query | ❌ Tué (bloqué Cloudflare) | — | — |

## Post-processing effectué (16 mars 2026)

1. **postprocess_meteo.py** — terrain_category, penetrometre_numeric, is_psf, meteo_score recalculé
   - Mapping penetrometre: "très sec"→2.0, "bon"→2.8, "bon souple"→3.3, "souple"→3.8, "très souple"→4.3, "lourd"→4.8, "très lourd"→5.5
2. **postprocess_rapports.py** — jour_semaine, mois, saison, is_quinte/quarte/tierce, discipline_norm, distance_category, is_surprise, rapport_gagnant_euros
3. **postprocess_marche.py** — cote_category, value_ratio, value_indicator, proba_category, taille_champ, popularite
4. **postprocess_equipements.py** — poids_category, poids_direction, oeilleres_bool, deferre_norm, equipment_change_score, position_poids
5. **postprocess_horse_stats.py** — class_category, gains_par_course, performance_category, specialiste_discipline, distance_pref_category, experience_category, is_en_forme/is_en_baisse

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

## Prochaines étapes (PLANNING)

### Lundi 17 mars (nouveau PC 64GB RAM + RTX 5070 Ti)
1. Transférer le dossier sur le nouveau PC
2. Finir performances_master (merge_performances_master.py est prêt)
3. Re-merge pedigree avec nouvelles données du script 14
4. Parser Racing Post cache files (10K fichiers → master)
5. Re-merge rapports/marché avec données fraîches de 28/38
6. Post-process pedigree_master

### Semaine 1 — Entity Resolution
7. Normalisation globale (noms chevaux, hippodromes, dates)
8. Relier TOUS les masters en une seule grande table via course_uid + partant_uid
9. Fuzzy matching pour les noms mal écrits

### Semaine 1-2 — Feature Engineering (80 → 528+ features)
10. Historique glissant (forme 5/10/20 courses, gains 30/60/90 jours)
11. Croisements cheval × contexte (hippodrome, distance, terrain)
12. Stats jockey + entraîneur + combo
13. Features pedigree calculées (lignée × terrain)
14. Features marché (mouvement de cote, signal des pros)
15. Interactions (cote × forme, poids × distance, âge × discipline)
16. ⚠️ INTÉGRER le savoir de l'expert hippique (cousin de Quentin) — voir QUESTIONS_COUSIN.md

### Semaine 2 — Export final
17. Point-in-Time correctness (pas de fuite du futur !)
18. Split train/validation/test PAR DATE (pas aléatoire)
19. Export features_matrix.parquet — prêt pour XGBoost + LSTM

## Priorité merge sources (moins prioritaire → plus prioritaire)

pedigree_query < canalturf < scraper_14 < PMU_08 < partants_enrichis < SIRE_IFCE

## GitHub

Repo privé : https://github.com/spins-ai/turf-data-pipeline
- Code uniquement (scripts .py + .md)
- Les données sont dans .gitignore (trop grosses)
- Permet la collaboration entre sessions Claude

## Contact

Projet personnel de Quentin. Expliquer les choses simplement en français.
