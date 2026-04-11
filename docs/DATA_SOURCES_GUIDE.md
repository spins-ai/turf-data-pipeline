# Guide des Sources de Donnees

## Sources principales actives

### PMU API (01-04)
- **Calendrier reunions**: `01_calendrier_reunions.py`
- **Liste courses**: `02_liste_courses.py` + `02b_liste_courses_2013.py`
- **Resultats**: `04_resultats.py`
- **Couverture**: 2013-2025, 467K+ courses
- **Frequence**: Quotidienne (API gratuite)

### Open-Meteo (00)
- **Meteo historique**: `00_enrichissement_meteo.py`
- **Donnees**: temperature, precipitation, vent, humidite
- **API**: Gratuite, sans cle

### Pedigree (12, 14)
- **Sources**: france-galop.com, letrot.com
- **Scripts**: `12_pedigree_scraper.py`, `14_pedigree_scraper.py`
- **Donnees**: pere, mere, pere de mere, origines

### Le Trot (02b)
- **Source**: letrot.com
- **Script**: `02b_scraper_letrot.py`
- **Donnees**: courses trot, resultats, palmares

### Nanaelie historique (16)
- **Source**: nanaelie archives 2004-2013
- **Script**: `16_collecte_nanaelie_2004_2013.py`
- **Donnees**: historique avant API PMU

## Fichiers Master

| Fichier | Taille | Records | Description |
|---------|--------|---------|-------------|
| partants_master.jsonl | 26.7 GB | 2,930,290 | Tous les partants, 181 colonnes |
| partants_master_crossref.jsonl | 26.7 GB | 2,930,290 | Cross-reference entre sources |
| partants_master_enrichi.jsonl | 26.2 GB | ~2,930,290 | Enrichi avec meteo+pedigree |
| courses_master.parquet | 23 MB | ~467K | Infos courses |
| meteo_master.parquet | 6.8 MB | - | Meteo par hippodrome+date |
| marche_master.parquet | 6.7 MB | - | Donnees de marche/cotes |

## Builder Outputs

- **Repertoire**: `D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/`
- **311 builders** avec sortie JSONL valide
- **~3,994 features** au total
- **Taille totale**: ~80 GB de JSONL

## Sources non encore implementees (placeholders)

Les dossiers suivants existent mais sont vides (sources futures):
- 72_tattersalls, 73_goffs, 74_arqana (ventes aux encheres)
- 87_bloodstock, 88_weatherbys (elevage)
- 95_standardbred_ca, 96_noaa_weather, 97_meteostat (international)
- 100_magic_millions, 103_tierce_magazine, 104_turfpronos (media)
- 112_visual_crossing, 122_hippodrome_details (references)

## Pipeline de donnees

```
PMU API → 01-04 scripts → 02_DONNEES_BRUTES/
                              ↓
                    scripts/enrichissement/ → 03_DONNEES_MASTER/
                              ↓
                    feature_builders/ → builder_outputs/
                              ↓
                    consolidation → 04_FEATURES/ (Parquet final)
```
