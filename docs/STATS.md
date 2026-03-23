# Statistiques du Pipeline Turf Data

> Genere automatiquement le 2026-03-23 22:22:03

## Vue d'ensemble

| Metrique | Valeur |
|---|---|
| Total courses | **257,789** |
| Total partants | **2,930,290** |
| Chevaux uniques | **278,466** |
| Jockeys/Drivers uniques | **29,190** |
| Hippodromes uniques | **527** |
| Plage de dates | **2013-02-19 -> 2026-03-12** |
| Total features | **178** (from partants_master fields; features_matrix not yet built) |
| Taux de remplissage moyen | **93.2%** (echantillon de 1,000 enregistrements) |

## Stockage

| Repertoire | Taille |
|---|---|
| `data_master/` | 81.5 GB |
| `output/` | 467.0 GB |
| **Total** | **548.5 GB** |

## Sources de donnees

- **Sources actives** : 93 (repertoires dans `output/` contenant des fichiers de donnees)

## Details techniques

- Fichier principal : `data_master/partants_master.jsonl` (24.4 GB)
- Nombre de champs par enregistrement : 178
- Script de collecte : `scripts/collect_stats.py`
