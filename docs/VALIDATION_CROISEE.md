# Rapport de Validation Croisée

*Généré le 2026-03-20 04:56*

## Résumé

| Test | Statut | Détail |
|------|--------|--------|
| PMU vs Le Trot (résultats) | OK | 100.0% concordance |
| PMU vs Exchange (cotes) | FAIL | 0% concordance |
| Cohérence pedigree | OK | 96.84% concordance |
| Cohérence dates | OK |  |
| Nombre de partants | OK | 97.26% concordance |

## PMU vs Le Trot (résultats)

- **pmu_count**: 2094968
- **letrot_count**: 330140
- **common**: 114244
- **mismatches**: 0
- **match_rate**: 100.0
- *Temps*: 75.9s

## PMU vs Exchange (cotes)

- **pmu_count**: 0
- **exchange_count**: 0
- **common**: 0
- **compared**: 0
- **avg_diff_pct**: 0
- **outliers_30pct**: 0
- **match_rate**: 0
- *Temps*: 0.5s

## Cohérence pedigree

- **pedigree_count**: 275952
- **checked**: 255865
- **mismatches_pere**: 7991
- **mismatches_mere**: 8158
- **match_rate**: 96.84
- *Temps*: 81.5s

### Exemples d'anomalies (max 10)

```json
{"cheval": "RAVANELLO", "pere_partants": "CYBELE DES ESSARTS", "pere_pedigree": "INSERT GEDE"}
{"cheval": "SERGENT DU RIB", "pere_partants": "HERMINE DU RIB", "pere_pedigree": "BACCARAT DU PONT"}
{"cheval": "SOUMOULOU", "pere_partants": "NOUVELLE PERLE", "pere_pedigree": "FIRST DE RETZ"}
{"cheval": "SAGE DE GUERINIERE", "pere_partants": "HALDANE", "pere_pedigree": "GANYMEDE"}
{"cheval": "RAMSEY DU HAM", "pere_partants": "UFANIA DU BUISSON", "pere_pedigree": "INDY DE VIVE"}
```

## Cohérence dates

- **total_dates**: 4700
- **future_dates**: 0
- **invalid_dates**: 0
- **abnormal_days**: 0
- **avg_partants_per_day**: 615.2
- *Temps*: 73.1s

## Nombre de partants

- **courses_checked**: 255095
- **mismatches**: 6992
- **match_rate**: 97.26
- *Temps*: 73.0s

### Exemples d'anomalies (max 10)

```json
{"course_uid": "c30b75c51997ffac", "declare": 23, "reel": 46, "diff": 23}
{"course_uid": "616dc30c3fbabe10", "declare": 22, "reel": 2, "diff": -20}
{"course_uid": "a90ffcfdc9601d8d", "declare": 22, "reel": 42, "diff": 20}
{"course_uid": "90800120db99f4e6", "declare": 20, "reel": 1, "diff": -19}
{"course_uid": "af8eada55a66557f", "declare": 20, "reel": 39, "diff": 19}
{"course_uid": "4160f008cc8f40cb", "declare": 20, "reel": 1, "diff": -19}
{"course_uid": "c607827e7dbb4f18", "declare": 19, "reel": 38, "diff": 19}
{"course_uid": "6df8a891a600f6cd", "declare": 23, "reel": 5, "diff": -18}
{"course_uid": "ca299cbad41e7135", "declare": 23, "reel": 5, "diff": -18}
{"course_uid": "64582a6bc87e915c", "declare": 18, "reel": 36, "diff": 18}
```
