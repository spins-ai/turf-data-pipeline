# Rapport de Validation Croisée

*Généré le 2026-03-19 07:14*

## Résumé

| Test | Statut | Détail |
|------|--------|--------|
| PMU vs Le Trot (résultats) | OK | 100.0% concordance |
| PMU vs Exchange (cotes) | FAIL | 0% concordance |
| Cohérence pedigree | OK | 96.88% concordance |
| Cohérence dates | OK |  |
| Nombre de partants | OK | 100.0% concordance |

## PMU vs Le Trot (résultats)

- **pmu_count**: 2143257
- **letrot_count**: 330877
- **common**: 114981
- **mismatches**: 0
- **match_rate**: 100.0
- *Temps*: 80.3s

## PMU vs Exchange (cotes)

- **pmu_count**: 0
- **exchange_count**: 0
- **common**: 0
- **compared**: 0
- **avg_diff_pct**: 0
- **outliers_30pct**: 0
- **match_rate**: 0
- *Temps*: 0.6s

## Cohérence pedigree

- **pedigree_count**: 275952
- **checked**: 257587
- **mismatches_pere**: 7946
- **mismatches_mere**: 8109
- **match_rate**: 96.88
- *Temps*: 84.7s

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
- **avg_partants_per_day**: 623.5
- *Temps*: 81.3s

## Nombre de partants

- **courses_checked**: 257788
- **mismatches**: 0
- **match_rate**: 100.0
- *Temps*: 87.5s
