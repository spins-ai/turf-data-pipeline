# Audit Deduplication Features - 2026-04-09

## Methodologie
- Echantillon 500 records (zone tail 80%) par builder
- Fingerprint MD5 pour doublons exacts
- Correlation Pearson pour quasi-doublons (|r| > 0.99)
- 3,862 features analysees, 228,461 paires comparees

## Resultats

### Doublons exacts intra-builder: 494 paires
Principalement des features a fill rate 0% (toutes constantes dans l'echantillon):
- `temporal_context_features`: 194 paires (features toutes a 0)
- `feature_improvements`: 185 paires (features toutes a 0)
- `commentaire_deep_nlp`: 30 paires (pas de commentaires dans les donnees)
- `position_distribution`: 16 paires
- `avis_entraineur_nlp`: 15 paires (pas d'avis entraineur)

**Action**: Ces features seront supprimees automatiquement lors du filtrage fill rate <10%.

### Doublons exacts inter-builder: 10 paires
- 6 sont des colonnes contexte (`course_uid`, `date_reunion_iso`) copiees dans 3 builders
- 4 sont des features constantes croisees

**Action**: Supprimer colonnes contexte redondantes lors de la consolidation Parquet.

### Quasi-doublons inter-builder: 263 paires (|r| > 0.99)
Features reellement dupliquees entre builders differents:
| Feature A | Feature B | r |
|-----------|-----------|---|
| br_earnings_per_race | clm_gains_per_race | 1.0 |
| br_earnings_per_race | fcm_career_roi_estimate | 1.0 |
| br_weight_burden | fcm_weight_x_distance | 1.0 |
| br_win_rate | mth_win_rate_career | 1.0 |
| betting_edge/kelly_fraction | mef_kelly_fraction | 1.0 |
| betting_edge/market_prob | pc_implied_prob | 1.0 |
| betting_edge/edge_percentage | mef_expected_value | 1.0 |
| bayes_jockey_win_rate | bshr_jockey_wr | 0.998 |
| bayes_trainer_win_rate | bshr_trainer_wr | 0.999 |

**Action**: Lors de la consolidation (tache 39), garder 1 feature par groupe de doublons.
Critere de selection: garder celle avec le meilleur fill rate.

## Plan de nettoyage
1. Filtrer features fill rate <10% (elimine ~237 features + les 494 intra-dupes)
2. Lors de la consolidation Parquet, dedup par correlation > 0.99
3. Garder la feature avec le meilleur nom + fill rate dans chaque groupe
4. CSV complet: `D:/turf-data-pipeline/04_FEATURES/dedup_audit.csv`
