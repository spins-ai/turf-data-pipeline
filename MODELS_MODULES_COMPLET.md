# LISTE COMPLETE MODELES + MODULES - PROJET PREDICTION HIPPIQUE
# 68 modules, 16 phases, du data au betting

# ================================================================
# Phase 1 -- Infrastructure data (fondations)
# ================================================================
# Sans ca rien ne fonctionne.
1. data_ingestion_manager.py
2. data_schema_validator.py
3. historical_dataset_builder.py
4. data_quality_monitor.py
5. missing_values_handler.py
6. outlier_cleaner.py
7. data_normalizer.py
8. cache_manager.py

# ================================================================
# Phase 2 -- Feature Engineering (la vraie puissance)
# ================================================================
# C'est la partie la plus importante du projet.
9. advanced_feature_generator.py
10. rolling_stats_generator.py
11. temporal_feature_builder.py
12. odds_feature_builder.py
13. jockey_trainer_synergy_builder.py
14. pedigree_feature_builder.py
15. track_bias_detector.py
16. pace_profile_builder.py
17. sectional_feature_builder.py
18. field_strength_builder.py

# Objectif : produire 100-300 features solides.

# ================================================================
# Phase 3 -- Selection automatique des features
# ================================================================
19. selection_auto_features.py
20. feature_subset_optimizer.py

# Objectif : garder les variables les plus utiles.

# ================================================================
# Phase 4 -- Modeles ML core
# ================================================================
21. logistic_regression_baseline.py
22. random_forest.py
23. xgboost.py
24. lightgbm.py
25. catboost.py

# Objectif : base de predictions stable.

# ================================================================
# Phase 5 -- Modeles deep learning
# ================================================================
26. mlp.py
27. lstm.py
28. gru.py
29. tabnet.py
30. tft.py

# Objectif : capturer les patterns complexes.

# ================================================================
# Phase 6 -- Modeles avances
# ================================================================
31. gnn.py
32. bayesian_nn.py
33. survival_model.py
34. quantile_regressor.py

# Objectif : ajouter des signaux differents.

# ================================================================
# Phase 7 -- AutoML
# ================================================================
35. autogluon_model.py
36. tpot_pipeline.py
37. h2o_model.py

# Objectif : tester des combinaisons automatiquement.

# ================================================================
# Phase 8 -- Fusion des modeles
# ================================================================
38. stacking_classifier.py
39. blending.py
40. meta_model.py

# Objectif : obtenir la prediction finale.

# ================================================================
# Phase 9 -- Calibration des probabilites
# ================================================================
41. calibration_inter_blocs.py
42. platt_scaling_module.py
43. isotonic_calibration_module.py

# Objectif : probabilites realistes.

# ================================================================
# Phase 10 -- Detection outsiders
# ================================================================
44. anomalie_detector.py
45. retour_forme_hidden.py
46. gan_turf.py

# Objectif : reperer les cotes anormales.

# ================================================================
# Phase 11 -- Moteur strategique betting
# ================================================================
47. roi_predictor.py
48. value_hunter_rl.py
49. meta_selector.py
50. ZURI_OUTSIDER_ENGINE.py

# Objectif : transformer predictions en value bets.

# ================================================================
# Phase 12 -- Simulation de course
# ================================================================
51. monte_carlo_simulator.py
52. race_simulation_engine.py

# Objectif : estimer distribution des resultats.

# ================================================================
# Phase 13 -- Optimisation des mises
# ================================================================
53. bet_sizing_engine.py
54. kelly_strategy.py
55. fractional_kelly.py
56. ticket_optimizer.py
57. tickets_combines.py

# Objectif : generer les paris optimaux.

# ================================================================
# Phase 14 -- Adaptation continue
# ================================================================
58. auto_recalibration.py
59. model_decay_detector.py
60. concept_drift_detector.py

# Objectif : eviter que les modeles deviennent obsoletes.

# ================================================================
# Phase 15 -- Monitoring
# ================================================================
61. telemetrie_monitoring.py
62. dashboard_generation.py
63. alert_manager.py

# Objectif : surveiller le systeme.

# ================================================================
# Phase 16 -- Orchestration finale
# ================================================================
64. orchestrateur_pipeline.py
65. workflow_dependency_manager.py
66. job_scheduler.py
67. multi_model_controller.py
68. failover_manager.py

# Objectif : automatiser tout le pipeline.

# ================================================================
# VISION GLOBALE
# ================================================================
#
# DATA -> Feature engineering -> 20+ modeles ML -> Stacking
#   -> Calibration -> Outsider detection -> ROI predictor
#   -> Monte Carlo -> Bet sizing -> Execution -> Monitoring
#

# ================================================================
# PILIERS QUALITE
# ================================================================
# 1. Performance
# 2. Securite
# 3. Stabilite
# 4. Redondance
# 5. Auditabilite
# 6. Strategie
# 7. Intelligence
# 8. Orchestration
# 9. Compatibilite systeme
# 10. Auto-adaptativite
# 11. Synchronisation inter-blocs
# 12. Modularite
# 13. Telemetrie
# 14. Debugging
# 15. Stress-test
# 16. Rentabilite turf
# 17. Resilience algorithmique
# 18. Explainability
# 19. Cycle auto-apprenant
# 20. Alignement turf/marche
# 21. Tracabilite
# 22. Meta-configuration
# 23. GPU-awareness, monitoring, haute disponibilite

# ================================================================
# DONNEES PRIORITAIRES POUR NOURRIR LES MODELES
# ================================================================
# CRITIQUE :
#   - Cotes exchange (Betfair/Smarkets) -> value_hunter, ROI, anomalie
#   - Sectionals detailles -> pace_profile, simulation
#   - Stats jockey/entraineur -> synergy builder
#
# IMPORTANT :
#   - Pedigree complet -> pedigree_feature_builder
#   - Terrain/Going -> track_bias_detector
#   - Odds mouvements -> odds_feature_builder
#   - Commentaires courses -> retour_forme_hidden
