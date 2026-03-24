
## Phase post-TODO : Audit + Optimisation (à faire après les 1107 tâches)

### 1. Audit données manquantes
- Quelles données gratuites on rate encore
- Quels trous combler avec croisements entre sources
- Quelles features supplémentaires calculer
- Data critique pour les modèles ML

### 2. Optimisation performance
- Parquet partout (lecture 10-100x plus rapide)
- Index DuckDB pour requêtes instantanées
- Parallélisation feature builders
- Caching intelligent (pas recalculer ce qui a pas changé)

### 3. Optimisation qualité
- Fill rate par feature (virer <10%, fusionner redondantes)
- Corrélations inter-features (supprimer doublons)
- Feature importance pré-modèles

### 4. Optimisation stockage
- Compresser/archiver données brutes
- Garder que Parquet pour modèles (~5 GB vs 250 GB)
- Stratégie de rétention

### 5. Puis → Nouveau dossier MODÈLES ML/DL
- CatBoost, XGBoost, LightGBM
- Stacking ensemble
- Meta selector
- PAS avant que data soit parfait
