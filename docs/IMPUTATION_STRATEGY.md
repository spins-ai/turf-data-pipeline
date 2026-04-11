# Strategie d'Imputation des Donnees Manquantes

## Principes
1. **Ne pas imputer les features categoriques** — laisser le modele gerer (CatBoost/LightGBM supportent les NaN nativement)
2. **Imputer seulement les features numeriques critiques** avec fill rate > 50%
3. **Ne jamais imputer les targets** (is_gagnant, position_arrivee) — si manquant, exclure le record
4. **Respecter la temporalite** — imputation basee sur des donnees passees uniquement

## Strategies par categorie de features

### 1. Features de performance (win_rate, place_rate, elo, etc.)
- **Methode**: Mediane par discipline (galop/trot/obstacle)
- **Raison**: La moyenne est tiree par les outliers, la mediane est plus robuste
- **Quand**: Seulement si le cheval a 0 courses (debut de carriere)
- **Alternative**: Laisser NaN et utiliser des modeles tolerants aux NaN

### 2. Features de cote/marche (cote_prob, implied_prob, etc.)
- **Methode**: NaN → le modele traitera comme "pas de cote dispo"
- **Raison**: Une cote imputee serait un signal trompeur
- **Ne PAS imputer**: les rapports (rapport_simple_gagnant) sont des resultats, pas des features

### 3. Features physiques (poids, distance, age)
- **poids_porte_kg** (46% fill): Mediane par type_course + discipline
- **distance** (99% fill): Pas besoin d'imputation
- **age** (99% fill): Pas besoin d'imputation

### 4. Features temporelles (jours_repos, nb_courses_30j, etc.)
- **Methode**: -1 ou NaN pour "inconnu"
- **Raison**: L'absence d'info temporelle est un signal en soi (cheval inconnu)

### 5. Features de vitesse (temps_ms, reduction_km_ms)
- **39% fill** — trop bas pour imputer de maniere fiable
- **Methode**: Laisser NaN. Les modeles tree-based gerent bien les NaN.
- **Alternative future**: Imputer par regression a partir de position + distance + conditions

### 6. Features pedigree
- **Methode**: Mediane globale pour les features numeriques (sire_wr, etc.)
- **Raison**: Pedigree stable dans le temps, mediane globale OK

## Implementation
L'imputation sera faite APRES la consolidation Parquet (tache 39) et AVANT l'entrainement.
Le script de consolidation stockera les NaN propres (pas de -1, pas de 0 par defaut).
L'imputation sera un step distinct dans le pipeline ML.

## Features a ne PAS imputer (laisser NaN)
- Toute feature avec fill rate < 20%
- Toute feature categorique
- Toute feature de resultat (position, rapport, gains)
- Toute feature de sequence (musique, odds_sequence)

## Resume: ordre de priorite
1. NaN natif pour tree-based models (CatBoost, LightGBM, XGBoost) — pas besoin d'imputer
2. Mediane par strate pour les rares features numeriques critiques
3. Indicateur de missingness (is_missing_X) si le pattern de donnees manquantes est informatif
