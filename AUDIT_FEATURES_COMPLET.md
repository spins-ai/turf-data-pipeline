# AUDIT COMPLET DES FEATURES — Avril 2026

## Données source : partants_master.jsonl
- **2,930,290 records** | **181 colonnes** | **2013-2026**
- **260 fichiers .jsonl** de features déjà créés (~3,300+ features)

---

## 1. DOMAINES DE DONNÉES DISPONIBLES

### A. Identité (100% fill)
- `horse_id`, `nom_cheval`, `jockey_driver`, `entraineur`, `proprietaire`, `eleveur` (24%)

### B. Pedigree (80-98% fill)
- `pere`, `mere`, `pere_mere` (37%), `pgr_*` (23 champs), `ped_*` (9 champs)
- Lignée père, mère, père-mère, consanguinité, vitesse/endurance index

### C. Conditions de course (100% fill)
- `discipline`, `distance`, `hippodrome_normalise`, `type_piste`, `corde` (99%)
- `nombre_partants`, `date_reunion_iso`, `cnd_*` (8 champs conditions)

### D. Profil cheval (92-100% fill)
- `age`, `sexe`, `race`, `robe` (93%), `pgr_robe`, `pgr_sexe`

### E. Performance (41-76% fill)
- `position_arrivee` (76%), `is_gagnant` (8%), `is_place` (24%)
- `temps_ms` (41%), `reduction_km_ms` (41%)
- `ecart_precedent` (29%)

### F. Marché / Cotes (48-63% fill)
- `cote_finale` (62%), `cote_reference` (50%), `proba_implicite` (62%)
- `mkt_*` (overround, CLV, drift, sharp money, etc.)
- `mch_*` (combinaisons, rang cote, favori, enjeux)

### G. Équipement (13-65% fill)
- `oeilleres` (65%), `deferre` (13%), `jument_pleine` (<1%)

### H. Carrière cumulée (56-85% fill)
- `nb_courses_carriere` (85%), `nb_victoires_carriere` (63%)
- `nb_places_carriere` (79%), `nb_places_2eme` (40%), `nb_places_3eme` (39%)
- `gains_carriere_euros` (56%), `gains_annee_euros` (35%)

### I. Séquence / Historique (47-58% fill)
- `musique` (95% — string codée type "DM3M122A6A2A0A3A0A0A")
- `seq_*` (positions, cotes, repos, trends, volatilité, momentum)

### J. Rapports / Dividendes (60-97% fill)
- `rap_rapport_simple_gagnant` (97%), tiercé, quarté, quinté
- `rap_dividend_moyen`, `rap_market_concentration`
- `rap_ri_e_*` — rapports internet détaillés

### K. Texte NLP (commentaire/avis)
- `commentaire_apres_course`, `avis_entraineur`
- `cnd_conditions_texte_original` — texte des conditions de course

### L. Réseau (gnn_*) (32-88% fill)
- Degree cheval, nb chevaux par jockey/entraineur, duos, win rates

### M. Speed figures (spd_*) (20-94% fill)
- `spd_speed_figure` (41%), class rating, field strength, bias corde

### N. Météo (met_*) (59-63% fill)
- Pluie mm, terrain prédit, impact score, PSF

### O. Handicap (6-62% fill)
- `handicap_valeur` (15%), `handicap_distance_m` (62%)
- `poids_porte_kg` (45%), `surcharge_decharge_kg` (7%)

---

## 2. FEATURES EXISTANTES PAR CATÉGORIE (~260 builders, ~3,300 features)

### Déjà bien couvert ✅
- Target encoding (target_leakfree, target_encoding, bayesian_shrinkage)
- Forme récente (recency_weighted, momentum, streak, sequence, lag)
- Cotes/marché (value_detection, market_overreaction, odds_*, betting_*, CLV)
- Jockey/Trainer (form_deep, trainer_deep, affinity, combo, switch, compatibility)
- Distance/Surface/Hippo (preference, optimal_conditions, aptitude, surface_interaction)
- NLP (commentaire_deep_nlp, avis_entraineur_nlp, condition_text_nlp)
- Pedigree (advanced, deep, surface_interaction, distance_cross, stallion_stats)
- Z-scores intra-course (race_zscore — 28 features)
- Statistiques par entité (ELO, bayesian ratings, career_stats, consistency)
- Équipement (equipment_change, equipment_combo, equipment_impact)
- Rivaux/Adversaires (head_to_head, encounter_history, opponent_strength)
- Class (claiming_class, class_transition, field_quality)
- Temporel (days_since, freshness, fatigue, temporal_cyclical, seasonality)
- Vitesse (speed_figures, speed_distance_profile, race_rhythm)
- Réseau (network_centrality, graph_features, connection_strength)

---

## 3. FEATURES MANQUANTES — À CRÉER

### PRIORITÉ 1 : Indispensables pour les modèles (impact élevé)

#### 3.1 — `musique_lag_features` (~20 features) ⭐⭐⭐
La musique encode les N dernières courses sous forme "DM3M122A6A2A0A3A0A0A".
Parser chaque caractère en features individuelles :
- Position course N-1, N-2, ..., N-5 (lag individuel)
- Discipline course N-1 (A=attelé, M=monté)
- Disqualifié N-1 (D), tombé (T), arrêté (A), rétrogradé (R)
- Nombre de "1" dans les 5 dernières, 10 dernières
- Pattern de progression/régression (1-2-3 vs 5-4-3)
- **Musique déjà partiellement couverte (musique_decoder 10 + musique_advanced 12) mais les lag individuels manquent**

#### 3.2 — `handicap_deep` (~12 features) ⭐⭐⭐
- Valeur handicap vs moyenne du champ
- Évolution du handicap (monte/descend)
- Handicap × distance interaction
- Surcharge/décharge impact historique
- Win rate par tranche de handicap
- Handicap relatif au meilleur du champ
- **Fill rate 15% mais crucial pour courses à handicap**

#### 3.3 — `speed_form_composite` (~10 features) ⭐⭐⭐
- Meilleur temps sur cette distance (best reduction_km à cette distance)
- Temps moyen vs meilleur temps du champ
- Amélioration/dégradation du temps sur les 5 dernières courses
- Speed figure relative au champ actuel
- **Combine temps_ms, reduction_km_ms et spd_speed_figure en features contextuelles**

#### 3.4 — `proprietaire_eleveur_deep` (~10 features) ⭐⭐
- Win rate propriétaire (shrunk bayesien)
- Taille écurie propriétaire
- Diversité hippodromes propriétaire  
- Éleveur × discipline win rate
- Propriétaire en forme récente
- **proprietaire rempli à 100%, eleveur à 24% — sous-exploités**

#### 3.5 — `robe_phenotype` (~8 features) ⭐⭐
- Win rate par robe × discipline
- Robe × surface interaction
- Robe × distance interaction
- **robe rempli à 93%, pgr_robe à 89% — jamais exploité directement**

### PRIORITÉ 2 : Utiles pour améliorer les modèles

#### 3.6 — `pays_origin_features` (~8 features) ⭐⭐
- Win rate par pays d'origine du cheval
- Pays × discipline interaction
- Cheval étranger (is_foreign)
- Pays entrainement × hippodrome
- **pays_cheval 62%, pgr_pays_naissance 82%**

#### 3.7 — `incident_history` (~8 features) ⭐⭐
- Nb incidents dans les N dernières courses
- Type d'incident le plus fréquent
- Taux d'incidents par hippodrome
- Jours depuis dernier incident
- Impact incident sur course suivante
- **incident rempli à 19%, is_disqualifie 16%**

#### 3.8 — `poids_impact_deep` (~10 features) ⭐⭐
- Poids relatif au champ (déjà en poids_relatif mais seulement 4 features)
- Poids × distance interaction
- Poids tendance (augmente/diminue)
- Surcharge vs décharge impact
- Poids optimal estimé
- **poids_porte_kg 45%, surcharge 7%**

#### 3.9 — `ecart_repos_features` (~8 features) ⭐⭐
- Parser ecart_precedent (string) en jours
- Repos optimal par cheval
- Repos × distance interaction
- Repos vs médiane historique du cheval
- **ecart_precedent 29%, seq_jours_depuis_derniere 58%**

#### 3.10 — `inedit_debutant` (~6 features) ⭐⭐
- Is_inedit (premier départ)
- Éleveur track record pour les inédits
- Sire track record pour les inédits
- Trainer track record premiers départs
- Jockey track record premiers départs
- **is_inedit 3.6% mais signal fort quand présent**

#### 3.11 — `market_exotic_features` (~12 features) ⭐⭐
- Tiercé/Quarté/Quinté rapports comme proxy de difficulté
- Ratio simple/couple/trio (profondeur marché)
- rap_market_concentration vs win probability
- Enjeux totaux par course × horse
- **rap_* très riche (44+ champs) mais sous-exploité pour exotic bets**

#### 3.12 — `combinaison_marche_deep` (~8 features) ⭐
- mch_rang_combinaison relatif au champ
- mch_pct_masse (part de marché du cheval)
- Enjeu vs cote divergence
- Smart money detection (gros enjeu + cote qui baisse)
- **mch_* 51-85% fill — partiellement couvert par combinaisons_marche**

### PRIORITÉ 3 : Features avancées / Meta

#### 3.13 — `interaction_triple_advanced` (~15 features) ⭐
- Distance × surface × discipline (triple interaction)
- Age × distance × corde
- Jockey × hippodrome × discipline
- **Certaines combos existent mais pas les triples les plus importantes**

#### 3.14 — `time_decay_all_entities` (~12 features) ⭐
- EWMA sur TOUTES les entités (sire, propriétaire, éleveur, hippodrome)
- Pas seulement cheval/jockey/trainer comme recency_weighted
- **Compléterait recency_weighted_builder**

#### 3.15 — `rank_within_race_extended` (~20 features) ⭐
- Rang intra-course pour : EWMA win rate, days_since_last, speed_figure, ELO
- **race_zscore couvre 10 champs bruts mais pas les features calculées**

#### 3.16 — `conditions_texte_mining` (~8 features) ⭐
- Parser cnd_conditions_texte_original pour : montant prix, conditions spéciales
- Course à réclamer, course de groupe
- Restrictions d'âge/sexe/gains extraites du texte
- **cnd_conditions_texte_original 100% fill mais text brut**

#### 3.17 — `field_heterogeneity` (~6 features) ⭐
- Diversité des âges dans le champ
- Diversité des disciplines passées des chevaux
- Nb de chevaux avec >50 courses vs inédits
- Ratio favoris/outsiders dans le champ

#### 3.18 — `cote_trajectory_intraday` (~6 features) ⭐
- cote_reference → cote_finale : direction, amplitude, volatilité
- Mouvement marché en % (déjà partiellement couvert)
- **50% fill pour cote_reference mais feature très prédictive**

#### 3.19 — `pere_mere_interaction` (~10 features) ⭐
- Père × mère : crossing spécifique connu ?
- Père-mère (broodmare sire) win rate × conditions
- Consanguinité × performance
- **pere_mere 37% fill, ped_inbreeding_count 0% actuel mais data existe**

#### 3.20 — `engagement_supplement` (~4 features)
- supplement_euros comme signal de confiance
- Engagement tardif/supplément élevé = signal
- **supplement_euros fill faible mais signal quand présent**

---

## 4. RÉSUMÉ CHIFFRÉ

| Catégorie | Builders à créer | Features estimées |
|-----------|-----------------|-------------------|
| PRIORITÉ 1 (indispensables) | 5 | ~60 |
| PRIORITÉ 2 (utiles) | 7 | ~70 |
| PRIORITÉ 3 (avancées) | 8 | ~81 |
| **TOTAL** | **20** | **~211** |

### État actuel vs potentiel
- **Features existantes** : ~3,300
- **Features à créer** : ~211
- **Total possible** : ~3,500+

### Builders existants : 260
### Builders à créer : 20
### **Total final : ~280 builders**

---

## 5. ORDRE D'EXÉCUTION RECOMMANDÉ

### Phase 1 — Priorité 1 (à faire immédiatement)
1. `musique_lag_features` — lag individuels position N-1 à N-5
2. `handicap_deep` — handicap contextuel et évolution
3. `speed_form_composite` — temps/vitesse contextuels
4. `proprietaire_eleveur_deep` — entités sous-exploitées
5. `robe_phenotype` — robe × conditions

### Phase 2 — Priorité 2 (avant les modèles ML)
6. `pays_origin_features` — pays d'origine
7. `incident_history` — historique incidents
8. `poids_impact_deep` — poids avancé
9. `ecart_repos_features` — repos optimisé
10. `inedit_debutant` — premiers départs
11. `market_exotic_features` — rapports exotiques
12. `combinaison_marche_deep` — marché approfondi

### Phase 3 — Priorité 3 (optimisation post-premier modèle)
13. `interaction_triple_advanced`
14. `time_decay_all_entities`
15. `rank_within_race_extended`
16. `conditions_texte_mining`
17. `field_heterogeneity`
18. `cote_trajectory_intraday`
19. `pere_mere_interaction`
20. `engagement_supplement`

---

## 6. NOTE IMPORTANTE

Après ~3,500 features, le rendement marginal diminue fortement.
La prochaine étape la plus impactante n'est PAS de créer plus de features mais de :
1. **Assembler en Parquet** (~5 Go vs 250 Go JSONL)
2. **Feature selection** (SHAP, mutual info, corrélation → garder ~500-800 features)
3. **Premier modèle CatBoost** pour identifier les top features
4. **Itérer** : créer des features ciblées basées sur les erreurs du modèle
