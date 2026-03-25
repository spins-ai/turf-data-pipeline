# Data Dictionary — partants_master.jsonl

Generated from the first 100 records. Total unique fields: **131**.

## Table of Contents

- [Base](#base) — Informations de base du partant et de la course
- [Rapports](#rapports) — Rapports de course (dividendes, combinaisons)
- [Pedigree](#pedigree) — Pedigree / origines du cheval (SIRE/IFCE)
- [Marché](#marche) — Marché des paris (combinaisons, types de pari)

---

## Base

Informations de base du partant et de la course

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `age` | int | 8 | Âge du cheval en années |
| `allure` | string | trot | Allure (trot, galop) |
| `cle_partant` | string | 2013-02-19\|vincennes\|R1\|C1\|1 | Clé composite du partant (date\|hippo\|R\|C\|num) |
| `corde` | string | gauche | Numéro de corde |
| `cote_finale` | null | null | Cote finale du partant |
| `cote_reference` | null | null | Cote de référence du partant |
| `course_uid` | string | 09f2d0d6935882c1 | Identifiant unique de la course |
| `date_reunion_iso` | string | 2013-02-19 | Date de la réunion (ISO 8601) |
| `deferre` | string |  | Ferrage du cheval (DA, DP, etc.) |
| `discipline` | string | TROT_ATTELE | Discipline (TROT_ATTELE, PLAT, OBSTACLE, ...) |
| `distance` | int | 2850 | Distance de la course en mètres |
| `ecart_precedent` | string |  | Écart avec le partant précédent |
| `eleveur` | string |  | Nom de l'éleveur |
| `entraineur` | string | M. IZAAC | Nom de l'entraîneur |
| `gains_annee_euros` | null | null | Gains de l'année en cours (euros) |
| `gains_carriere_euros` | null | null | Gains totaux en carrière (euros) |
| `handicap_distance_m` | int | 2850 | Distance avec handicap en mètres |
| `handicap_valeur` | null | null | Valeur du handicap |
| `hippodrome_normalise` | string | vincennes | Nom normalisé de l'hippodrome |
| `horse_id` | string | 7287b9f261df | Identifiant unique du cheval |
| `incident` | string |  | Incident en course (DA, DQ, AR, etc.) |
| `is_disqualifie` | bool | False | Is disqualifie |
| `is_gagnant` | bool | False | Is gagnant |
| `is_inedit` | bool | False | Cheval inédit (première course) |
| `is_place` | bool | False | Is place |
| `jockey_driver` | string | CH. MARTENS | Nom du jockey ou driver |
| `jockey_driver_change` | bool | False | Changement de jockey/driver |
| `jument_pleine` | bool | False | Jument pleine |
| `mere` | string | Go lucky | Nom de la mère |
| `musique` | string | DM3M122A6A2A0A3A0A0A | Musique (historique récent des performances) |
| `nb_courses_carriere` | int | 79 | Nombre total de courses en carrière |
| `nb_places_2eme` | null | null | Nombre de 2èmes places en carrière |
| `nb_places_3eme` | null | null | Nombre de 3èmes places en carrière |
| `nb_places_carriere` | int | 34 | Nombre total de places en carrière |
| `nb_victoires_carriere` | int | 5 | Nombre total de victoires en carrière |
| `nom_cheval` | string | RODGERS | Nom du cheval |
| `nombre_partants` | int | 17 | Nombre partants |
| `num_pmu` | int | 1 | Numéro PMU du partant |
| `numero_course` | int | 1 | Numéro de la course (C1, C2, ...) |
| `numero_reunion` | int | 1 | Numéro de la réunion (R1, R2, ...) |
| `oeilleres` | string |  | Type d'oeillères portées |
| `partant_uid` | string | a6350c910e06d08f | Identifiant unique du partant |
| `pays_cheval` | string | France | Pays d'origine du cheval |
| `pere` | string | Fredegonde | Nom du père |
| `pere_mere` | string |  | Nom du père de la mère |
| `place_corde` | int | 1 | Place à la corde (numéro) |
| `poids_base_kg` | null | null | Poids de base en kilogrammes |
| `poids_porte_kg` | int | 57 | Poids porté en kilogrammes |
| `position_arrivee` | int | 9 | Position arrivee |
| `proba_implicite` | null | null | Proba implicite |
| `proprietaire` | string | Ecurie PELHEM | Nom du propriétaire |
| `race` | string | TROTTEUR FRANCAIS | Race du cheval |
| `reduction_km_ms` | int | 710000 | Reduction km ms |
| `reunion_uid` | string | 3eb7d66f31636f63 | Identifiant unique de la réunion |
| `robe` | string | bai | Robe (couleur) du cheval |
| `sexe` | string | hongres | Sexe du cheval (males, femelles, hongres) |
| `source` | string | pmu | Source de données (pmu, etc.) |
| `statut` | string | partant | Statut du partant (partant, non-partant, etc.) |
| `surcharge_decharge_kg` | null | null | Surcharge ou décharge en kg |
| `taux_reclamation_euros` | null | null | Taux de réclamation en euros |
| `temps_ms` | int | 213700 | Temps ms |
| `timestamp_collecte` | string | 2026-03-13T23:44:36Z | Timestamp collecte |
| `type_piste` | string | cendrée | Type piste |

## Rapports

Rapports de course (dividendes, combinaisons)

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `rap__nb_sources` | int | 3 | Nombre de sources pour les rapports |
| `rap__sources` | list | ['rapports_merged', '21_rapports', '04_resultats'] | Sources |
| `rap_combinaison` | string | 4-5-15 | Combinaison du rapport |
| `rap_combinaison_couple_gagnant` | string | 4-5 | Combinaison couple gagnant |
| `rap_combinaison_gagnant` | string | 4 | Combinaison gagnant |
| `rap_combinaison_place_1` | string | 4 | Combinaison place 1 |
| `rap_combinaison_place_2` | string | 5 | Combinaison place 2 |
| `rap_combinaison_place_3` | string | 15 | Combinaison place 3 |
| `rap_dividende_euros` | float | 3.6 | Dividende du rapport en euros |
| `rap_hippodrome` | string | vincennes | Hippodrome |
| `rap_nb_gagnants` | float | 17881.07 | Nombre de gagnants pour ce rapport |
| `rap_rapport_2sur4_max` | int | 540 | Rapport 2sur4 max |
| `rap_rapport_2sur4_min` | int | 540 | Rapport 2sur4 min |
| `rap_rapport_2sur4_nb_combinaisons` | int | 7 | Rapport 2sur4 nb combinaisons |
| `rap_rapport_couple_gagnant` | int | 2510 | Rapport couple gagnant |
| `rap_rapport_couple_place_1` | int | 930 | Rapport couple place 1 |
| `rap_rapport_couple_place_2` | int | 2310 | Rapport couple place 2 |
| `rap_rapport_couple_place_3` | int | 2570 | Rapport couple place 3 |
| `rap_rapport_multi_4` | int | 28350 | Rapport multi 4 |
| `rap_rapport_multi_5` | int | 5670 | Rapport multi 5 |
| `rap_rapport_multi_6` | int | 1890 | Rapport multi 6 |
| `rap_rapport_multi_7` | int | 810 | Rapport multi 7 |
| `rap_rapport_quarte_bonus` | int | 2340 | Rapport quarte bonus |
| `rap_rapport_quarte_ordre` | int | 9360 | Rapport quarte ordre |
| `rap_rapport_quinte_bonus3` | int | 180 | Rapport quinte bonus3 |
| `rap_rapport_quinte_ordre` | int | 4980 | Rapport quinte ordre |
| `rap_rapport_simple_gagnant` | int | 600 | Rapport simple gagnant |
| `rap_rapport_simple_place_1` | int | 260 | Rapport simple place 1 |
| `rap_rapport_simple_place_2` | int | 300 | Rapport simple place 2 |
| `rap_rapport_simple_place_3` | int | 580 | Rapport simple place 3 |
| `rap_rapport_tierce_ordre` | int | 12590 | Rapport tierce ordre |
| `rap_rapport_uid` | string | 709e9775a0d407fa | Identifiant unique du rapport |
| `rap_rapports_raw` | list | [{'libelle': 'Ordre + Tirelire', 'dividende': 0, 'dividen... | Rapports raw |
| `rap_source_rapports_definitifs` | bool | True | Source rapports definitifs |
| `rap_type_pari` | string | quinte_plus | Type pari |

## Pedigree

Pedigree / origines du cheval (SIRE/IFCE)

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `pgr__nb_sources` | int | 2 | Nombre de sources pedigree |
| `pgr__sources` | list | ['partants_enrichis', 'sire_ifce'] | Liste des sources pedigree |
| `pgr_age` | int | 8 | Âge du cheval (pedigree) |
| `pgr_age_ans` | float | 20.862422997946613 | Âge précis en années (décimal) |
| `pgr_annee_naissance` | int | 2005 | Année de naissance |
| `pgr_consommation` | string | O | Consommation (O = oui, N = non) |
| `pgr_date_deces` | string | 2021-02-10 | Date deces |
| `pgr_date_naissance` | string | 2005-05-03 | Date de naissance du cheval |
| `pgr_eleveur` | string | Ecurie des CHARMES | Eleveur |
| `pgr_mere` | string | Go lucky | Nom de la mère (pedigree) |
| `pgr_pays_cheval` | string | France | Pays du cheval (pedigree) |
| `pgr_pays_naissance` | string | FRANCE | Pays de naissance |
| `pgr_pere` | string | Fredegonde | Nom du père (pedigree) |
| `pgr_race` | string | TROTTEUR FR. | Race (pedigree) |
| `pgr_robe` | string | BAI | Robe (pedigree) |
| `pgr_sexe` | string | H | Sexe (pedigree, code court) |
| `pgr_sexe_age` | string | M7 | Sexe age |
| `pgr_sire_annee_naissance` | int | 2005 | Année de naissance (SIRE/IFCE) |
| `pgr_sire_consommation` | string | O | Consommation selon SIRE (O/N) |
| `pgr_sire_date_naissance` | string | 2005-05-03 | Date de naissance (SIRE/IFCE) |
| `pgr_sire_pays_naissance` | string | FRANCE | Pays de naissance (SIRE/IFCE) |
| `pgr_sire_vivant` | bool | True | Cheval vivant selon SIRE/IFCE |
| `pgr_source_02_partants` | bool | True | Données pedigree issues des partants |
| `pgr_source_02b` | bool | True | Source 02b |
| `pgr_source_canalturf` | bool | True | Source canalturf |
| `pgr_source_sire` | bool | True | Données issues de SIRE/IFCE |
| `pgr_vivant` | bool | True | Cheval vivant (booléen) |

## Marché

Marché des paris (combinaisons, types de pari)

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `mch__nb_sources` | int | 1 | Nombre de sources marché |
| `mch__sources` | list | ['28_combinaisons'] | Liste des sources marché |
| `mch_combinaison` | list | [3, 4, 5, 7, 13] | Combinaison des numéros de partants |
| `mch_hippodrome` | string | vincennes | Hippodrome (données marché) |
| `mch_rang_combinaison` | int | 5 | Rang de la combinaison dans le marché |
| `mch_type_pari` | string | QUINTE_PLUS | Type de pari (QUINTE_PLUS, etc.) |

---

## Champs definitivement inaccessibles

Certains champs de `partants_master` resteront a faible taux de remplissage car les donnees sources ne sont pas disponibles ou accessibles. Cette section documente chaque cas et la raison.

| Champ | Fill rate actuel | Raison de l'inaccessibilite | Alternative |
|-------|-----------------|----------------------------|-------------|
| `poids_base_kg` | ~8.7% | Le poids de base n'est pas expose dans l'API publique PMU. Seules les courses a handicap publient cette donnee. Les pages web PMU affichent parfois le poids mais sans API stable. | Estimer via `poids_porte_kg - surcharge_decharge_kg` quand les deux sont connus. |
| `surcharge_decharge_kg` | ~8.7% | Depend directement de `poids_base_kg` : la surcharge/decharge = difference entre poids porte et poids de base. Meme limitation API. | Calculable si `poids_base_kg` et `poids_porte_kg` sont tous deux disponibles. |
| `avis_entraineur` | ~9.2% | L'API PMU retourne presque toujours NEUTRE (<0.1% non-neutre). Le texte riche des avis n'est disponible que sur les pages detail HTML avec scraping Selenium/Playwright, et seulement pour les courses recentes. | Utiliser `commentaire_apres_course` comme proxy qualitatif post-course. |
| `temps_ms` | ~39% | Les temps de course ne sont enregistres que pour les partants ayant termine la course (pas les non-partants, disqualifies, arretes). Les courses anciennes (avant 2016) ont moins de couverture. Racing Post UK a des sectionals detailles mais sous abonnement payant. | `reduction_km_ms` est calculable quand `temps_ms` et `distance` sont connus. Pour les non-classes, le temps est naturellement absent. |
| `reduction_km_ms` | ~39% | Derive directement de `temps_ms` et `distance`. Si le temps est absent, la reduction kilometrique ne peut pas etre calculee. | Imputable par moyenne de la course si au moins un temps est connu pour la meme course. |
| `poids_porte_kg` | ~45.8% | Champ rempli principalement pour les courses de galop (plat, obstacle). En trot, il n'y a pas de notion de poids porte. L'API PMU expose ce champ de maniere inconsistante. | Pour le trot, mettre null est correct (pas de poids porte). Pour le galop, le scraping des pages detail PMU ou Racing Post peut completer. |
| `cote_finale` | variable | Les cotes finales ne sont pas toujours archivees pour les courses anciennes. L'API PMU ne garantit pas la persistance des cotes apres la course. | Utiliser `cote_reference` ou `proba_implicite` comme approximation. |
| `cote_reference` | variable | Meme probleme que `cote_finale` : les cotes de reference sont ephemeres dans l'API PMU. | Utiliser les cotes exchange Smarkets/Betfair quand disponibles. |
| `gains_annee_euros` | variable | Ce champ change au cours de l'annee. Les donnees historiques ne preservent pas le snapshot au moment de la course. | Calculable en sommant les allocations gagnees dans `rapports_master` pour l'annee en cours. |
| `gains_carriere_euros` | variable | Meme probleme de snapshot temporel que `gains_annee_euros`. | Calculable en sommant toutes les allocations historiques. |

### Sources payantes qui pourraient ameliorer la couverture

| Source | Champs potentiels | Cout estime | Priorite |
|--------|-------------------|-------------|----------|
| Racing Post / Timeform Pro | `temps_ms` sectionals detailles, RPR, TopSpeed | ~200-500 GBP/an | Haute |
| Betfair API | `cote_finale` exchange, volume marche | Gratuit (avec cle API) | Haute |
| Meteo France API | Donnees meteorologiques station par hippodrome | Variable (forfaits) | Moyenne |
| OptixEQ | Speed figures avances, pace analysis | ~500 USD/an | Basse |
| Equimetre / France Galop institutional | Biometrie, donnees GPS embarquees | Acces institutionnel | Basse |

### Champs structurellement absents (par design)

Ces champs sont naturellement vides pour certains types de courses et ne necessitent pas d'enrichissement :

- **`poids_porte_kg`** pour le trot : les trotteurs ne portent pas de poids additionnel
- **`handicap_valeur`** pour les non-handicaps : seules les courses a handicap ont une valeur de handicap
- **`taux_reclamation_euros`** pour les non-reclamer : seules les courses a reclamer (~5.7% du total) ont ce champ
- **`temps_ms`** pour les non-partants/disqualifies : un cheval qui ne termine pas la course n'a pas de temps
- **`ecart_precedent`** pour le 1er : le premier partant n'a pas d'ecart avec le precedent

