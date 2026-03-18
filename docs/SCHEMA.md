# Schemas des donnees

Documentation des schemas principaux du pipeline hippique : tables masters, champs, types, et cles de jointure.

---

## Cles de jointure entre tables

| Cle | Format | Description | Tables |
|-----|--------|-------------|--------|
| `course_uid` | `YYYY-MM-DD\|RX\|CY` | Identifiant unique d'une course | courses_master, partants_master, meteo_master, rapports_master, marche_master |
| `partant_uid` | `YYYY-MM-DD\|RX\|CY\|PZ` | Identifiant unique d'un partant dans une course | partants_master, equipements_master, marche_master |
| `reunion_uid` | `YYYY-MM-DD\|RX` | Identifiant unique d'une reunion | courses_master, meteo_master |
| `nom_cheval` | MAJUSCULES sans accents | Nom normalise du cheval | pedigree_master, horse_stats_master, partants_master |
| `horse_id` | Entier PMU | Identifiant numerique PMU du cheval | partants_master (quand disponible) |
| `date_reunion_iso` | `YYYY-MM-DD` | Date de la reunion | Toutes les tables |
| `hippodrome_normalise` | MAJUSCULES sans accents | Nom normalise de l'hippodrome | courses_master, meteo_master |

### Diagramme des jointures

```
partants_master (2.7M records)
    |-- course_uid --> courses_master (257K)
    |-- course_uid --> meteo_master (479K)
    |-- course_uid --> rapports_master (217K)
    |-- partant_uid --> equipements_master (573K)
    |-- partant_uid --> marche_master (151K)
    |-- nom_cheval --> pedigree_master (1.4M)
    |-- nom_cheval --> horse_stats_master (80K)
```

---

## 1. partants_master (table principale)

Le fichier maitre central contenant toutes les informations par partant et par course.

**Records** : ~2,930,290 | **Champs** : 66+ | **Cle primaire** : `partant_uid`

### Champs d'identification

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| partant_uid | string | Identifiant unique du partant | 100% |
| course_uid | string | Identifiant unique de la course | 100% |
| reunion_uid | string | Identifiant de la reunion | 100% |
| date_reunion_iso | string (date) | Date de la reunion ISO 8601 | 100% |
| numero_reunion | int | Numero de la reunion dans la journee | 100% |
| numero_course | int | Numero de la course dans la reunion | 100% |
| num_pmu | int | Numero du partant dans la course | 100% |
| cle_partant | string | Cle technique interne | ~90% |
| horse_id | int | Identifiant PMU du cheval | ~80% |

### Champs cheval

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| nom_cheval | string | Nom du cheval | 100% |
| age | int | Age du cheval | ~95% |
| sexe | string | Sexe (M/F/H) | ~95% |
| race | string | Race (PS/AQPS/TF/...) | ~80% |
| robe | string | Couleur de la robe | ~80% |
| pays_cheval | string | Pays d'origine | ~70% |
| pays_entrainement | string | Pays d'entrainement | ~60% |
| eleveur | string | Nom de l'eleveur | ~40% |
| jument_pleine | bool | Jument pleine (gestante) | ~95% |
| is_inedit | bool | Premier depart (debutant) | 100% |

### Champs pedigree (basique)

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| pere | string | Nom du pere | ~85% |
| mere | string | Nom de la mere | ~85% |
| pere_mere | string | Nom du pere de la mere | ~60% |

### Champs course / contexte

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| discipline | string | Discipline (TROT_ATTELE, PLAT, etc.) | 100% |
| distance | int | Distance en metres | 100% |
| hippodrome_normalise | string | Hippodrome normalise | 100% |
| place_corde | int | Position a la corde / stall | ~80% |

### Champs jockey / entraineur

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| jockey_driver | string | Nom du jockey ou driver | ~98% |
| jockey_driver_change | bool | Changement de jockey | ~90% |
| entraineur | string | Nom de l'entraineur | ~95% |
| proprietaire | string | Nom du proprietaire | ~85% |

### Champs performance / resultat

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| position_arrivee | int | Position a l'arrivee | ~95% |
| is_gagnant | bool | Vainqueur de la course | 100% |
| is_place | bool | Dans les 3 premiers | 100% |
| is_disqualifie | bool | Disqualifie | ~83% |
| ecart_precedent | string | Ecart avec le precedent | ~70% |
| temps_ms | int | Temps en millisecondes | ~60% |
| reduction_km_ms | float | Reduction kilometrique en ms | ~50% |
| incident | string | Incident en course | ~30% |
| commentaire_apres_course | string | Commentaire apres course | ~20% |
| statut | string | Statut (partant/non-partant) | 100% |

### Champs financiers

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| cote_finale | float | Cote finale PMU | ~90% |
| cote_reference | float | Cote de reference | ~80% |
| proba_implicite | float | 1/cote (probabilite implicite) | ~90% |
| gains_carriere_euros | float | Gains de carriere en euros | ~85% |
| gains_annee_euros | float | Gains de l'annee en euros | ~80% |
| engagement | float | Montant de l'engagement | ~70% |
| supplement_euros | float | Supplement paye | ~30% |
| taux_reclamation_euros | float | Taux de reclamation | ~10% |

### Champs statistiques carriere

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| nb_courses_carriere | int | Nombre de courses en carriere | ~85% |
| nb_victoires_carriere | int | Nombre de victoires | ~85% |
| nb_places_carriere | int | Nombre de places (top 3) | ~85% |
| nb_places_2eme | int | Nombre de 2emes places | ~40% |
| nb_places_3eme | int | Nombre de 3emes places | ~40% |
| musique | string | Chaine de forme (ex: "1p3p2a") | ~90% |

### Champs equipement / poids

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| oeilleres | string | Type d'oeilleres | ~95% |
| deferre | string | Type de deferre | ~95% |
| poids_base_kg | float | Poids de base en kg | ~80% |
| poids_porte_kg | float | Poids porte en kg | ~85% |
| poids_monte_change | bool | Changement de monte | ~80% |
| surcharge_decharge_kg | float | Surcharge ou decharge en kg | ~60% |
| handicap_valeur | float | Valeur handicap | ~40% |
| handicap_distance_m | int | Distance handicap (trot) | ~30% |

### Champs techniques

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| source | string | Source de la donnee (PMU/LETROT/...) | 100% |
| timestamp_collecte | string (datetime) | Date/heure de collecte | 100% |
| avis_entraineur | string | Avis de l'entraineur | ~15% |
| allure | string | Allure (trot) | ~40% |

---

## 2. courses_master

Table des courses avec conditions et resultats.

**Records** : ~257,806 | **Champs** : 36 | **Cle primaire** : `course_uid`

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| course_uid | string | Identifiant unique de la course | 100% |
| reunion_uid | string | Identifiant de la reunion | 100% |
| cle_course | string | Cle technique interne | ~90% |
| date_reunion_iso | string (date) | Date ISO | 100% |
| numero_reunion | int | Numero reunion | 100% |
| numero_course | int | Numero course | 100% |
| hippodrome | string | Hippodrome (brut) | 100% |
| hippodrome_normalise | string | Hippodrome normalise | 100% |
| pays | string | Pays | ~95% |
| discipline | string | Discipline (TROT_ATTELE, PLAT, etc.) | 100% |
| specialite | string | Specialite detaillee | ~90% |
| distance | int | Distance en metres | 100% |
| type_piste | string | Type de piste (gazon, sable, PSF, etc.) | ~90% |
| corde | string | Corde (gauche/droite) | ~85% |
| penetrometre | string | Etat du terrain | ~44% |
| mode_depart | string | Mode de depart (autostart, volte) | ~70% |
| conditions_texte | string | Conditions textuelles completes | ~95% |
| condition_age | string | Condition d'age | ~49% |
| condition_sexe | string | Condition de sexe | ~80% |
| categorie | string | Categorie de course | ~85% |
| libelle | string | Libelle de la course | ~95% |
| allocation_totale | float | Allocation totale en euros | ~90% |
| allocation_1er | float | Allocation du 1er | ~85% |
| nombre_partants | int | Nombre de partants | 100% |
| heure_depart | string (time) | Heure de depart | ~95% |
| parcours | string | Description du parcours | ~60% |
| ordre_arrivee | string/list | Ordre d'arrivee | ~90% |
| duree_course_ms | int | Duree en ms | ~50% |
| paris_types | list | Types de paris disponibles | ~90% |
| course_trackee | bool | Course avec timing track | ~80% |
| replay_disponible | bool | Replay video disponible | ~70% |
| incidents | string | Incidents de course | ~30% |
| url_source | string | URL source | ~50% |
| source | string | Source de la donnee | 100% |
| statut | string | Statut de la course | 100% |
| timestamp_collecte | string (datetime) | Horodatage de collecte | 100% |

---

## 3. pedigree_master

Table des pedigrees avec jusqu'a 4 generations d'ascendance.

**Records** : 1,413,913 chevaux | **Champs** : 56 | **Cle primaire** : `nom_cheval`

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| nom_cheval | string | Nom du cheval | 100% |
| sexe | string | Sexe (M/F/H) | 100% |
| race | string | Race | 100% |
| robe | string | Couleur de la robe | 94% |
| annee_naissance | int | Annee de naissance | 90% |
| pays_naissance | string | Pays de naissance | ~70% |
| pere | string | Nom du pere (sire) | ~20% |
| mere | string | Nom de la mere (dam) | ~20% |
| pere_pere | string | Grand-pere paternel | ~14% |
| mere_pere | string | Grand-pere maternel (broodmare sire) | ~14% |
| pere_mere | string | Grand-mere paternelle | ~14% |
| mere_mere | string | Grand-mere maternelle | ~14% |
| sire_* | string | Lignee paternelle 3e/4e generation | ~14% |
| dam_* | string | Lignee maternelle 3e/4e generation | ~14% |
| source | string | Source (PMU/PedigreeQuery/SIRE) | 100% |

**Priorite merge** : `pedigree_query < canalturf < scraper_14 < PMU_08 < partants_enrichis < SIRE_IFCE`

---

## 4. meteo_master

Table meteo par course, fusionnant Meteostat + Open-Meteo + Meteo France.

**Records** : 479,377 | **Champs** : 33 (apres enrichissement) | **Cle de jointure** : `course_uid`

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| course_uid | string | Identifiant de la course | 100% |
| date_reunion_iso | string | Date | 100% |
| hippodrome | string | Hippodrome | 100% |
| temperature_c | float | Temperature en Celsius | 100% |
| temp_min_c | float | Temperature minimale | ~95% |
| temp_max_c | float | Temperature maximale | ~95% |
| humidity_pct | float | Humidite relative (%) | 100% |
| precipitation_mm | float | Precipitations horaires (mm) | 100% |
| precip_total_mm | float | Precipitations journalieres (mm) | ~95% |
| wind_speed_kmh | float | Vitesse du vent (km/h) | 100% |
| wind_gusts_kmh | float | Rafales (km/h) | ~90% |
| wind_direction_deg | float | Direction du vent (degres) | ~80% |
| weather_code | int | Code meteo WMO | ~95% |
| type_piste | string | Type de piste | 100% |
| penetrometre | string | Etat du terrain textuel | ~44% |
| nb_sources | int | Nombre de sources croisees | 100% |

### Champs enrichis (post-processing)

| Champ | Type | Description |
|-------|------|-------------|
| terrain_category | string | Categorie de terrain (bon/souple/lourd/...) |
| penetrometre_numeric | float | Penetrometre converti en numerique (2.0-5.5) |
| is_psf | bool | Piste en sable fibre |
| meteo_score | float | Score meteo composite (confort) |
| is_rainy | bool | Flag pluie |
| is_windy | bool | Vent > 30 km/h |
| is_hot | bool | Temperature > 30C |
| is_cold | bool | Temperature < 5C |
| comfort_index | float | Index de confort combine |
| wind_impact | float | Score perturbation vent |
| ground_moisture | float | Humidite du sol estimee |

---

## 5. rapports_master

Table des rapports de paris (dividendes) par course.

**Records** : 217,569 | **Champs** : 329 | **Cle de jointure** : `course_uid`

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| course_uid | string | Identifiant de la course | 100% |
| discipline | string | Discipline | 98% |
| distance | int | Distance | 98% |
| hippodrome | string | Hippodrome | 98% |
| simple_gagnant | dict | Rapports simple gagnant | ~95% |
| simple_place | dict | Rapports simple place | ~90% |
| couple_gagnant | dict | Rapports couple gagnant | ~85% |
| couple_place | dict | Rapports couple place | ~80% |
| tierce_* | dict | Rapports tierce (ordre/desordre) | ~70% |
| quarte_* | dict | Rapports quarte | ~50% |
| quinte_* | dict | Rapports quinte | ~30% |
| rapport_national_* | float | Rapports nationaux (mise 200) | ~90% |
| rapport_internet_* | float | Rapports internet (mise 100) | ~60% |

### Champs enrichis (post-processing)

| Champ | Type | Description |
|-------|------|-------------|
| jour_semaine | int | Jour de la semaine (0-6) |
| mois | int | Mois (1-12) |
| saison | string | Saison (printemps/ete/automne/hiver) |
| is_quinte | bool | Course Quinte+ |
| is_quarte | bool | Course Quarte+ |
| is_tierce | bool | Course Tierce |
| is_surprise | bool | Rapport gagnant > seuil surprise |
| distance_category | string | Sprint/mile/intermediaire/long/marathon |
| rapport_gagnant_euros | float | Rapport gagnant en euros |

---

## 6. marche_master

Table des donnees de marche (cotes, enjeux, combinaisons).

**Records** : 151,258 | **Champs** : 31 (apres enrichissement) | **Cle de jointure** : `partant_uid` ou `course_uid`

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| partant_uid | string | Identifiant du partant | 100% |
| course_uid | string | Identifiant de la course | 100% |
| cote_finale | float | Cote finale | 99% |
| cote_reference | float | Cote de reference | ~80% |
| proba_implicite | float | Probabilite implicite | 99% |
| masse_enjeux | float | Masse d'enjeux | ~70% |

### Champs enrichis (post-processing)

| Champ | Type | Description |
|-------|------|-------------|
| cote_category | string | Categorie de cote (favori/outsider/...) |
| value_ratio | float | Ratio de valeur |
| value_indicator | string | Indicateur de valeur (surcote/souscote) |
| proba_category | string | Categorie de probabilite |
| taille_champ | int | Taille du champ |
| popularite | int | Rang de popularite |

---

## 7. equipements_master

Table des equipements et poids par partant.

**Records** : 573,111 | **Champs** : 36 (apres enrichissement) | **Cle de jointure** : `partant_uid`

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| partant_uid | string | Identifiant du partant | 100% |
| oeilleres | string | Type d'oeilleres | 100% |
| deferre | string | Type de deferre | 99% |
| poids_porte_kg | float | Poids porte en kg | 99% |
| poids_base_kg | float | Poids de base | ~80% |
| surcharge_decharge_kg | float | Surcharge/decharge | ~60% |

### Champs enrichis (post-processing)

| Champ | Type | Description |
|-------|------|-------------|
| poids_category | string | Categorie de poids (leger/moyen/lourd) |
| poids_direction | string | Direction du poids vs precedent |
| oeilleres_bool | bool | Oeilleres oui/non |
| deferre_norm | string | Deferre normalise |
| equipment_change_score | float | Score de changement d'equipement |
| position_poids | string | Position dans le classement poids |

---

## 8. horse_stats_master

Table des statistiques agregees par cheval.

**Records** : 80,656 chevaux | **Champs** : 39 (apres enrichissement) | **Cle de jointure** : `nom_cheval`

| Champ | Type | Description | Taux remplissage |
|-------|------|-------------|-----------------|
| nom_cheval | string | Nom du cheval | 100% |
| nb_courses_total | int | Total courses | 100% |
| nb_victoires | int | Total victoires | 100% |
| nb_places | int | Total places | 100% |
| gains_total | float | Gains totaux en euros | ~95% |
| nb_disciplines | int | Nombre de disciplines | 100% |
| nb_hippodromes | int | Nombre d'hippodromes | 100% |
| anciennete_jours | int | Jours depuis premier depart | 100% |

### Champs enrichis (post-processing)

| Champ | Type | Description |
|-------|------|-------------|
| class_category | string | Categorie de classe |
| gains_par_course | float | Gains moyens par course |
| performance_category | string | Categorie de performance |
| specialiste_discipline | bool | Specialiste d'une discipline |
| distance_pref_category | string | Preference de distance |
| experience_category | string | Categorie d'experience |
| is_en_forme | bool | Cheval en forme recente |
| is_en_baisse | bool | Cheval en baisse de forme |

---

## 9. Autres tables

### horse_profiles_externes (CanalTurf + TurfoStats + Geny)
**Records** : 9,159 chevaux | **Champs** : 44

Donnees de profil cheval depuis les sites communautaires. Jointure par `nom_cheval`.

### courses_externes
**Records** : 8,332 | **Champs** : 7

Donnees de course depuis sites externes. Jointure par `course_uid` ou `date + hippodrome`.

### performances_master (a creer)
**Records** : ~487K partants | **Champs** : ~50

5 dernieres performances detaillees par partant. Necessite 64 GB RAM pour le merge.

---

## Mapping type piste

| Valeur brute | Valeur normalisee |
|-------------|-------------------|
| GAZON | gazon |
| PSF | psf |
| SABLE | sable |
| CENDRÉE | cendree |
| FIBRE | fibre |
| HERBE | gazon |
| ALL WEATHER | psf |

## Mapping penetrometre

| Valeur textuelle | Valeur numerique |
|-----------------|-----------------|
| tres sec | 2.0 |
| bon | 2.8 |
| bon souple | 3.3 |
| souple | 3.8 |
| tres souple | 4.3 |
| lourd | 4.8 |
| tres lourd | 5.5 |
