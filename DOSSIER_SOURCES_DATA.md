# DOSSIER COMPLET — SOURCES DE DONNÉES POUR MODÈLES PRÉDICTIFS
# Courses Hippiques France — Mars 2026

---

## TABLE DES MATIÈRES
1. Sources déjà collectées (recap)
2. Nouvelles sources à collecter
3. Techniques anti-détection scraping
4. Mapping données → modules (68 modèles)
5. Planning d'extraction

---

## 1. SOURCES DÉJÀ COLLECTÉES

| Source | Script | Données | Volume | Période |
|--------|--------|---------|--------|---------|
| PMU API offline | 02, 02b | Courses, partants, cotes, équipements, poids, musique | ~2.3M partants | 2013→2026 (en cours) |
| PMU API online | 01, 04 | Calendrier, résultats, rapports définitifs | 41K réunions | 2013→2026 |
| Nanaelie/Open PMU | 16 | Arrivées top 5 historiques | 3 295 courses | 2004→2013 |
| Meteostat | 13 | Température, humidité, vent, précipitations | 31 778 courses | 2013→2026 |
| PedigreeQuery.com | 14 | Pedigree 4 générations (pur-sang) | ~18K/58K chevaux | En cours |
| Le Trot | 02b_scraper | Courses trot hors PMU | 36K courses | Variable |
| Equidia | patch_terrain | État terrain (pénétromètre) | ~100 hippodromes | Ponctuel |

---

## 2. NOUVELLES SOURCES À COLLECTER

### SOURCE 1 — IFCE/SIRE Fichier des Équidés ⭐⭐⭐
- **URL** : https://www.data.gouv.fr/datasets/fichier-des-equides
- **Données** : 4 millions d'équidés immatriculés en France depuis 1976
- **Champs** : race (breed), sexe (M/F/H), robe, date_naissance, pays_naissance, nom, destiné_consommation, date_mort
- **Format** : CSV, Licence Ouverte
- **Temps d'extraction** : ⚡ 2 minutes (un seul fichier à télécharger)
- **Risque anti-bot** : AUCUN (téléchargement direct data.gouv.fr)
- **Fréquence MAJ** : Annuelle (dernière : mars 2025)
- **Jointure** : nom_cheval → features (âge exact jour-J, race confirmée, pays origine, robe, vivant/mort)

### SOURCE 2 — Turf BZH Export CSV ⭐⭐⭐
- **URL** : https://www.turf.bzh/export-journee.php
- **Données** : Export complet d'une journée de courses (58 colonnes)
- **Champs** : tous les partants + rapports simple gagnant/placé, musique, poids, distance, terrain, discipline, hippodrome, cotes, etc.
- **Format** : CSV téléchargeable par jour
- **Temps d'extraction** : 🔧 ~4h pour 10 ans (1 requête/jour × 3650 jours, 1 req/sec)
- **Risque anti-bot** : FAIBLE (site communautaire, pas de protection lourde)
- **Jointure** : date + hippodrome + num_course → rapports définitifs historiques

### SOURCE 3 — Aspiturf Base de données ⭐⭐
- **URL** : https://aspiturf.com/
- **Données** : Base complète depuis 2014, CSV téléchargeables par course
- **Champs** : Partants, résultats, cotes, conditions
- **Format** : CSV (inscription gratuite requise)
- **Temps d'extraction** : 🔧 ~6h (scraping après inscription)
- **Risque anti-bot** : MOYEN (nécessite session/cookies après login)
- **Jointure** : Validation croisée avec données PMU

### SOURCE 4 — rpscrape (Racing Post) ⭐⭐⭐
- **URL** : https://github.com/joenano/rpscrape
- **Données** : Résultats Racing Post pour courses françaises
- **Champs** : RPR (Racing Post Rating), Top Speed, going, draw, class, distance, prize, odds, trainer, jockey, weight, age, OR (Official Rating)
- **Format** : CSV configurable
- **Temps d'extraction** : 🔧 ~8-12h pour toutes les courses FR flat+jumps (2010-2026)
- **Risque anti-bot** : ÉLEVÉ (Racing Post a des protections anti-scraping)
- **Prérequis** : Compte Racing Post (gratuit), Python 3.13+, credentials dans .env
- **Jointure** : nom_cheval + date + hippodrome → RPR, Top Speed (ratings indépendants)
- **⚠️ Important** : Ne couvre que le galop (flat/jumps), PAS le trot

### SOURCE 5 — LeTrot Records de piste ⭐⭐
- **URL** : https://www.letrot.com/stats/champrecords/hippodrome
- **Données** : Records par hippodrome, par distance, par spécialité (trot attelé/monté)
- **Champs** : hippodrome, distance, record_temps, cheval_recordman, date_record, driver
- **Format** : HTML (scraping requis)
- **Temps d'extraction** : 🔧 ~30 min (236 hippodromes × quelques pages)
- **Risque anti-bot** : FAIBLE (site institutionnel, pas de protection)
- **Jointure** : hippodrome + distance → feature "temps vs record piste"

### SOURCE 6 — IFCE Stats & Cartes ⭐⭐
- **URL** : https://statscartes.ifce.fr/dashboard/47
- **Données** : Statistiques officielles de la filière courses (trot, plat, obstacle)
- **Champs** : stats par hippodrome, par entraîneur, par jockey, par cheval (agrégées)
- **Format** : JSON (API interne du dashboard)
- **Temps d'extraction** : 🔧 ~1h (quelques endpoints JSON)
- **Risque anti-bot** : AUCUN (données publiques IFCE)
- **Jointure** : hippodrome/jockey/entraîneur → stats officielles filière

### SOURCE 7 — PMU Performances détaillées ⭐⭐⭐
- **URL** : https://online.turfinfo.api.pmu.fr/rest/client/61/programme/{date}/R{n}/C{n}/performances-detaillees/pretty
- **Données** : 5 dernières performances détaillées de chaque cheval
- **Champs** : date, hippodrome, distance, position, écart, temps, terrain, cote, nb_partants (pour chaque perf)
- **Format** : JSON
- **Temps d'extraction** : 🔧 ~20-30h (1 requête par course × ~200K courses)
- **Risque anti-bot** : MOYEN (API non-documentée PMU, mêmes précautions que script 02)
- **Jointure** : partant_uid → forme détaillée (5 derniers résultats avec contexte complet)

### SOURCE 8 — Kaggle nanaelie PMU Dataset ⭐
- **URL** : https://www.kaggle.com/datasets/nanaelie/historical-pmu-horse-racing-dataset
- **Données** : Dataset PMU historique bulk (2004-2026)
- **Format** : CSV/Parquet (download Kaggle)
- **Temps d'extraction** : ⚡ 5 min (download direct avec kaggle CLI)
- **Risque anti-bot** : AUCUN (Kaggle API officielle)
- **Prérequis** : Compte Kaggle + token API
- **Jointure** : Validation/backup des données PMU existantes

### SOURCE 9 — Boturfers Stats Hippodromes ⭐
- **URL** : https://www.boturfers.fr/hippodrome
- **Données** : Stats par hippodrome (nb courses/an, rapport moyen, discipline dominante)
- **Champs** : hippodrome, nb_courses_12mois, rapport_moyen_sg, disciplines
- **Format** : HTML (scraping)
- **Temps d'extraction** : 🔧 ~15 min (250 hippodromes, 1 page chacun)
- **Risque anti-bot** : FAIBLE
- **Jointure** : hippodrome → stats méta-hippodrome

### SOURCE 10 — France Galop Valeurs Handicap ⭐⭐
- **URL** : https://www.france-galop.com/en/horses-and-people/ratings
- **Données** : Valeurs handicap officielles (échelle 20-62)
- **Format** : Web (scraping) — ⚠️ site anti-scraping (JavaScript lourd)
- **Temps d'extraction** : 🔧 Difficile à estimer, site protégé
- **Alternative** : Le champ "valeur" est déjà dans l'API PMU pour les courses handicap
- **Jointure** : cheval → valeur handicap officielle

---

## 3. TECHNIQUES ANTI-DÉTECTION SCRAPING

### 3.1 — Headers & User-Agent

```python
import random

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}
```

### 3.2 — Pause intelligente (pas régulière)

```python
import time, random

def smart_pause(base=1.0, jitter=0.5):
    """Pause avec variation aléatoire pour imiter un humain"""
    pause = base + random.uniform(-jitter, jitter)
    # Parfois une pause plus longue (comme si on lisait la page)
    if random.random() < 0.1:  # 10% du temps
        pause += random.uniform(3, 8)
    time.sleep(max(0.3, pause))
```

### 3.3 — Session persistante avec cookies

```python
import requests

session = requests.Session()
# Visiter la page d'accueil d'abord (comme un humain)
session.get("https://www.site.com/", headers=HEADERS)
time.sleep(2)
# Puis commencer le scraping
response = session.get("https://www.site.com/data", headers=HEADERS)
```

### 3.4 — Rotation de User-Agent par session

```python
def new_session():
    """Créer une nouvelle session avec UA aléatoire"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "fr-FR,fr;q=0.9",
    })
    return s

# Changer de session toutes les ~100 requêtes
session = new_session()
for i, url in enumerate(urls):
    if i % 100 == 0 and i > 0:
        session.close()
        session = new_session()
        time.sleep(random.uniform(5, 15))  # Pause entre sessions
    response = session.get(url, headers=HEADERS)
    smart_pause()
```

### 3.5 — Respecter robots.txt

```python
from urllib.robotparser import RobotFileParser

def check_robots(base_url, path):
    """Vérifier si le scraping est autorisé"""
    rp = RobotFileParser()
    rp.set_url(f"{base_url}/robots.txt")
    rp.read()
    return rp.can_fetch("*", f"{base_url}{path}")
```

### 3.6 — Retry avec backoff exponentiel

```python
def fetch_with_retry(session, url, max_retries=3):
    """Retry intelligent avec backoff"""
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 429:  # Rate limited
                wait = 2 ** attempt * 30  # 30s, 60s, 120s
                print(f"Rate limited, attente {wait}s...")
                time.sleep(wait)
                continue
            if response.status_code == 403:  # Blocked
                session.close()
                session = new_session()
                time.sleep(random.uniform(30, 60))
                continue
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            time.sleep(2 ** attempt * 5)
    return None
```

### 3.7 — Horaires de scraping

```python
# Scraper pendant les heures "normales" de navigation
# Éviter 2h-6h du matin (suspicieux)
# Meilleur créneau : 9h-23h (trafic normal)
from datetime import datetime

def is_good_time():
    hour = datetime.now().hour
    return 8 <= hour <= 23
```

### 3.8 — Cache systématique

```python
import hashlib, json, os

def cached_fetch(session, url, cache_dir="cache"):
    """Ne jamais re-télécharger une URL déjà en cache"""
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    cache_file = os.path.join(cache_dir, f"{url_hash}.json")

    if os.path.exists(cache_file):
        with open(cache_file) as f:
            return json.load(f)

    response = fetch_with_retry(session, url)
    if response:
        data = response.json()
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump(data, f)
        return data
    return None
```

---

## 4. MAPPING DONNÉES → 68 MODULES

### Phase 1 — Infrastructure data (modules 1-8)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 1. data_ingestion_manager | Toutes les sources brutes | PMU API, SIRE, Turf BZH, Nanaelie |
| 2. data_schema_validator | Schémas de validation | Généré à partir des données |
| 3. historical_dataset_builder | Dataset historique complet | PMU 02+02b fusionné |
| 4. data_quality_monitor | Métriques qualité | Toutes sources (cross-validation) |
| 5. missing_values_handler | Taux de remplissage | Toutes features |
| 6. outlier_cleaner | Détection outliers | Cotes, poids, temps |
| 7. data_normalizer | Normalisation | Toutes features numériques |
| 8. cache_manager | Cache multi-niveaux | Toutes sources |

### Phase 2 — Feature Engineering (modules 9-18)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 9. advanced_feature_generator | Base complète | PMU (02), SIRE (âge, race, robe) |
| 10. rolling_stats_generator | Historique séquentiel | PMU historique 2013-2026 |
| 11. temporal_feature_builder | Dates, saisons, heure | PMU (heure_depart), calendrier |
| 12. odds_feature_builder | Cotes multi-sources | PMU cotes, **Turf BZH rapports**, **Betfair BSP** |
| 13. jockey_trainer_synergy_builder | Paires jockey×cheval, jockey×entraîneur | PMU (06), historique croisé |
| 14. pedigree_feature_builder | Pedigree complet | PedigreeQuery (14), **SIRE** (race, pays), PMU (père, mère) |
| 15. track_bias_detector | Stats par hippodrome | PMU historique, **LeTrot records**, **Boturfers stats**, **IFCE Stats&Cartes** |
| 16. pace_profile_builder | Positions en course, temps | PMU (11 sectionals), **LeTrot records** |
| 17. sectional_feature_builder | Temps sectoriels | PMU (11), **rpscrape** (Top Speed) |
| 18. field_strength_builder | Force du lot | PMU cotes, historiques adversaires |

### Phase 3 — Sélection features (modules 19-20)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 19. selection_auto_features | Matrice features complète | Master builder output |
| 20. feature_subset_optimizer | Labels + features | Labels builder + features |

### Phase 4 — Modèles ML core (modules 21-25)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 21-25. LR/RF/XGB/LGBM/CatBoost | Features matrix + labels | Output Phase 2-3 |

→ Plus il y a de features en Phase 2, plus ces modèles sont puissants.

### Phase 5 — Deep Learning (modules 26-30)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 26. MLP | Features tabulaires | Idem Phase 4 |
| 27. LSTM | **Séquences temporelles** ordonnées | Historique course par course (rolling) |
| 28. GRU | Idem LSTM | Idem |
| 29. TabNet | Features tabulaires (attention) | Idem Phase 4 |
| 30. TFT (Temporal Fusion Transformer) | Séquences + features statiques | Historique + **météo séquentielle** |

→ LSTM/GRU/TFT ont besoin de **séquences** : les N dernières courses de chaque cheval avec toutes les features. C'est pourquoi les **performances détaillées** (source 7) sont critiques.

### Phase 6 — Modèles avancés (modules 31-34)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 31. GNN (Graph Neural Network) | **Relations entre entités** | Graphe jockey↔cheval↔entraîneur↔hippodrome |
| 32. Bayesian NN | Distributions de probabilité | Features + incertitude |
| 33. Survival model | Temps entre courses, durée carrière | PMU historique, **SIRE** (date_mort) |
| 34. Quantile regressor | Distribution des performances | Features + labels |

→ Le GNN utilise le **graphe social** du turf : qui monte quel cheval, pour quel entraîneur, sur quel hippodrome. Plus on a de données relationnelles, mieux il fonctionne.
→ Le survival model bénéficie directement du **SIRE** (date naissance/mort = durée de carrière).

### Phase 7 — AutoML (modules 35-37)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 35-37. AutoGluon/TPOT/H2O | Features matrix complète | Output Phase 2-3 |

### Phase 8 — Fusion (modules 38-40)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 38-40. Stacking/Blending/Meta | Prédictions de tous les modèles | Outputs Phases 4-7 |

### Phase 9 — Calibration (modules 41-43)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 41-43. Calibration | Probabilités prédites vs réelles | Prédictions + résultats réels |

### Phase 10 — Outsiders (modules 44-46)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 44. anomalie_detector | **Cotes anormales**, patterns cachés | PMU cotes, **Turf BZH** (rapports historiques), **Betfair BSP** |
| 45. retour_forme_hidden | Signaux faibles de retour en forme | Historique détaillé, **performances détaillées PMU** (source 7) |
| 46. gan_turf | Données d'entraînement GAN | Dataset complet pour génération synthetic |

→ L'anomalie detector a besoin de **plusieurs sources de cotes** pour détecter les écarts. Turf BZH + Betfair BSP sont essentiels.

### Phase 11 — Betting (modules 47-50)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 47. roi_predictor | Historique rapports/cotes | **Turf BZH** (rapports définitifs), PMU |
| 48. value_hunter_rl | Cotes marché vs probabilités modèle | Multi-sources cotes |
| 49. meta_selector | Performance historique des modèles | Logs + résultats |
| 50. ZURI_OUTSIDER_ENGINE | Signaux outsider | Tous les détecteurs Phase 10 |

### Phase 12 — Simulation (modules 51-52)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 51. monte_carlo_simulator | Distributions de performance | Features + historique |
| 52. race_simulation_engine | Positions, vitesses, terrain | **LeTrot records**, sectionals, météo |

### Phase 13 — Mises (modules 53-57)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 53-57. Kelly/Sizing/Tickets | Probabilités calibrées + cotes marché | Outputs Phase 9 + cotes live |

### Phase 14 — Adaptation (modules 58-60)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 58. auto_recalibration | Résultats récents | PMU résultats (script 04) |
| 59. model_decay_detector | Performance rolling | Logs prédictions |
| 60. concept_drift_detector | Distribution features au fil du temps | Features historiques |

### Phase 15 — Monitoring (modules 61-63)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 61-63. Monitoring/Dashboard/Alerts | Métriques temps réel | Tous les modules |

### Phase 16 — Orchestration (modules 64-68)

| Module | Données nécessaires | Sources |
|--------|-------------------|---------|
| 64-68. Pipeline/Scheduler/Controller | Configuration + dépendances | Tous les modules |

---

## 5. PLANNING D'EXTRACTION

### Immédiat (pendant que scripts 02/14 tournent)

| Priorité | Source | Temps | Action |
|----------|--------|-------|--------|
| 1 | SIRE/IFCE | 2 min | Download CSV data.gouv.fr |
| 2 | LeTrot records | 30 min | Script scraping |
| 3 | Boturfers stats | 15 min | Script scraping |
| 4 | IFCE Stats&Cartes | 1h | Script JSON API |

### Court terme (après config)

| Priorité | Source | Temps | Action |
|----------|--------|-------|--------|
| 5 | Turf BZH | 4h | Script CSV par jour |
| 6 | rpscrape France | 8-12h | Install + run |
| 7 | PMU perfs détaillées | 20-30h | Nouveau script |
| 8 | Aspiturf | 6h | Inscription + script |

### Total estimé : ~45h de scraping (en parallèle = ~15h réelles)

---

## RÉSUMÉ IMPACT

| Source | Features ajoutées | Modules impactés |
|--------|------------------|-----------------|
| SIRE | age_exact, race, robe, pays_naissance, vivant | 9, 14, 31, 33 |
| Turf BZH | rapport_sg, rapport_sp (historiques) | 12, 44, 47, 48 |
| rpscrape | RPR, Top_Speed, OR | 9, 17, 18 |
| LeTrot records | record_piste, pct_record | 15, 16, 52 |
| Boturfers | avg_rapport_hippo, nb_courses_hippo | 15, 52 |
| IFCE Stats | stats_officielles_filiere | 15 |
| PMU perfs | forme_detaillee_5 (5 sous-features) | 10, 27, 28, 30, 45 |

**Total : +25-35 nouvelles features → objectif 120-150 features**
