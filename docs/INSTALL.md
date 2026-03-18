# Guide d'installation

---

## Prerequis

- **Python** : 3.12+ (teste avec 3.12)
- **OS** : Windows 11 (dev principal), macOS (MacBook M1 pour collecte), Linux (compatible)
- **RAM** : 16 GB minimum pour la collecte, 64 GB recommande pour le merge et le feature engineering
- **Disque** : 100 GB d'espace libre (donnees brutes ~52 GB + masters ~10 GB + features + backups)
- **GPU** : Optionnel (RTX 5070 Ti pour Phase 2 -- modeles ML)

---

## Installation

### 1. Cloner le repository

```bash
git clone https://github.com/spins-ai/turf-data-pipeline.git
cd turf-data-pipeline
```

Note : le repository contient uniquement le code (scripts .py + docs .md). Les donnees (~52 GB) ne sont pas dans git (exclues via .gitignore).

### 2. Creer un environnement virtuel

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 3. Installer les dependances

```bash
pip install -r requirements.txt
```

Le fichier `requirements.txt` contient :

```
requests
beautifulsoup4
lxml
ijson
pandas
numpy
urllib3
meteostat
scikit-learn
scipy
xgboost
lightgbm
catboost
pyarrow
pyyaml
```

### 4. Dependances optionnelles

Pour certaines fonctionnalites specifiques :

```bash
# Pour le scraping avance (anti-detection)
pip install fake-useragent cloudscraper

# Pour les notebooks d'analyse
pip install jupyter matplotlib seaborn

# Pour DuckDB (requetes SQL sur les donnees)
pip install duckdb

# Pour le format Parquet compresse zstd
pip install zstandard

# Pour Racing Post (rpscrape)
pip install selenium webdriver-manager
```

---

## Configuration

### Fichier .env (optionnel)

Creer un fichier `.env` a la racine pour les cles API :

```
# Kaggle (pour 15_download_external_datasets.py)
KAGGLE_USERNAME=votre_username
KAGGLE_KEY=votre_api_key

# Meteo France (pour 35_meteo_france_api.py)
METEOFRANCE_API_KEY=votre_cle

# Smarkets (pour 30_smarkets_exchange.py)
SMARKETS_API_TOKEN=votre_token
```

### Fichier config YAML (optionnel)

Un fichier `config_exemple.yaml` est fourni comme modele :

```bash
cp config_exemple.yaml config.yaml
# Editer config.yaml avec vos parametres
```

---

## Structure des donnees

Les donnees collectees sont stockees dans `output/` avec un sous-dossier par source :

```
output/
  01_calendrier_reunions/
  02_liste_courses/
    courses_brut.jsonl
    courses_normalisees.jsonl
    partants_brut.jsonl
    partants_normalises.jsonl
  04_resultats/
  05_historique_chevaux/
  ...
```

Les fichiers masters fusionnes sont dans `data_master/` :

```
data_master/
  pedigree_master.json
  pedigree_master.parquet
  meteo_master.json
  meteo_master.parquet
  rapports_master.json
  marche_master.json
  equipements_master.json
  horse_stats_master.json
  ...
```

### Restauration des donnees

Si vous avez un backup des donnees :

```bash
# Copier le dossier output/ depuis le backup
cp -r /chemin/backup/output/ ./output/

# Copier le dossier data_master/
cp -r /chemin/backup/data_master/ ./data_master/
```

---

## Execution du pipeline

### Collecte (scripts individuels)

Chaque script peut etre lance independamment :

```bash
# Exemple : calendrier des reunions
python 01_calendrier_reunions.py --date-debut 2013-01-01 --date-fin 2026-03-18

# Exemple : liste des courses (avec reprise automatique)
python 02_liste_courses.py

# Exemple : reset du checkpoint d'un script
python 22_performances_detaillees.py --reset-checkpoint
```

Options communes a la plupart des scripts :

| Option | Description | Defaut |
|--------|-------------|--------|
| `--date-debut` | Date de debut (YYYY-MM-DD) | Premiere date disponible |
| `--date-fin` | Date de fin (YYYY-MM-DD) | Date du jour |
| `--timeout` | Timeout HTTP en secondes | 30 |
| `--retry` | Nombre de retry max | 3 |
| `--pause` | Pause entre requetes (secondes) | 0.5 |
| `--log-level` | Niveau de log (DEBUG/INFO/WARNING/ERROR) | INFO |
| `--reset-checkpoint` | Remet le checkpoint a zero | - |
| `--no-reprise` | Desactive la reprise automatique | - |

### Monitoring automatique

Un script de surveillance relance automatiquement les scripts qui crashent :

```bash
bash monitor_and_relaunch.sh
```

### Pipeline complet (apres collecte)

```bash
# Etape 2 : Audit
python audit_data_integrity.py

# Etape 3 : Nettoyage
python nettoyage_global.py

# Etape 4 : Deduplication + Comblage
python deduplication.py
python comblage_trous.py

# Etape 5 : Fusion
python merge_02_02b_courses_master.py
python merge_pedigree_master.py
python merge_meteo_master.py
python merge_rapports_21_38.py
python merge_marche_master.py
python merge_equipements_master.py
python merge_performances_master.py

# Post-processing
python postprocess_meteo.py
python postprocess_rapports.py
python postprocess_marche.py
python postprocess_equipements.py
python postprocess_horse_stats.py

# Mega-merge
python mega_merge_partants_master.py

# Etape 6 : Features
python feature_builders/master_feature_builder.py

# Etape 11 : Tests qualite
python quality/test_json_integrity.py
python quality/test_zero_bytes.py
python quality/test_record_counts.py
python quality/test_features_quality.py
```

---

## Contraintes memoire

Le pipeline est concu pour fonctionner avec des contraintes memoire strictes :

| Phase | RAM utilisee | Technique |
|-------|-------------|-----------|
| Collecte | ~15 MB/script | JSONL streaming, ijson, append |
| Merge domaine | ~8-16 GB | Streaming par lots |
| Mega-merge | ~32-64 GB | Index en memoire |
| Feature engineering | ~16-32 GB | Calcul par blocs |

Sur une machine avec 16 GB de RAM (MacBook M1) :
- La collecte fonctionne parfaitement (tous les scripts sont patches JSONL)
- Les merges par domaine fonctionnent
- Le mega-merge et le feature engineering necessitent 64 GB

---

## Depannage

### Script qui crashe
Les scripts ont des checkpoints automatiques. Relancer simplement le script : il reprend la ou il s'est arrete.

### Fichier JSON tronque
Lancer `python audit_data_integrity.py` pour detecter les fichiers corrompus.

### Checkpoint obsolete
Si de nouvelles courses ont ete ajoutees par le script 02, il faut reset les checkpoints des scripts dependants (21, 22, 28, 38) :
```bash
python 21_rapports_definitifs.py --reset-checkpoint
python 22_performances_detaillees.py --reset-checkpoint
```

### Erreur memoire (MemoryError)
Verifier que les scripts utilisent bien le format JSONL et pas `json.load()` sur les gros fichiers. Utiliser `ijson` pour le streaming.

### Cloudflare / Rate limiting
Certains sites (PedigreeQuery, Racing Post) ont des protections anti-bot. Utiliser un proxy ou augmenter les delais entre requetes.
