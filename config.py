"""
config.py — Configuration centralisee du pipeline turf-data.

Pilier 9 : config.py centralise avec tous les parametres (chemins, URLs, cles).

Usage dans un script :
    from config import (
        BASE_DIR, DATA_DIR, RAW_DIR, DATA_MASTER_DIR, FEATURES_DIR,
        BUILDER_OUTPUTS_DIR, CONSOLIDATED, SELECTED, PARTANTS_MASTER_PARQUET,
        PERFORMANCES_MASTER, PMU_API_BASE_URL, DEFAULT_DATE_DEBUT,
        DEFAULT_DATE_FIN, RAM_LIMITS, output_path, data_master_path,
    )

Toutes les paths sont des pathlib.Path.
Les scripts existants peuvent continuer a utiliser leurs propres constantes,
mais les nouveaux scripts DEVRAIENT importer depuis config.py.
"""

from __future__ import annotations

import os
from pathlib import Path

# ============================================================================
# DIRECTORIES — racine et sous-dossiers principaux
# ============================================================================

BASE_DIR: Path = Path(__file__).resolve().parent
"""Racine du projet (dossier contenant config.py, scripts, feature_builders)."""

# Data lives on D: drive for performance (SSD) and space
DATA_DIR: Path = Path("D:/turf-data-pipeline")
"""Racine des donnees sur le disque D: (SSD rapide, ~500 Go)."""

RAW_DIR: Path = DATA_DIR / "02_DONNEES_BRUTES"
"""Donnees brutes collectees (JSON, JSONL, CSV, Parquet)."""

DATA_MASTER_DIR: Path = DATA_DIR / "03_DONNEES_MASTER"
"""Fichiers master consolides (partants_master, courses_master, etc.)."""

FEATURES_DIR: Path = DATA_DIR / "04_FEATURES"
"""Features consolidees, selectionnees, DuckDB."""

BUILDER_OUTPUTS_DIR: Path = RAW_DIR / "builder_outputs"
"""Sorties intermediaires des feature builders (JSONL par builder)."""

# Legacy aliases (backward compat for old scripts)
OUTPUT_DIR: Path = RAW_DIR
"""Alias vers RAW_DIR pour compatibilite avec anciens scripts."""

LABELS_DIR: Path = FEATURES_DIR / "labels"
"""Labels d'entrainement produits par generate_labels.py."""

LOGS_DIR: Path = BASE_DIR / "logs"
"""Fichiers de log (pipeline.log, scraper logs, etc.)."""

CACHE_DIR: Path = BASE_DIR / "cache"
"""Cache global (reponses API, checkpoints de scrapers)."""

EXPORTS_DIR: Path = DATA_DIR / "05_EXPORTS"
"""Fichiers exportes (parquet chunks, triple format, etc.)."""

QUALITY_DIR: Path = DATA_DIR / "06_QUALITY"
"""Rapports de qualite de donnees."""

MODELS_DIR: Path = BASE_DIR / "models"
"""Modeles ML/DL entraines (futur)."""

PIPELINE_DIR: Path = BASE_DIR / "pipeline"
"""Modules structures du pipeline (phase_01, phase_02, etc.)."""

SECURITY_DIR: Path = BASE_DIR / "security"
"""Fichiers lies a la securite (cles, tokens)."""

CONFIG_DIR: Path = BASE_DIR / "config"
"""Fichiers de configuration YAML."""

# ============================================================================
# OUTPUT SUB-DIRECTORIES — un par script de collecte/enrichissement
# ============================================================================

# Phase 0 : Collecte brute (scripts 00-49)
OUTPUT_METEO: Path = RAW_DIR / "00_enrichissement_meteo"
OUTPUT_CALENDRIER: Path = RAW_DIR / "01_calendrier_reunions"
OUTPUT_COURSES: Path = RAW_DIR / "02_liste_courses"
OUTPUT_COURSES_RAW_PMU: Path = RAW_DIR / "02_liste_courses_raw_pmu"
OUTPUT_COURSES_2013: Path = RAW_DIR / "02b_liste_courses_2013"
OUTPUT_LETROT: Path = RAW_DIR / "02b_scraper_letrot"
OUTPUT_RESULTATS: Path = RAW_DIR / "04_resultats"
OUTPUT_HISTORIQUE_CHEVAUX: Path = RAW_DIR / "05_historique_chevaux"
OUTPUT_HISTORIQUE_JOCKEYS: Path = RAW_DIR / "06_historique_jockeys"
OUTPUT_COTES_MARCHE: Path = RAW_DIR / "07_cotes_marche"
OUTPUT_PEDIGREE: Path = RAW_DIR / "08_pedigree"
OUTPUT_EQUIPEMENTS: Path = RAW_DIR / "09_equipements"
OUTPUT_POIDS: Path = RAW_DIR / "10_poids_handicaps"
OUTPUT_SECTIONALS: Path = RAW_DIR / "11_sectionals"
OUTPUT_PEDIGREE_12: Path = RAW_DIR / "12_pedigree"
OUTPUT_METEO_HIST: Path = RAW_DIR / "13_meteo_historique"
OUTPUT_PEDIGREE_14: Path = RAW_DIR / "14_pedigree"
OUTPUT_EXTERNAL: Path = RAW_DIR / "15_external_datasets"
OUTPUT_NANAELIE: Path = RAW_DIR / "16_nanaelie"
OUTPUT_SIRE_IFCE: Path = RAW_DIR / "17_sire_ifce"
OUTPUT_LETROT_RECORDS: Path = RAW_DIR / "18_letrot_records"
OUTPUT_BOTURFERS: Path = RAW_DIR / "19_boturfers_stats"
OUTPUT_IFCE: Path = RAW_DIR / "20_ifce_stats"
OUTPUT_RAPPORTS_DEF: Path = RAW_DIR / "21_rapports_definitifs"
OUTPUT_PERF_DETAILLEES: Path = RAW_DIR / "22_performances_detaillees"
OUTPUT_PRONOSTICS: Path = RAW_DIR / "23_pronostics"
OUTPUT_CANALTURF: Path = RAW_DIR / "24_canalturf"
OUTPUT_TURFOSTATS: Path = RAW_DIR / "25_turfostats"
OUTPUT_GENY: Path = RAW_DIR / "26_geny"
OUTPUT_CITATIONS: Path = RAW_DIR / "27_citations_enjeux"
OUTPUT_COMBINAISONS: Path = RAW_DIR / "28_combinaisons_marche"
OUTPUT_ARQANA: Path = RAW_DIR / "29_arqana_ventes"
OUTPUT_SMARKETS: Path = RAW_DIR / "30_smarkets_exchange"
OUTPUT_ZONE_TURF: Path = RAW_DIR / "31_zone_turf"
OUTPUT_TURFOMANIA: Path = RAW_DIR / "32_turfomania"
OUTPUT_TURF_FR: Path = RAW_DIR / "33_turf_fr"
OUTPUT_UNIBET: Path = RAW_DIR / "34_unibet_cotes"
OUTPUT_METEO_FRANCE: Path = RAW_DIR / "35_meteo_france"
OUTPUT_PEDIGREE_QUERY: Path = RAW_DIR / "36_pedigree_query"
OUTPUT_RACING_POST: Path = RAW_DIR / "37_racing_post"
OUTPUT_RAPPORTS_INTERNET: Path = RAW_DIR / "38_rapports_internet"
OUTPUT_REUNIONS_ENRICHIES: Path = RAW_DIR / "39_reunions_enrichies"
OUTPUT_ENRICHISSEMENT: Path = RAW_DIR / "40_enrichissement_partants"

# Phase 0 : Calculs/croisements (scripts 41-49)
OUTPUT_SEQUENCES: Path = RAW_DIR / "41_sequences"
OUTPUT_CROISEMENT_RP: Path = RAW_DIR / "42_croisement_rp"
OUTPUT_CROISEMENT_METEO: Path = RAW_DIR / "43_croisement_meteo"
OUTPUT_CROISEMENT_PEDIGREE: Path = RAW_DIR / "44_croisement_pedigree"
OUTPUT_GRAPHE_GNN: Path = RAW_DIR / "45_graphe_gnn"
OUTPUT_TRACK_BIAS: Path = RAW_DIR / "46_track_bias_speed"
OUTPUT_CONDITIONS_TEXTE: Path = RAW_DIR / "48_conditions_texte"
OUTPUT_ECART_COTES: Path = RAW_DIR / "49_ecart_cotes"

# Phase 0 : Scrapers externes (scripts 51-99)
OUTPUT_ZETURF: Path = RAW_DIR / "51_zeturf"
OUTPUT_TURFOMANIA_52: Path = RAW_DIR / "52_turfomania"
OUTPUT_PARIS_TURF: Path = RAW_DIR / "53_paris_turf"
OUTPUT_TURFINFO: Path = RAW_DIR / "54_turfinfo"
OUTPUT_EQUIDIA: Path = RAW_DIR / "55_equidia_data"
OUTPUT_TIMEFORM: Path = RAW_DIR / "56_timeform"
OUTPUT_SPORTING_LIFE: Path = RAW_DIR / "57_sporting_life"
OUTPUT_AT_THE_RACES: Path = RAW_DIR / "58_at_the_races"
OUTPUT_RACING_TV: Path = RAW_DIR / "59_racing_tv"
OUTPUT_ODDSCHECKER: Path = RAW_DIR / "60_oddschecker"
OUTPUT_EQUIBASE: Path = RAW_DIR / "61_equibase"
OUTPUT_HRN: Path = RAW_DIR / "62_horse_racing_nation"
OUTPUT_DRF: Path = RAW_DIR / "63_daily_racing_form"
OUTPUT_PUNTERS: Path = RAW_DIR / "64_punters"
OUTPUT_RACENET: Path = RAW_DIR / "65_racenet"
OUTPUT_HKJC: Path = RAW_DIR / "66_hkjc"
OUTPUT_JRA: Path = RAW_DIR / "67_jra"
OUTPUT_BETFAIR: Path = RAW_DIR / "68_betfair_exchange"
OUTPUT_ODDSPORTAL: Path = RAW_DIR / "69_oddsportal"
OUTPUT_BETEXPLORER: Path = RAW_DIR / "70_betexplorer"

# PMU API raw data
OUTPUT_PMU_API: Path = RAW_DIR / "101_pmu_api"
OUTPUT_LETROT_83: Path = RAW_DIR / "83_letrot"

# Post-processing directories
OUTPUT_COMBLAGE: Path = RAW_DIR / "comblage"
OUTPUT_DEDUP: Path = RAW_DIR / "dedup"
OUTPUT_NETTOYAGE: Path = RAW_DIR / "nettoyage"
OUTPUT_AUDIT: Path = RAW_DIR / "audit"
OUTPUT_RAPPORTS_MERGED: Path = RAW_DIR / "rapports_merged"
OUTPUT_METEO_COMPLETE: Path = RAW_DIR / "meteo_complete"
OUTPUT_PEDIGREE_COMPLETE: Path = RAW_DIR / "pedigree_complete"
OUTPUT_ELO: Path = RAW_DIR / "elo_ratings"
OUTPUT_FIELD_STRENGTH: Path = RAW_DIR / "field_strength"

# ============================================================================
# DATA MASTER — fichiers principaux
# ============================================================================

PARTANTS_MASTER: Path = DATA_MASTER_DIR / "partants_master.parquet"
"""Master partants: 2.93M rows × ~520 cols, principal fichier de reference."""

PARTANTS_MASTER_JSONL: Path = DATA_MASTER_DIR / "partants_master.jsonl"
"""Version JSONL du master partants (legacy, 33 Go)."""

COURSES_MASTER: Path = DATA_MASTER_DIR / "courses_master.parquet"
COURSES_MASTER_JSONL: Path = DATA_MASTER_DIR / "courses_master.jsonl"

PERFORMANCES_MASTER: Path = DATA_MASTER_DIR / "performances_master.parquet"
"""Performances detaillees: 6M rows, 114 Mo — base pour features vitesse/forme."""

HORSE_STATS_MASTER: Path = DATA_MASTER_DIR / "horse_stats_master.parquet"
"""Stats par cheval: 80K chevaux, 17 cols."""

METEO_MASTER: Path = DATA_MASTER_DIR / "meteo_master.parquet"
"""Donnees meteo par reunion."""

EQUIPEMENTS_MASTER: Path = DATA_MASTER_DIR / "equipements_master.json"
COURSE_PROFILES: Path = DATA_MASTER_DIR / "course_profiles.jsonl"

# Features — fichiers finaux
CONSOLIDATED: Path = FEATURES_DIR / "features_consolidated.parquet"
"""Toutes les features consolidees: 2.93M rows × 3297 cols, 6.79 Go."""

SELECTED: Path = FEATURES_DIR / "features_selected.parquet"
"""Top 500 features selectionnees par LightGBM: 2.93M rows × 502 cols."""

DUCKDB: Path = FEATURES_DIR / "features.duckdb"
"""Base DuckDB pour requetes SQL rapides sur les features selectionnees."""

# Labels
TRAINING_LABELS: Path = FEATURES_DIR / "training_labels.parquet"

# Legacy aliases
FEATURES_MATRIX: Path = CONSOLIDATED
FEATURES_MATRIX_PARQUET: Path = CONSOLIDATED

# ============================================================================
# REFERENCES entre scripts (fichiers d'interface)
# ============================================================================

REFERENCES_REUNIONS_02: Path = OUTPUT_CALENDRIER / "reunions_references_02.json"
REFERENCES_COURSES_04: Path = OUTPUT_COURSES / "courses_references_04.json"

# ============================================================================
# EXTERNAL DATA PATHS (utilises par master_feature_builder.py)
# ============================================================================

SMARKETS_DATA: Path = OUTPUT_SMARKETS / "smarkets.jsonl"
REUNIONS_DATA: Path = OUTPUT_REUNIONS_ENRICHIES / "reunions.jsonl"
ENRICHED_DATA: Path = OUTPUT_ENRICHISSEMENT / "partants_enrichis.jsonl"
CANALTURF_DATA: Path = OUTPUT_CANALTURF / "canalturf.jsonl"
TURFOSTATS_DATA: Path = OUTPUT_TURFOSTATS / "turfostats.jsonl"
GENY_DATA: Path = OUTPUT_GENY / "geny.jsonl"
COURSES_NORM_DATA: Path = OUTPUT_COURSES / "courses_normalisees.jsonl"
PERF_DET_DATA: Path = OUTPUT_PERF_DETAILLEES / "perf_detaillees_enriched.jsonl"
COTES_DATA: Path = OUTPUT_COTES_MARCHE / "cotes_marche.json"
EQUIP_HIST_DATA: Path = OUTPUT_EQUIPEMENTS / "equipements_historique.json"
POIDS_HIST_DATA: Path = OUTPUT_POIDS / "poids_handicaps.json"
SECTIONALS_DATA: Path = OUTPUT_SECTIONALS / "sectionals.parquet"
CHEVAL_HIST_DATA: Path = OUTPUT_HISTORIQUE_CHEVAUX / "historique_chevaux.json"
JOCKEY_HIST_DATA: Path = OUTPUT_HISTORIQUE_JOCKEYS / "historique_jockeys.json"
ENTRAINEUR_HIST_DATA: Path = OUTPUT_HISTORIQUE_JOCKEYS / "historique_entraineurs.json"
PERE_DATA: Path = OUTPUT_PEDIGREE / "pedigree_peres.json"
MERE_DATA: Path = OUTPUT_PEDIGREE / "pedigree_meres.json"
PRONOSTICS_DATA: Path = OUTPUT_PRONOSTICS / "pronostics.jsonl"

# ============================================================================
# API URLS
# ============================================================================

PMU_API_BASE_URL: str = "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme"
"""Base URL de l'API PMU (programme, resultats, rapports)."""

PMU_API_PARTICIPANTS_URL: str = "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date}/R{reunion}/C{course}/participants"
"""Template URL pour les participants PMU."""

OPENMETEO_API_URL: str = "https://archive-api.open-meteo.com/v1/archive"
"""API Open-Meteo pour les donnees meteo historiques."""

# ============================================================================
# DEFAULT DATE RANGES
# ============================================================================

DEFAULT_DATE_DEBUT: str = "2014-01-01"
"""Date de debut par defaut pour la collecte historique."""

DEFAULT_DATE_FIN: str = "2026-12-31"
"""Date de fin par defaut (annee en cours + marge)."""

# ============================================================================
# HTTP / SCRAPING DEFAULTS
# ============================================================================

HTTP_TIMEOUT: int = 30
"""Timeout par defaut pour les requetes HTTP (secondes)."""

HTTP_RETRY_MAX: int = 3
"""Nombre maximum de retries HTTP."""

HTTP_RETRY_BACKOFF: float = 1.0
"""Backoff factor entre les retries."""

DEFAULT_PAUSE: float = 0.5
"""Pause par defaut entre les requetes (secondes)."""

DEFAULT_BATCH_SIZE: int = 500
"""Taille de batch par defaut pour les requetes API."""

# ============================================================================
# RAM LIMITS — budget memoire par type de tache
# ============================================================================

RAM_LIMITS: dict = {
    "scraper_light": 512,        # Mo — scrapers simples (requests/cloudscraper)
    "scraper_playwright": 1024,  # Mo — scrapers Playwright (navigateur headless)
    "merge": 2048,               # Mo — scripts de merge
    "feature_builder": 2048,     # Mo — feature builders individuels
    "master_feature": 4096,      # Mo — master_feature_builder (2 passes)
    "mega_merge": 4096,          # Mo — mega_merge_partants_master
    "quality": 1024,             # Mo — tests de qualite
    "labels": 1024,              # Mo — generation de labels
    "default": 1024,             # Mo — scripts non categorises
}
"""Budget RAM maximum par type de tache (en Mo). Max 3 taches lourdes simultanees."""

MAX_CONCURRENT_HEAVY: int = 3
"""Nombre maximum de taches lourdes (>= 2 Go RAM) en parallele."""

MAX_WORKERS: int = 4
"""Nombre de workers pour l'execution parallele dans le pipeline."""

# ============================================================================
# PYTHON EXECUTABLE
# ============================================================================

PYTHON_EXE: str = os.environ.get(
    "PYTHON_EXE",
    r"C:\Users\celia\AppData\Local\Programs\Python\Python312\python.exe",
)
"""Chemin vers l'executable Python a utiliser pour lancer les sous-scripts."""

# ============================================================================
# PIPELINE CHECKPOINT
# ============================================================================

CHECKPOINT_FILE: Path = BASE_DIR / "pipeline_checkpoint.json"
"""Fichier de checkpoint pour la reprise du pipeline."""

# ============================================================================
# LOGGING
# ============================================================================

LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
"""Niveau de log par defaut."""

PIPELINE_LOG: Path = LOGS_DIR / "pipeline.log"
"""Fichier de log principal du pipeline."""

# ============================================================================
# EXPORT FORMATS
# ============================================================================

EXPORT_CSV: bool = True
EXPORT_PARQUET: bool = True
EXPORT_JSON: bool = True

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def output_path(script_num: int | str, filename: str = "") -> Path:
    """Retourne le dossier 02_DONNEES_BRUTES/XX_... pour un script donne.

    >>> output_path(2, "courses_brut.jsonl")
    WindowsPath('D:/turf-data-pipeline/02_DONNEES_BRUTES/02_liste_courses/courses_brut.jsonl')
    """
    prefix = f"{int(script_num):02d}_"
    if RAW_DIR.exists():
        for d in sorted(RAW_DIR.iterdir()):
            if d.is_dir() and d.name.startswith(prefix):
                return d / filename if filename else d
    fallback = RAW_DIR / f"{prefix}unknown"
    return fallback / filename if filename else fallback


def data_master_path(filename: str) -> Path:
    """Retourne un chemin dans 03_DONNEES_MASTER/.

    >>> data_master_path("partants_master.parquet")
    WindowsPath('D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet')
    """
    return DATA_MASTER_DIR / filename


def builder_output_path(builder_name: str, filename: str = "") -> Path:
    """Retourne le dossier builder_outputs/<name>/ pour un builder.

    >>> builder_output_path("elo_x", "elo_x_features.jsonl")
    WindowsPath('D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/elo_x/elo_x_features.jsonl')
    """
    d = BUILDER_OUTPUTS_DIR / builder_name
    return d / filename if filename else d


def ensure_dirs() -> None:
    """Cree les repertoires principaux s'ils n'existent pas."""
    for d in [RAW_DIR, DATA_MASTER_DIR, FEATURES_DIR, LABELS_DIR,
              LOGS_DIR, CACHE_DIR, BUILDER_OUTPUTS_DIR]:
        d.mkdir(parents=True, exist_ok=True)
