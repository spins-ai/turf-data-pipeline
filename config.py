"""
config.py — Configuration centralisee du pipeline turf-data.

Pilier 9 : config.py centralise avec tous les parametres (chemins, URLs, cles).

Usage dans un script :
    from config import (
        BASE_DIR, OUTPUT_DIR, DATA_MASTER_DIR, FEATURES_DIR, LABELS_DIR,
        LOGS_DIR, PMU_API_BASE_URL, DEFAULT_DATE_DEBUT, DEFAULT_DATE_FIN,
        RAM_LIMITS, output_path, data_master_path,
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
"""Racine du projet (dossier contenant config.py)."""

OUTPUT_DIR: Path = BASE_DIR / "output"
"""Dossier principal de sortie (tous les scripts XX_*.py)."""

DATA_MASTER_DIR: Path = BASE_DIR / "data_master"
"""Fichiers master consolides (partants_master.jsonl, courses_master.jsonl, etc.)."""

FEATURES_DIR: Path = OUTPUT_DIR / "features"
"""Matrice de features construite par master_feature_builder.py."""

LABELS_DIR: Path = OUTPUT_DIR / "labels"
"""Labels d'entrainement produits par generate_labels.py."""

LOGS_DIR: Path = BASE_DIR / "logs"
"""Fichiers de log (pipeline.log, scraper logs, etc.)."""

CACHE_DIR: Path = BASE_DIR / "cache"
"""Cache global (reponses API, checkpoints de scrapers)."""

EXPORTS_DIR: Path = OUTPUT_DIR / "exports"
"""Fichiers exportes (parquet chunks, triple format, etc.)."""

QUALITY_DIR: Path = OUTPUT_DIR / "quality"
"""Rapports de qualite de donnees."""

MODELS_DIR: Path = BASE_DIR / "models"
"""Modeles ML/DL entraines."""

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
OUTPUT_METEO: Path = OUTPUT_DIR / "00_enrichissement_meteo"
OUTPUT_CALENDRIER: Path = OUTPUT_DIR / "01_calendrier_reunions"
OUTPUT_COURSES: Path = OUTPUT_DIR / "02_liste_courses"
OUTPUT_COURSES_RAW_PMU: Path = OUTPUT_DIR / "02_liste_courses_raw_pmu"
OUTPUT_COURSES_2013: Path = OUTPUT_DIR / "02b_liste_courses_2013"
OUTPUT_LETROT: Path = OUTPUT_DIR / "02b_scraper_letrot"
OUTPUT_RESULTATS: Path = OUTPUT_DIR / "04_resultats"
OUTPUT_HISTORIQUE_CHEVAUX: Path = OUTPUT_DIR / "05_historique_chevaux"
OUTPUT_HISTORIQUE_JOCKEYS: Path = OUTPUT_DIR / "06_historique_jockeys"
OUTPUT_COTES_MARCHE: Path = OUTPUT_DIR / "07_cotes_marche"
OUTPUT_PEDIGREE: Path = OUTPUT_DIR / "08_pedigree"
OUTPUT_EQUIPEMENTS: Path = OUTPUT_DIR / "09_equipements"
OUTPUT_POIDS: Path = OUTPUT_DIR / "10_poids_handicaps"
OUTPUT_SECTIONALS: Path = OUTPUT_DIR / "11_sectionals"
OUTPUT_PEDIGREE_12: Path = OUTPUT_DIR / "12_pedigree"
OUTPUT_METEO_HIST: Path = OUTPUT_DIR / "13_meteo_historique"
OUTPUT_PEDIGREE_14: Path = OUTPUT_DIR / "14_pedigree"
OUTPUT_EXTERNAL: Path = OUTPUT_DIR / "15_external_datasets"
OUTPUT_NANAELIE: Path = OUTPUT_DIR / "16_nanaelie"
OUTPUT_SIRE_IFCE: Path = OUTPUT_DIR / "17_sire_ifce"
OUTPUT_LETROT_RECORDS: Path = OUTPUT_DIR / "18_letrot_records"
OUTPUT_BOTURFERS: Path = OUTPUT_DIR / "19_boturfers_stats"
OUTPUT_IFCE: Path = OUTPUT_DIR / "20_ifce_stats"
OUTPUT_RAPPORTS_DEF: Path = OUTPUT_DIR / "21_rapports_definitifs"
OUTPUT_PERF_DETAILLEES: Path = OUTPUT_DIR / "22_performances_detaillees"
OUTPUT_PRONOSTICS: Path = OUTPUT_DIR / "23_pronostics"
OUTPUT_CANALTURF: Path = OUTPUT_DIR / "24_canalturf"
OUTPUT_TURFOSTATS: Path = OUTPUT_DIR / "25_turfostats"
OUTPUT_GENY: Path = OUTPUT_DIR / "26_geny"
OUTPUT_CITATIONS: Path = OUTPUT_DIR / "27_citations_enjeux"
OUTPUT_COMBINAISONS: Path = OUTPUT_DIR / "28_combinaisons_marche"
OUTPUT_ARQANA: Path = OUTPUT_DIR / "29_arqana_ventes"
OUTPUT_SMARKETS: Path = OUTPUT_DIR / "30_smarkets_exchange"
OUTPUT_ZONE_TURF: Path = OUTPUT_DIR / "31_zone_turf"
OUTPUT_TURFOMANIA: Path = OUTPUT_DIR / "32_turfomania"
OUTPUT_TURF_FR: Path = OUTPUT_DIR / "33_turf_fr"
OUTPUT_UNIBET: Path = OUTPUT_DIR / "34_unibet_cotes"
OUTPUT_METEO_FRANCE: Path = OUTPUT_DIR / "35_meteo_france"
OUTPUT_PEDIGREE_QUERY: Path = OUTPUT_DIR / "36_pedigree_query"
OUTPUT_RACING_POST: Path = OUTPUT_DIR / "37_racing_post"
OUTPUT_RAPPORTS_INTERNET: Path = OUTPUT_DIR / "38_rapports_internet"
OUTPUT_REUNIONS_ENRICHIES: Path = OUTPUT_DIR / "39_reunions_enrichies"
OUTPUT_ENRICHISSEMENT: Path = OUTPUT_DIR / "40_enrichissement_partants"

# Phase 0 : Calculs/croisements (scripts 41-49)
OUTPUT_SEQUENCES: Path = OUTPUT_DIR / "41_sequences"
OUTPUT_CROISEMENT_RP: Path = OUTPUT_DIR / "42_croisement_rp"
OUTPUT_CROISEMENT_METEO: Path = OUTPUT_DIR / "43_croisement_meteo"
OUTPUT_CROISEMENT_PEDIGREE: Path = OUTPUT_DIR / "44_croisement_pedigree"
OUTPUT_GRAPHE_GNN: Path = OUTPUT_DIR / "45_graphe_gnn"
OUTPUT_TRACK_BIAS: Path = OUTPUT_DIR / "46_track_bias_speed"
OUTPUT_CONDITIONS_TEXTE: Path = OUTPUT_DIR / "48_conditions_texte"
OUTPUT_ECART_COTES: Path = OUTPUT_DIR / "49_ecart_cotes"

# Phase 0 : Scrapers externes (scripts 51-99)
OUTPUT_ZETURF: Path = OUTPUT_DIR / "51_zeturf"
OUTPUT_TURFOMANIA_52: Path = OUTPUT_DIR / "52_turfomania"
OUTPUT_PARIS_TURF: Path = OUTPUT_DIR / "53_paris_turf"
OUTPUT_TURFINFO: Path = OUTPUT_DIR / "54_turfinfo"
OUTPUT_EQUIDIA: Path = OUTPUT_DIR / "55_equidia_data"
OUTPUT_TIMEFORM: Path = OUTPUT_DIR / "56_timeform"
OUTPUT_SPORTING_LIFE: Path = OUTPUT_DIR / "57_sporting_life"
OUTPUT_AT_THE_RACES: Path = OUTPUT_DIR / "58_at_the_races"
OUTPUT_RACING_TV: Path = OUTPUT_DIR / "59_racing_tv"
OUTPUT_ODDSCHECKER: Path = OUTPUT_DIR / "60_oddschecker"
OUTPUT_EQUIBASE: Path = OUTPUT_DIR / "61_equibase"
OUTPUT_HRN: Path = OUTPUT_DIR / "62_horse_racing_nation"
OUTPUT_DRF: Path = OUTPUT_DIR / "63_daily_racing_form"
OUTPUT_PUNTERS: Path = OUTPUT_DIR / "64_punters"
OUTPUT_RACENET: Path = OUTPUT_DIR / "65_racenet"
OUTPUT_HKJC: Path = OUTPUT_DIR / "66_hkjc"
OUTPUT_JRA: Path = OUTPUT_DIR / "67_jra"
OUTPUT_BETFAIR: Path = OUTPUT_DIR / "68_betfair_exchange"
OUTPUT_ODDSPORTAL: Path = OUTPUT_DIR / "69_oddsportal"
OUTPUT_BETEXPLORER: Path = OUTPUT_DIR / "70_betexplorer"

# Post-processing directories
OUTPUT_COMBLAGE: Path = OUTPUT_DIR / "comblage"
OUTPUT_DEDUP: Path = OUTPUT_DIR / "dedup"
OUTPUT_NETTOYAGE: Path = OUTPUT_DIR / "nettoyage"
OUTPUT_AUDIT: Path = OUTPUT_DIR / "audit"
OUTPUT_RAPPORTS_MERGED: Path = OUTPUT_DIR / "rapports_merged"
OUTPUT_METEO_COMPLETE: Path = OUTPUT_DIR / "meteo_complete"
OUTPUT_PEDIGREE_COMPLETE: Path = OUTPUT_DIR / "pedigree_complete"
OUTPUT_ELO: Path = OUTPUT_DIR / "elo_ratings"
OUTPUT_FIELD_STRENGTH: Path = OUTPUT_DIR / "field_strength"

# ============================================================================
# DATA MASTER — fichiers principaux
# ============================================================================

PARTANTS_MASTER: Path = DATA_MASTER_DIR / "partants_master.jsonl"
PARTANTS_MASTER_ENRICHI: Path = DATA_MASTER_DIR / "partants_master_enrichi.jsonl"
COURSES_MASTER: Path = DATA_MASTER_DIR / "courses_master.jsonl"
COURSES_MASTER_PARQUET: Path = DATA_MASTER_DIR / "courses_master.parquet"
EQUIPEMENTS_MASTER: Path = DATA_MASTER_DIR / "equipements_master.json"
COURSE_PROFILES: Path = DATA_MASTER_DIR / "course_profiles.jsonl"

# Features matrix
FEATURES_MATRIX: Path = FEATURES_DIR / "features_matrix.jsonl"
FEATURES_MATRIX_PARQUET: Path = FEATURES_DIR / "features_matrix.parquet"

# Labels
TRAINING_LABELS: Path = LABELS_DIR / "training_labels.jsonl"

# ============================================================================
# REFERENCES entre scripts (fichiers d'interface)
# ============================================================================

REFERENCES_REUNIONS_02: Path = OUTPUT_CALENDRIER / "reunions_references_02.json"
REFERENCES_COURSES_04: Path = OUTPUT_COURSES / "courses_references_04.json"

# ============================================================================
# EXTERNAL DATA PATHS (utilises par master_feature_builder.py)
# ============================================================================

SMARKETS_DATA: Path = OUTPUT_SMARKETS / "smarkets.jsonl"
RACING_POST_DATA: Path = OUTPUT_RACING_POST / "racing_post.jsonl"
REUNIONS_DATA: Path = OUTPUT_REUNIONS_ENRICHIES / "reunions.jsonl"
ENRICHED_DATA: Path = OUTPUT_ENRICHISSEMENT / "partants_enrichis.jsonl"
CANALTURF_DATA: Path = OUTPUT_CANALTURF / "canalturf.jsonl"
TURFOSTATS_DATA: Path = OUTPUT_TURFOSTATS / "turfostats.jsonl"
GENY_DATA: Path = OUTPUT_GENY / "geny.jsonl"
COURSES_NORM_DATA: Path = OUTPUT_COURSES / "courses_normalisees.jsonl"
PERF_DET_DATA: Path = OUTPUT_PERF_DETAILLEES / "performances_detaillees.jsonl"
COTES_DATA: Path = OUTPUT_COTES_MARCHE / "cotes_marche.json"
EQUIP_HIST_DATA: Path = OUTPUT_EQUIPEMENTS / "equipements_historique.json"
POIDS_HIST_DATA: Path = OUTPUT_POIDS / "poids_handicaps.json"
SECTIONALS_DATA: Path = OUTPUT_SECTIONALS / "sectionals.json"
CHEVAL_HIST_DATA: Path = OUTPUT_HISTORIQUE_CHEVAUX / "historique_chevaux.json"
JOCKEY_HIST_DATA: Path = OUTPUT_HISTORIQUE_JOCKEYS / "historique_jockeys.json"
ENTRAINEUR_HIST_DATA: Path = OUTPUT_HISTORIQUE_JOCKEYS / "historique_entraineurs.json"
PERE_DATA: Path = OUTPUT_PEDIGREE / "pedigree_peres.json"
MERE_DATA: Path = OUTPUT_PEDIGREE / "pedigree_meres.json"

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
    """Retourne le dossier output/XX_... pour un script donne.

    >>> output_path(2, "courses_brut.jsonl")
    PosixPath('.../output/02_liste_courses/courses_brut.jsonl')
    """
    # Cherche un dossier existant qui commence par le numero
    prefix = f"{int(script_num):02d}_"
    for d in sorted(OUTPUT_DIR.iterdir()):
        if d.is_dir() and d.name.startswith(prefix):
            return d / filename if filename else d
    # Fallback : cree le chemin avec juste le numero
    fallback = OUTPUT_DIR / f"{prefix}unknown"
    return fallback / filename if filename else fallback


def data_master_path(filename: str) -> Path:
    """Retourne un chemin dans data_master/.

    >>> data_master_path("partants_master.jsonl")
    PosixPath('.../data_master/partants_master.jsonl')
    """
    return DATA_MASTER_DIR / filename


def ensure_dirs() -> None:
    """Cree les repertoires principaux s'ils n'existent pas."""
    for d in [OUTPUT_DIR, DATA_MASTER_DIR, FEATURES_DIR, LABELS_DIR,
              LOGS_DIR, CACHE_DIR, EXPORTS_DIR, QUALITY_DIR, CONFIG_DIR]:
        d.mkdir(parents=True, exist_ok=True)
