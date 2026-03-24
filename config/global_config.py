"""
config/global_config.py
========================
Centralized configuration re-exported from the root config.py.

This module exists so that scripts inside subdirectories can do:
    from config.global_config import BASE_DIR, OUTPUT_DIR, DATA_MASTER_DIR

All paths, URLs, RAM limits, and helper functions are re-exported from
the canonical source of truth: <project_root>/config.py.

If you need to add new configuration values, add them to the root config.py
and they will be automatically available here.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the project root is on sys.path so we can import the root config.py
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Re-export everything from root config
from config import (  # noqa: F401, E402
    # Directories
    BASE_DIR,
    OUTPUT_DIR,
    DATA_MASTER_DIR,
    FEATURES_DIR,
    LABELS_DIR,
    LOGS_DIR,
    CACHE_DIR,
    EXPORTS_DIR,
    QUALITY_DIR,
    MODELS_DIR,
    PIPELINE_DIR,
    SECURITY_DIR,
    CONFIG_DIR,
    # Output sub-directories (Phase 0)
    OUTPUT_METEO,
    OUTPUT_CALENDRIER,
    OUTPUT_COURSES,
    OUTPUT_COURSES_RAW_PMU,
    OUTPUT_COURSES_2013,
    OUTPUT_LETROT,
    OUTPUT_RESULTATS,
    OUTPUT_HISTORIQUE_CHEVAUX,
    OUTPUT_HISTORIQUE_JOCKEYS,
    OUTPUT_COTES_MARCHE,
    OUTPUT_PEDIGREE,
    OUTPUT_EQUIPEMENTS,
    OUTPUT_POIDS,
    OUTPUT_SECTIONALS,
    OUTPUT_PEDIGREE_12,
    OUTPUT_METEO_HIST,
    OUTPUT_PEDIGREE_14,
    OUTPUT_SIRE_IFCE,
    OUTPUT_RAPPORTS_DEF,
    OUTPUT_PERF_DETAILLEES,
    OUTPUT_PRONOSTICS,
    OUTPUT_CANALTURF,
    OUTPUT_TURFOSTATS,
    OUTPUT_GENY,
    OUTPUT_CITATIONS,
    OUTPUT_COMBINAISONS,
    OUTPUT_SMARKETS,
    OUTPUT_PEDIGREE_QUERY,
    OUTPUT_RACING_POST,
    OUTPUT_RAPPORTS_INTERNET,
    OUTPUT_REUNIONS_ENRICHIES,
    OUTPUT_ENRICHISSEMENT,
    # Calculation outputs (41-49)
    OUTPUT_SEQUENCES,
    OUTPUT_CROISEMENT_RP,
    OUTPUT_CROISEMENT_METEO,
    OUTPUT_CROISEMENT_PEDIGREE,
    OUTPUT_GRAPHE_GNN,
    OUTPUT_TRACK_BIAS,
    OUTPUT_CONDITIONS_TEXTE,
    OUTPUT_ECART_COTES,
    # Data master paths
    PARTANTS_MASTER,
    PARTANTS_MASTER_ENRICHI,
    COURSES_MASTER,
    FEATURES_MATRIX,
    FEATURES_MATRIX_PARQUET,
    TRAINING_LABELS,
    # API URLs
    PMU_API_BASE_URL,
    OPENMETEO_API_URL,
    # Defaults
    DEFAULT_DATE_DEBUT,
    DEFAULT_DATE_FIN,
    HTTP_TIMEOUT,
    HTTP_RETRY_MAX,
    HTTP_RETRY_BACKOFF,
    DEFAULT_PAUSE,
    DEFAULT_BATCH_SIZE,
    # RAM limits
    RAM_LIMITS,
    MAX_CONCURRENT_HEAVY,
    MAX_WORKERS,
    # Other
    PYTHON_EXE,
    CHECKPOINT_FILE,
    LOG_LEVEL,
    PIPELINE_LOG,
    EXPORT_CSV,
    EXPORT_PARQUET,
    EXPORT_JSON,
    # Helper functions
    output_path,
    data_master_path,
    ensure_dirs,
)
