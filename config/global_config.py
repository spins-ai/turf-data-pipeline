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
from config import BASE_DIR, OUTPUT_DIR, DATA_MASTER_DIR
