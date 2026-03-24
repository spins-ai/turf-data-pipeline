"""
scripts/scrapers/ — Alias pour scripts/collection/
====================================================
Les scrapers de collecte sont dans scripts/collection/.
Ce dossier existe pour compatibilite avec la nomenclature TODO.
"""
import sys
from pathlib import Path

# Re-export depuis collection/
_collection_dir = Path(__file__).parent.parent / "collection"
if str(_collection_dir) not in sys.path:
    sys.path.insert(0, str(_collection_dir))
