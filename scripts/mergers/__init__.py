"""
scripts/mergers/ — Alias pour scripts/merge/
=============================================
Les scripts de fusion sont dans scripts/merge/.
Ce dossier existe pour compatibilite avec la nomenclature TODO.
"""
import sys
from pathlib import Path

_merge_dir = Path(__file__).parent.parent / "merge"
if str(_merge_dir) not in sys.path:
    sys.path.insert(0, str(_merge_dir))
