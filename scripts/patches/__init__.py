"""
scripts/patches/ — Scripts de correction de donnees
====================================================
Contient les scripts patch_brutes_*.py et fill_empty_fields.py.
Note: les scripts sont principalement dans scripts/utils/ et scripts/pipeline/.
"""
import sys
from pathlib import Path

_utils_dir = Path(__file__).parent.parent / "utils"
if str(_utils_dir) not in sys.path:
    sys.path.insert(0, str(_utils_dir))
