# This module re-exports from the project root hippodromes_db.py
import importlib.util
import os

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "hippodromes_db.py"))
_spec = importlib.util.spec_from_file_location("hippodromes_db", _root)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

HIPPODROMES_DB = _mod.HIPPODROMES_DB
