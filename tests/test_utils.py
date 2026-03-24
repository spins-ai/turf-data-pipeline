#!/usr/bin/env python3
"""
tests/test_utils.py
===================
Tests unitaires pour les utilitaires partages du pipeline.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ajouter la racine du projet au path pour les imports
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ============================================================
# Tests safe_int
# ============================================================

class TestSafeInt:
    """Tests pour utils.types.safe_int."""

    def test_int_from_string(self):
        from utils.types import safe_int
        assert safe_int("42") == 42

    def test_int_from_int(self):
        from utils.types import safe_int
        assert safe_int(7) == 7

    def test_int_from_float_string(self):
        from utils.types import safe_int
        # "3.14" ne peut pas etre converti en int directement
        assert safe_int("3.14") is None

    def test_int_from_none(self):
        from utils.types import safe_int
        assert safe_int(None) is None

    def test_int_from_none_with_default(self):
        from utils.types import safe_int
        assert safe_int(None, default=0) == 0

    def test_int_from_invalid_string(self):
        from utils.types import safe_int
        assert safe_int("abc", default=-1) == -1

    def test_int_from_empty_string(self):
        from utils.types import safe_int
        assert safe_int("") is None


# ============================================================
# Tests safe_float
# ============================================================

class TestSafeFloat:
    """Tests pour utils.types.safe_float."""

    def test_float_from_string(self):
        from utils.types import safe_float
        assert safe_float("3.14") == 3.14

    def test_float_from_int(self):
        from utils.types import safe_float
        assert safe_float(5) == 5.0

    def test_float_from_none(self):
        from utils.types import safe_float
        assert safe_float(None) is None

    def test_float_from_none_with_default(self):
        from utils.types import safe_float
        assert safe_float(None, default=0.0) == 0.0

    def test_float_from_invalid_string(self):
        from utils.types import safe_float
        assert safe_float("xyz", default=-1.0) == -1.0

    def test_float_from_empty_string(self):
        from utils.types import safe_float
        assert safe_float("") is None

    def test_float_from_negative(self):
        from utils.types import safe_float
        assert safe_float("-2.5") == -2.5


# ============================================================
# Tests normalize_name
# ============================================================

class TestNormalizeName:
    """Tests pour utils.normalize.normalize_name."""

    def test_basic_upper(self):
        from utils.normalize import normalize_name
        assert normalize_name("prince") == "PRINCE"

    def test_strip_accents(self):
        from utils.normalize import normalize_name
        result = normalize_name("etoile du berger")
        assert result == "ETOILE DU BERGER"

    def test_strip_accented_chars(self):
        from utils.normalize import normalize_name
        result = normalize_name("\u00c9TOILE DU BERGER")
        assert result == "ETOILE DU BERGER"

    def test_strip_country_suffix(self):
        from utils.normalize import normalize_name
        result = normalize_name("LUCKY STAR (IRE)")
        assert result == "LUCKY STAR"

    def test_apostrophe_replaced(self):
        from utils.normalize import normalize_name
        result = normalize_name("Prince d'Or")
        assert "D" in result and "OR" in result

    def test_none_returns_empty(self):
        from utils.normalize import normalize_name
        assert normalize_name(None) == ""

    def test_empty_returns_empty(self):
        from utils.normalize import normalize_name
        assert normalize_name("") == ""


# ============================================================
# Tests setup_logging
# ============================================================

class TestSetupLogging:
    """Tests pour utils.logging_setup.setup_logging."""

    def test_returns_logger(self, tmp_path):
        from utils.logging_setup import setup_logging
        logger = setup_logging("test_logger_unit", log_dir=tmp_path)
        assert isinstance(logger, logging.Logger)

    def test_logger_has_name(self, tmp_path):
        from utils.logging_setup import setup_logging
        logger = setup_logging("test_named_logger", log_dir=tmp_path)
        assert logger.name == "test_named_logger"

    def test_logger_has_handlers(self, tmp_path):
        from utils.logging_setup import setup_logging
        logger = setup_logging("test_handler_logger", log_dir=tmp_path)
        assert len(logger.handlers) >= 1

    def test_log_file_created(self, tmp_path):
        from utils.logging_setup import setup_logging
        logger = setup_logging("test_file_logger", log_dir=tmp_path)
        logger.info("test message")
        log_file = tmp_path / "test_file_logger.log"
        assert log_file.exists()


# ============================================================
# Tests load_checkpoint
# ============================================================

class TestLoadCheckpoint:
    """Tests pour utils.scraping.load_checkpoint."""

    def test_missing_file_returns_dict(self, tmp_path):
        from utils.scraping import load_checkpoint
        result = load_checkpoint(tmp_path / "nonexistent.json")
        assert isinstance(result, dict)
        assert result == {}

    def test_valid_checkpoint(self, tmp_path):
        import json
        from utils.scraping import load_checkpoint
        cp_file = tmp_path / "checkpoint.json"
        cp_file.write_text(json.dumps({"page": 42, "done": ["a", "b"]}), encoding="utf-8")
        result = load_checkpoint(cp_file)
        assert isinstance(result, dict)
        assert result["page"] == 42

    def test_corrupted_file_returns_dict(self, tmp_path):
        from utils.scraping import load_checkpoint
        cp_file = tmp_path / "bad.json"
        cp_file.write_text("{invalid json", encoding="utf-8")
        result = load_checkpoint(cp_file)
        assert isinstance(result, dict)
