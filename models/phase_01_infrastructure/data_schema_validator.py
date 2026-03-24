# -*- coding: utf-8 -*-
"""
Data Schema Validator
=====================
Validate schema of DataFrames against expected field definitions:
required fields, types, value ranges. Flag violations with a report.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("data_schema_validator")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"

# ---------------------------------------------------------------------------
# Expected schema definition
# ---------------------------------------------------------------------------
# Format: field_name -> {type, nullable, min, max}
PARTANTS_SCHEMA: Dict[str, Dict[str, Any]] = {
    "partant_uid": {"dtype": "string", "nullable": False},
    "course_uid": {"dtype": "string", "nullable": False},
    "date_reunion_iso": {"dtype": "datetime", "nullable": False},
    "horse_id": {"dtype": "string", "nullable": False},
    "nom_cheval": {"dtype": "string", "nullable": False},
    "num_pmu": {"dtype": "int", "nullable": False, "min": 1, "max": 30},
    "age": {"dtype": "int", "nullable": True, "min": 2, "max": 20},
    "distance": {"dtype": "int", "nullable": True, "min": 800, "max": 8000},
    "nb_courses_carriere": {"dtype": "int", "nullable": True, "min": 0},
    "nb_victoires_carriere": {"dtype": "int", "nullable": True, "min": 0},
    "place_arrivee": {"dtype": "int", "nullable": True, "min": 0, "max": 30},
    "cote_prob": {"dtype": "float", "nullable": True, "min": 0.0},
    "poids_porte_kg": {"dtype": "float", "nullable": True, "min": 40, "max": 85},
}

FEATURES_SCHEMA: Dict[str, Dict[str, Any]] = {
    "partant_uid": {"dtype": "string", "nullable": False},
    "course_uid": {"dtype": "string", "nullable": False},
    "date_reunion_iso": {"dtype": "datetime", "nullable": False},
}


class DataSchemaValidator:
    """Validate DataFrame columns against a schema specification."""

    def __init__(self, schema: Optional[Dict[str, Dict[str, Any]]] = None):
        self.schema = schema or PARTANTS_SCHEMA
        self.violations: List[Dict[str, Any]] = []

    def validate(self, df: pd.DataFrame, label: str = "dataset") -> pd.DataFrame:
        """Run all validations and return a violations report DataFrame."""
        self.violations = []
        logger.info("Validating schema for '%s' (%d rows, %d cols) ...", label, len(df), len(df.columns))

        self._check_required_fields(df, label)
        self._check_types(df, label)
        self._check_ranges(df, label)
        self._check_duplicates(df, label)

        report = pd.DataFrame(self.violations)
        n_err = len(report)
        if n_err == 0:
            logger.info("  [OK] No violations found for '%s'", label)
        else:
            logger.warning("  [WARN] %d violation(s) found for '%s'", n_err, label)
        return report

    # ------------------------------------------------------------------
    def _check_required_fields(self, df: pd.DataFrame, label: str):
        for field, spec in self.schema.items():
            if not spec.get("nullable", True):
                if field not in df.columns:
                    self.violations.append({
                        "dataset": label,
                        "field": field,
                        "check": "required_missing",
                        "detail": "Required column not found in DataFrame",
                        "count": 1,
                    })
                else:
                    n_null = int(df[field].isna().sum())
                    if n_null > 0:
                        self.violations.append({
                            "dataset": label,
                            "field": field,
                            "check": "null_in_required",
                            "detail": f"{n_null} null values in non-nullable field",
                            "count": n_null,
                        })

    def _check_types(self, df: pd.DataFrame, label: str):
        type_map = {
            "int": (np.integer, int),
            "float": (np.floating, float, np.integer, int),
            "string": (str, object),
        }
        for field, spec in self.schema.items():
            if field not in df.columns:
                continue
            expected = spec.get("dtype")
            if expected == "datetime":
                if not pd.api.types.is_datetime64_any_dtype(df[field]):
                    self.violations.append({
                        "dataset": label,
                        "field": field,
                        "check": "type_mismatch",
                        "detail": f"Expected datetime, got {df[field].dtype}",
                        "count": 1,
                    })
            elif expected in type_map:
                col = df[field].dropna()
                if len(col) > 0 and expected in ("int", "float"):
                    if not pd.api.types.is_numeric_dtype(col):
                        self.violations.append({
                            "dataset": label,
                            "field": field,
                            "check": "type_mismatch",
                            "detail": f"Expected numeric ({expected}), got {col.dtype}",
                            "count": 1,
                        })

    def _check_ranges(self, df: pd.DataFrame, label: str):
        for field, spec in self.schema.items():
            if field not in df.columns:
                continue
            col = df[field].dropna()
            if not pd.api.types.is_numeric_dtype(col):
                continue
            lo = spec.get("min")
            hi = spec.get("max")
            if lo is not None:
                n_below = int((col < lo).sum())
                if n_below > 0:
                    self.violations.append({
                        "dataset": label,
                        "field": field,
                        "check": "below_min",
                        "detail": f"{n_below} values below minimum {lo}",
                        "count": n_below,
                    })
            if hi is not None:
                n_above = int((col > hi).sum())
                if n_above > 0:
                    self.violations.append({
                        "dataset": label,
                        "field": field,
                        "check": "above_max",
                        "detail": f"{n_above} values above maximum {hi}",
                        "count": n_above,
                    })

    def _check_duplicates(self, df: pd.DataFrame, label: str):
        if "partant_uid" in df.columns:
            n_dup = int(df["partant_uid"].duplicated().sum())
            if n_dup > 0:
                self.violations.append({
                    "dataset": label,
                    "field": "partant_uid",
                    "check": "duplicate_key",
                    "detail": f"{n_dup} duplicate partant_uid values",
                    "count": n_dup,
                })

    # ------------------------------------------------------------------
    def save_report(self, report: pd.DataFrame, name: str = "schema_violations") -> Path:
        out = OUTPUT_DIR / f"{name}.csv"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        report.to_csv(out, index=False, encoding="utf-8")
        logger.info("  Saved report -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Data Schema Validator")
    parser.add_argument("--input", required=True, help="Path to Parquet or JSONL file to validate")
    parser.add_argument("--schema", choices=["partants", "features"], default="partants")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    inp = Path(args.input)
    if inp.suffix == ".parquet":
        df = pd.read_parquet(inp)
    else:
        df = pd.read_json(inp, lines=True, encoding="utf-8")
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

    schema = PARTANTS_SCHEMA if args.schema == "partants" else FEATURES_SCHEMA
    validator = DataSchemaValidator(schema=schema)
    report = validator.validate(df, label=inp.stem)

    if not report.empty:
        validator.save_report(report)
        print(report.to_string(index=False))
    else:
        print("[OK] No schema violations detected.")


if __name__ == "__main__":
    main()
