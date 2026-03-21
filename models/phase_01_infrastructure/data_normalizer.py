# -*- coding: utf-8 -*-
"""
Data Normalizer
===============
StandardScaler, MinMaxScaler, RobustScaler for numeric features.
Save and load fitted scalers for inference.
"""

import argparse
import json
import logging
import os
import pickle
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("data_normalizer")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class DataNormalizer:
    """Normalize numeric features with configurable scalers."""

    SCALER_TYPES = {"standard", "minmax", "robust", "none"}

    def __init__(
        self,
        method: str = "standard",
        exclude_cols: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
    ):
        if method not in self.SCALER_TYPES:
            raise ValueError(f"Unknown scaler: {method}. Choose from {self.SCALER_TYPES}")
        self.method = method
        self.exclude_cols = set(exclude_cols or [
            "partant_uid", "course_uid", "reunion_uid", "horse_id",
            "date_reunion_iso", "num_pmu", "place_arrivee",
            "label_win", "label_place",
        ])
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.params_: Dict[str, Dict[str, float]] = {}
        self.columns_: List[str] = []

    # ------------------------------------------------------------------
    # Fit
    # ------------------------------------------------------------------
    def fit(self, df: pd.DataFrame) -> "DataNormalizer":
        """Learn scaling parameters from training data."""
        self.columns_ = self._get_numeric_cols(df)
        self.params_ = {}
        logger.info("Fitting %s scaler on %d columns ...", self.method, len(self.columns_))

        for col in self.columns_:
            s = df[col].dropna()
            if len(s) == 0:
                continue

            if self.method == "standard":
                self.params_[col] = {
                    "mean": float(s.mean()),
                    "std": float(s.std()) if s.std() != 0 else 1.0,
                }
            elif self.method == "minmax":
                self.params_[col] = {
                    "min": float(s.min()),
                    "max": float(s.max()) if s.max() != s.min() else float(s.min()) + 1.0,
                }
            elif self.method == "robust":
                self.params_[col] = {
                    "median": float(s.median()),
                    "iqr": float(s.quantile(0.75) - s.quantile(0.25)) or 1.0,
                }

        logger.info("  -> parameters learned for %d columns", len(self.params_))
        return self

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply normalization to DataFrame."""
        df = df.copy()

        for col, p in self.params_.items():
            if col not in df.columns:
                continue

            if self.method == "standard":
                df[col] = (df[col] - p["mean"]) / p["std"]
            elif self.method == "minmax":
                df[col] = (df[col] - p["min"]) / (p["max"] - p["min"])
            elif self.method == "robust":
                df[col] = (df[col] - p["median"]) / p["iqr"]

        logger.info("  Normalized %d columns (%s)", len(self.params_), self.method)
        return df

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.fit(df).transform(df)

    # ------------------------------------------------------------------
    # Inverse transform
    # ------------------------------------------------------------------
    def inverse_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reverse the normalization."""
        df = df.copy()
        for col, p in self.params_.items():
            if col not in df.columns:
                continue
            if self.method == "standard":
                df[col] = df[col] * p["std"] + p["mean"]
            elif self.method == "minmax":
                df[col] = df[col] * (p["max"] - p["min"]) + p["min"]
            elif self.method == "robust":
                df[col] = df[col] * p["iqr"] + p["median"]
        return df

    # ------------------------------------------------------------------
    # Save / Load
    # ------------------------------------------------------------------
    def save_scaler(self, name: str = "scaler") -> Path:
        """Save scaler parameters as JSON."""
        out = self.output_dir / f"{name}_{self.method}.json"
        payload = {
            "method": self.method,
            "columns": self.columns_,
            "params": self.params_,
        }
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=True)
        logger.info("  Saved scaler -> %s", out)
        return out

    @classmethod
    def load_scaler(cls, path: str) -> "DataNormalizer":
        """Load scaler from JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        obj = cls(method=payload["method"])
        obj.columns_ = payload["columns"]
        obj.params_ = payload["params"]
        logger.info("  Loaded scaler from %s (%s, %d cols)", path, obj.method, len(obj.params_))
        return obj

    def save_sklearn_scaler(self, name: str = "scaler") -> Path:
        """Save a fitted sklearn-style scaler via pickle."""
        out = self.output_dir / f"{name}_{self.method}.pkl"
        with open(out, "wb") as f:
            pickle.dump({"method": self.method, "params": self.params_, "columns": self.columns_}, f)
        logger.info("  Saved sklearn-compatible scaler -> %s", out)
        return out

    # ------------------------------------------------------------------
    def _get_numeric_cols(self, df: pd.DataFrame) -> List[str]:
        return [c for c in df.select_dtypes(include=[np.number]).columns if c not in self.exclude_cols]

    def save(self, df: pd.DataFrame, name: str = "normalized_dataset") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Data Normalizer")
    parser.add_argument("--input", required=True, help="Parquet file")
    parser.add_argument("--method", choices=["standard", "minmax", "robust"], default="standard")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    normalizer = DataNormalizer(method=args.method, output_dir=args.output_dir)
    df_norm = normalizer.fit_transform(df)
    normalizer.save(df_norm)
    normalizer.save_scaler()
    print("[OK] Normalization complete (%s)." % args.method)


if __name__ == "__main__":
    main()
