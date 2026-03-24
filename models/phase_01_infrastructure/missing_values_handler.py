# -*- coding: utf-8 -*-
"""
Missing Values Handler
======================
Per-field imputation strategy: mean, median, mode, forward-fill,
group-mean, or model-based imputation via KNN / iterative.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("missing_values_handler")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"

# ---------------------------------------------------------------------------
# Default imputation strategies per field type
# ---------------------------------------------------------------------------
DEFAULT_STRATEGIES: Dict[str, str] = {
    # Numeric horse stats
    "age": "median",
    "distance": "median",
    "nb_courses_carriere": "median",
    "nb_victoires_carriere": "median",
    "nb_places_carriere": "median",
    "nb_places_2eme": "zero",
    "nb_places_3eme": "zero",
    "gains_carriere_euros": "median",
    "gains_annee_euros": "median",
    "poids_porte_kg": "group_median",
    "poids_base_kg": "group_median",
    "surcharge_decharge_kg": "zero",
    "handicap_valeur": "median",
    "place_corde": "median",
    "cote_prob": "median",
    "cote_direct": "median",
    "supplement_euros": "zero",
    # Categorical
    "sexe": "mode",
    "race": "mode",
    "robe": "mode",
    "allure": "mode",
    "discipline": "mode",
    # Forward-fill (per horse over time)
    "jockey_driver": "ffill",
    "entraineur": "ffill",
}


class MissingValuesHandler:
    """Apply per-field imputation strategies."""

    VALID_STRATEGIES = {"mean", "median", "mode", "zero", "ffill", "bfill",
                        "group_median", "group_mean", "knn", "drop", "constant"}

    def __init__(
        self,
        strategies: Optional[Dict[str, str]] = None,
        group_col: str = "discipline",
        output_dir: Optional[str] = None,
    ):
        self.strategies = strategies or DEFAULT_STRATEGIES
        self.group_col = group_col
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.fill_values_: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Fit: compute fill values from training data
    # ------------------------------------------------------------------
    def fit(self, df: pd.DataFrame) -> "MissingValuesHandler":
        """Learn fill values from training data."""
        logger.info("Fitting imputation values on %d rows ...", len(df))
        self.fill_values_ = {}

        for col, strat in self.strategies.items():
            if col not in df.columns:
                continue
            if strat == "mean":
                self.fill_values_[col] = float(df[col].mean()) if pd.api.types.is_numeric_dtype(df[col]) else None
            elif strat == "median":
                self.fill_values_[col] = float(df[col].median()) if pd.api.types.is_numeric_dtype(df[col]) else None
            elif strat == "mode":
                modes = df[col].mode()
                self.fill_values_[col] = modes.iloc[0] if len(modes) > 0 else None
            elif strat == "zero":
                self.fill_values_[col] = 0
            elif strat == "constant":
                self.fill_values_[col] = "UNKNOWN"
            elif strat in ("group_median", "group_mean"):
                if self.group_col in df.columns:
                    agg = "median" if strat == "group_median" else "mean"
                    grouped = df.groupby(self.group_col)[col].agg(agg).to_dict()
                    self.fill_values_[col] = {"_grouped": grouped, "_fallback": float(df[col].median())}
                else:
                    self.fill_values_[col] = float(df[col].median()) if pd.api.types.is_numeric_dtype(df[col]) else None
            # ffill/bfill/knn/drop don't need pre-fitted values

        logger.info("  -> learned fill values for %d columns", len(self.fill_values_))
        return self

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply imputation strategies to DataFrame."""
        df = df.copy()
        before_nulls = int(df.isna().sum().sum())
        logger.info("Imputing missing values (%d total nulls) ...", before_nulls)

        for col, strat in self.strategies.items():
            if col not in df.columns or df[col].isna().sum() == 0:
                continue

            n_missing = int(df[col].isna().sum())

            if strat in ("mean", "median", "mode", "zero", "constant"):
                fv = self.fill_values_.get(col)
                if fv is not None:
                    df[col] = df[col].fillna(fv)

            elif strat in ("group_median", "group_mean"):
                fv = self.fill_values_.get(col, {})
                if isinstance(fv, dict) and "_grouped" in fv:
                    grouped = fv["_grouped"]
                    fallback = fv["_fallback"]
                    if self.group_col in df.columns:
                        df[col] = df.apply(
                            lambda r: grouped.get(r[self.group_col], fallback)
                            if pd.isna(r[col]) else r[col],
                            axis=1,
                        )
                    else:
                        df[col] = df[col].fillna(fallback)
                elif fv is not None:
                    df[col] = df[col].fillna(fv)

            elif strat == "ffill":
                sort_cols = []
                if "horse_id" in df.columns:
                    sort_cols.append("horse_id")
                if "date_reunion_iso" in df.columns:
                    sort_cols.append("date_reunion_iso")
                if sort_cols:
                    df = df.sort_values(sort_cols)
                    if "horse_id" in df.columns:
                        df[col] = df.groupby("horse_id")[col].ffill()
                    else:
                        df[col] = df[col].ffill()
                else:
                    df[col] = df[col].ffill()

            elif strat == "bfill":
                df[col] = df[col].bfill()

            elif strat == "knn":
                df = self._knn_impute(df, col)

            elif strat == "drop":
                df = df.dropna(subset=[col])

            filled = n_missing - int(df[col].isna().sum()) if col in df.columns else n_missing
            logger.info("  %s: %d/%d filled (%s)", col, filled, n_missing, strat)

        after_nulls = int(df.isna().sum().sum())
        logger.info("  -> nulls: %d -> %d (filled %d)", before_nulls, after_nulls, before_nulls - after_nulls)
        return df

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fit on df and transform it."""
        return self.fit(df).transform(df)

    # ------------------------------------------------------------------
    # KNN imputation (optional, needs sklearn)
    # ------------------------------------------------------------------
    @staticmethod
    def _knn_impute(df: pd.DataFrame, col: str, n_neighbors: int = 5) -> pd.DataFrame:
        try:
            from sklearn.impute import KNNImputer
        except ImportError:
            logger.warning("  sklearn not available for KNN imputation on %s; using median", col)
            df[col] = df[col].fillna(df[col].median())
            return df

        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if col not in num_cols:
            return df
        imputer = KNNImputer(n_neighbors=n_neighbors)
        df[num_cols] = imputer.fit_transform(df[num_cols])
        return df

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    def null_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return per-column null counts and rates."""
        rows = []
        for col in df.columns:
            n = int(df[col].isna().sum())
            rows.append({
                "column": col,
                "null_count": n,
                "null_pct": round(n / len(df) * 100, 2) if len(df) else 0,
                "strategy": self.strategies.get(col, "none"),
            })
        return pd.DataFrame(rows).sort_values("null_count", ascending=False)

    # ------------------------------------------------------------------
    def save(self, df: pd.DataFrame, name: str = "imputed_dataset") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out

    def save_fill_values(self, name: str = "fill_values") -> Path:
        """Persist learned fill values for inference."""
        out = self.output_dir / f"{name}.json"
        serializable = {}
        for k, v in self.fill_values_.items():
            if isinstance(v, (int, float, str, bool, type(None))):
                serializable[k] = v
            elif isinstance(v, dict):
                serializable[k] = {
                    sk: (sv if isinstance(sv, (int, float, str)) else str(sv))
                    for sk, sv in v.items()
                }
            else:
                serializable[k] = str(v)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2, ensure_ascii=True)
        logger.info("  Saved fill values -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Missing Values Handler")
    parser.add_argument("--input", required=True, help="Parquet file to impute")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--report-only", action="store_true", help="Only show null report")
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    handler = MissingValuesHandler(output_dir=args.output_dir)

    if args.report_only:
        report = handler.null_report(df)
        print(report.to_string(index=False))
    else:
        df_clean = handler.fit_transform(df)
        handler.save(df_clean)
        handler.save_fill_values()
        print("[OK] Imputation complete.")


if __name__ == "__main__":
    main()
