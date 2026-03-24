# -*- coding: utf-8 -*-
"""
Outlier Cleaner
===============
Detect and handle outliers using IQR, Z-score, and Isolation Forest.
Supports clip, remove, or flag strategies.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("outlier_cleaner")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class OutlierCleaner:
    """Detect and handle outliers in numeric features."""

    def __init__(
        self,
        method: str = "iqr",
        action: str = "clip",
        iqr_factor: float = 1.5,
        z_threshold: float = 3.5,
        contamination: float = 0.05,
        exclude_cols: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        method : 'iqr', 'zscore', 'isolation_forest', or 'combined'
        action : 'clip' (cap values), 'nan' (set to NaN), 'remove' (drop rows), 'flag' (add column)
        iqr_factor : multiplier for IQR method (default 1.5)
        z_threshold : Z-score threshold (default 3.5)
        contamination : expected outlier fraction for Isolation Forest
        exclude_cols : columns to skip
        """
        self.method = method
        self.action = action
        self.iqr_factor = iqr_factor
        self.z_threshold = z_threshold
        self.contamination = contamination
        self.exclude_cols = set(exclude_cols or [
            "partant_uid", "course_uid", "reunion_uid", "horse_id",
            "date_reunion_iso", "num_pmu", "place_arrivee",
            "label_win", "label_place",
        ])
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.bounds_: Dict[str, Tuple[float, float]] = {}

    # ------------------------------------------------------------------
    # Fit bounds from training data
    # ------------------------------------------------------------------
    def fit(self, df: pd.DataFrame) -> "OutlierCleaner":
        """Learn outlier bounds from training data."""
        self.bounds_ = {}
        num_cols = self._get_numeric_cols(df)
        logger.info("Fitting outlier bounds on %d numeric columns ...", len(num_cols))

        for col in num_cols:
            s = df[col].dropna()
            if len(s) < 10:
                continue

            if self.method in ("iqr", "combined"):
                q1 = float(s.quantile(0.25))
                q3 = float(s.quantile(0.75))
                iqr = q3 - q1
                lo = q1 - self.iqr_factor * iqr
                hi = q3 + self.iqr_factor * iqr
            elif self.method == "zscore":
                mu = float(s.mean())
                sigma = float(s.std())
                if sigma == 0:
                    continue
                lo = mu - self.z_threshold * sigma
                hi = mu + self.z_threshold * sigma
            else:
                # For isolation_forest, no pre-computed bounds
                continue

            self.bounds_[col] = (lo, hi)

        logger.info("  -> bounds computed for %d columns", len(self.bounds_))
        return self

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply outlier handling to DataFrame."""
        df = df.copy()
        total_flagged = 0

        if self.method == "isolation_forest":
            return self._isolation_forest_transform(df)

        for col, (lo, hi) in self.bounds_.items():
            if col not in df.columns:
                continue
            mask = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
            n_out = int(mask.sum())
            if n_out == 0:
                continue
            total_flagged += n_out

            if self.action == "clip":
                df[col] = df[col].clip(lower=lo, upper=hi)
            elif self.action == "nan":
                df.loc[mask, col] = np.nan
            elif self.action == "remove":
                df = df[~mask]
            elif self.action == "flag":
                df[f"{col}_outlier"] = mask.astype(int)

            logger.info("  %s: %d outliers (%.1f%%) [%.2f, %.2f] -> %s",
                        col, n_out, n_out / len(df) * 100, lo, hi, self.action)

        logger.info("  -> total outliers handled: %d", total_flagged)
        return df

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.fit(df).transform(df)

    # ------------------------------------------------------------------
    # Isolation Forest
    # ------------------------------------------------------------------
    def _isolation_forest_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            logger.warning("  sklearn not available; falling back to IQR method")
            self.method = "iqr"
            return self.fit_transform(df)

        num_cols = self._get_numeric_cols(df)
        if not num_cols:
            return df

        X = df[num_cols].fillna(0).values
        iso = IsolationForest(contamination=self.contamination, random_state=42, n_jobs=-1)
        preds = iso.fit_predict(X)
        mask = preds == -1
        n_out = int(mask.sum())
        logger.info("  Isolation Forest flagged %d outlier rows (%.1f%%)", n_out, n_out / len(df) * 100)

        if self.action == "remove":
            df = df[~mask].reset_index(drop=True)
        elif self.action == "flag":
            df["is_outlier_row"] = mask.astype(int)
        elif self.action == "nan":
            for col in num_cols:
                df.loc[mask, col] = np.nan

        return df

    # ------------------------------------------------------------------
    # Detection report (no modification)
    # ------------------------------------------------------------------
    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a report of outliers without modifying data."""
        if not self.bounds_:
            self.fit(df)

        rows = []
        for col, (lo, hi) in self.bounds_.items():
            if col not in df.columns:
                continue
            s = df[col].dropna()
            n_below = int((s < lo).sum())
            n_above = int((s > hi).sum())
            n_total = n_below + n_above
            rows.append({
                "column": col,
                "lower_bound": round(lo, 4),
                "upper_bound": round(hi, 4),
                "n_below": n_below,
                "n_above": n_above,
                "n_total": n_total,
                "pct_outlier": round(n_total / len(s) * 100, 2) if len(s) else 0,
            })
        return pd.DataFrame(rows).sort_values("n_total", ascending=False)

    # ------------------------------------------------------------------
    def _get_numeric_cols(self, df: pd.DataFrame) -> List[str]:
        return [c for c in df.select_dtypes(include=[np.number]).columns if c not in self.exclude_cols]

    def save(self, df: pd.DataFrame, name: str = "outlier_cleaned") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Outlier Cleaner")
    parser.add_argument("--input", required=True, help="Parquet file")
    parser.add_argument("--method", choices=["iqr", "zscore", "isolation_forest"], default="iqr")
    parser.add_argument("--action", choices=["clip", "nan", "remove", "flag"], default="clip")
    parser.add_argument("--iqr-factor", type=float, default=1.5)
    parser.add_argument("--z-threshold", type=float, default=3.5)
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    cleaner = OutlierCleaner(
        method=args.method, action=args.action,
        iqr_factor=args.iqr_factor, z_threshold=args.z_threshold,
        output_dir=args.output_dir,
    )

    if args.report_only:
        report = cleaner.detect(df)
        print(report.to_string(index=False))
    else:
        df_clean = cleaner.fit_transform(df)
        cleaner.save(df_clean)
        print("[OK] Outlier cleaning complete.")


if __name__ == "__main__":
    main()
