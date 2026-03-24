# -*- coding: utf-8 -*-
"""
Data Quality Monitor
====================
Check null rates, distribution shifts, outliers per batch.
Generate quality reports for continuous monitoring.
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

logger = setup_logging("data_quality_monitor")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class DataQualityMonitor:
    """Monitor data quality across batches."""

    # Thresholds
    NULL_RATE_WARN = 0.10
    NULL_RATE_CRIT = 0.50
    SHIFT_THRESHOLD = 0.20  # relative change in mean
    OUTLIER_Z_THRESHOLD = 4.0

    def __init__(
        self,
        reference_stats: Optional[Dict[str, Dict[str, float]]] = None,
        output_dir: Optional[str] = None,
    ):
        self.reference_stats = reference_stats or {}
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.alerts: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Compute reference from training set
    # ------------------------------------------------------------------
    def fit_reference(self, df: pd.DataFrame):
        """Compute reference statistics from a training DataFrame."""
        self.reference_stats = {}
        num_cols = df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            s = df[col].dropna()
            if len(s) == 0:
                continue
            self.reference_stats[col] = {
                "mean": float(s.mean()),
                "std": float(s.std()),
                "median": float(s.median()),
                "q01": float(s.quantile(0.01)),
                "q99": float(s.quantile(0.99)),
                "null_rate": float(df[col].isna().mean()),
                "count": int(len(s)),
            }
        logger.info("  Reference stats computed for %d numeric columns", len(self.reference_stats))

    # ------------------------------------------------------------------
    # Run quality checks on a batch
    # ------------------------------------------------------------------
    def check_batch(self, df: pd.DataFrame, batch_id: str = "batch") -> pd.DataFrame:
        """Run all quality checks on a batch DataFrame."""
        self.alerts = []
        logger.info("Quality check on '%s' (%d rows, %d cols) ...", batch_id, len(df), len(df.columns))

        self._check_null_rates(df, batch_id)
        self._check_distribution_shift(df, batch_id)
        self._check_outlier_rates(df, batch_id)
        self._check_row_count(df, batch_id)
        self._check_constant_columns(df, batch_id)

        report = pd.DataFrame(self.alerts)
        n_warn = len(report[report["level"] == "WARNING"]) if not report.empty else 0
        n_crit = len(report[report["level"] == "CRITICAL"]) if not report.empty else 0
        logger.info("  -> %d warnings, %d critical alerts", n_warn, n_crit)
        return report

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------
    def _check_null_rates(self, df: pd.DataFrame, batch_id: str):
        for col in df.columns:
            rate = float(df[col].isna().mean())
            if rate >= self.NULL_RATE_CRIT:
                self.alerts.append({
                    "batch": batch_id, "check": "null_rate", "column": col,
                    "level": "CRITICAL", "value": round(rate, 4),
                    "detail": f"Null rate {rate:.1%} exceeds {self.NULL_RATE_CRIT:.0%}",
                })
            elif rate >= self.NULL_RATE_WARN:
                self.alerts.append({
                    "batch": batch_id, "check": "null_rate", "column": col,
                    "level": "WARNING", "value": round(rate, 4),
                    "detail": f"Null rate {rate:.1%} exceeds {self.NULL_RATE_WARN:.0%}",
                })

    def _check_distribution_shift(self, df: pd.DataFrame, batch_id: str):
        if not self.reference_stats:
            return
        num_cols = df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            if col not in self.reference_stats:
                continue
            ref = self.reference_stats[col]
            batch_mean = float(df[col].dropna().mean()) if df[col].notna().any() else None
            if batch_mean is None or ref["std"] == 0:
                continue
            # Relative shift
            shift = abs(batch_mean - ref["mean"]) / max(abs(ref["mean"]), 1e-9)
            if shift > self.SHIFT_THRESHOLD:
                self.alerts.append({
                    "batch": batch_id, "check": "distribution_shift", "column": col,
                    "level": "WARNING", "value": round(shift, 4),
                    "detail": f"Mean shifted {shift:.1%}: ref={ref['mean']:.4f}, batch={batch_mean:.4f}",
                })

    def _check_outlier_rates(self, df: pd.DataFrame, batch_id: str):
        if not self.reference_stats:
            return
        num_cols = df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            if col not in self.reference_stats:
                continue
            ref = self.reference_stats[col]
            if ref["std"] == 0:
                continue
            s = df[col].dropna()
            if len(s) == 0:
                continue
            z = np.abs((s - ref["mean"]) / ref["std"])
            outlier_rate = float((z > self.OUTLIER_Z_THRESHOLD).mean())
            if outlier_rate > 0.05:
                self.alerts.append({
                    "batch": batch_id, "check": "outlier_rate", "column": col,
                    "level": "WARNING", "value": round(outlier_rate, 4),
                    "detail": f"{outlier_rate:.1%} of values are >4 sigma from reference mean",
                })

    def _check_row_count(self, df: pd.DataFrame, batch_id: str):
        if len(df) == 0:
            self.alerts.append({
                "batch": batch_id, "check": "empty_batch", "column": "_all_",
                "level": "CRITICAL", "value": 0,
                "detail": "Batch is empty",
            })

    def _check_constant_columns(self, df: pd.DataFrame, batch_id: str):
        for col in df.select_dtypes(include=[np.number]).columns:
            if df[col].dropna().nunique() <= 1:
                self.alerts.append({
                    "batch": batch_id, "check": "constant_column", "column": col,
                    "level": "WARNING", "value": 0,
                    "detail": f"Column has <= 1 unique value",
                })

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    def column_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a per-column summary (nulls, uniques, dtype, sample)."""
        rows = []
        for col in df.columns:
            rows.append({
                "column": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isna().sum()),
                "null_pct": round(float(df[col].isna().mean()) * 100, 2),
                "nunique": int(df[col].nunique()),
                "sample": str(df[col].dropna().iloc[0]) if df[col].notna().any() else "N/A",
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    def save_report(self, report: pd.DataFrame, name: str = "quality_report") -> Path:
        out = self.output_dir / f"{name}.csv"
        report.to_csv(out, index=False, encoding="utf-8")
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Data Quality Monitor")
    parser.add_argument("--input", required=True, help="Parquet file to check")
    parser.add_argument("--reference", default=None, help="Parquet file for reference stats")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    monitor = DataQualityMonitor(output_dir=args.output_dir)

    if args.reference:
        ref = pd.read_parquet(args.reference)
        monitor.fit_reference(ref)

    report = monitor.check_batch(df, batch_id=Path(args.input).stem)
    if not report.empty:
        monitor.save_report(report)
        print(report.to_string(index=False))
    else:
        print("[OK] No quality issues detected.")

    summary = monitor.column_summary(df)
    monitor.save_report(summary, name="column_summary")
    print("[OK] Quality monitoring complete.")


if __name__ == "__main__":
    main()
