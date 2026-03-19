# -*- coding: utf-8 -*-
"""
Historical Dataset Builder
===========================
Build train / validation / test splits by date to prevent future leakage.
Ensures point-in-time correctness: the model never sees data from the future.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class HistoricalDatasetBuilder:
    """
    Split racing data chronologically.

    Default strategy
    ----------------
    - train : everything before *val_start*
    - val   : from *val_start* to *test_start* (exclusive)
    - test  : from *test_start* onward

    If no explicit dates are given the data is split 70 / 15 / 15 by date rank.
    """

    DATE_COL = "date_reunion_iso"

    def __init__(
        self,
        val_start: Optional[str] = None,
        test_start: Optional[str] = None,
        train_ratio: float = 0.70,
        val_ratio: float = 0.15,
        output_dir: Optional[str] = None,
        purge_days: int = 0,
    ):
        self.val_start = pd.Timestamp(val_start) if val_start else None
        self.test_start = pd.Timestamp(test_start) if test_start else None
        self.train_ratio = train_ratio
        self.val_ratio = val_ratio
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.purge_days = purge_days  # gap between train end and val start
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    def build_splits(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Return (train, val, test) DataFrames split by date."""
        df = self._ensure_date(df)
        df = df.sort_values(self.DATE_COL).reset_index(drop=True)

        if self.val_start and self.test_start:
            splits = self._split_by_dates(df)
        else:
            splits = self._split_by_ratio(df)

        train, val, test = splits
        self._log_splits(train, val, test)
        self._validate_no_leakage(train, val, test)
        return train, val, test

    # ------------------------------------------------------------------
    # Splitting strategies
    # ------------------------------------------------------------------
    def _split_by_dates(self, df: pd.DataFrame):
        purge_delta = pd.Timedelta(days=self.purge_days)
        train = df[df[self.DATE_COL] < (self.val_start - purge_delta)]
        val = df[(df[self.DATE_COL] >= self.val_start) & (df[self.DATE_COL] < self.test_start)]
        test = df[df[self.DATE_COL] >= self.test_start]
        return train, val, test

    def _split_by_ratio(self, df: pd.DataFrame):
        unique_dates = np.sort(df[self.DATE_COL].dropna().unique())
        n = len(unique_dates)
        train_end = unique_dates[int(n * self.train_ratio)]
        val_end = unique_dates[int(n * (self.train_ratio + self.val_ratio))]

        purge_delta = pd.Timedelta(days=self.purge_days)
        train = df[df[self.DATE_COL] < (train_end - purge_delta)]
        val = df[(df[self.DATE_COL] >= train_end) & (df[self.DATE_COL] < val_end)]
        test = df[df[self.DATE_COL] >= val_end]
        return train, val, test

    # ------------------------------------------------------------------
    # Expanding-window cross-validation
    # ------------------------------------------------------------------
    def expanding_window_cv(
        self,
        df: pd.DataFrame,
        n_splits: int = 5,
        min_train_days: int = 180,
        val_days: int = 30,
    ):
        """
        Yield (train, val) with expanding training window.

        Each fold adds *val_days* to training and slides the validation window.
        """
        df = self._ensure_date(df)
        df = df.sort_values(self.DATE_COL).reset_index(drop=True)

        dates = np.sort(df[self.DATE_COL].dropna().unique())
        start = dates[0]
        total_span = (dates[-1] - start).days
        step = max(1, (total_span - min_train_days - val_days) // n_splits)

        for i in range(n_splits):
            train_end = start + pd.Timedelta(days=min_train_days + i * step)
            val_start = train_end + pd.Timedelta(days=self.purge_days)
            val_end = val_start + pd.Timedelta(days=val_days)

            train = df[df[self.DATE_COL] < train_end]
            val = df[(df[self.DATE_COL] >= val_start) & (df[self.DATE_COL] < val_end)]

            if len(train) == 0 or len(val) == 0:
                continue
            logger.info(
                "  Fold %d: train %s..%s (%d), val %s..%s (%d)",
                i,
                str(train[self.DATE_COL].min().date()),
                str(train[self.DATE_COL].max().date()),
                len(train),
                str(val[self.DATE_COL].min().date()),
                str(val[self.DATE_COL].max().date()),
                len(val),
            )
            yield train, val

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def _validate_no_leakage(self, train, val, test):
        """Assert train dates < val dates < test dates."""
        if len(train) and len(val):
            max_train = train[self.DATE_COL].max()
            min_val = val[self.DATE_COL].min()
            if max_train >= min_val:
                logger.error(
                    "  LEAKAGE: train max date %s >= val min date %s",
                    max_train, min_val,
                )
            else:
                logger.info("  [OK] No train->val leakage (gap: %s)", min_val - max_train)

        if len(val) and len(test):
            max_val = val[self.DATE_COL].max()
            min_test = test[self.DATE_COL].min()
            if max_val >= min_test:
                logger.error(
                    "  LEAKAGE: val max date %s >= test min date %s",
                    max_val, min_test,
                )
            else:
                logger.info("  [OK] No val->test leakage (gap: %s)", min_test - max_val)

    def _log_splits(self, train, val, test):
        for name, split in [("train", train), ("val", val), ("test", test)]:
            if len(split) == 0:
                logger.warning("  %s: EMPTY", name)
            else:
                logger.info(
                    "  %s: %d rows  [%s .. %s]",
                    name,
                    len(split),
                    split[self.DATE_COL].min().date(),
                    split[self.DATE_COL].max().date(),
                )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _ensure_date(df: pd.DataFrame) -> pd.DataFrame:
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(
                df["date_reunion_iso"], errors="coerce"
            )
        return df

    def save_splits(
        self,
        train: pd.DataFrame,
        val: pd.DataFrame,
        test: pd.DataFrame,
        prefix: str = "split",
    ):
        for name, split in [("train", train), ("val", val), ("test", test)]:
            out = self.output_dir / f"{prefix}_{name}.parquet"
            split.to_parquet(out, index=False)
            logger.info("  Saved %s (%d rows)", out, len(split))


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Historical Dataset Builder")
    parser.add_argument("--input", required=True, help="Parquet input")
    parser.add_argument("--val-start", default=None, help="Validation start date YYYY-MM-DD")
    parser.add_argument("--test-start", default=None, help="Test start date YYYY-MM-DD")
    parser.add_argument("--purge-days", type=int, default=0)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    df = pd.read_parquet(args.input)
    builder = HistoricalDatasetBuilder(
        val_start=args.val_start,
        test_start=args.test_start,
        output_dir=args.output_dir,
        purge_days=args.purge_days,
    )
    train, val, test = builder.build_splits(df)
    builder.save_splits(train, val, test)
    print("[OK] Splits created.")


if __name__ == "__main__":
    main()
