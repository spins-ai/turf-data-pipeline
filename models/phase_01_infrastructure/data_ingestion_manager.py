# -*- coding: utf-8 -*-
"""
Data Ingestion Manager
======================
Load partants_master.jsonl, features_matrix.jsonl and label files.
Stream large JSONL files line-by-line and output clean DataFrames.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Generator, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
FEATURES_DIR = PROJECT_ROOT / "output" / "features"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"

PARTANTS_FILE = DATA_MASTER / "partants_master.jsonl"
FEATURES_FILE = FEATURES_DIR / "features_matrix.jsonl"

# Key columns always expected
KEY_COLS = ["partant_uid", "course_uid", "date_reunion_iso", "horse_id"]


class DataIngestionManager:
    """Load and stream JSONL data into pandas DataFrames."""

    def __init__(
        self,
        partants_path: Optional[str] = None,
        features_path: Optional[str] = None,
        labels_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        chunk_size: int = 50_000,
    ):
        self.partants_path = Path(partants_path) if partants_path else PARTANTS_FILE
        self.features_path = Path(features_path) if features_path else FEATURES_FILE
        self.labels_path = Path(labels_path) if labels_path else None
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.chunk_size = chunk_size
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Streaming helpers
    # ------------------------------------------------------------------
    @staticmethod
    def stream_jsonl(path: Path) -> Generator[dict, None, None]:
        """Yield dicts one line at a time from a JSONL file."""
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("  [skip] bad JSON at line %d in %s", lineno, path.name)

    @staticmethod
    def count_lines(path: Path) -> int:
        """Fast line count."""
        n = 0
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for _ in fh:
                n += 1
        return n

    # ------------------------------------------------------------------
    # Chunked loading
    # ------------------------------------------------------------------
    def load_jsonl_chunked(
        self, path: Path, columns: Optional[List[str]] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """Yield DataFrames of *chunk_size* rows from a JSONL file."""
        buf: List[dict] = []
        for rec in self.stream_jsonl(path):
            buf.append(rec)
            if len(buf) >= self.chunk_size:
                df = pd.DataFrame(buf)
                if columns:
                    df = df[[c for c in columns if c in df.columns]]
                yield df
                buf = []
        if buf:
            df = pd.DataFrame(buf)
            if columns:
                df = df[[c for c in columns if c in df.columns]]
            yield df

    # ------------------------------------------------------------------
    # Full-load helpers
    # ------------------------------------------------------------------
    def load_partants(self, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Load partants_master.jsonl into a single DataFrame."""
        logger.info("Loading partants from %s ...", self.partants_path)
        chunks = list(self.load_jsonl_chunked(self.partants_path, columns=columns))
        if not chunks:
            logger.warning("  No data found in %s", self.partants_path)
            return pd.DataFrame()
        df = pd.concat(chunks, ignore_index=True)
        df = self._coerce_dates(df)
        logger.info("  -> %d rows, %d cols loaded", len(df), len(df.columns))
        return df

    def load_features(self, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Load features_matrix.jsonl into a single DataFrame."""
        logger.info("Loading features from %s ...", self.features_path)
        chunks = list(self.load_jsonl_chunked(self.features_path, columns=columns))
        if not chunks:
            logger.warning("  No data found in %s", self.features_path)
            return pd.DataFrame()
        df = pd.concat(chunks, ignore_index=True)
        df = self._coerce_dates(df)
        logger.info("  -> %d rows, %d cols loaded", len(df), len(df.columns))
        return df

    def load_labels(self) -> pd.DataFrame:
        """Load labels file (JSONL or CSV)."""
        if self.labels_path is None:
            logger.info("No labels path specified; deriving from partants ...")
            return self._derive_labels()
        path = Path(self.labels_path)
        logger.info("Loading labels from %s ...", path)
        if path.suffix == ".csv":
            return pd.read_csv(path, encoding="utf-8")
        chunks = list(self.load_jsonl_chunked(path))
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    # ------------------------------------------------------------------
    # Label derivation
    # ------------------------------------------------------------------
    def _derive_labels(self) -> pd.DataFrame:
        """Derive binary labels from partants (place_arrivee == 1 -> win)."""
        cols = ["partant_uid", "course_uid", "date_reunion_iso", "place_arrivee"]
        df = self.load_partants(columns=cols)
        if "place_arrivee" not in df.columns:
            logger.warning("  place_arrivee not found; cannot derive labels")
            return pd.DataFrame()
        df["label_win"] = (df["place_arrivee"] == 1).astype(int)
        df["label_place"] = (df["place_arrivee"].between(1, 3)).astype(int)
        return df[["partant_uid", "course_uid", "date_reunion_iso", "label_win", "label_place"]]

    # ------------------------------------------------------------------
    # Merge partants + features + labels
    # ------------------------------------------------------------------
    def build_dataset(self) -> pd.DataFrame:
        """Merge partants, features, and labels on partant_uid."""
        partants = self.load_partants()
        features = self.load_features()
        labels = self.load_labels()

        logger.info("Merging dataset ...")
        # Merge features (drop overlapping non-key columns)
        feat_only = [c for c in features.columns if c not in partants.columns or c == "partant_uid"]
        df = partants.merge(features[feat_only], on="partant_uid", how="left")

        # Merge labels
        if not labels.empty and "partant_uid" in labels.columns:
            lbl_only = [c for c in labels.columns if c not in df.columns or c == "partant_uid"]
            df = df.merge(labels[lbl_only], on="partant_uid", how="left")

        logger.info("  -> merged dataset: %d rows, %d cols", len(df), len(df.columns))
        return df

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    @staticmethod
    def _coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
        """Convert date_reunion_iso to datetime if present."""
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(
                df["date_reunion_iso"], errors="coerce"
            )
        return df

    def save(self, df: pd.DataFrame, name: str = "ingested_dataset") -> Path:
        """Save DataFrame to Parquet."""
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved %s (%d rows)", out, len(df))
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Data Ingestion Manager")
    parser.add_argument("--partants", default=None, help="Path to partants_master.jsonl")
    parser.add_argument("--features", default=None, help="Path to features_matrix.jsonl")
    parser.add_argument("--labels", default=None, help="Path to labels file")
    parser.add_argument("--output-dir", default=None, help="Output directory")
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--merge", action="store_true", help="Build merged dataset")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    mgr = DataIngestionManager(
        partants_path=args.partants,
        features_path=args.features,
        labels_path=args.labels,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
    )

    if args.merge:
        df = mgr.build_dataset()
        mgr.save(df, "merged_dataset")
    else:
        df_p = mgr.load_partants()
        mgr.save(df_p, "partants_loaded")
        df_f = mgr.load_features()
        mgr.save(df_f, "features_loaded")

    print("[OK] Ingestion complete.")


if __name__ == "__main__":
    main()
