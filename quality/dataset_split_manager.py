#!/usr/bin/env python3
"""
quality/dataset_split_manager.py
================================
Gestionnaire de decoupage temporel train/val/test pour le pipeline ML.

Decoupage par date (et non aleatoire) pour eviter les fuites temporelles.
Supporte la validation walk-forward avec fenetre glissante et ensemble
d'entrainement extensible.

Aucun appel API : traitement 100% local.

Usage :
    python3 quality/dataset_split_manager.py --features path/to/features.parquet --labels path/to/labels.parquet
    python3 quality/dataset_split_manager.py --features path/to/features.parquet --labels path/to/labels.parquet \
        --train-end 2023-12-31 --val-end 2024-06-30
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
OUTPUT_DIR = _PROJECT_ROOT / "output" / "quality"

DEFAULT_TRAIN_END = "2024-01-01"
DEFAULT_VAL_END = "2024-07-01"

DATE_COL = "date_reunion_iso"


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# STRUCTURES
# ===========================================================================

@dataclass
class SplitInfo:
    """Metadata pour un split."""
    name: str
    date_min: str
    date_max: str
    n_rows: int
    n_races: int
    indices: list[int] = field(default_factory=list)


@dataclass
class SplitResult:
    """Resultat complet du decoupage."""
    train: SplitInfo
    val: SplitInfo
    test: SplitInfo
    train_end: str
    val_end: str
    leakage_check: bool  # True = pas de fuite


# ===========================================================================
# CORE
# ===========================================================================

class DatasetSplitManager:
    """Gestionnaire de decoupage temporel train/val/test.

    Parameters
    ----------
    train_end : str
        Date exclusive de fin d'entrainement (format YYYY-MM-DD).
        Les donnees avec date < train_end vont dans train.
    val_end : str
        Date exclusive de fin de validation.
        Les donnees avec train_end <= date < val_end vont dans val.
        Les donnees avec date >= val_end vont dans test.
    date_col : str
        Nom de la colonne de date dans le DataFrame.
    """

    def __init__(
        self,
        train_end: str = DEFAULT_TRAIN_END,
        val_end: str = DEFAULT_VAL_END,
        date_col: str = DATE_COL,
    ):
        self.train_end = train_end
        self.val_end = val_end
        self.date_col = date_col
        self.logger = setup_logging("dataset_split_manager")

    def split(self, df: pd.DataFrame) -> SplitResult:
        """Decoupe le DataFrame en train/val/test par date.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec une colonne date (date_col).

        Returns
        -------
        SplitResult avec les indices et metadata de chaque split.
        """
        self.logger.info("Decoupage temporel : train < %s, val [%s, %s), test >= %s",
                         self.train_end, self.train_end, self.val_end, self.val_end)

        dates = pd.to_datetime(df[self.date_col])
        train_end_dt = pd.Timestamp(self.train_end)
        val_end_dt = pd.Timestamp(self.val_end)

        mask_train = dates < train_end_dt
        mask_val = (dates >= train_end_dt) & (dates < val_end_dt)
        mask_test = dates >= val_end_dt

        idx_train = df.index[mask_train].tolist()
        idx_val = df.index[mask_val].tolist()
        idx_test = df.index[mask_test].tolist()

        def _make_info(name: str, mask: pd.Series, indices: list[int]) -> SplitInfo:
            subset = df.loc[mask]
            if len(subset) == 0:
                return SplitInfo(name=name, date_min="", date_max="", n_rows=0, n_races=0, indices=indices)
            date_vals = dates[mask]
            n_races = subset["course_uid"].nunique() if "course_uid" in subset.columns else 0
            return SplitInfo(
                name=name,
                date_min=str(date_vals.min().date()),
                date_max=str(date_vals.max().date()),
                n_rows=len(subset),
                n_races=n_races,
                indices=indices,
            )

        train_info = _make_info("train", mask_train, idx_train)
        val_info = _make_info("val", mask_val, idx_val)
        test_info = _make_info("test", mask_test, idx_test)

        self.logger.info("  train: %d lignes (%s -> %s)", train_info.n_rows, train_info.date_min, train_info.date_max)
        self.logger.info("  val  : %d lignes (%s -> %s)", val_info.n_rows, val_info.date_min, val_info.date_max)
        self.logger.info("  test : %d lignes (%s -> %s)", test_info.n_rows, test_info.date_min, test_info.date_max)

        # Verification anti-fuite
        leakage_ok = self._validate_no_leakage(df, dates, train_end_dt, val_end_dt)

        return SplitResult(
            train=train_info,
            val=val_info,
            test=test_info,
            train_end=self.train_end,
            val_end=self.val_end,
            leakage_check=leakage_ok,
        )

    def split_dataframes(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Retourne directement les 3 DataFrames (train, val, test).

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame complet.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        """
        result = self.split(df)
        return (
            df.loc[result.train.indices],
            df.loc[result.val.indices],
            df.loc[result.test.indices],
        )

    def walk_forward_splits(
        self,
        df: pd.DataFrame,
        n_splits: int = 5,
        val_months: int = 3,
        expanding: bool = True,
        min_train_months: int = 12,
    ) -> list[tuple[list[int], list[int]]]:
        """Genere des splits walk-forward pour validation temporelle.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec colonne date.
        n_splits : int
            Nombre de folds.
        val_months : int
            Duree de chaque fenetre de validation en mois.
        expanding : bool
            Si True, l'ensemble d'entrainement s'etend. Si False, fenetre glissante fixe.
        min_train_months : int
            Duree minimale de l'ensemble d'entrainement en mois.

        Returns
        -------
        list[tuple[list[int], list[int]]]
            Liste de (train_indices, val_indices) pour chaque fold.
        """
        self.logger.info("Walk-forward : %d splits, val=%d mois, expanding=%s",
                         n_splits, val_months, expanding)

        dates = pd.to_datetime(df[self.date_col])
        date_min = dates.min()
        date_max = dates.max()

        total_months = (date_max.year - date_min.year) * 12 + (date_max.month - date_min.month)
        val_total_months = n_splits * val_months
        train_start_offset = max(min_train_months, total_months - val_total_months)

        splits = []
        for i in range(n_splits):
            val_start = date_min + pd.DateOffset(months=train_start_offset + i * val_months)
            val_end = val_start + pd.DateOffset(months=val_months)

            if expanding:
                train_start = date_min
            else:
                train_start = val_start - pd.DateOffset(months=min_train_months)

            mask_train = (dates >= train_start) & (dates < val_start)
            mask_val = (dates >= val_start) & (dates < val_end)

            idx_train = df.index[mask_train].tolist()
            idx_val = df.index[mask_val].tolist()

            if len(idx_train) > 0 and len(idx_val) > 0:
                self.logger.info("  Fold %d: train=%d (%s->%s), val=%d (%s->%s)",
                                 i, len(idx_train),
                                 dates[mask_train].min().date(), dates[mask_train].max().date(),
                                 len(idx_val),
                                 dates[mask_val].min().date(), dates[mask_val].max().date())
                splits.append((idx_train, idx_val))

        self.logger.info("  %d folds generes", len(splits))
        return splits

    def _validate_no_leakage(
        self,
        df: pd.DataFrame,
        dates: pd.Series,
        train_end_dt: pd.Timestamp,
        val_end_dt: pd.Timestamp,
    ) -> bool:
        """Verifie qu'aucune donnee future ne fuit dans les ensembles anterieurs.

        Returns
        -------
        bool
            True si aucune fuite detectee.
        """
        # Verifier que les dates de train sont strictement avant train_end
        train_dates = dates[dates < train_end_dt]
        val_dates = dates[(dates >= train_end_dt) & (dates < val_end_dt)]
        test_dates = dates[dates >= val_end_dt]

        leakage_ok = True

        if len(train_dates) > 0 and len(val_dates) > 0:
            if train_dates.max() >= val_dates.min():
                self.logger.warning("FUITE: dates train chevauchent dates val !")
                leakage_ok = False

        if len(val_dates) > 0 and len(test_dates) > 0:
            if val_dates.max() >= test_dates.min():
                self.logger.warning("FUITE: dates val chevauchent dates test !")
                leakage_ok = False

        if len(train_dates) > 0 and len(test_dates) > 0:
            if train_dates.max() >= test_dates.min():
                self.logger.warning("FUITE: dates train chevauchent dates test !")
                leakage_ok = False

        if leakage_ok:
            self.logger.info("  Verification anti-fuite : OK")
        return leakage_ok

    def export_split_indices(self, result: SplitResult, output_dir: Path) -> None:
        """Exporte les indices de chaque split en JSON.

        Parameters
        ----------
        result : SplitResult
            Resultat du decoupage.
        output_dir : Path
            Repertoire de sortie.
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        report = {
            "train_end": result.train_end,
            "val_end": result.val_end,
            "leakage_check": result.leakage_check,
            "train": {
                "n_rows": result.train.n_rows,
                "n_races": result.train.n_races,
                "date_min": result.train.date_min,
                "date_max": result.train.date_max,
            },
            "val": {
                "n_rows": result.val.n_rows,
                "n_races": result.val.n_races,
                "date_min": result.val.date_min,
                "date_max": result.val.date_max,
            },
            "test": {
                "n_rows": result.test.n_rows,
                "n_races": result.test.n_races,
                "date_min": result.test.date_min,
                "date_max": result.test.date_max,
            },
        }

        report_path = output_dir / "split_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self.logger.info("Rapport split sauve: %s", report_path)

        # Indices separement (peuvent etre volumineux)
        for split_info in [result.train, result.val, result.test]:
            idx_path = output_dir / f"split_indices_{split_info.name}.json"
            with open(idx_path, "w", encoding="utf-8") as f:
                json.dump(split_info.indices, f)
            self.logger.info("Indices %s sauves: %s (%d)", split_info.name, idx_path, len(split_info.indices))


# ===========================================================================
# MAIN (CLI)
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Gestionnaire de decoupage temporel train/val/test"
    )
    parser.add_argument(
        "--features", type=str, required=True,
        help="Chemin vers le fichier features (Parquet/CSV/JSON)"
    )
    parser.add_argument(
        "--labels", type=str, default=None,
        help="Chemin vers le fichier labels (optionnel, pour jointure)"
    )
    parser.add_argument(
        "--train-end", type=str, default=DEFAULT_TRAIN_END,
        help=f"Date fin train exclusive (defaut: {DEFAULT_TRAIN_END})"
    )
    parser.add_argument(
        "--val-end", type=str, default=DEFAULT_VAL_END,
        help=f"Date fin val exclusive (defaut: {DEFAULT_VAL_END})"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie"
    )
    parser.add_argument(
        "--walk-forward", action="store_true",
        help="Generer des splits walk-forward"
    )
    parser.add_argument(
        "--n-splits", type=int, default=5,
        help="Nombre de folds walk-forward (defaut: 5)"
    )
    args = parser.parse_args()

    logger = setup_logging("dataset_split_manager")
    logger.info("=" * 70)
    logger.info("dataset_split_manager.py — Decoupage temporel")
    logger.info("=" * 70)

    # Chargement
    features_path = Path(args.features)
    if not features_path.exists():
        logger.error("Fichier introuvable: %s", features_path)
        sys.exit(1)

    suffix = features_path.suffix.lower()
    if suffix == ".parquet":
        df = pd.read_parquet(features_path)
    elif suffix == ".csv":
        df = pd.read_csv(features_path)
    elif suffix == ".json":
        df = pd.read_json(features_path)
    else:
        logger.error("Format non supporte: %s", suffix)
        sys.exit(1)

    logger.info("Charge: %d lignes, %d colonnes", len(df), len(df.columns))

    # Split
    manager = DatasetSplitManager(
        train_end=args.train_end,
        val_end=args.val_end,
    )

    result = manager.split(df)
    output_dir = Path(args.output_dir)
    manager.export_split_indices(result, output_dir)

    # Walk-forward optionnel
    if args.walk_forward:
        wf_splits = manager.walk_forward_splits(df, n_splits=args.n_splits)
        wf_path = output_dir / "walk_forward_splits.json"
        wf_data = [
            {"fold": i, "train_n": len(tr), "val_n": len(va)}
            for i, (tr, va) in enumerate(wf_splits)
        ]
        with open(wf_path, "w", encoding="utf-8") as f:
            json.dump(wf_data, f, indent=2)
        logger.info("Walk-forward splits sauves: %s", wf_path)

    logger.info("Termine.")


if __name__ == "__main__":
    main()
