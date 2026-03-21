#!/usr/bin/env python3
"""
quality/feature_stability_monitor.py
=====================================
Moniteur de stabilite des features dans le temps.

Calcule des statistiques mensuelles par feature et detecte la derive
(drift) via le PSI (Population Stability Index) entre une periode de
reference et une periode courante.

Aucun appel API : traitement 100% local.

Usage :
    python3 quality/feature_stability_monitor.py --features path/to/features.parquet
    python3 quality/feature_stability_monitor.py --features path/to/features.parquet \
        --ref-end 2023-06-30 --psi-threshold 0.25
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path("logs")
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output" / "quality"

DATE_COL = "date_reunion_iso"
PSI_THRESHOLD = 0.25
PSI_N_BINS = 10

# Colonnes exclues de l'analyse (identifiants, metadata)
EXCLUDE_COLS = {
    "partant_uid", "course_uid", "reunion_uid", "date_reunion_iso",
    "nom_cheval", "cle_partant", "source", "timestamp_collecte",
    "hippodrome_normalise", "hippodrome",
}


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("feature_stability_monitor")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "feature_stability_monitor.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ===========================================================================
# PSI
# ===========================================================================

def compute_psi(reference: np.ndarray, current: np.ndarray, n_bins: int = PSI_N_BINS) -> float:
    """Calcule le Population Stability Index entre deux distributions.

    Parameters
    ----------
    reference : np.ndarray
        Distribution de reference (periode historique).
    current : np.ndarray
        Distribution courante a comparer.
    n_bins : int
        Nombre de bins pour discretiser.

    Returns
    -------
    float
        Valeur PSI. > 0.25 indique un drift significatif.
    """
    # Retirer NaN
    ref_clean = reference[~np.isnan(reference)]
    cur_clean = current[~np.isnan(current)]

    if len(ref_clean) < 10 or len(cur_clean) < 10:
        return 0.0

    # Creer les bins a partir de la reference
    _, bin_edges = np.histogram(ref_clean, bins=n_bins)

    ref_counts = np.histogram(ref_clean, bins=bin_edges)[0]
    cur_counts = np.histogram(cur_clean, bins=bin_edges)[0]

    # Convertir en proportions avec epsilon pour eviter log(0)
    eps = 1e-6
    ref_pct = ref_counts / ref_counts.sum() + eps
    cur_pct = cur_counts / cur_counts.sum() + eps

    # PSI = sum((cur - ref) * ln(cur/ref))
    psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    return float(psi)


# ===========================================================================
# STRUCTURES
# ===========================================================================

@dataclass
class MonthlyStats:
    """Statistiques mensuelles pour une feature."""
    period: str  # YYYY-MM
    mean: Optional[float] = None
    std: Optional[float] = None
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    missing_pct: float = 0.0
    count: int = 0


@dataclass
class FeatureStabilityReport:
    """Rapport de stabilite pour une feature."""
    feature: str
    psi: float
    is_unstable: bool
    monthly_stats: list[MonthlyStats] = field(default_factory=list)


# ===========================================================================
# CORE
# ===========================================================================

class FeatureStabilityMonitor:
    """Moniteur de stabilite des features dans le temps.

    Parameters
    ----------
    date_col : str
        Nom de la colonne de date.
    psi_threshold : float
        Seuil PSI au-dela duquel une feature est consideree instable.
    n_bins : int
        Nombre de bins pour le calcul PSI.
    ref_end : str or None
        Date de fin de la periode de reference (format YYYY-MM-DD).
        Si None, utilise la premiere moitie des donnees.
    """

    def __init__(
        self,
        date_col: str = DATE_COL,
        psi_threshold: float = PSI_THRESHOLD,
        n_bins: int = PSI_N_BINS,
        ref_end: Optional[str] = None,
    ):
        self.date_col = date_col
        self.psi_threshold = psi_threshold
        self.n_bins = n_bins
        self.ref_end = ref_end
        self.logger = setup_logging()

    def compute_monthly_stats(self, df: pd.DataFrame) -> dict[str, list[MonthlyStats]]:
        """Calcule les statistiques mensuelles pour chaque feature numerique.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec colonne date et features.

        Returns
        -------
        dict[str, list[MonthlyStats]]
            Cle = nom de feature, valeur = liste de stats mensuelles.
        """
        dates = pd.to_datetime(df[self.date_col])
        df = df.copy()
        df["_period"] = dates.dt.to_period("M").astype(str)

        numeric_cols = [
            c for c in df.select_dtypes(include=[np.number]).columns
            if c not in EXCLUDE_COLS and not c.startswith("_")
        ]

        self.logger.info("Calcul stats mensuelles pour %d features numeriques", len(numeric_cols))

        result: dict[str, list[MonthlyStats]] = {}

        for col in numeric_cols:
            monthly = []
            for period, group in df.groupby("_period", sort=True):
                vals = group[col]
                n_total = len(vals)
                n_missing = vals.isna().sum()
                clean = vals.dropna()

                stats = MonthlyStats(
                    period=str(period),
                    count=n_total,
                    missing_pct=round(n_missing / n_total * 100, 2) if n_total > 0 else 0.0,
                )
                if len(clean) > 0:
                    stats.mean = round(float(clean.mean()), 6)
                    stats.std = round(float(clean.std()), 6) if len(clean) > 1 else 0.0
                    stats.min_val = float(clean.min())
                    stats.max_val = float(clean.max())

                monthly.append(stats)
            result[col] = monthly

        return result

    def detect_drift(self, df: pd.DataFrame) -> list[FeatureStabilityReport]:
        """Detecte le drift via PSI pour chaque feature numerique.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec colonne date et features.

        Returns
        -------
        list[FeatureStabilityReport]
            Rapport de stabilite par feature, trie par PSI decroissant.
        """
        dates = pd.to_datetime(df[self.date_col])

        # Determiner la periode de reference
        if self.ref_end:
            ref_end_dt = pd.Timestamp(self.ref_end)
        else:
            # Premiere moitie des donnees
            mid_date = dates.min() + (dates.max() - dates.min()) / 2
            ref_end_dt = mid_date

        mask_ref = dates < ref_end_dt
        mask_cur = dates >= ref_end_dt

        self.logger.info("Periode reference : < %s (%d lignes)", ref_end_dt.date(), mask_ref.sum())
        self.logger.info("Periode courante  : >= %s (%d lignes)", ref_end_dt.date(), mask_cur.sum())

        numeric_cols = [
            c for c in df.select_dtypes(include=[np.number]).columns
            if c not in EXCLUDE_COLS
        ]

        # Calcul des stats mensuelles
        monthly_stats = self.compute_monthly_stats(df)

        reports: list[FeatureStabilityReport] = []

        for col in numeric_cols:
            ref_vals = df.loc[mask_ref, col].values.astype(float)
            cur_vals = df.loc[mask_cur, col].values.astype(float)

            psi = compute_psi(ref_vals, cur_vals, self.n_bins)
            is_unstable = psi > self.psi_threshold

            report = FeatureStabilityReport(
                feature=col,
                psi=round(psi, 6),
                is_unstable=is_unstable,
                monthly_stats=monthly_stats.get(col, []),
            )
            reports.append(report)

            if is_unstable:
                self.logger.warning("  INSTABLE: %s — PSI=%.4f (seuil=%.2f)", col, psi, self.psi_threshold)

        # Trier par PSI decroissant
        reports.sort(key=lambda r: r.psi, reverse=True)

        n_unstable = sum(1 for r in reports if r.is_unstable)
        self.logger.info("  %d/%d features instables (PSI > %.2f)", n_unstable, len(reports), self.psi_threshold)

        return reports

    def generate_report(self, df: pd.DataFrame) -> dict:
        """Genere un rapport complet de stabilite.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec colonne date et features.

        Returns
        -------
        dict
            Rapport JSON-serialisable.
        """
        self.logger.info("=" * 70)
        self.logger.info("Rapport de stabilite des features")
        self.logger.info("=" * 70)

        reports = self.detect_drift(df)

        result = {
            "n_features": len(reports),
            "n_unstable": sum(1 for r in reports if r.is_unstable),
            "psi_threshold": self.psi_threshold,
            "unstable_features": [r.feature for r in reports if r.is_unstable],
            "features": [],
        }

        for r in reports:
            feat_data = {
                "feature": r.feature,
                "psi": r.psi,
                "is_unstable": r.is_unstable,
                "monthly_stats": [
                    {
                        "period": s.period,
                        "mean": s.mean,
                        "std": s.std,
                        "min": s.min_val,
                        "max": s.max_val,
                        "missing_pct": s.missing_pct,
                        "count": s.count,
                    }
                    for s in r.monthly_stats
                ],
            }
            result["features"].append(feat_data)

        return result


# ===========================================================================
# MAIN (CLI)
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Moniteur de stabilite des features (drift PSI)"
    )
    parser.add_argument(
        "--features", type=str, required=True,
        help="Chemin vers le fichier features (Parquet/CSV/JSON)"
    )
    parser.add_argument(
        "--ref-end", type=str, default=None,
        help="Date fin de la periode de reference (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--psi-threshold", type=float, default=PSI_THRESHOLD,
        help=f"Seuil PSI pour instabilite (defaut: {PSI_THRESHOLD})"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Chemin pour sauvegarder le rapport JSON"
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("feature_stability_monitor.py — Stabilite des features")
    logger.info("=" * 70)

    features_path = Path(args.features)
    if not features_path.exists():
        logger.error("Fichier introuvable: %s", features_path)
        sys.exit(1)

    suffix = features_path.suffix.lower()

    # For large Parquet files, use column-batch processing to limit RAM
    COLUMN_BATCH_SIZE = 50  # Process 50 columns at a time
    monitor = FeatureStabilityMonitor(
        psi_threshold=args.psi_threshold,
        ref_end=args.ref_end,
    )

    if suffix == ".parquet":
        import pyarrow.parquet as pq_reader
        schema = pq_reader.read_schema(features_path)
        all_cols = [f.name for f in schema]
        logger.info("Schema: %d colonnes totales", len(all_cols))

        # Find numeric columns by reading a small sample
        date_col = monitor.date_col
        sample_df = pd.read_parquet(features_path, columns=all_cols[:20] + ([date_col] if date_col not in all_cols[:20] else []))
        # We need the date column
        if date_col not in all_cols:
            logger.error("Colonne date '%s' introuvable", date_col)
            sys.exit(1)

        # Read only the date column first to build ref/cur masks
        logger.info("Lecture colonne date '%s' ...", date_col)
        dates_df = pd.read_parquet(features_path, columns=[date_col])
        dates = pd.to_datetime(dates_df[date_col])
        n_rows = len(dates)
        logger.info("Charge: %d lignes, %d colonnes disponibles", n_rows, len(all_cols))

        # Determine ref/cur split
        if args.ref_end:
            ref_end_dt = pd.Timestamp(args.ref_end)
        else:
            mid_date = dates.min() + (dates.max() - dates.min()) / 2
            ref_end_dt = mid_date
        mask_ref = dates < ref_end_dt
        mask_cur = dates >= ref_end_dt
        logger.info("Periode reference : < %s (%d lignes)", ref_end_dt.date(), mask_ref.sum())
        logger.info("Periode courante  : >= %s (%d lignes)", ref_end_dt.date(), mask_cur.sum())

        # Process columns in batches
        feature_cols = [c for c in all_cols if c not in EXCLUDE_COLS and c != date_col]
        all_reports = []
        n_unstable = 0

        for batch_start in range(0, len(feature_cols), COLUMN_BATCH_SIZE):
            batch_cols = feature_cols[batch_start:batch_start + COLUMN_BATCH_SIZE]
            logger.info("Batch %d-%d / %d colonnes ...",
                        batch_start + 1, batch_start + len(batch_cols), len(feature_cols))

            # Read only this batch of columns
            batch_df = pd.read_parquet(features_path, columns=batch_cols)

            # Filter to numeric columns only
            numeric_in_batch = batch_df.select_dtypes(include=[np.number]).columns.tolist()

            for col in numeric_in_batch:
                ref_vals = batch_df.loc[mask_ref, col].values.astype(float)
                cur_vals = batch_df.loc[mask_cur, col].values.astype(float)
                psi = compute_psi(ref_vals, cur_vals, monitor.n_bins)
                is_unstable = psi > monitor.psi_threshold

                report_item = FeatureStabilityReport(
                    feature=col,
                    psi=round(psi, 6),
                    is_unstable=is_unstable,
                    monthly_stats=[],  # Skip monthly stats to save RAM
                )
                all_reports.append(report_item)
                if is_unstable:
                    n_unstable += 1
                    logger.warning("  INSTABLE: %s — PSI=%.4f (seuil=%.2f)", col, psi, monitor.psi_threshold)

            del batch_df  # Free RAM

        all_reports.sort(key=lambda r: r.psi, reverse=True)
        logger.info("  %d/%d features instables (PSI > %.2f)", n_unstable, len(all_reports), monitor.psi_threshold)

        report = {
            "n_features": len(all_reports),
            "n_unstable": n_unstable,
            "psi_threshold": monitor.psi_threshold,
            "unstable_features": [r.feature for r in all_reports if r.is_unstable],
            "features": [
                {"feature": r.feature, "psi": r.psi, "is_unstable": r.is_unstable}
                for r in all_reports
            ],
        }

    else:
        # CSV/JSON — load fully (assumed to be small)
        if suffix == ".csv":
            df = pd.read_csv(features_path)
        elif suffix == ".json":
            df = pd.read_json(features_path)
        else:
            logger.error("Format non supporte: %s", suffix)
            sys.exit(1)

        logger.info("Charge: %d lignes, %d colonnes", len(df), len(df.columns))
        report = monitor.generate_report(df)

    # Affichage resume
    print(f"\n{'='*70}")
    print(f"RAPPORT DE STABILITE DES FEATURES")
    print(f"{'='*70}")
    print(f"Features analysees : {report['n_features']}")
    print(f"Features instables : {report['n_unstable']}")
    if report["unstable_features"]:
        print(f"\nFeatures instables (PSI > {args.psi_threshold}) :")
        for feat in report["unstable_features"]:
            psi_val = next((f["psi"] for f in report["features"] if f["feature"] == feat), 0)
            print(f"  ! {feat}: PSI={psi_val:.4f}")

    # Sauvegarder
    output_path = Path(args.output) if args.output else Path(__file__).resolve().parent.parent / "output" / "quality" / "feature_stability_report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Rapport sauve: %s", output_path)

    # Code de sortie
    if report["n_unstable"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
