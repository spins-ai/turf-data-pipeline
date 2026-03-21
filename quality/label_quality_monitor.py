#!/usr/bin/env python3
"""
quality/label_quality_monitor.py
=================================
Moniteur de qualite des labels (variables cibles).

Verifie :
  1. Completude de ordreArrivee (pas de trous 1..N)
  2. Courses avec positions manquantes
  3. Gestion des disqualifications (DQ)
  4. Taux de victoire ~ 1/N sur l'ensemble du dataset
  5. Distributions par type de label (win, place, rang)
  6. Valeurs impossibles

Aucun appel API : traitement 100% local.

Usage :
    python3 quality/label_quality_monitor.py --labels path/to/labels.json
    python3 quality/label_quality_monitor.py --labels path/to/labels.parquet --partants path/to/partants.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path("logs")
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output" / "quality"

# Tolerance pour le taux de victoire attendu
WIN_RATE_TOLERANCE = 0.03  # +/- 3 points de pourcentage


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("label_quality_monitor")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "label_quality_monitor.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ===========================================================================
# CORE
# ===========================================================================

class LabelQualityMonitor:
    """Moniteur de qualite des labels.

    Parameters
    ----------
    win_rate_tolerance : float
        Tolerance autour du taux de victoire attendu (1/N).
    """

    def __init__(self, win_rate_tolerance: float = WIN_RATE_TOLERANCE):
        self.win_rate_tolerance = win_rate_tolerance
        self.logger = setup_logging()

    def check_arrival_order_completeness(
        self,
        df: pd.DataFrame,
    ) -> dict:
        """Verifie que ordreArrivee est complet (pas de trous 1..N) par course.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec colonnes course_uid et y_rang.

        Returns
        -------
        dict avec n_courses_checked, n_courses_with_gaps, flagged_courses.
        """
        self.logger.info("Verification completude ordreArrivee...")

        flagged: list[dict] = []
        n_checked = 0

        for course_uid, group in df.groupby("course_uid"):
            n_checked += 1
            ranks = group["y_rang"].dropna().astype(int).tolist()

            if not ranks:
                flagged.append({
                    "course_uid": course_uid,
                    "issue": "aucune_position",
                    "detail": "Toutes les positions sont manquantes",
                })
                continue

            ranks_sorted = sorted(ranks)
            expected = list(range(1, max(ranks_sorted) + 1))

            # Detecter les trous
            missing = set(expected) - set(ranks_sorted)
            duplicates = [r for r, c in Counter(ranks_sorted).items() if c > 1]

            if missing:
                flagged.append({
                    "course_uid": course_uid,
                    "issue": "positions_manquantes",
                    "detail": f"Positions manquantes: {sorted(missing)}",
                })

            if duplicates:
                flagged.append({
                    "course_uid": course_uid,
                    "issue": "positions_dupliquees",
                    "detail": f"Positions en double: {duplicates}",
                })

        self.logger.info("  %d courses verifiees, %d avec problemes", n_checked, len(flagged))

        return {
            "n_courses_checked": n_checked,
            "n_courses_with_gaps": len(flagged),
            "flagged_courses": flagged,
        }

    def check_missing_positions(self, df: pd.DataFrame) -> dict:
        """Identifie les courses avec des positions manquantes.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec colonnes course_uid et y_rang.

        Returns
        -------
        dict avec statistiques de completude.
        """
        self.logger.info("Verification positions manquantes...")

        total = len(df)
        n_missing = df["y_rang"].isna().sum()
        pct_missing = round(n_missing / total * 100, 2) if total > 0 else 0.0

        # Par course : proportion de positions manquantes
        course_stats = []
        for course_uid, group in df.groupby("course_uid"):
            n = len(group)
            n_miss = group["y_rang"].isna().sum()
            if n_miss > 0:
                course_stats.append({
                    "course_uid": course_uid,
                    "n_partants": n,
                    "n_missing": int(n_miss),
                    "pct_missing": round(n_miss / n * 100, 1),
                })

        self.logger.info("  %d/%d positions manquantes (%.2f%%)", n_missing, total, pct_missing)
        self.logger.info("  %d courses ont au moins une position manquante", len(course_stats))

        return {
            "total_partants": total,
            "n_missing_positions": int(n_missing),
            "pct_missing": pct_missing,
            "n_courses_with_missing": len(course_stats),
            "courses_with_missing": course_stats[:100],  # limiter la sortie
        }

    def check_dq_handling(self, df: pd.DataFrame) -> dict:
        """Verifie la gestion des disqualifications.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec y_rang et eventuellement statut/is_disqualifie.

        Returns
        -------
        dict avec statistiques DQ.
        """
        self.logger.info("Verification gestion DQ...")

        # Detecter les colonnes DQ disponibles
        has_dq_col = "is_disqualifie" in df.columns
        has_statut = "statut" in df.columns

        n_dq = 0
        dq_with_position = 0
        dq_without_position = 0

        if has_dq_col:
            mask_dq = df["is_disqualifie"].fillna(False).astype(bool)
            n_dq = mask_dq.sum()
            dq_rows = df[mask_dq]
            dq_with_position = dq_rows["y_rang"].notna().sum()
            dq_without_position = dq_rows["y_rang"].isna().sum()
        elif has_statut:
            mask_dq = df["statut"].str.contains("disq", case=False, na=False)
            n_dq = mask_dq.sum()
            dq_rows = df[mask_dq]
            dq_with_position = dq_rows["y_rang"].notna().sum()
            dq_without_position = dq_rows["y_rang"].isna().sum()

        self.logger.info("  %d disqualifications detectees", n_dq)
        if n_dq > 0:
            self.logger.info("    dont %d avec position, %d sans position",
                             dq_with_position, dq_without_position)

        return {
            "n_disqualified": int(n_dq),
            "dq_with_position": int(dq_with_position),
            "dq_without_position": int(dq_without_position),
            "dq_column_found": has_dq_col or has_statut,
        }

    def check_win_rate(self, df: pd.DataFrame) -> dict:
        """Verifie que le taux de victoire est coherent avec 1/N.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec y_gagnant et course_uid.

        Returns
        -------
        dict avec taux observe vs attendu.
        """
        self.logger.info("Verification taux de victoire...")

        if "y_gagnant" not in df.columns:
            self.logger.warning("  Colonne y_gagnant absente")
            return {"error": "colonne y_gagnant absente"}

        # Taux global
        win_rate_global = df["y_gagnant"].mean()

        # Taux attendu : moyenne de 1/N par course
        expected_rates = []
        for _, group in df.groupby("course_uid"):
            n = len(group)
            if n > 0:
                expected_rates.append(1.0 / n)
        expected_rate = np.mean(expected_rates) if expected_rates else 0.0

        deviation = abs(win_rate_global - expected_rate)
        is_ok = deviation <= self.win_rate_tolerance

        # Verifier qu'exactement 1 gagnant par course
        courses_multi_winners = 0
        courses_no_winner = 0
        for course_uid, group in df.groupby("course_uid"):
            n_winners = group["y_gagnant"].sum()
            if n_winners > 1:
                courses_multi_winners += 1
            elif n_winners == 0:
                courses_no_winner += 1

        self.logger.info("  Taux victoire observe: %.4f, attendu: %.4f (ecart: %.4f)",
                         win_rate_global, expected_rate, deviation)
        self.logger.info("  Courses multi-gagnants: %d, sans gagnant: %d",
                         courses_multi_winners, courses_no_winner)

        if not is_ok:
            self.logger.warning("  ALERTE: ecart taux victoire > %.2f", self.win_rate_tolerance)

        return {
            "win_rate_observed": round(float(win_rate_global), 6),
            "win_rate_expected": round(float(expected_rate), 6),
            "deviation": round(float(deviation), 6),
            "is_consistent": is_ok,
            "courses_multi_winners": int(courses_multi_winners),
            "courses_no_winner": int(courses_no_winner),
        }

    def check_label_distributions(self, df: pd.DataFrame) -> dict:
        """Calcule les distributions par type de label.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec les colonnes de labels.

        Returns
        -------
        dict avec distributions pour chaque label.
        """
        self.logger.info("Distributions des labels...")

        distributions = {}
        label_cols = [c for c in df.columns if c.startswith("y_")]

        for col in label_cols:
            vals = df[col].dropna()
            if len(vals) == 0:
                distributions[col] = {"count": 0, "all_missing": True}
                continue

            stats: dict = {
                "count": int(len(vals)),
                "missing": int(df[col].isna().sum()),
                "missing_pct": round(df[col].isna().mean() * 100, 2),
            }

            unique = vals.nunique()
            if unique <= 10:
                # Label binaire ou categoriel
                value_counts = vals.value_counts().to_dict()
                stats["value_counts"] = {str(k): int(v) for k, v in value_counts.items()}
                stats["type"] = "binary" if unique == 2 else "categorical"
            else:
                # Label continu
                stats["type"] = "continuous"
                stats["mean"] = round(float(vals.mean()), 6)
                stats["std"] = round(float(vals.std()), 6)
                stats["min"] = float(vals.min())
                stats["max"] = float(vals.max())
                stats["median"] = float(vals.median())
                stats["q25"] = float(vals.quantile(0.25))
                stats["q75"] = float(vals.quantile(0.75))

            distributions[col] = stats
            self.logger.info("  %s: %s, n=%d, missing=%.1f%%",
                             col, stats.get("type", "?"), stats["count"], stats.get("missing_pct", 0))

        return distributions

    def check_impossible_values(self, df: pd.DataFrame) -> dict:
        """Detecte les valeurs impossibles dans les labels.

        Regles :
          - y_gagnant doit etre 0 ou 1
          - y_place_top3 doit etre 0 ou 1
          - y_place_top5 doit etre 0 ou 1
          - y_rang doit etre >= 1
          - y_rang_normalise doit etre dans [0, 1]
          - Si y_gagnant=1, alors y_rang doit etre 1
          - Si y_gagnant=1, alors y_place_top3 doit etre 1

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec les colonnes de labels.

        Returns
        -------
        dict avec les anomalies detectees.
        """
        self.logger.info("Verification valeurs impossibles...")

        anomalies: list[dict] = []

        # y_gagnant: doit etre 0 ou 1
        if "y_gagnant" in df.columns:
            invalid = df[~df["y_gagnant"].isin([0, 1, None, np.nan])]["y_gagnant"]
            if len(invalid) > 0:
                anomalies.append({
                    "label": "y_gagnant",
                    "issue": "valeurs hors {0, 1}",
                    "n_anomalies": int(len(invalid)),
                    "examples": invalid.head(5).tolist(),
                })

        # y_place_top3 / y_place_top5 : doit etre 0 ou 1
        for col in ["y_place_top3", "y_place_top5"]:
            if col in df.columns:
                invalid = df[~df[col].isin([0, 1, None, np.nan])][col]
                if len(invalid) > 0:
                    anomalies.append({
                        "label": col,
                        "issue": "valeurs hors {0, 1}",
                        "n_anomalies": int(len(invalid)),
                        "examples": invalid.head(5).tolist(),
                    })

        # y_rang : doit etre >= 1
        if "y_rang" in df.columns:
            vals = df["y_rang"].dropna()
            invalid = vals[vals < 1]
            if len(invalid) > 0:
                anomalies.append({
                    "label": "y_rang",
                    "issue": "valeurs < 1",
                    "n_anomalies": int(len(invalid)),
                    "examples": invalid.head(5).tolist(),
                })

        # y_rang_normalise : doit etre dans [0, 1]
        if "y_rang_normalise" in df.columns:
            vals = df["y_rang_normalise"].dropna()
            invalid = vals[(vals < 0) | (vals > 1)]
            if len(invalid) > 0:
                anomalies.append({
                    "label": "y_rang_normalise",
                    "issue": "valeurs hors [0, 1]",
                    "n_anomalies": int(len(invalid)),
                    "examples": invalid.head(5).tolist(),
                })

        # Coherence : y_gagnant=1 => y_rang=1
        if "y_gagnant" in df.columns and "y_rang" in df.columns:
            mask = (df["y_gagnant"] == 1) & (df["y_rang"].notna()) & (df["y_rang"] != 1)
            if mask.sum() > 0:
                anomalies.append({
                    "label": "y_gagnant vs y_rang",
                    "issue": "y_gagnant=1 mais y_rang != 1",
                    "n_anomalies": int(mask.sum()),
                })

        # Coherence : y_gagnant=1 => y_place_top3=1
        if "y_gagnant" in df.columns and "y_place_top3" in df.columns:
            mask = (df["y_gagnant"] == 1) & (df["y_place_top3"] == 0)
            if mask.sum() > 0:
                anomalies.append({
                    "label": "y_gagnant vs y_place_top3",
                    "issue": "y_gagnant=1 mais y_place_top3=0",
                    "n_anomalies": int(mask.sum()),
                })

        self.logger.info("  %d types d'anomalies detectees", len(anomalies))
        for a in anomalies:
            self.logger.warning("  ANOMALIE: %s — %s (%d cas)", a["label"], a["issue"], a["n_anomalies"])

        return {
            "n_anomaly_types": len(anomalies),
            "anomalies": anomalies,
        }

    def generate_report(self, df: pd.DataFrame) -> dict:
        """Genere le rapport complet de qualite des labels.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame avec les labels.

        Returns
        -------
        dict
            Rapport JSON-serialisable.
        """
        self.logger.info("=" * 70)
        self.logger.info("Rapport qualite des labels")
        self.logger.info("=" * 70)
        self.logger.info("  %d lignes, %d colonnes", len(df), len(df.columns))

        report = {
            "n_total": len(df),
            "n_courses": df["course_uid"].nunique() if "course_uid" in df.columns else None,
        }

        report["arrival_order"] = self.check_arrival_order_completeness(df)
        report["missing_positions"] = self.check_missing_positions(df)
        report["dq_handling"] = self.check_dq_handling(df)
        report["win_rate"] = self.check_win_rate(df)
        report["distributions"] = self.check_label_distributions(df)
        report["impossible_values"] = self.check_impossible_values(df)

        return report


# ===========================================================================
# MAIN (CLI)
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Moniteur de qualite des labels"
    )
    parser.add_argument(
        "--labels", type=str, required=True,
        help="Chemin vers le fichier labels (JSON/Parquet/CSV)"
    )
    parser.add_argument(
        "--partants", type=str, default=None,
        help="Chemin vers partants (pour infos DQ, optionnel)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Chemin pour sauvegarder le rapport JSON"
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("label_quality_monitor.py — Qualite des labels")
    logger.info("=" * 70)

    labels_path = Path(args.labels)
    if not labels_path.exists():
        logger.error("Fichier introuvable: %s", labels_path)
        sys.exit(1)

    suffix = labels_path.suffix.lower()
    if suffix == ".parquet":
        df = pd.read_parquet(labels_path)
    elif suffix == ".csv":
        df = pd.read_csv(labels_path)
    elif suffix == ".json":
        df = pd.read_json(labels_path)
    else:
        logger.error("Format non supporte: %s", suffix)
        sys.exit(1)

    # Jointure optionnelle avec partants pour DQ
    if args.partants:
        partants_path = Path(args.partants)
        if partants_path.exists():
            suffix_p = partants_path.suffix.lower()
            if suffix_p == ".parquet":
                df_partants = pd.read_parquet(partants_path)
            elif suffix_p == ".csv":
                df_partants = pd.read_csv(partants_path)
            elif suffix_p == ".json":
                df_partants = pd.read_json(partants_path)
            else:
                df_partants = None
                logger.warning("Format partants non supporte: %s", suffix_p)

            if df_partants is not None:
                dq_cols = [c for c in ["is_disqualifie", "statut"] if c in df_partants.columns]
                if dq_cols and "partant_uid" in df_partants.columns:
                    merge_cols = ["partant_uid"] + dq_cols
                    df = df.merge(
                        df_partants[merge_cols].drop_duplicates(),
                        on="partant_uid",
                        how="left",
                    )
                    logger.info("Jointure partants : colonnes DQ ajoutees")

    # Map column names to expected names if needed
    col_map = {
        "position": "y_rang",
        "is_winner": "y_gagnant",
        "is_place": "y_place",
    }
    for old_col, new_col in col_map.items():
        if old_col in df.columns and new_col not in df.columns:
            df[new_col] = df[old_col]

    logger.info("Charge: %d lignes, %d colonnes", len(df), len(df.columns))

    monitor = LabelQualityMonitor()
    report = monitor.generate_report(df)

    # Affichage resume
    print(f"\n{'='*70}")
    print(f"RAPPORT QUALITE DES LABELS")
    print(f"{'='*70}")
    print(f"Partants : {report['n_total']}")
    print(f"Courses  : {report.get('n_courses', '?')}")

    wr = report.get("win_rate", {})
    if "win_rate_observed" in wr:
        print(f"\nTaux victoire : {wr['win_rate_observed']:.4f} (attendu: {wr['win_rate_expected']:.4f})")
        print(f"  Coherent : {'OUI' if wr['is_consistent'] else 'NON'}")

    imp = report.get("impossible_values", {})
    n_anom = imp.get("n_anomaly_types", 0)
    print(f"\nAnomalies : {n_anom} type(s)")

    # Sauvegarder
    output_path = Path(args.output) if args.output else OUTPUT_DIR / "label_quality_report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Rapport sauve: %s", output_path)

    logger.info("Termine.")


if __name__ == "__main__":
    main()
