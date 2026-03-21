#!/usr/bin/env python3
"""
quality/leakage_detector.py
===========================
Detecteur de fuite de donnees (data leakage) dans le pipeline ML.

Verifie :
  1. Correlations suspectes entre features et y_gagnant (|corr| > 0.5)
  2. Violations temporelles : aucune feature ne doit utiliser de donnees
     posterieures a la date de la course cible.

Aucun appel API : traitement 100% local.

Usage :
    python3 quality/leakage_detector.py
    python3 quality/leakage_detector.py --features path/to/features.json --labels path/to/labels.json
    python3 quality/leakage_detector.py --features path/to/features.parquet --labels path/to/labels.parquet
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
from pathlib import Path
from typing import Any, Optional

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
CORRELATION_THRESHOLD = 0.5


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("leakage_detector")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "leakage_detector.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ===========================================================================
# CHARGEMENT
# ===========================================================================

def charger_donnees(path: Path, logger: logging.Logger) -> list[dict]:
    """Charge un fichier JSON, Parquet ou CSV en liste de dicts."""
    suffix = path.suffix.lower()
    logger.info("Chargement: %s", path)

    if suffix == ".json":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    elif suffix == ".parquet":
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(path)
            data = table.to_pylist()
        except ImportError:
            logger.error("pyarrow requis pour lire les fichiers Parquet")
            sys.exit(1)
    elif suffix == ".csv":
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data = list(reader)
        # Tenter de convertir les valeurs numeriques
        for row in data:
            for k, v in row.items():
                if v == "":
                    row[k] = None
                else:
                    try:
                        row[k] = float(v)
                        if row[k] == int(row[k]):
                            row[k] = int(row[k])
                    except (ValueError, TypeError):
                        pass
    else:
        logger.error("Format non supporte: %s (utiliser .json, .parquet ou .csv)", suffix)
        sys.exit(1)

    logger.info("  %d entrees chargees", len(data))
    return data


# ===========================================================================
# STATISTIQUES
# ===========================================================================

def pearson_correlation(xs: list[float], ys: list[float]) -> Optional[float]:
    """
    Calcule le coefficient de correlation de Pearson entre deux listes.
    Retourne None si le calcul est impossible (variance nulle, pas assez de valeurs).
    """
    n = len(xs)
    if n < 3:
        return None

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))

    if den_x == 0 or den_y == 0:
        return None

    return num / (den_x * den_y)


# ===========================================================================
# DETECTION
# ===========================================================================

def detect_leakage(features_path: str, labels_path: str) -> dict:
    """
    Detecte les fuites de donnees potentielles.

    Args:
        features_path: chemin vers le fichier features (JSON/Parquet/CSV)
        labels_path: chemin vers le fichier labels (JSON/Parquet/CSV)

    Returns:
        dict avec:
          - suspicious_features: liste des features suspectes (|corr| > 0.5)
          - correlation_report: {feature: correlation} pour toutes les features numeriques
          - temporal_violations: liste des violations temporelles detectees
    """
    logger = setup_logging()

    features_data = charger_donnees(Path(features_path), logger)
    labels_data = charger_donnees(Path(labels_path), logger)

    # Indexer les labels par partant_uid
    labels_idx: dict[str, dict] = {}
    for lbl in labels_data:
        uid = lbl.get("partant_uid", "")
        if uid:
            labels_idx[uid] = lbl

    logger.info("  %d labels indexes par partant_uid", len(labels_idx))

    # Identifier les colonnes de features numeriques
    # Exclure les colonnes d'identifiants et de metadata
    exclude_cols = {
        "partant_uid", "course_uid", "reunion_uid", "date_reunion_iso",
        "nom_cheval", "cle_partant", "source", "timestamp_collecte",
        "hippodrome_normalise", "hippodrome",
    }

    if not features_data:
        logger.warning("Aucune donnee de features")
        return {
            "suspicious_features": [],
            "correlation_report": {},
            "temporal_violations": [],
        }

    all_cols = set(features_data[0].keys())
    feature_cols = [c for c in all_cols if c not in exclude_cols]

    # --- 1. Correlations avec y_gagnant ---
    logger.info("Calcul des correlations avec y_gagnant...")

    correlation_report: dict[str, float] = {}
    suspicious_features: list[str] = []

    for col in sorted(feature_cols):
        xs = []
        ys = []
        for feat in features_data:
            uid = feat.get("partant_uid", "")
            lbl = labels_idx.get(uid)
            if lbl is None:
                continue

            val = feat.get(col)
            y = lbl.get("y_gagnant")

            if val is None or y is None:
                continue

            # Tenter conversion numerique
            try:
                val_f = float(val)
                y_f = float(y)
            except (ValueError, TypeError):
                continue

            if math.isnan(val_f) or math.isinf(val_f):
                continue

            xs.append(val_f)
            ys.append(y_f)

        if len(xs) < 10:
            continue

        corr = pearson_correlation(xs, ys)
        if corr is not None:
            corr = round(corr, 4)
            correlation_report[col] = corr
            if abs(corr) > CORRELATION_THRESHOLD:
                suspicious_features.append(col)
                logger.warning(
                    "  SUSPECT: %s — corr=%.4f (seuil=%.1f)",
                    col, corr, CORRELATION_THRESHOLD
                )

    logger.info("  %d features numeriques analysees", len(correlation_report))
    logger.info("  %d features suspectes (|corr| > %.1f)", len(suspicious_features), CORRELATION_THRESHOLD)

    # --- 2. Violations temporelles ---
    logger.info("Verification des violations temporelles...")

    temporal_violations: list[str] = []

    # Strategie : pour chaque feature contenant "date" ou "prev" dans le nom,
    # verifier que les valeurs de date referencees sont < date_reunion_iso du partant.
    # On verifie aussi les colonnes contenant des dates ISO.

    date_like_cols = [
        c for c in feature_cols
        if any(kw in c.lower() for kw in ("date", "prev", "precedent", "futur", "next", "suivant"))
    ]

    for col in date_like_cols:
        violations_count = 0
        checked = 0

        for feat in features_data:
            date_course = feat.get("date_reunion_iso", "")
            val = feat.get(col)

            if not date_course or val is None:
                continue

            # Si la valeur ressemble a une date ISO (YYYY-MM-DD)
            if isinstance(val, str) and len(val) >= 10:
                try:
                    date_val = val[:10]
                    # Simple comparaison lexicographique pour les dates ISO
                    if date_val > date_course:
                        violations_count += 1
                except (ValueError, TypeError):
                    pass
            checked += 1

        if violations_count > 0:
            msg = f"{col}: {violations_count}/{checked} valeurs referencent des dates futures"
            temporal_violations.append(msg)
            logger.warning("  VIOLATION TEMPORELLE: %s", msg)

    # Verification complementaire : colonnes qui pourraient contenir
    # des resultats futurs (position, gains, etc.) via des noms suspects
    future_keywords = ("futur", "next", "suivant", "apres", "after", "target", "y_")
    for col in feature_cols:
        col_lower = col.lower()
        if any(kw in col_lower for kw in future_keywords):
            if col not in [v.split(":")[0] for v in temporal_violations]:
                msg = f"{col}: nom de colonne suggerant une utilisation de donnees futures"
                temporal_violations.append(msg)
                logger.warning("  VIOLATION POTENTIELLE: %s", msg)

    if not temporal_violations:
        logger.info("  Aucune violation temporelle detectee")

    # --- Resume ---
    report = {
        "suspicious_features": sorted(suspicious_features),
        "correlation_report": dict(sorted(correlation_report.items(), key=lambda x: -abs(x[1]))),
        "temporal_violations": temporal_violations,
    }

    return report


def format_report(report: dict) -> str:
    """Formate le rapport de leakage en texte lisible."""
    lines = []
    lines.append("=" * 70)
    lines.append("RAPPORT DE DETECTION DE LEAKAGE")
    lines.append("=" * 70)

    # Features suspectes
    suspicious = report.get("suspicious_features", [])
    lines.append(f"\n--- Features suspectes (|corr| > {CORRELATION_THRESHOLD}) : {len(suspicious)} ---")
    if suspicious:
        corr_report = report.get("correlation_report", {})
        for feat in suspicious:
            corr = corr_report.get(feat, "?")
            lines.append(f"  ! {feat}: corr = {corr}")
    else:
        lines.append("  Aucune feature suspecte.")

    # Top correlations
    corr_report = report.get("correlation_report", {})
    lines.append(f"\n--- Top 20 correlations avec y_gagnant ---")
    for i, (feat, corr) in enumerate(corr_report.items()):
        if i >= 20:
            break
        marker = " !" if feat in suspicious else "  "
        lines.append(f" {marker} {feat}: {corr:.4f}")

    # Violations temporelles
    violations = report.get("temporal_violations", [])
    lines.append(f"\n--- Violations temporelles : {len(violations)} ---")
    if violations:
        for v in violations:
            lines.append(f"  ! {v}")
    else:
        lines.append("  Aucune violation temporelle.")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


# ===========================================================================
# MAIN (CLI)
# ===========================================================================

def main():
    global CORRELATION_THRESHOLD

    parser = argparse.ArgumentParser(
        description="Detecteur de data leakage dans les features ML"
    )
    parser.add_argument(
        "--features", type=str, required=True,
        help="Chemin vers le fichier features (JSON/Parquet/CSV)"
    )
    parser.add_argument(
        "--labels", type=str, required=True,
        help="Chemin vers le fichier labels (JSON/Parquet/CSV)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Chemin pour sauvegarder le rapport JSON (optionnel)"
    )
    parser.add_argument(
        "--threshold", type=float, default=CORRELATION_THRESHOLD,
        help=f"Seuil de correlation suspecte (defaut: {CORRELATION_THRESHOLD})"
    )
    args = parser.parse_args()

    CORRELATION_THRESHOLD = args.threshold

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("leakage_detector.py — Detection de data leakage")
    logger.info("=" * 70)

    features_path = Path(args.features)
    labels_path = Path(args.labels)

    if not features_path.exists():
        logger.error("Fichier introuvable: %s", features_path)
        sys.exit(1)
    if not labels_path.exists():
        logger.error("Fichier introuvable: %s", labels_path)
        sys.exit(1)

    report = detect_leakage(str(features_path), str(labels_path))

    # Afficher le rapport
    txt = format_report(report)
    print(txt)

    # Sauvegarder si demande
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        logger.info("Rapport sauve: %s", output_path)

    # Code de sortie
    nb_suspect = len(report.get("suspicious_features", []))
    nb_violations = len(report.get("temporal_violations", []))
    if nb_suspect > 0 or nb_violations > 0:
        logger.warning("ATTENTION: %d features suspectes, %d violations temporelles", nb_suspect, nb_violations)
        sys.exit(1)
    else:
        logger.info("Aucun leakage detecte.")
        sys.exit(0)


if __name__ == "__main__":
    main()
