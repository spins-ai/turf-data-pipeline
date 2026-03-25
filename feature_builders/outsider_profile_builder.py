#!/usr/bin/env python3
"""
feature_builders.outsider_profile_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Outsider-detection features for Phase 10 (Outsider Detection) model modules.

Computes statistical anomaly scores and upset-frequency metrics from
existing partants_master data -- no trained ML model required.
Uses z-score deviation across multiple dimensions within each race field
to identify horses whose profile diverges from the field norm.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant outsider-profile features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the running statistics -- no future leakage.

Produces:
  - outsider_profile_features.jsonl   in output/outsider_profile/

Features per partant (6):
  - anomaly_score         : composite z-score deviation across cote, Elo,
                            nb_courses, and gains.  Higher = more anomalous
                            profile relative to the field.
  - upset_freq_hippodrome : historical upset rate at this hippodrome
                            (fraction of races won by horses with cote >= 10).
  - upset_freq_discipline : historical upset rate for this discipline.
  - upset_freq_distance   : historical upset rate at this distance bucket.
  - is_profile_outlier    : 1 if anomaly_score > 2.0 (2 sigma), else 0.
  - longshot_upset_score  : anomaly_score * upset_freq for the conditions.
                            High = anomalous profile in upset-prone conditions.

Usage:
    python feature_builders/outsider_profile_builder.py
    python feature_builders/outsider_profile_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "outsider_profile"

# Anomaly score threshold for outlier flag
OUTLIER_THRESHOLD = 2.0

# Upset definition: cote >= this value
UPSET_COTE_THRESHOLD = 10.0

# Distance bucketing (metres)
DISTANCE_BUCKETS = [
    (0, 1200, "sprint"),
    (1200, 1600, "mile"),
    (1600, 2200, "intermediate"),
    (2200, 2800, "staying"),
    (2800, 99999, "long"),
]

# Bayesian prior for upset-rate shrinkage
PRIOR_RACES = 20
GLOBAL_UPSET_RATE = 0.10  # ~10% baseline

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_bucket(distance_m: int) -> str:
    """Map distance in metres to a bucket label."""
    for lo, hi, label in DISTANCE_BUCKETS:
        if lo <= distance_m < hi:
            return label
    return "unknown"


class _UpsetTracker:
    """Bayesian upset-rate tracker per condition group."""

    __slots__ = ("upsets", "races")

    def __init__(self) -> None:
        self.upsets: int = 0
        self.races: int = 0

    def upset_rate(self) -> float:
        """Shrinkage estimator toward global upset rate."""
        return (GLOBAL_UPSET_RATE * PRIOR_RACES + self.upsets) / (
            PRIOR_RACES + self.races
        )


def _zscore(value: float, values: list[float]) -> float:
    """Compute z-score of value within the list. Returns 0 if std=0."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(var) if var > 0 else 0.0
    if std == 0.0:
        return 0.0
    return (value - mean) / std


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_outsider_profile_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build outsider-profile features from partants_master.jsonl."""
    logger.info("=== Outsider Profile Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote_finale = rec.get("cote_finale") or rec.get("rapport_final")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "cote_finale": cote_finale,
            "nb_courses": rec.get("nb_courses_carriere") or 0,
            "gains": rec.get("gains_carriere_euros") or 0,
            "hippodrome": rec.get("hippodrome_normalise", ""),
            "discipline": rec.get("discipline", ""),
            "distance": rec.get("distance") or 0,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    upset_by_hippo: dict[str, _UpsetTracker] = defaultdict(_UpsetTracker)
    upset_by_disc: dict[str, _UpsetTracker] = defaultdict(_UpsetTracker)
    upset_by_dist: dict[str, _UpsetTracker] = defaultdict(_UpsetTracker)

    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # Gather condition keys from first record
        hippo = course_group[0]["hippodrome"]
        disc = course_group[0]["discipline"]
        dist_m = course_group[0]["distance"]
        dist_bucket = _distance_bucket(dist_m)

        # -- Snapshot pre-race upset frequencies --
        uf_hippo = upset_by_hippo[hippo].upset_rate() if hippo else GLOBAL_UPSET_RATE
        uf_disc = upset_by_disc[disc].upset_rate() if disc else GLOBAL_UPSET_RATE
        uf_dist = upset_by_dist[dist_bucket].upset_rate()

        # Combined upset frequency for this condition
        uf_combined = (uf_hippo + uf_disc + uf_dist) / 3.0

        # -- Collect numerical vectors for z-score anomaly --
        cotes: list[float] = []
        nb_courses_list: list[float] = []
        gains_list: list[float] = []

        parsed: list[dict] = []
        for rec in course_group:
            cote = None
            if rec["cote_finale"] is not None:
                try:
                    cote = float(rec["cote_finale"])
                    if cote <= 0:
                        cote = None
                except (ValueError, TypeError):
                    cote = None

            nb_c = float(rec["nb_courses"])
            g = float(rec["gains"])

            parsed.append({
                "rec": rec,
                "cote": cote,
                "nb_courses": nb_c,
                "gains": g,
            })

            if cote is not None:
                cotes.append(cote)
            nb_courses_list.append(nb_c)
            gains_list.append(g)

        # -- Emit features for each partant --
        post_updates: list[tuple[bool, Optional[float]]] = []

        for pr in parsed:
            rec = pr["rec"]
            cote = pr["cote"]

            # Z-scores (higher = more different from field)
            z_cote = abs(_zscore(cote, cotes)) if cote is not None and len(cotes) >= 2 else 0.0
            z_nb = abs(_zscore(pr["nb_courses"], nb_courses_list)) if len(nb_courses_list) >= 2 else 0.0
            z_gains = abs(_zscore(pr["gains"], gains_list)) if len(gains_list) >= 2 else 0.0

            # Composite anomaly score: RMS of z-scores
            n_z = 0
            sum_sq = 0.0
            for z in (z_cote, z_nb, z_gains):
                if z > 0:
                    sum_sq += z * z
                    n_z += 1

            anomaly = round(math.sqrt(sum_sq / n_z), 4) if n_z > 0 else 0.0

            is_outlier = 1 if anomaly > OUTLIER_THRESHOLD else 0

            # Longshot upset score: anomaly * combined upset frequency
            longshot_score = round(anomaly * uf_combined, 4) if anomaly > 0 else 0.0

            results.append({
                "partant_uid": rec["uid"],
                "anomaly_score": anomaly,
                "upset_freq_hippodrome": round(uf_hippo, 4),
                "upset_freq_discipline": round(uf_disc, 4),
                "upset_freq_distance": round(uf_dist, 4),
                "is_profile_outlier": is_outlier,
                "longshot_upset_score": longshot_score,
            })

            post_updates.append((rec["gagnant"], cote))

        # -- Update upset trackers after race --
        # An "upset" = winner had cote >= UPSET_COTE_THRESHOLD
        winner_cote = None
        for pr in parsed:
            if pr["rec"]["gagnant"] and pr["cote"] is not None:
                winner_cote = pr["cote"]
                break

        is_upset = winner_cote is not None and winner_cote >= UPSET_COTE_THRESHOLD

        if hippo:
            upset_by_hippo[hippo].races += 1
            if is_upset:
                upset_by_hippo[hippo].upsets += 1
        if disc:
            upset_by_disc[disc].races += 1
            if is_upset:
                upset_by_disc[disc].upsets += 1
        upset_by_dist[dist_bucket].races += 1
        if is_upset:
            upset_by_dist[dist_bucket].upsets += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Outsider profile build termine: %d features en %.1fs "
        "(hippodromes: %d, disciplines: %d, dist_buckets: %d)",
        len(results),
        elapsed,
        len(upset_by_hippo),
        len(upset_by_disc),
        len(upset_by_dist),
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features outsider-profile a partir de partants_master"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: output/outsider_profile/)",
    )
    args = parser.parse_args()

    logger = setup_logging("outsider_profile_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_outsider_profile_features(input_path, logger)

    # Save
    out_path = output_dir / "outsider_profile_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
