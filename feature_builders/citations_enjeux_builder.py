#!/usr/bin/env python3
"""
feature_builders.citations_enjeux_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-references citations_enjeux data (betting citations showing which
horses are cited in different bet types) with partants_master to create
per-partant citation features.

Two-phase streaming approach (max ~6 GB RAM):
  Phase 1 -- Stream citations_enjeux.jsonl, aggregate by (course_uid, num_pmu)
  Phase 2 -- Stream partants_master.jsonl, lookup and compute features

Produces:
  - citations_enjeux_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/citations_enjeux/

Features per partant (8):
  - cit_nb_citations         : nombre de citations (tous paris) pour ce cheval
  - cit_best_citation_pos    : meilleure position de citation (1=favori)
  - cit_avg_citation_pos     : position moyenne de citation
  - cit_nb_pari_types_cited  : nombre de types de paris ou le cheval est cite
  - cit_is_favoris           : 1 si le cheval est marque favori au moins une fois
  - cit_avg_citation_ratio   : ratio moyen de citation
  - cit_citation_consistency : ecart-type des positions de citation
  - cit_top3_frequency       : proportion des paris ou le cheval est cite en top 3

Usage:
    python feature_builders/citations_enjeux_builder.py
"""

from __future__ import annotations

import gc
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

CITATIONS_PATH = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/27_citations_enjeux/citations_enjeux.jsonl"
)
PARTANTS_PATH = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/citations_enjeux"
)
OUTPUT_FILE = OUTPUT_DIR / "citations_enjeux_features.jsonl"

_LOG_EVERY = 500_000

# Feature names
FEATURE_NAMES = [
    "cit_nb_citations",
    "cit_best_citation_pos",
    "cit_avg_citation_pos",
    "cit_nb_pari_types_cited",
    "cit_is_favoris",
    "cit_avg_citation_ratio",
    "cit_citation_consistency",
    "cit_top3_frequency",
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(val: Any) -> Optional[int]:
    """Safely convert a value to int."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        v = float(val)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


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
    logger.info("Lecture terminee %s: %d records, %d erreurs JSON", path.name, count, errors)


# ===========================================================================
# PHASE 1: BUILD CITATION AGGREGATES
# ===========================================================================


def _build_citation_index(citations_path: Path, logger) -> dict:
    """Stream citations_enjeux.jsonl and aggregate by (course_uid, num_pmu).

    For each (course_uid, num_pmu) pair, stores a compact aggregate:
      {
        "positions": [list of citation_position values],
        "ratios":    [list of citation_ratio values],
        "pari_types": set of type_pari values,
        "favoris":   bool (any favoris=True),
      }

    Returns dict keyed by "course_uid|num_pmu".
    """
    logger.info("Phase 1: Indexation citations_enjeux...")
    t0 = time.time()

    index: dict[str, dict] = {}
    n_read = 0
    n_skipped = 0
    n_indexed = 0

    for rec in _iter_jsonl(citations_path, logger):
        n_read += 1

        # Skip records without num_pmu or marked unavailable
        num_pmu = _safe_int(rec.get("num_pmu"))
        if num_pmu is None:
            n_skipped += 1
            continue

        course_uid = rec.get("course_uid")
        if not course_uid:
            n_skipped += 1
            continue

        # Skip indisponible records
        if rec.get("indisponible") is True:
            n_skipped += 1
            continue

        key = f"{course_uid}|{num_pmu}"

        if key not in index:
            index[key] = {
                "positions": [],
                "ratios": [],
                "pari_types": set(),
                "favoris": False,
            }

        entry = index[key]

        # citation_position
        pos = _safe_int(rec.get("citation_position"))
        if pos is not None:
            entry["positions"].append(pos)

        # citation_ratio
        ratio = _safe_float(rec.get("citation_ratio"))
        if ratio is not None:
            entry["ratios"].append(ratio)

        # type_pari
        tp = rec.get("type_pari")
        if tp:
            entry["pari_types"].add(tp)

        # favoris
        if rec.get("favoris") is True:
            entry["favoris"] = True

        n_indexed += 1

        if n_read % _LOG_EVERY == 0:
            logger.info("  Phase 1: %d citations lues, %d indexees...", n_read, n_indexed)

        # Periodic GC every 2M records
        if n_read % 2_000_000 == 0:
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Phase 1 terminee: %d citations lues, %d ignorees, %d indexees, "
        "%d cles uniques en %.1fs",
        n_read, n_skipped, n_indexed, len(index), elapsed,
    )
    gc.collect()
    return index


# ===========================================================================
# PHASE 2: COMPUTE FEATURES PER PARTANT
# ===========================================================================


def _compute_features(agg: dict) -> dict[str, Optional[float]]:
    """Compute the 8 citation features from an aggregate entry."""
    positions = agg["positions"]
    ratios = agg["ratios"]
    pari_types = agg["pari_types"]
    is_favoris = agg["favoris"]

    nb_citations = len(positions)

    feat: dict[str, Optional[float]] = {}

    # cit_nb_citations
    feat["cit_nb_citations"] = nb_citations if nb_citations > 0 else None

    # cit_best_citation_pos
    feat["cit_best_citation_pos"] = min(positions) if positions else None

    # cit_avg_citation_pos
    if positions:
        feat["cit_avg_citation_pos"] = round(sum(positions) / len(positions), 4)
    else:
        feat["cit_avg_citation_pos"] = None

    # cit_nb_pari_types_cited
    feat["cit_nb_pari_types_cited"] = len(pari_types) if pari_types else None

    # cit_is_favoris
    feat["cit_is_favoris"] = 1 if is_favoris else 0

    # cit_avg_citation_ratio
    if ratios:
        feat["cit_avg_citation_ratio"] = round(sum(ratios) / len(ratios), 4)
    else:
        feat["cit_avg_citation_ratio"] = None

    # cit_citation_consistency (std dev of positions)
    if len(positions) >= 2:
        mean = sum(positions) / len(positions)
        variance = sum((p - mean) ** 2 for p in positions) / len(positions)
        feat["cit_citation_consistency"] = round(math.sqrt(variance), 4)
    else:
        feat["cit_citation_consistency"] = None

    # cit_top3_frequency (proportion of citations where position <= 3)
    if positions:
        top3_count = sum(1 for p in positions if p <= 3)
        feat["cit_top3_frequency"] = round(top3_count / len(positions), 4)
    else:
        feat["cit_top3_frequency"] = None

    return feat


def _null_features() -> dict[str, None]:
    """Return a dict with all feature names set to None."""
    return {name: None for name in FEATURE_NAMES}


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    logger = setup_logging("citations_enjeux_builder")
    logger.info("=" * 70)
    logger.info("citations_enjeux_builder.py")
    logger.info("=" * 70)

    # Validate inputs
    if not CITATIONS_PATH.exists():
        logger.error("Citations file not found: %s", CITATIONS_PATH)
        sys.exit(1)
    if not PARTANTS_PATH.exists():
        logger.error("Partants file not found: %s", PARTANTS_PATH)
        sys.exit(1)

    logger.info("Citations input: %s", CITATIONS_PATH)
    logger.info("Partants input:  %s", PARTANTS_PATH)
    logger.info("Output:          %s", OUTPUT_FILE)

    # Phase 1: Build citation index
    citation_index = _build_citation_index(CITATIONS_PATH, logger)

    # Phase 2: Stream partants, compute features, write output
    logger.info("Phase 2: Calcul des features par partant...")
    t0 = time.time()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = OUTPUT_FILE.with_suffix(".tmp")

    n_partants = 0
    n_matched = 0
    fill_counts = {name: 0 for name in FEATURE_NAMES}

    with open(tmp_path, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(PARTANTS_PATH, logger):
            n_partants += 1

            course_uid = rec.get("course_uid", "")
            num_pmu = _safe_int(rec.get("num_pmu"))
            partant_uid = rec.get("partant_uid", "")
            date_reunion_iso = rec.get("date_reunion_iso", "")

            # Lookup in citation index
            if num_pmu is not None and course_uid:
                key = f"{course_uid}|{num_pmu}"
                agg = citation_index.get(key)
            else:
                agg = None

            if agg is not None:
                features = _compute_features(agg)
                n_matched += 1
            else:
                features = _null_features()

            # Build output record
            out_rec = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_reunion_iso,
            }
            out_rec.update(features)

            fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")

            # Track fill rates
            for name in FEATURE_NAMES:
                if features.get(name) is not None:
                    fill_counts[name] += 1

            if n_partants % _LOG_EVERY == 0:
                logger.info(
                    "  Phase 2: %d partants traites, %d matches...",
                    n_partants, n_matched,
                )

            # Periodic GC every 1M records
            if n_partants % 1_000_000 == 0:
                gc.collect()

    # Atomic rename
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    os.rename(tmp_path, OUTPUT_FILE)

    elapsed = time.time() - t0
    logger.info(
        "Phase 2 terminee: %d partants, %d matches (%.1f%%) en %.1fs",
        n_partants, n_matched,
        (n_matched / n_partants * 100) if n_partants else 0,
        elapsed,
    )

    # Fill rates
    logger.info("=" * 50)
    logger.info("Fill rates:")
    for name in FEATURE_NAMES:
        count = fill_counts[name]
        pct = (count / n_partants * 100) if n_partants else 0
        logger.info("  %-30s %8d / %d  (%.1f%%)", name, count, n_partants, pct)
    logger.info("=" * 50)

    logger.info("Output: %s (%d records)", OUTPUT_FILE, n_partants)
    logger.info("Done.")


if __name__ == "__main__":
    main()
