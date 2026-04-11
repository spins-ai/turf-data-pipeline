#!/usr/bin/env python3
"""
feature_builders.pronostics_consensus_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-references expert pronostics (predictions) with partants_master to
produce per-partant consensus features.

Two-phase streaming approach:
  Phase 1 — Stream pronostics.jsonl, build lookup dict keyed by
            (date_reunion_iso, numero_reunion, num_course).
  Phase 2 — Stream partants_master.jsonl, match each partant to pronostic
            data via (date, reunion, course, num_pmu), compute features.

Produces:
  - pronostics_consensus_features.jsonl

Features per partant (8):
  - prn_expert_rank          : rank in expert pronostic (1-8, None if not cited)
  - prn_expert_cited         : 1 if horse is cited in pronostic, 0 otherwise
  - prn_expert_top3          : 1 if horse is in top 3 of pronostic
  - prn_expert_cote          : pronostic odds for this horse (parsed from "4/1" -> 4.0)
  - prn_expert_cote_vs_market: ratio pronostic odds / market odds (when both exist)
  - prn_nb_chevaux_pronostic : number of horses cited in the pronostic
  - prn_is_favoris_expert    : 1 if horse is rank 1
  - prn_rank_normalized      : rank / nb_chevaux_pronostic (relative position)

Usage:
    python feature_builders/pronostics_consensus_builder.py
    python feature_builders/pronostics_consensus_builder.py --input-master path/to/partants_master.jsonl --input-prono path/to/pronostics.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_MASTER_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_PRONO_DEFAULT = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/23_pronostics/pronostics.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pronostics_consensus")
OUTPUT_FILE = OUTPUT_DIR / "pronostics_consensus_features.jsonl"

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _parse_cote(cote_str: Any) -> Optional[float]:
    """Parse pronostic odds string like '4/1' -> 4.0, '5/2' -> 2.5.

    Also handles plain numeric values and edge cases.
    Returns None if unparseable.
    """
    if cote_str is None:
        return None
    if isinstance(cote_str, (int, float)):
        val = float(cote_str)
        return val if val > 0 else None
    s = str(cote_str).strip()
    if not s:
        return None
    # Try "numerator/denominator" format
    if "/" in s:
        parts = s.split("/", 1)
        try:
            num = float(parts[0].strip())
            den = float(parts[1].strip())
            if den == 0:
                return None
            return round(num / den, 4)
        except (ValueError, IndexError):
            return None
    # Try plain numeric
    try:
        val = float(s)
        return val if val > 0 else None
    except ValueError:
        return None


def _iter_jsonl(path: Path, logger: logging.Logger):
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
# PHASE 1: BUILD PRONOSTIC LOOKUP
# ===========================================================================


def _build_prono_lookup(prono_path: Path, logger: logging.Logger) -> dict:
    """Stream pronostics.jsonl and build lookup dict.

    Returns:
        dict keyed by (date_reunion_iso, numero_reunion, num_course) ->
            {"horses": {num_pmu: {"rank": int, "cote": float|None}}, "nb_cited": int}
    """
    t0 = time.time()
    lookup: dict[tuple, dict] = {}
    n_read = 0
    n_skipped = 0

    for rec in _iter_jsonl(prono_path, logger):
        n_read += 1
        date_iso = rec.get("date_reunion_iso")
        num_reunion = rec.get("numero_reunion")
        num_course = rec.get("num_course")

        if not date_iso or num_reunion is None or num_course is None:
            n_skipped += 1
            continue

        try:
            num_reunion = int(num_reunion)
            num_course = int(num_course)
        except (ValueError, TypeError):
            n_skipped += 1
            continue

        key = (str(date_iso), num_reunion, num_course)

        horses: dict[int, dict] = {}
        nb_cited = 0

        for rang in range(1, 9):  # prono_rang_1 to prono_rang_8
            num_field = f"prono_rang_{rang}_num"
            cote_field = f"prono_rang_{rang}_cote"

            num_cheval = rec.get(num_field)
            if num_cheval is None:
                continue

            try:
                num_cheval = int(num_cheval)
            except (ValueError, TypeError):
                continue

            cote_parsed = _parse_cote(rec.get(cote_field))
            horses[num_cheval] = {"rank": rang, "cote": cote_parsed}
            nb_cited += 1

        if horses:
            lookup[key] = {"horses": horses, "nb_cited": nb_cited}

        if n_read % _LOG_EVERY == 0:
            logger.info("  Phase 1: %d pronostics lus...", n_read)

    elapsed = time.time() - t0
    logger.info(
        "Phase 1 terminee: %d pronostics lus, %d ignores, %d courses avec pronostic en %.1fs",
        n_read, n_skipped, len(lookup), elapsed,
    )
    return lookup


# ===========================================================================
# PHASE 2: STREAM PARTANTS, COMPUTE FEATURES
# ===========================================================================


def _compute_features(
    master_path: Path,
    prono_lookup: dict,
    output_path: Path,
    logger: logging.Logger,
) -> int:
    """Stream partants_master.jsonl, match to pronostics, compute and write features.

    Returns number of records written.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    feature_names = [
        "prn_expert_rank",
        "prn_expert_cited",
        "prn_expert_top3",
        "prn_expert_cote",
        "prn_expert_cote_vs_market",
        "prn_nb_chevaux_pronostic",
        "prn_is_favoris_expert",
        "prn_rank_normalized",
    ]
    fill_counts = {k: 0 for k in feature_names}

    n_read = 0
    n_written = 0
    n_matched = 0
    t0 = time.time()

    with open(master_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1

            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid")
            date_iso = rec.get("date_reunion_iso", "")
            num_reunion = rec.get("numero_reunion")
            num_course = rec.get("numero_course") or rec.get("num_course")
            num_pmu = rec.get("num_pmu")

            if not partant_uid:
                continue

            # Build output record skeleton
            out: dict[str, Any] = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_iso,
            }

            # Try to match with pronostic lookup
            prono_entry = None
            if date_iso and num_reunion is not None and num_course is not None:
                try:
                    lookup_key = (str(date_iso), int(num_reunion), int(num_course))
                    prono_entry = prono_lookup.get(lookup_key)
                except (ValueError, TypeError):
                    pass

            if prono_entry is not None and num_pmu is not None:
                try:
                    num_pmu_int = int(num_pmu)
                except (ValueError, TypeError):
                    num_pmu_int = None

                horses = prono_entry["horses"]
                nb_cited = prono_entry["nb_cited"]
                horse_info = horses.get(num_pmu_int) if num_pmu_int is not None else None

                if horse_info is not None:
                    n_matched += 1
                    rank = horse_info["rank"]
                    cote_prono = horse_info["cote"]

                    out["prn_expert_rank"] = rank
                    fill_counts["prn_expert_rank"] += 1

                    out["prn_expert_cited"] = 1
                    fill_counts["prn_expert_cited"] += 1

                    out["prn_expert_top3"] = 1 if rank <= 3 else 0
                    fill_counts["prn_expert_top3"] += 1

                    out["prn_expert_cote"] = cote_prono
                    if cote_prono is not None:
                        fill_counts["prn_expert_cote"] += 1

                    # cote_vs_market: ratio prono odds / market odds
                    cote_market = rec.get("cote_finale") or rec.get("cote_reference")
                    if cote_prono is not None and cote_market is not None:
                        try:
                            cote_market_f = float(cote_market)
                            if cote_market_f > 0:
                                out["prn_expert_cote_vs_market"] = round(cote_prono / cote_market_f, 4)
                                fill_counts["prn_expert_cote_vs_market"] += 1
                            else:
                                out["prn_expert_cote_vs_market"] = None
                        except (ValueError, TypeError):
                            out["prn_expert_cote_vs_market"] = None
                    else:
                        out["prn_expert_cote_vs_market"] = None

                    out["prn_nb_chevaux_pronostic"] = nb_cited
                    fill_counts["prn_nb_chevaux_pronostic"] += 1

                    out["prn_is_favoris_expert"] = 1 if rank == 1 else 0
                    fill_counts["prn_is_favoris_expert"] += 1

                    out["prn_rank_normalized"] = round(rank / nb_cited, 4) if nb_cited > 0 else None
                    if out["prn_rank_normalized"] is not None:
                        fill_counts["prn_rank_normalized"] += 1

                else:
                    # Horse not cited in pronostic but pronostic exists for this race
                    out["prn_expert_rank"] = None
                    out["prn_expert_cited"] = 0
                    fill_counts["prn_expert_cited"] += 1
                    out["prn_expert_top3"] = 0
                    fill_counts["prn_expert_top3"] += 1
                    out["prn_expert_cote"] = None
                    out["prn_expert_cote_vs_market"] = None
                    out["prn_nb_chevaux_pronostic"] = nb_cited
                    fill_counts["prn_nb_chevaux_pronostic"] += 1
                    out["prn_is_favoris_expert"] = 0
                    fill_counts["prn_is_favoris_expert"] += 1
                    out["prn_rank_normalized"] = None
            else:
                # No pronostic found for this race
                out["prn_expert_rank"] = None
                out["prn_expert_cited"] = None
                out["prn_expert_top3"] = None
                out["prn_expert_cote"] = None
                out["prn_expert_cote_vs_market"] = None
                out["prn_nb_chevaux_pronostic"] = None
                out["prn_is_favoris_expert"] = None
                out["prn_rank_normalized"] = None

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

            if n_read % _LOG_EVERY == 0:
                logger.info("  Phase 2: %d/%d records traites, %d matched...", n_read, n_read, n_matched)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Phase 2 terminee: %d records lus, %d ecrits, %d chevaux matches en %.1fs",
        n_read, n_written, n_matched, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# MAIN
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features pronostics consensus a partir de partants_master + pronostics"
    )
    parser.add_argument(
        "--input-master", type=str, default=None,
        help=f"Chemin vers partants_master.jsonl (defaut: {INPUT_MASTER_DEFAULT})",
    )
    parser.add_argument(
        "--input-prono", type=str, default=None,
        help=f"Chemin vers pronostics.jsonl (defaut: {INPUT_PRONO_DEFAULT})",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("pronostics_consensus_builder")

    # Resolve input paths
    master_path = Path(args.input_master) if args.input_master else INPUT_MASTER_DEFAULT
    prono_path = Path(args.input_prono) if args.input_prono else INPUT_PRONO_DEFAULT
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_file = output_dir / "pronostics_consensus_features.jsonl"

    if not master_path.exists():
        logger.error("Fichier master introuvable: %s", master_path)
        sys.exit(1)
    if not prono_path.exists():
        logger.error("Fichier pronostics introuvable: %s", prono_path)
        sys.exit(1)

    logger.info("=== Pronostics Consensus Builder ===")
    logger.info("Input master : %s", master_path)
    logger.info("Input prono  : %s", prono_path)
    logger.info("Output       : %s", output_file)

    t0 = time.time()

    # Phase 1: build pronostic lookup
    logger.info("--- Phase 1: Chargement pronostics ---")
    prono_lookup = _build_prono_lookup(prono_path, logger)
    gc.collect()

    # Phase 2: stream master, compute features
    logger.info("--- Phase 2: Calcul features ---")
    n_written = _compute_features(master_path, prono_lookup, output_file, logger)
    gc.collect()

    elapsed = time.time() - t0
    logger.info("=== Termine: %d records en %.1fs ===", n_written, elapsed)


if __name__ == "__main__":
    main()
