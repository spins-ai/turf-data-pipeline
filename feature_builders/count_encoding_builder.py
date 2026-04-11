#!/usr/bin/env python3
"""
feature_builders.count_encoding_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Count/frequency encodings for categorical features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant count encoding features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the count -- no future leakage.  Features are captured
BEFORE updating the counters for the current race.

Produces:
  - count_encoding.jsonl   in output/count_encoding/

Features per partant (10):
  - ce_horse_count        : total race count for this horse so far
  - ce_jockey_count       : total ride count for this jockey so far
  - ce_trainer_count      : total runner count for this trainer so far
  - ce_sire_count         : total offspring run count for this sire
  - ce_hippo_count        : total race count at this hippodrome
  - ce_owner_count        : total runner count for this owner
  - ce_horse_log_count    : log(1 + horse_count)
  - ce_jockey_log_count   : log(1 + jockey_count)
  - ce_trainer_log_count  : log(1 + trainer_count)
  - ce_hippo_log_count    : log(1 + hippo_count)

Usage:
    python feature_builders/count_encoding_builder.py
    python feature_builders/count_encoding_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
    python feature_builders/count_encoding_builder.py --output-dir D:/my/output
"""

from __future__ import annotations

import argparse
import gc
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

INPUT_PARTANTS = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/count_encoding"
)

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
                    logger.warning(
                        "Ligne JSON invalide ignoree (erreur %d)", errors
                    )
    logger.info(
        "Lecture terminee: %d records, %d erreurs JSON", count, errors
    )


# ===========================================================================
# HELPERS
# ===========================================================================


def _norm(value: Any) -> Optional[str]:
    """Normalise a categorical value to a stripped lowercase string, or None."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s.lower()


def _log1p(count: int) -> float:
    """Return log(1 + count), rounded to 6 decimal places."""
    return round(math.log1p(count), 6)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_count_encoding_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build count/frequency encoding features for every partant.

    The algorithm runs in two phases:

    Phase 1 – Streaming read
        Load only the minimal fields needed for counting and identification
        into a list of slim dicts, freeing all other memory.

    Phase 2 – Sort + sweep
        Sort chronologically (date, course_uid, num_pmu) then sweep once,
        snapshotting counts BEFORE updating them for the current race.
        This guarantees no future leakage.
    """
    logger.info("=== Count Encoding Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "course": rec.get("course_uid", "") or "",
            "num": rec.get("num_pmu") or 0,
            # categorical keys – normalised at read time to avoid repeated work
            "horse": _norm(rec.get("horse_id") or rec.get("nom_cheval")),
            "jockey": _norm(rec.get("jockey") or rec.get("nom_jockey")),
            "trainer": _norm(
                rec.get("entraineur") or rec.get("nom_entraineur")
            ),
            "sire": _norm(rec.get("nom_pere")),
            "hippo": _norm(rec.get("hippodrome")),
            "owner": _norm(
                rec.get("proprietaire") or rec.get("nom_proprietaire")
            ),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )
    gc.collect()

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(
        key=lambda r: (r["date"], r["course"], int(r["num"]) if r["num"] else 0)
    )
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Sweep, snapshot then update --
    t2 = time.time()

    # Counters: category value -> cumulative count seen so far
    horse_cnt: dict[str, int] = defaultdict(int)
    jockey_cnt: dict[str, int] = defaultdict(int)
    trainer_cnt: dict[str, int] = defaultdict(int)
    sire_cnt: dict[str, int] = defaultdict(int)
    hippo_cnt: dict[str, int] = defaultdict(int)
    owner_cnt: dict[str, int] = defaultdict(int)

    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)

    # We process race by race (group by course_uid) so that within a race all
    # partants see the same pre-race counts (no within-race ordering bias).
    i = 0
    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all partants of this race
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # --- Snapshot pre-race counts and emit features ---
        # Also accumulate updates to apply after the full group is processed.
        post_updates: list[dict] = []

        for rec in course_group:
            h = rec["horse"]
            j = rec["jockey"]
            tr = rec["trainer"]
            si = rec["sire"]
            hi = rec["hippo"]
            ow = rec["owner"]

            # Pre-race counts (BEFORE this race is added)
            hc = horse_cnt[h] if h else 0
            jc = jockey_cnt[j] if j else 0
            trc = trainer_cnt[tr] if tr else 0
            sic = sire_cnt[si] if si else 0
            hic = hippo_cnt[hi] if hi else 0
            owc = owner_cnt[ow] if ow else 0

            results.append(
                {
                    "partant_uid": rec["uid"],
                    "ce_horse_count": hc,
                    "ce_jockey_count": jc,
                    "ce_trainer_count": trc,
                    "ce_sire_count": sic,
                    "ce_hippo_count": hic,
                    "ce_owner_count": owc,
                    "ce_horse_log_count": _log1p(hc),
                    "ce_jockey_log_count": _log1p(jc),
                    "ce_trainer_log_count": _log1p(trc),
                    "ce_hippo_log_count": _log1p(hic),
                }
            )

            post_updates.append(
                {"horse": h, "jockey": j, "trainer": tr, "sire": si,
                 "hippo": hi, "owner": ow}
            )

        # --- Update counters after the full race group ---
        for upd in post_updates:
            if upd["horse"]:
                horse_cnt[upd["horse"]] += 1
            if upd["jockey"]:
                jockey_cnt[upd["jockey"]] += 1
            if upd["trainer"]:
                trainer_cnt[upd["trainer"]] += 1
            if upd["sire"]:
                sire_cnt[upd["sire"]] += 1
            if upd["hippo"]:
                hippo_cnt[upd["hippo"]] += 1
            if upd["owner"]:
                owner_cnt[upd["owner"]] += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info(
                "  Traite %d / %d records...", n_processed, total
            )

    elapsed = time.time() - t0
    logger.info(
        "Count encoding build termine: %d features en %.1fs "
        "(chevaux uniques: %d, jockeys: %d, entraineurs: %d, "
        "hippodromes: %d)",
        len(results),
        elapsed,
        len(horse_cnt),
        len(jockey_cnt),
        len(trainer_cnt),
        len(hippo_cnt),
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features count-encoding a partir de "
            "partants_master"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help=(
            "Chemin vers partants_master.jsonl "
            "(defaut: D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl)"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: OUTPUT_DIR)",
    )
    args = parser.parse_args()

    logger = setup_logging("count_encoding_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_count_encoding_features(input_path, logger)

    # Save
    out_path = output_dir / "count_encoding.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats: fill rates
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info(
                "  %s: %d/%d (%.1f%%)", k, v, total, 100.0 * v / total
            )


if __name__ == "__main__":
    main()
