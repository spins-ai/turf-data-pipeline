#!/usr/bin/env python3
"""
feature_builders.family_sibling_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse family/sibling performance features by tracking horses that share
the same sire (pere) or dam (mere).

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.  State is snapshotted
BEFORE updating with the current race result.

Produces:
  - family_sibling_features.jsonl  in builder_outputs/family_sibling/

Features per partant (10):
  - fam_sire_win_rate            : win rate of all horses by same sire seen so far
  - fam_sire_place_rate          : place rate of all horses by same sire
  - fam_sire_nb_runners          : total runners by same sire seen so far
  - fam_dam_win_rate             : win rate of all horses by same dam
  - fam_dam_nb_runners           : total runners by same dam
  - fam_siblings_avg_gains       : average gains_carriere_euros of siblings (same pere)
  - fam_sire_distance_affinity   : sire win rate at similar distance bucket
  - fam_sire_discipline_affinity : sire win rate in same discipline
  - fam_family_class_rating      : average spd_class_rating of siblings (same pere)
  - fam_dam_best_offspring_wr    : best win rate among dam's offspring

Usage:
    python feature_builders/family_sibling_builder.py
    python feature_builders/family_sibling_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/family_sibling")
_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _sf(val) -> Optional[float]:
    """Safe float conversion."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return None


def _si(val) -> Optional[int]:
    """Safe int conversion."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_rate(wins: int, total: int) -> Optional[float]:
    """Win rate with zero-division guard."""
    if total < 1:
        return None
    return round(wins / total, 4)


def _distance_bucket(distance: Optional[int]) -> Optional[str]:
    """Map distance (m) to a bucket: short / mid / long."""
    if distance is None:
        return None
    if distance < 1600:
        return "short"
    elif distance <= 2400:
        return "mid"
    else:
        return "long"


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


def _new_sire_state() -> dict[str, Any]:
    """Create a fresh sire accumulator."""
    return {
        "wins": 0,
        "places": 0,
        "total": 0,
        "gains_sum": 0.0,
        "gains_count": 0,
        # distance bucket: {bucket: {wins, total}}
        "dist_wins": defaultdict(int),
        "dist_total": defaultdict(int),
        # discipline: {disc: {wins, total}}
        "disc_wins": defaultdict(int),
        "disc_total": defaultdict(int),
        # class ratings collected
        "class_ratings": [],
        # offspring tracking: {horse_id: {wins, total}}
        "offspring": defaultdict(lambda: {"wins": 0, "total": 0}),
    }


def _new_dam_state() -> dict[str, Any]:
    """Create a fresh dam accumulator."""
    return {
        "wins": 0,
        "total": 0,
        # offspring tracking: {horse_id: {wins, total}}
        "offspring": defaultdict(lambda: {"wins": 0, "total": 0}),
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build(input_path: Path, output_dir: Path, logger) -> None:
    t0 = time.time()
    logger.info("=== Family Sibling Builder ===")
    logger.info("Input: %s", input_path)

    # ------------------------------------------------------------------
    # Phase 1: Index + sort chronologically
    # ------------------------------------------------------------------
    logger.info("Phase 1: loading and indexing records...")
    slim_records: list[dict] = []
    n_read = 0
    n_errors = 0

    with open(input_path, "r", encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Lu %d records...", n_read)

            slim_records.append({
                "uid": rec.get("partant_uid"),
                "date": str(rec.get("date_reunion_iso", "") or "")[:10],
                "course": str(rec.get("course_uid", "") or ""),
                "num": rec.get("num_pmu", 0) or 0,
                "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
                "pere": (rec.get("pere") or "").strip(),
                "mere": (rec.get("mere") or "").strip(),
                "is_gagnant": bool(rec.get("is_gagnant")),
                "is_place": bool(rec.get("is_place")),
                "gains": _sf(rec.get("gains_carriere_euros")),
                "distance": _si(rec.get("distance")),
                "discipline": (rec.get("discipline") or "").strip(),
                "class_rating": _sf(rec.get("spd_class_rating")),
            })

    logger.info(
        "Phase 1 terminee: %d records en %.1fs (%d erreurs JSON)",
        len(slim_records), time.time() - t0, n_errors,
    )

    # Sort chronologically by date, course, num_pmu
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 2: Seek-based processing -- course by course
    # ------------------------------------------------------------------
    logger.info("Phase 2: computing features...")
    t2 = time.time()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "family_sibling_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    # State accumulators
    sire_state: dict[str, dict] = defaultdict(_new_sire_state)
    dam_state: dict[str, dict] = defaultdict(_new_dam_state)

    feat_names = [
        "fam_sire_win_rate",
        "fam_sire_place_rate",
        "fam_sire_nb_runners",
        "fam_dam_win_rate",
        "fam_dam_nb_runners",
        "fam_siblings_avg_gains",
        "fam_sire_distance_affinity",
        "fam_sire_discipline_affinity",
        "fam_family_class_rating",
        "fam_dam_best_offspring_wr",
    ]
    fill = {k: 0 for k in feat_names}
    n_written = 0

    i = 0
    total = len(slim_records)

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        while i < total:
            # Gather all records for this course
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

            # -- Snapshot pre-race state for all runners, then update --
            deferred_updates: list[dict] = []

            for rec in course_group:
                pere = rec["pere"]
                mere = rec["mere"]
                horse_id = rec["horse_id"]
                dist_bucket = _distance_bucket(rec["distance"])
                discipline = rec["discipline"]

                out: dict[str, Any] = {"partant_uid": rec["uid"]}

                # ---------- SIRE features (snapshot BEFORE update) ----------
                if pere:
                    ss = sire_state[pere]

                    # 1. fam_sire_win_rate
                    val = _safe_rate(ss["wins"], ss["total"])
                    out["fam_sire_win_rate"] = val
                    if val is not None:
                        fill["fam_sire_win_rate"] += 1

                    # 2. fam_sire_place_rate
                    val = _safe_rate(ss["places"], ss["total"])
                    out["fam_sire_place_rate"] = val
                    if val is not None:
                        fill["fam_sire_place_rate"] += 1

                    # 3. fam_sire_nb_runners
                    val = ss["total"] if ss["total"] > 0 else None
                    out["fam_sire_nb_runners"] = val
                    if val is not None:
                        fill["fam_sire_nb_runners"] += 1

                    # 6. fam_siblings_avg_gains
                    if ss["gains_count"] > 0:
                        val = round(ss["gains_sum"] / ss["gains_count"], 2)
                        out["fam_siblings_avg_gains"] = val
                        fill["fam_siblings_avg_gains"] += 1
                    else:
                        out["fam_siblings_avg_gains"] = None

                    # 7. fam_sire_distance_affinity
                    if dist_bucket and ss["dist_total"][dist_bucket] > 0:
                        val = _safe_rate(
                            ss["dist_wins"][dist_bucket],
                            ss["dist_total"][dist_bucket],
                        )
                        out["fam_sire_distance_affinity"] = val
                        if val is not None:
                            fill["fam_sire_distance_affinity"] += 1
                    else:
                        out["fam_sire_distance_affinity"] = None

                    # 8. fam_sire_discipline_affinity
                    if discipline and ss["disc_total"][discipline] > 0:
                        val = _safe_rate(
                            ss["disc_wins"][discipline],
                            ss["disc_total"][discipline],
                        )
                        out["fam_sire_discipline_affinity"] = val
                        if val is not None:
                            fill["fam_sire_discipline_affinity"] += 1
                    else:
                        out["fam_sire_discipline_affinity"] = None

                    # 9. fam_family_class_rating
                    if ss["class_ratings"]:
                        val = round(
                            sum(ss["class_ratings"]) / len(ss["class_ratings"]), 2
                        )
                        out["fam_family_class_rating"] = val
                        fill["fam_family_class_rating"] += 1
                    else:
                        out["fam_family_class_rating"] = None

                else:
                    out["fam_sire_win_rate"] = None
                    out["fam_sire_place_rate"] = None
                    out["fam_sire_nb_runners"] = None
                    out["fam_siblings_avg_gains"] = None
                    out["fam_sire_distance_affinity"] = None
                    out["fam_sire_discipline_affinity"] = None
                    out["fam_family_class_rating"] = None

                # ---------- DAM features (snapshot BEFORE update) ----------
                if mere:
                    ds = dam_state[mere]

                    # 4. fam_dam_win_rate
                    val = _safe_rate(ds["wins"], ds["total"])
                    out["fam_dam_win_rate"] = val
                    if val is not None:
                        fill["fam_dam_win_rate"] += 1

                    # 5. fam_dam_nb_runners
                    val = ds["total"] if ds["total"] > 0 else None
                    out["fam_dam_nb_runners"] = val
                    if val is not None:
                        fill["fam_dam_nb_runners"] += 1

                    # 10. fam_dam_best_offspring_wr
                    best_wr: Optional[float] = None
                    for _hid, stats in ds["offspring"].items():
                        if stats["total"] >= 3:  # min 3 races for meaningful wr
                            wr = stats["wins"] / stats["total"]
                            if best_wr is None or wr > best_wr:
                                best_wr = wr
                    if best_wr is not None:
                        out["fam_dam_best_offspring_wr"] = round(best_wr, 4)
                        fill["fam_dam_best_offspring_wr"] += 1
                    else:
                        out["fam_dam_best_offspring_wr"] = None

                else:
                    out["fam_dam_win_rate"] = None
                    out["fam_dam_nb_runners"] = None
                    out["fam_dam_best_offspring_wr"] = None

                fout.write(json.dumps(out, ensure_ascii=False) + "\n")
                n_written += 1

                # Collect deferred update info
                deferred_updates.append(rec)

            # -- Update state AFTER all runners in this course are snapshotted --
            for rec in deferred_updates:
                pere = rec["pere"]
                mere = rec["mere"]
                horse_id = rec["horse_id"]
                is_gagnant = rec["is_gagnant"]
                is_place = rec["is_place"]
                gains = rec["gains"]
                dist_bucket = _distance_bucket(rec["distance"])
                discipline = rec["discipline"]
                class_rating = rec["class_rating"]

                if pere:
                    ss = sire_state[pere]
                    ss["total"] += 1
                    if is_gagnant:
                        ss["wins"] += 1
                    if is_place:
                        ss["places"] += 1
                    if gains is not None:
                        ss["gains_sum"] += gains
                        ss["gains_count"] += 1
                    if dist_bucket:
                        ss["dist_total"][dist_bucket] += 1
                        if is_gagnant:
                            ss["dist_wins"][dist_bucket] += 1
                    if discipline:
                        ss["disc_total"][discipline] += 1
                        if is_gagnant:
                            ss["disc_wins"][discipline] += 1
                    if class_rating is not None:
                        ss["class_ratings"].append(class_rating)
                    # Update offspring tracking for sire
                    if horse_id:
                        ss["offspring"][horse_id]["total"] += 1
                        if is_gagnant:
                            ss["offspring"][horse_id]["wins"] += 1

                if mere:
                    ds = dam_state[mere]
                    ds["total"] += 1
                    if is_gagnant:
                        ds["wins"] += 1
                    if horse_id:
                        ds["offspring"][horse_id]["total"] += 1
                        if is_gagnant:
                            ds["offspring"][horse_id]["wins"] += 1

            if n_written % _LOG_EVERY == 0 and n_written > 0:
                logger.info("  Traite %d / %d records...", n_written, total)
                gc.collect()

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Termine: %d records en %.1fs (peres: %d, meres: %d)",
        n_written, elapsed, len(sire_state), len(dam_state),
    )
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %-35s: %7d / %d (%.1f%%)", k, v, n_written, pct)


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Family/sibling performance features builder"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: INPUT_PARTANTS)",
    )
    args = parser.parse_args()

    logger = setup_logging("family_sibling_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    build(input_path, OUTPUT_DIR, logger)


if __name__ == "__main__":
    main()
