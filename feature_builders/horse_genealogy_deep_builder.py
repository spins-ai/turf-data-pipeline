#!/usr/bin/env python3
"""
feature_builders.horse_genealogy_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep horse genealogy features -- exploiting sire/dam lineage for predictive value.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant genealogy features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the sire/dam/broodmare-sire statistics -- no future leakage.

Produces:
  - horse_genealogy_deep.jsonl   in builder_outputs/horse_genealogy_deep/

Features per partant (10):
  - hgd_sire_runners_seen       : total offspring by this sire seen so far
  - hgd_sire_win_rate           : sire offspring win rate from past
  - hgd_sire_place_rate         : sire offspring place rate (top 3)
  - hgd_dam_runners_seen        : total offspring by this dam seen so far
  - hgd_dam_win_rate            : dam offspring win rate from past
  - hgd_sire_x_discipline_wr   : sire's offspring win rate in this discipline
  - hgd_sire_x_distance_wr     : sire's offspring win rate at this distance bucket
  - hgd_broodmare_sire_wr      : pere_mere's offspring win rate (broodmare sire signal)
  - hgd_sire_class_avg         : average gains_carriere of sire's offspring
  - hgd_sire_precocity         : sire offspring win rate at age <= 3 vs age > 3

Usage:
    python feature_builders/horse_genealogy_deep_builder.py
    python feature_builders/horse_genealogy_deep_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_genealogy_deep")

# Progress log every N records
_LOG_EVERY = 500_000


def _dist_bucket(distance: Any) -> Optional[str]:
    """Map raw distance (m) to a bucket for sire x distance stats."""
    try:
        d = int(distance)
    except (TypeError, ValueError):
        return None
    if d < 1400:
        return "sprint"
    elif d < 1800:
        return "mile"
    elif d < 2400:
        return "inter"
    else:
        return "staying"


def _safe_rate(wins: int, total: int) -> Optional[float]:
    """Win rate with minimum-sample guard (at least 1 runner)."""
    if total < 1:
        return None
    return round(wins / total, 6)


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _SireStats:
    """Tracks sire offspring performance across multiple dimensions.

    Uses __slots__ for memory efficiency with potentially 50K+ sires.
    """

    __slots__ = (
        "total", "wins", "places",
        "disc_total", "disc_wins",
        "dist_total", "dist_wins",
        "young_total", "young_wins",
        "old_total", "old_wins",
        "gains_sum", "gains_count",
    )

    def __init__(self) -> None:
        self.total: int = 0
        self.wins: int = 0
        self.places: int = 0
        # per-discipline
        self.disc_total: dict[str, int] = defaultdict(int)
        self.disc_wins: dict[str, int] = defaultdict(int)
        # per-distance bucket
        self.dist_total: dict[str, int] = defaultdict(int)
        self.dist_wins: dict[str, int] = defaultdict(int)
        # precocity: age <= 3 vs > 3
        self.young_total: int = 0
        self.young_wins: int = 0
        self.old_total: int = 0
        self.old_wins: int = 0
        # gains (class proxy)
        self.gains_sum: float = 0.0
        self.gains_count: int = 0

    def snapshot_base(self) -> dict[str, Any]:
        """Snapshot sire-level features BEFORE update."""
        return {
            "hgd_sire_runners_seen": self.total if self.total > 0 else None,
            "hgd_sire_win_rate": _safe_rate(self.wins, self.total),
            "hgd_sire_place_rate": _safe_rate(self.places, self.total),
            "hgd_sire_class_avg": (
                round(self.gains_sum / self.gains_count, 2)
                if self.gains_count > 0 else None
            ),
        }

    def snapshot_discipline(self, discipline: Optional[str]) -> Optional[float]:
        if not discipline:
            return None
        t = self.disc_total.get(discipline, 0)
        w = self.disc_wins.get(discipline, 0)
        return _safe_rate(w, t)

    def snapshot_distance(self, dist_bucket: Optional[str]) -> Optional[float]:
        if not dist_bucket:
            return None
        t = self.dist_total.get(dist_bucket, 0)
        w = self.dist_wins.get(dist_bucket, 0)
        return _safe_rate(w, t)

    def snapshot_precocity(self) -> Optional[float]:
        """Ratio: young win rate / old win rate. >1 means sire is precocious."""
        young_wr = _safe_rate(self.young_wins, self.young_total)
        old_wr = _safe_rate(self.old_wins, self.old_total)
        if young_wr is None or old_wr is None:
            return None
        if old_wr == 0:
            return round(young_wr * 10, 6) if young_wr > 0 else None
        return round(young_wr / old_wr, 6)

    def update(
        self,
        is_winner: bool,
        is_place: bool,
        discipline: Optional[str],
        dist_bucket: Optional[str],
        age: Optional[int],
        gains: Optional[float],
    ) -> None:
        self.total += 1
        if is_winner:
            self.wins += 1
        if is_place:
            self.places += 1
        if discipline:
            self.disc_total[discipline] += 1
            if is_winner:
                self.disc_wins[discipline] += 1
        if dist_bucket:
            self.dist_total[dist_bucket] += 1
            if is_winner:
                self.dist_wins[dist_bucket] += 1
        if age is not None:
            if age <= 3:
                self.young_total += 1
                if is_winner:
                    self.young_wins += 1
            else:
                self.old_total += 1
                if is_winner:
                    self.old_wins += 1
        if gains is not None and gains > 0:
            self.gains_sum += gains
            self.gains_count += 1


class _DamStats:
    """Tracks dam offspring performance (simpler than sire)."""

    __slots__ = ("total", "wins")

    def __init__(self) -> None:
        self.total: int = 0
        self.wins: int = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "hgd_dam_runners_seen": self.total if self.total > 0 else None,
            "hgd_dam_win_rate": _safe_rate(self.wins, self.total),
        }

    def update(self, is_winner: bool) -> None:
        self.total += 1
        if is_winner:
            self.wins += 1


class _BroodmareSireStats:
    """Tracks broodmare sire (pere_mere) offspring performance."""

    __slots__ = ("total", "wins")

    def __init__(self) -> None:
        self.total: int = 0
        self.wins: int = 0

    def snapshot(self) -> Optional[float]:
        return _safe_rate(self.wins, self.total)

    def update(self, is_winner: bool) -> None:
        self.total += 1
        if is_winner:
            self.wins += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort + seek-based processing)
# ===========================================================================


def build_horse_genealogy_deep_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build deep genealogy features from partants_master.jsonl.

    Two-phase approach:
      1. Read sort keys + file byte offsets into memory (lightweight index).
      2. Sort chronologically.
      3. Seek-based course-by-course processing, streaming output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Horse Genealogy Deep Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()

    sire_stats: dict[str, _SireStats] = defaultdict(_SireStats)
    dam_stats: dict[str, _DamStats] = defaultdict(_DamStats)
    broodmare_sire_stats: dict[str, _BroodmareSireStats] = defaultdict(_BroodmareSireStats)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "hgd_sire_runners_seen": 0,
        "hgd_sire_win_rate": 0,
        "hgd_sire_place_rate": 0,
        "hgd_dam_runners_seen": 0,
        "hgd_dam_win_rate": 0,
        "hgd_sire_x_discipline_wr": 0,
        "hgd_sire_x_distance_wr": 0,
        "hgd_broodmare_sire_wr": 0,
        "hgd_sire_class_avg": 0,
        "hgd_sire_precocity": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            age = rec.get("age")
            try:
                age = int(age) if age is not None else None
            except (ValueError, TypeError):
                age = None

            distance = rec.get("distance")
            try:
                distance = int(distance) if distance is not None else None
            except (ValueError, TypeError):
                distance = None

            gains = rec.get("gains_carriere") or rec.get("gains_carriere_euros")
            try:
                gains = float(gains) if gains is not None else None
            except (ValueError, TypeError):
                gains = None

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            # Placement: top 3 = place
            place_arrivee = rec.get("place_arrivee")
            try:
                place_arrivee = int(place_arrivee) if place_arrivee is not None else None
            except (ValueError, TypeError):
                place_arrivee = None

            is_place = place_arrivee is not None and 1 <= place_arrivee <= 3

            return {
                "uid": rec.get("partant_uid"),
                "pere": (rec.get("pere") or "").strip().upper(),
                "mere": (rec.get("mere") or "").strip().upper(),
                "pere_mere": (rec.get("pere_mere") or "").strip().upper(),
                "gagnant": bool(rec.get("is_gagnant")),
                "is_place": is_place,
                "discipline": discipline,
                "dist_bucket": _dist_bucket(distance),
                "age": age,
                "gains": gains,
            }

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read only this course's records from disk
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race stats and emit features --
            post_updates: list[dict] = []

            for rec in course_group:
                pere = rec["pere"]
                mere = rec["mere"]
                pere_mere = rec["pere_mere"]
                discipline = rec["discipline"]
                dist_bucket = rec["dist_bucket"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                # --- Sire features ---
                if pere:
                    ss = sire_stats[pere]
                    base = ss.snapshot_base()
                    features.update(base)
                    for k, v in base.items():
                        if v is not None and k in fill_counts:
                            fill_counts[k] += 1

                    disc_wr = ss.snapshot_discipline(discipline)
                    features["hgd_sire_x_discipline_wr"] = disc_wr
                    if disc_wr is not None:
                        fill_counts["hgd_sire_x_discipline_wr"] += 1

                    dist_wr = ss.snapshot_distance(dist_bucket)
                    features["hgd_sire_x_distance_wr"] = dist_wr
                    if dist_wr is not None:
                        fill_counts["hgd_sire_x_distance_wr"] += 1

                    prec = ss.snapshot_precocity()
                    features["hgd_sire_precocity"] = prec
                    if prec is not None:
                        fill_counts["hgd_sire_precocity"] += 1
                else:
                    features["hgd_sire_runners_seen"] = None
                    features["hgd_sire_win_rate"] = None
                    features["hgd_sire_place_rate"] = None
                    features["hgd_sire_class_avg"] = None
                    features["hgd_sire_x_discipline_wr"] = None
                    features["hgd_sire_x_distance_wr"] = None
                    features["hgd_sire_precocity"] = None

                # --- Dam features ---
                if mere:
                    ds = dam_stats[mere]
                    dam_snap = ds.snapshot()
                    features.update(dam_snap)
                    for k, v in dam_snap.items():
                        if v is not None and k in fill_counts:
                            fill_counts[k] += 1
                else:
                    features["hgd_dam_runners_seen"] = None
                    features["hgd_dam_win_rate"] = None

                # --- Broodmare sire features ---
                if pere_mere:
                    bms = broodmare_sire_stats[pere_mere]
                    bms_wr = bms.snapshot()
                    features["hgd_broodmare_sire_wr"] = bms_wr
                    if bms_wr is not None:
                        fill_counts["hgd_broodmare_sire_wr"] += 1
                else:
                    features["hgd_broodmare_sire_wr"] = None

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append(rec)

            # -- Update stats after race (post-race, no leakage) --
            for rec in post_updates:
                pere = rec["pere"]
                mere = rec["mere"]
                pere_mere = rec["pere_mere"]
                is_winner = rec["gagnant"]
                is_place = rec["is_place"]
                discipline = rec["discipline"]
                dist_bucket = rec["dist_bucket"]
                age = rec["age"]
                gains = rec["gains"]

                if pere:
                    sire_stats[pere].update(
                        is_winner, is_place, discipline, dist_bucket, age, gains
                    )

                if mere:
                    dam_stats[mere].update(is_winner)

                if pere_mere:
                    broodmare_sire_stats[pere_mere].update(is_winner)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Horse genealogy deep build termine: %d features en %.1fs "
        "(sires: %d, dams: %d, broodmare sires: %d)",
        n_written, elapsed,
        len(sire_stats), len(dam_stats), len(broodmare_sire_stats),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

    return n_written


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
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features genealogie profonde a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/horse_genealogy_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("horse_genealogy_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "horse_genealogy_deep.jsonl"
    build_horse_genealogy_deep_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
