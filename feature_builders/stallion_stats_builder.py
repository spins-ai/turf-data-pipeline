#!/usr/bin/env python3
"""
feature_builders.stallion_stats_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sire (stallion) performance statistics over time.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant stallion-lineage features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the sire's statistics -- no future leakage.  Features are
emitted with the pre-race snapshot; trackers are updated after.

Produces:
  - stallion_stats.jsonl   in output/stallion_stats/

Features per partant (8):
  - ss_sire_win_rate       : sire's progeny overall win rate (up to now)
  - ss_sire_runners        : total number of progeny runners seen for this sire
  - ss_sire_avg_earnings   : average earnings per runner for sire's progeny
  - ss_sire_distance_wr    : sire's progeny win rate at this distance bucket
  - ss_sire_terrain_wr     : sire's progeny win rate on this terrain type
  - ss_sire_class_level    : average allocation of races sire's progeny have won at
  - ss_sire_hot            : 1 if sire's progeny win rate in last 100 races > overall
  - ss_sire_consistency    : std dev of sire progeny positions (low = consistent)

Usage:
    python feature_builders/stallion_stats_builder.py
    python feature_builders/stallion_stats_builder.py --input /path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/stallion_stats")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# Distance buckets (metres)
_DISTANCE_BUCKETS = [
    (0, 1400, "court"),
    (1400, 1800, "moyen_court"),
    (1800, 2200, "moyen"),
    (2200, 2800, "moyen_long"),
    (2800, 99999, "long"),
]

# Cap on win_allocations list to bound memory per sire
_MAX_WIN_ALLOCS = 500
# Cap on positions deque for consistency calculation
_POSITIONS_MAXLEN = 200
# Recent runs window for "hot sire" detection
_RECENT_MAXLEN = 100


# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_bucket(distance_m: Optional[float]) -> str:
    """Map a distance in metres to a named bucket."""
    if distance_m is None:
        return "unknown"
    try:
        d = float(distance_m)
    except (TypeError, ValueError):
        return "unknown"
    for lo, hi, name in _DISTANCE_BUCKETS:
        if lo <= d < hi:
            return name
    return "long"


def _safe_float(val, default=None) -> Optional[float]:
    """Parse a value to float, returning default on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _position_pct(pos_arrivee, n_partants) -> Optional[float]:
    """
    Convert finishing position to a percentile in [0, 1].
    1st place = 0.0  (best), last = 1.0 (worst).
    Returns None when inputs are unusable.
    """
    pos = _safe_float(pos_arrivee)
    n = _safe_float(n_partants)
    if pos is None or n is None or n <= 0:
        return None
    # positions start at 1; clamp to [1, n]
    pos = max(1.0, min(pos, n))
    if n == 1:
        return 0.0
    return (pos - 1.0) / (n - 1.0)


def _stdev(values: deque) -> Optional[float]:
    """Population standard deviation of a deque of floats."""
    n = len(values)
    if n < 2:
        return None
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)


# ===========================================================================
# SIRE STATE
# ===========================================================================


class _SireState:
    """Mutable accumulator for one stallion's progeny statistics."""

    __slots__ = (
        "wins",
        "total",
        "earnings_sum",
        "per_distance",
        "per_terrain",
        "win_allocations",
        "recent",
        "positions",
    )

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0
        self.earnings_sum: float = 0.0
        # per_distance[bucket] = {"wins": int, "total": int}
        self.per_distance: dict[str, dict[str, int]] = defaultdict(
            lambda: {"wins": 0, "total": 0}
        )
        # per_terrain[terrain_code] = {"wins": int, "total": int}
        self.per_terrain: dict[str, dict[str, int]] = defaultdict(
            lambda: {"wins": 0, "total": 0}
        )
        # allocations of races that sire's progeny have WON (bounded)
        self.win_allocations: list[float] = []
        # last 100 race outcomes: True = winner
        self.recent: deque[bool] = deque(maxlen=_RECENT_MAXLEN)
        # last 200 normalised finishing positions
        self.positions: deque[float] = deque(maxlen=_POSITIONS_MAXLEN)

    # ------------------------------------------------------------------
    # Feature snapshot (call BEFORE updating with current race result)
    # ------------------------------------------------------------------

    def win_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.wins / self.total, 6)

    def avg_earnings(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.earnings_sum / self.total, 2)

    def distance_wr(self, bucket: str) -> Optional[float]:
        d = self.per_distance.get(bucket)
        if d is None or d["total"] == 0:
            return None
        return round(d["wins"] / d["total"], 6)

    def terrain_wr(self, terrain: str) -> Optional[float]:
        t = self.per_terrain.get(terrain)
        if t is None or t["total"] == 0:
            return None
        return round(t["wins"] / t["total"], 6)

    def class_level(self) -> Optional[float]:
        if not self.win_allocations:
            return None
        return round(sum(self.win_allocations) / len(self.win_allocations), 2)

    def is_hot(self) -> Optional[int]:
        """1 if recent win rate > overall win rate, else 0. None if no data."""
        overall = self.win_rate()
        if overall is None or len(self.recent) == 0:
            return None
        recent_wr = sum(self.recent) / len(self.recent)
        return int(recent_wr > overall)

    def consistency(self) -> Optional[float]:
        sd = _stdev(self.positions)
        if sd is None:
            return None
        return round(sd, 6)

    # ------------------------------------------------------------------
    # Update (call AFTER emitting features for the current race)
    # ------------------------------------------------------------------

    def update(
        self,
        is_winner: bool,
        earnings: Optional[float],
        bucket: str,
        terrain: str,
        allocation: Optional[float],
        pos_pct: Optional[float],
    ) -> None:
        self.total += 1
        if is_winner:
            self.wins += 1
        if earnings is not None:
            self.earnings_sum += earnings

        self.per_distance[bucket]["total"] += 1
        if is_winner:
            self.per_distance[bucket]["wins"] += 1

        self.per_terrain[terrain]["total"] += 1
        if is_winner:
            self.per_terrain[terrain]["wins"] += 1

        if is_winner and allocation is not None:
            if len(self.win_allocations) < _MAX_WIN_ALLOCS:
                self.win_allocations.append(allocation)

        self.recent.append(is_winner)

        if pos_pct is not None:
            self.positions.append(pos_pct)


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file one line at a time (streaming)."""
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
# MAIN BUILD
# ===========================================================================


def build_stallion_stats(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build stallion sire-statistics features from partants_master."""
    logger.info("=== Stallion Stats Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1 : Read minimal fields from disk (streaming)
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid":          rec.get("partant_uid"),
            "date":         rec.get("date_reunion_iso", ""),
            "course":       rec.get("course_uid", ""),
            "num":          rec.get("num_pmu", 0) or 0,
            "pere":         rec.get("nom_pere"),
            "gagnant":      bool(rec.get("is_gagnant")),
            "distance":     rec.get("distance"),
            "terrain":      rec.get("etat_terrain") or "inconnu",
            "allocation":   _safe_float(rec.get("allocation")),
            "pos_arrivee":  rec.get("position_arrivee"),
            "n_partants":   rec.get("nombre_partants"),
            "earnings":     _safe_float(rec.get("gains_carriere_euros")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0
    )

    # ------------------------------------------------------------------
    # Phase 2 : Sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3 : Process course by course -- emit features BEFORE update
    # ------------------------------------------------------------------
    t2 = time.time()
    sire_states: dict[str, _SireState] = defaultdict(_SireState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)
    i = 0

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all runners for this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # Deferred updates: compute after emitting all features for the race
        deferred: list[tuple] = []

        for rec in course_group:
            pere = rec["pere"]
            bucket = _distance_bucket(rec["distance"])
            terrain = str(rec["terrain"]) if rec["terrain"] else "inconnu"
            pos_pct = _position_pct(rec["pos_arrivee"], rec["n_partants"])

            if pere:
                st = sire_states[pere]

                feat_win_rate    = st.win_rate()
                feat_runners     = st.total if st.total > 0 else None
                feat_avg_earn    = st.avg_earnings()
                feat_dist_wr     = st.distance_wr(bucket)
                feat_terrain_wr  = st.terrain_wr(terrain)
                feat_class_lvl   = st.class_level()
                feat_hot         = st.is_hot()
                feat_consistency = st.consistency()
            else:
                # Unknown sire: emit all nulls
                feat_win_rate    = None
                feat_runners     = None
                feat_avg_earn    = None
                feat_dist_wr     = None
                feat_terrain_wr  = None
                feat_class_lvl   = None
                feat_hot         = None
                feat_consistency = None

            results.append({
                "partant_uid":         rec["uid"],
                "ss_sire_win_rate":    feat_win_rate,
                "ss_sire_runners":     feat_runners,
                "ss_sire_avg_earnings": feat_avg_earn,
                "ss_sire_distance_wr": feat_dist_wr,
                "ss_sire_terrain_wr":  feat_terrain_wr,
                "ss_sire_class_level": feat_class_lvl,
                "ss_sire_hot":         feat_hot,
                "ss_sire_consistency": feat_consistency,
            })

            deferred.append((
                pere,
                rec["gagnant"],
                rec["earnings"],
                bucket,
                terrain,
                rec["allocation"],
                pos_pct,
            ))

        # Update states after emitting all features (no leakage)
        for (pere, is_winner, earnings, bucket, terrain, allocation, pos_pct) in deferred:
            if not pere:
                continue
            sire_states[pere].update(
                is_winner=is_winner,
                earnings=earnings,
                bucket=bucket,
                terrain=terrain,
                allocation=allocation,
                pos_pct=pos_pct,
            )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Stallion stats build termine: %d features en %.1fs (etalons: %d)",
        len(results),
        elapsed,
        len(sire_states),
    )

    # Free memory before returning
    del slim_records, sire_states
    gc.collect()

    return results


# ===========================================================================
# SAVE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI arg or fallback candidates."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in _INPUT_CANDIDATES]}"
    )


def _save_jsonl(records: list[dict], path: Path, logger) -> None:
    """Write records to a JSONL file, one JSON object per line."""
    try:
        from utils.output import save_jsonl as _util_save
        _util_save(records, path, logger)
        return
    except ImportError:
        pass
    logger.info("Sauvegarde: %s (%d records)", path, len(records))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info("Sauvegarde terminee: %s", path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features stallion-stats a partir de partants_master"
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
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("stallion_stats_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_stallion_stats(input_path, logger)

    out_path = output_dir / "stallion_stats.jsonl"
    _save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", k, filled, total, 100 * filled / total)


if __name__ == "__main__":
    main()
