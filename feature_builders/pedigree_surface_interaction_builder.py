#!/usr/bin/env python3
"""
feature_builders.pedigree_surface_interaction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Interaction features between pedigree and surface/distance/conditions.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant pedigree x surface interaction
features using snapshot-before-update temporal integrity.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - pedigree_surface_interaction.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_surface_interaction/

Features per partant (11):
  - psi_sire_surface_wr        : sire win rate on this surface type (pere x type_piste)
  - psi_sire_dist_bucket_wr    : sire win rate at this distance bucket (pere x distance)
  - psi_sire_discipline_wr     : sire win rate in this discipline (pere x discipline)
  - psi_damsire_surface_wr     : dam's sire win rate on this surface (pere_mere x type_piste)
  - psi_sire_psf_avg_pos       : sire progeny avg position on synthetic (PSF) tracks
  - psi_speed_idx_x_dist       : pedigree speed index x actual distance interaction
  - psi_stamina_idx_x_dist     : pedigree stamina index x actual distance interaction
  - psi_inbreed_x_surface_perf : inbreeding score x surface performance ratio
  - psi_sire_hippo_wr          : sire performance at this specific hippodrome
  - psi_dam_dist_bucket_wr     : dam lineage performance at distance buckets
  - psi_sire_disc_dist_wr      : sire x discipline x distance triple interaction

Distance buckets:
  sprint (<1400m), mile (1400-1800m), intermediate (1800-2200m),
  staying (2200-2800m), long (>2800m)

Usage:
    python feature_builders/pedigree_surface_interaction_builder.py
    python feature_builders/pedigree_surface_interaction_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_surface_interaction")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
_OUTPUT_FALLBACK = _PROJECT_ROOT / "output" / "pedigree_surface_interaction"

_LOG_EVERY = 500_000
_MIN_RACES = 5  # minimum races for a stat to be considered reliable

# Bayesian prior for sire/dam win-rate shrinkage
_PRIOR_WEIGHT = 10
_GLOBAL_WIN_RATE = 0.08  # ~8% baseline


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _dist_bucket(dist: Any) -> Optional[str]:
    """Map raw distance (m) to a bucket label."""
    d = _safe_int(dist)
    if d is None:
        return None
    if d < 1400:
        return "sprint"
    elif d < 1800:
        return "mile"
    elif d < 2200:
        return "intermediate"
    elif d < 2800:
        return "staying"
    else:
        return "long"


def _dist_bucket_midpoint(bucket: Optional[str]) -> Optional[float]:
    """Return midpoint distance (m) for a bucket, for interaction features."""
    mapping = {
        "sprint": 1200.0,
        "mile": 1600.0,
        "intermediate": 2000.0,
        "staying": 2500.0,
        "long": 3200.0,
    }
    if bucket is None:
        return None
    return mapping.get(bucket)


def _bayes_rate(wins: int, total: int) -> Optional[float]:
    """Bayesian-shrinkage win rate toward global average."""
    if total < 1:
        return None
    return round(
        (_GLOBAL_WIN_RATE * _PRIOR_WEIGHT + wins) / (_PRIOR_WEIGHT + total), 4
    )


def _raw_rate(wins: int, total: int) -> Optional[float]:
    """Raw win rate. Returns None if total < _MIN_RACES."""
    if total < _MIN_RACES:
        return None
    return round(wins / total, 4)


def _resolve_input(cli_arg: Optional[str]) -> Path:
    if cli_arg:
        p = Path(cli_arg)
        if p.exists():
            return p
        raise FileNotFoundError(f"Input file not found: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"No input file found among default candidates. Pass --input explicitly.\n"
        f"Tried: {_INPUT_CANDIDATES}"
    )


def _resolve_output(cli_arg: Optional[str]) -> Path:
    if cli_arg:
        return Path(cli_arg)
    if OUTPUT_DIR.parent.exists():
        return OUTPUT_DIR
    return _OUTPUT_FALLBACK


# ===========================================================================
# STAT TRACKER — compact win/total counters
# ===========================================================================


class _WinCounter:
    """Compact wins/total counter with Bayesian and raw rate methods."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def bayes_wr(self) -> Optional[float]:
        return _bayes_rate(self.wins, self.total)

    def raw_wr(self) -> Optional[float]:
        return _raw_rate(self.wins, self.total)

    def update(self, is_win: bool) -> None:
        self.total += 1
        if is_win:
            self.wins += 1


class _AvgPositionCounter:
    """Tracks sum of positions and count for average calculation."""

    __slots__ = ("pos_sum", "total")

    def __init__(self) -> None:
        self.pos_sum: float = 0.0
        self.total: int = 0

    def avg_pos(self) -> Optional[float]:
        if self.total < _MIN_RACES:
            return None
        return round(self.pos_sum / self.total, 2)

    def update(self, position: int) -> None:
        self.pos_sum += position
        self.total += 1


# ===========================================================================
# STATE CONTAINER
# ===========================================================================


class _PedigreeState:
    """All pedigree x context interaction state.

    Keys use tuples/strings to avoid dict nesting overhead.
    Uses defaultdict for automatic initialization.
    """

    def __init__(self) -> None:
        # 1. Sire x surface: key = (pere, type_piste)
        self.sire_surface: dict[tuple[str, str], _WinCounter] = defaultdict(_WinCounter)

        # 2. Sire x distance bucket: key = (pere, dist_bucket)
        self.sire_dist: dict[tuple[str, str], _WinCounter] = defaultdict(_WinCounter)

        # 3. Sire x discipline: key = (pere, discipline)
        self.sire_disc: dict[tuple[str, str], _WinCounter] = defaultdict(_WinCounter)

        # 4. Dam's sire x surface: key = (pere_mere, type_piste)
        self.damsire_surface: dict[tuple[str, str], _WinCounter] = defaultdict(_WinCounter)

        # 5. Sire on PSF (synthetic): key = pere
        self.sire_psf_pos: dict[str, _AvgPositionCounter] = defaultdict(_AvgPositionCounter)

        # 6/7. Sire overall (for speed/stamina interaction baseline): key = pere
        self.sire_overall: dict[str, _WinCounter] = defaultdict(_WinCounter)

        # 8. Sire x surface (for inbreeding interaction): key = (pere, type_piste)
        #    Reuses sire_surface above

        # 9. Sire x hippodrome: key = (pere, hippodrome)
        self.sire_hippo: dict[tuple[str, str], _WinCounter] = defaultdict(_WinCounter)

        # 10. Dam x distance bucket: key = (mere, dist_bucket)
        self.dam_dist: dict[tuple[str, str], _WinCounter] = defaultdict(_WinCounter)

        # 11. Sire x discipline x distance: key = (pere, discipline, dist_bucket)
        self.sire_disc_dist: dict[tuple[str, str, str], _WinCounter] = defaultdict(_WinCounter)

    def snapshot(
        self,
        pere: Optional[str],
        mere: Optional[str],
        pere_mere: Optional[str],
        type_piste: Optional[str],
        dist_bucket: Optional[str],
        discipline: Optional[str],
        hippodrome: Optional[str],
        is_psf: bool,
        distance: Optional[int],
        ped_speed_idx: Optional[float],
        ped_stamina_idx: Optional[float],
        ped_inbreeding: Optional[float],
    ) -> dict[str, Any]:
        """Read all features from current state (BEFORE update)."""
        feats: dict[str, Any] = {
            "psi_sire_surface_wr": None,
            "psi_sire_dist_bucket_wr": None,
            "psi_sire_discipline_wr": None,
            "psi_damsire_surface_wr": None,
            "psi_sire_psf_avg_pos": None,
            "psi_speed_idx_x_dist": None,
            "psi_stamina_idx_x_dist": None,
            "psi_inbreed_x_surface_perf": None,
            "psi_sire_hippo_wr": None,
            "psi_dam_dist_bucket_wr": None,
            "psi_sire_disc_dist_wr": None,
        }

        # 1. Sire x surface win rate
        if pere and type_piste:
            key = (pere, type_piste)
            if key in self.sire_surface:
                feats["psi_sire_surface_wr"] = self.sire_surface[key].bayes_wr()

        # 2. Sire x distance bucket win rate
        if pere and dist_bucket:
            key = (pere, dist_bucket)
            if key in self.sire_dist:
                feats["psi_sire_dist_bucket_wr"] = self.sire_dist[key].bayes_wr()

        # 3. Sire x discipline win rate
        if pere and discipline:
            key = (pere, discipline)
            if key in self.sire_disc:
                feats["psi_sire_discipline_wr"] = self.sire_disc[key].bayes_wr()

        # 4. Dam's sire x surface win rate
        if pere_mere and type_piste:
            key = (pere_mere, type_piste)
            if key in self.damsire_surface:
                feats["psi_damsire_surface_wr"] = self.damsire_surface[key].bayes_wr()

        # 5. Sire progeny avg position on PSF
        if pere and is_psf:
            if pere in self.sire_psf_pos:
                feats["psi_sire_psf_avg_pos"] = self.sire_psf_pos[pere].avg_pos()

        # 6. Speed index x distance interaction
        #    = ped_speed_index * (1 - distance/3500)
        #    High speed index benefits more at shorter distances
        if ped_speed_idx is not None and distance is not None and distance > 0:
            # Normalize distance to [0, 1] range (0=sprint, 1=ultra long)
            dist_norm = min(distance / 3500.0, 1.0)
            # Speed matters more at short distances (inverse relationship)
            feats["psi_speed_idx_x_dist"] = round(ped_speed_idx * (1.0 - dist_norm), 4)

        # 7. Stamina index x distance interaction
        #    = ped_stamina_index * (distance/3500)
        #    High stamina index benefits more at longer distances
        if ped_stamina_idx is not None and distance is not None and distance > 0:
            dist_norm = min(distance / 3500.0, 1.0)
            feats["psi_stamina_idx_x_dist"] = round(ped_stamina_idx * dist_norm, 4)

        # 8. Inbreeding x surface performance
        #    = ped_inbreeding_score * sire_surface_wr (amplifies/dampens inbreeding signal)
        if ped_inbreeding is not None and pere and type_piste:
            key = (pere, type_piste)
            if key in self.sire_surface:
                sire_surf_wr = self.sire_surface[key].raw_wr()
                if sire_surf_wr is not None:
                    feats["psi_inbreed_x_surface_perf"] = round(
                        ped_inbreeding * sire_surf_wr, 6
                    )

        # 9. Sire x hippodrome win rate
        if pere and hippodrome:
            key = (pere, hippodrome)
            if key in self.sire_hippo:
                feats["psi_sire_hippo_wr"] = self.sire_hippo[key].bayes_wr()

        # 10. Dam x distance bucket win rate
        if mere and dist_bucket:
            key = (mere, dist_bucket)
            if key in self.dam_dist:
                feats["psi_dam_dist_bucket_wr"] = self.dam_dist[key].bayes_wr()

        # 11. Sire x discipline x distance triple interaction
        if pere and discipline and dist_bucket:
            key = (pere, discipline, dist_bucket)
            if key in self.sire_disc_dist:
                feats["psi_sire_disc_dist_wr"] = self.sire_disc_dist[key].bayes_wr()

        return feats

    def update(
        self,
        pere: Optional[str],
        mere: Optional[str],
        pere_mere: Optional[str],
        type_piste: Optional[str],
        dist_bucket: Optional[str],
        discipline: Optional[str],
        hippodrome: Optional[str],
        is_psf: bool,
        is_win: bool,
        position: Optional[int],
    ) -> None:
        """Update all counters AFTER features have been read."""
        # 1. Sire x surface
        if pere and type_piste:
            self.sire_surface[(pere, type_piste)].update(is_win)

        # 2. Sire x distance bucket
        if pere and dist_bucket:
            self.sire_dist[(pere, dist_bucket)].update(is_win)

        # 3. Sire x discipline
        if pere and discipline:
            self.sire_disc[(pere, discipline)].update(is_win)

        # 4. Dam's sire x surface
        if pere_mere and type_piste:
            self.damsire_surface[(pere_mere, type_piste)].update(is_win)

        # 5. Sire on PSF (synthetic) -- track avg position
        if pere and is_psf and position is not None and position > 0:
            self.sire_psf_pos[pere].update(position)

        # 6/7. Sire overall (used for interaction baselines)
        if pere:
            self.sire_overall[pere].update(is_win)

        # 9. Sire x hippodrome
        if pere and hippodrome:
            self.sire_hippo[(pere, hippodrome)].update(is_win)

        # 10. Dam x distance bucket
        if mere and dist_bucket:
            self.dam_dist[(mere, dist_bucket)].update(is_win)

        # 11. Sire x discipline x distance
        if pere and discipline and dist_bucket:
            self.sire_disc_dist[(pere, discipline, dist_bucket)].update(is_win)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_pedigree_surface_features(input_path: Path, output_dir: Path) -> None:
    logger = setup_logging("pedigree_surface_interaction_builder")
    logger.info("Input : %s", input_path)
    logger.info("Output: %s", output_dir)

    # -- Pass 1: load minimal fields + sort -----------------------------------
    logger.info("Pass 1: loading records...")
    t0 = time.time()
    records: list[dict[str, Any]] = []
    n_read = 0
    n_errors = 0

    with open(input_path, "r", encoding="utf-8") as fh:
        for line in fh:
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
                logger.info("  loaded %d records...", n_read)

            # Keep only fields we need to save RAM
            records.append({
                "uid": rec.get("partant_uid"),
                "date": str(rec.get("date_reunion_iso", "") or "")[:10],
                "course": rec.get("course_uid", ""),
                "num": _safe_int(rec.get("num_pmu")) or 0,
                "pere": (rec.get("pere") or "").strip().upper() or None,
                "mere": (rec.get("mere") or "").strip().upper() or None,
                "pere_mere": (rec.get("pere_mere") or "").strip().upper() or None,
                "type_piste": (rec.get("type_piste") or "").strip().lower() or None,
                "distance": _safe_int(rec.get("distance")),
                "discipline": (rec.get("discipline") or "").strip().lower() or None,
                "hippo": (rec.get("hippodrome_normalise") or "").strip().lower() or None,
                "is_psf": bool(rec.get("met_is_psf")),
                "is_gagnant": bool(rec.get("is_gagnant")),
                "position": _safe_int(rec.get("position_arrivee")),
                "speed_idx": _safe_float(rec.get("ped_speed_index")),
                "stamina_idx": _safe_float(rec.get("ped_stamina_index")),
                "inbreeding": _safe_float(rec.get("ped_inbreeding_score")),
            })

            if n_read % _LOG_EVERY == 0:
                gc.collect()

    logger.info(
        "Pass 1 done: %d records in %.1fs (%d JSON errors)",
        len(records), time.time() - t0, n_errors,
    )

    # Sort chronologically
    logger.info("Sorting chronologically...")
    t1 = time.time()
    records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Sorted in %.1fs", time.time() - t1)

    gc.collect()

    # -- Pass 2: compute features (course-by-course, snapshot-before-update) --
    logger.info("Pass 2: computing pedigree x surface interaction features...")
    t2 = time.time()

    state = _PedigreeState()
    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(records)

    while i < total:
        # Collect all records for this course
        course_uid = records[i]["course"]
        course_date = records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and records[i]["course"] == course_uid
            and records[i]["date"] == course_date
        ):
            course_group.append(records[i])
            i += 1

        # Phase A: snapshot features BEFORE update (temporal integrity)
        course_features: list[dict[str, Any]] = []
        for rec in course_group:
            dist_bucket = _dist_bucket(rec["distance"])

            feats: dict[str, Any] = {"partant_uid": rec["uid"]}
            feats.update(
                state.snapshot(
                    pere=rec["pere"],
                    mere=rec["mere"],
                    pere_mere=rec["pere_mere"],
                    type_piste=rec["type_piste"],
                    dist_bucket=dist_bucket,
                    discipline=rec["discipline"],
                    hippodrome=rec["hippo"],
                    is_psf=rec["is_psf"],
                    distance=rec["distance"],
                    ped_speed_idx=rec["speed_idx"],
                    ped_stamina_idx=rec["stamina_idx"],
                    ped_inbreeding=rec["inbreeding"],
                )
            )
            course_features.append(feats)

        # Phase B: update state AFTER all snapshots for this course
        for rec in course_group:
            dist_bucket = _dist_bucket(rec["distance"])
            state.update(
                pere=rec["pere"],
                mere=rec["mere"],
                pere_mere=rec["pere_mere"],
                type_piste=rec["type_piste"],
                dist_bucket=dist_bucket,
                discipline=rec["discipline"],
                hippodrome=rec["hippo"],
                is_psf=rec["is_psf"],
                is_win=rec["is_gagnant"],
                position=rec["position"],
            )

        results.extend(course_features)
        n_processed += len(course_group)

        if n_processed % _LOG_EVERY == 0 and n_processed > 0:
            logger.info("  processed %d / %d records...", n_processed, total)
            gc.collect()

    elapsed = time.time() - t2
    logger.info(
        "Pass 2 done: %d feature records in %.1fs", len(results), elapsed
    )

    # -- Save (atomic write via save_jsonl: .tmp then rename) -----------------
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "pedigree_surface_interaction.jsonl"

    logger.info("Saving to %s ...", out_path)
    save_jsonl(results, out_path, logger)

    # -- Fill-rate report -----------------------------------------------------
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total_count = len(results)
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %s: %d/%d (%.1f%%)",
                k, v, total_count, 100.0 * v / total_count,
            )

    # -- Stats summary --------------------------------------------------------
    n_sires = len(state.sire_overall)
    n_sire_surface = len(state.sire_surface)
    n_sire_dist = len(state.sire_dist)
    n_sire_disc = len(state.sire_disc)
    n_damsire_surface = len(state.damsire_surface)
    n_sire_hippo = len(state.sire_hippo)
    n_dam_dist = len(state.dam_dist)
    n_triple = len(state.sire_disc_dist)

    logger.info("=== State stats ===")
    logger.info("  Unique sires tracked: %d", n_sires)
    logger.info("  Sire x surface combos: %d", n_sire_surface)
    logger.info("  Sire x distance combos: %d", n_sire_dist)
    logger.info("  Sire x discipline combos: %d", n_sire_disc)
    logger.info("  Dam-sire x surface combos: %d", n_damsire_surface)
    logger.info("  Sire x hippodrome combos: %d", n_sire_hippo)
    logger.info("  Dam x distance combos: %d", n_dam_dist)
    logger.info("  Sire x disc x dist triples: %d", n_triple)

    # Free memory
    del records, results, state
    gc.collect()
    logger.info("Done.")


# ===========================================================================
# CLI
# ===========================================================================


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build pedigree x surface interaction features for each partant."
    )
    parser.add_argument(
        "--input",
        metavar="PATH",
        default=None,
        help="Path to partants_master.jsonl (default: auto-detected).",
    )
    parser.add_argument(
        "--output",
        metavar="DIR",
        default=None,
        help="Output directory (default: auto-detected).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    input_path = _resolve_input(args.input)
    output_dir = _resolve_output(args.output)
    build_pedigree_surface_features(input_path, output_dir)


if __name__ == "__main__":
    main()
