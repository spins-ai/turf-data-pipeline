#!/usr/bin/env python3
"""
feature_builders.cross_discipline_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-discipline features -- how horses perform when switching between
disciplines (Plat, Trot attele, Trot monte, Haies, Steeple, Cross).

Reads partants_master.jsonl in streaming mode (index + chronological sort
+ seek).  Tracks per-horse discipline history to detect switches and
measure discipline-specific performance.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the state -- no future leakage.  Snapshot is taken BEFORE
the state is updated with the current race result.

Produces:
  - cross_discipline.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cross_discipline/

Features per partant (8):
  - crd_nb_disciplines_tried      : number of distinct disciplines horse has run in
  - crd_is_specialist             : 1 if >80% of races in one discipline
  - crd_discipline_switch         : 1 if current discipline differs from last race
  - crd_current_discipline_wr     : horse's win rate in the current discipline
  - crd_best_discipline           : discipline with best win rate (encoded:
                                    Plat=0, Trot_attele=1, Trot_monte=2,
                                    Haies=3, Steeple=4, Cross=5)
  - crd_is_best_discipline        : 1 if current discipline matches best
  - crd_switch_success_rate       : win rate when switching disciplines vs staying
  - crd_discipline_experience_ratio : races in current discipline / total races

Usage:
    python feature_builders/cross_discipline_builder.py
    python feature_builders/cross_discipline_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cross_discipline")

# Progress / gc every N records
_LOG_EVERY = 500_000

# Specialist threshold: >80% of races in one discipline
_SPECIALIST_THRESHOLD = 0.80

# Discipline encoding map
_DISCIPLINE_ENCODING: dict[str, int] = {
    "plat": 0,
    "trot_attele": 1,
    "trot attele": 1,
    "attele": 1,
    "trot_monte": 2,
    "trot monte": 2,
    "monte": 2,
    "haies": 3,
    "steeple": 4,
    "steeple-chase": 4,
    "cross": 5,
    "cross-country": 5,
}


# ===========================================================================
# HELPERS
# ===========================================================================


def _norm_discipline(raw: Any) -> Optional[str]:
    """Normalise discipline string to a canonical key.

    Returns a lowercase key suitable for dict lookups, or None if unknown.
    """
    if not raw or not isinstance(raw, str):
        return None
    key = raw.strip().lower().replace("-", "_").replace("  ", " ")
    # Map known variants
    if key in _DISCIPLINE_ENCODING:
        return key
    # Try partial matching
    for canon in _DISCIPLINE_ENCODING:
        if canon in key or key in canon:
            return canon
    return key  # keep raw normalised even if not in our encoding map


def _encode_discipline(disc: Optional[str]) -> Optional[int]:
    """Encode discipline to integer, or None if unknown."""
    if disc is None:
        return None
    return _DISCIPLINE_ENCODING.get(disc)


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Lightweight per-horse discipline tracker.

    Uses __slots__ to minimise memory across ~200K+ horses.

    State:
      discipline_stats : {discipline_key -> [wins, total]}
      last_discipline  : discipline of the most recent race
      switch_wins      : wins when switching discipline
      switch_total     : total races when switching discipline
      stay_wins        : wins when staying in same discipline
      stay_total       : total races when staying in same discipline
      disciplines_set  : set of distinct disciplines tried
    """

    __slots__ = (
        "discipline_stats",
        "last_discipline",
        "switch_wins",
        "switch_total",
        "stay_wins",
        "stay_total",
        "disciplines_set",
    )

    def __init__(self) -> None:
        self.discipline_stats: dict[str, list[int]] = {}  # disc -> [wins, total]
        self.last_discipline: Optional[str] = None
        self.switch_wins: int = 0
        self.switch_total: int = 0
        self.stay_wins: int = 0
        self.stay_total: int = 0
        self.disciplines_set: set[str] = set()

    @property
    def total_races(self) -> int:
        return sum(wt[1] for wt in self.discipline_stats.values())


# ===========================================================================
# FEATURE COMPUTATION (snapshot BEFORE update)
# ===========================================================================


def _compute_features(
    state: _HorseState,
    current_discipline: Optional[str],
) -> dict[str, Any]:
    """Compute 8 cross-discipline features from horse state snapshot.

    All values are based on state BEFORE this race (temporal integrity).
    """
    feats: dict[str, Any] = {}
    total = state.total_races

    # 1. crd_nb_disciplines_tried
    feats["crd_nb_disciplines_tried"] = len(state.disciplines_set) if total > 0 else None

    # 2. crd_is_specialist
    if total > 0:
        max_pct = max(wt[1] / total for wt in state.discipline_stats.values())
        feats["crd_is_specialist"] = 1 if max_pct > _SPECIALIST_THRESHOLD else 0
    else:
        feats["crd_is_specialist"] = None

    # 3. crd_discipline_switch
    if current_discipline is not None and state.last_discipline is not None:
        feats["crd_discipline_switch"] = 1 if current_discipline != state.last_discipline else 0
    else:
        feats["crd_discipline_switch"] = None

    # 4. crd_current_discipline_wr
    if current_discipline is not None and current_discipline in state.discipline_stats:
        wins, runs = state.discipline_stats[current_discipline]
        feats["crd_current_discipline_wr"] = round(wins / runs, 4) if runs > 0 else None
    else:
        feats["crd_current_discipline_wr"] = None

    # 5. crd_best_discipline (encoded)
    if total > 0:
        best_disc = None
        best_wr = -1.0
        for disc, (wins, runs) in state.discipline_stats.items():
            if runs > 0:
                wr = wins / runs
                if wr > best_wr or (wr == best_wr and runs > state.discipline_stats.get(best_disc, [0, 0])[1]):
                    best_wr = wr
                    best_disc = disc
        feats["crd_best_discipline"] = _encode_discipline(best_disc)
    else:
        feats["crd_best_discipline"] = None

    # 6. crd_is_best_discipline
    if current_discipline is not None and total > 0 and feats["crd_best_discipline"] is not None:
        current_encoded = _encode_discipline(current_discipline)
        feats["crd_is_best_discipline"] = 1 if current_encoded == feats["crd_best_discipline"] else 0
    else:
        feats["crd_is_best_discipline"] = None

    # 7. crd_switch_success_rate
    # Win rate when switching vs staying -- expressed as switch win rate
    if state.switch_total > 0:
        feats["crd_switch_success_rate"] = round(state.switch_wins / state.switch_total, 4)
    else:
        feats["crd_switch_success_rate"] = None

    # 8. crd_discipline_experience_ratio
    if current_discipline is not None and total > 0:
        disc_total = state.discipline_stats.get(current_discipline, [0, 0])[1]
        feats["crd_discipline_experience_ratio"] = round(disc_total / total, 4)
    else:
        feats["crd_discipline_experience_ratio"] = None

    return feats


# ===========================================================================
# STATE UPDATE (after snapshot)
# ===========================================================================


def _update_state(
    state: _HorseState,
    discipline: Optional[str],
    is_winner: bool,
) -> None:
    """Update horse state after this race."""
    if discipline is None:
        return

    # Track switch vs stay (only if we have a previous discipline)
    if state.last_discipline is not None:
        if discipline != state.last_discipline:
            state.switch_total += 1
            if is_winner:
                state.switch_wins += 1
        else:
            state.stay_total += 1
            if is_winner:
                state.stay_wins += 1

    # Update discipline stats
    if discipline not in state.discipline_stats:
        state.discipline_stats[discipline] = [0, 0]
    state.discipline_stats[discipline][1] += 1
    if is_winner:
        state.discipline_stats[discipline][0] += 1

    # Update disciplines set
    state.disciplines_set.add(discipline)

    # Update last discipline
    state.last_discipline = discipline


# ===========================================================================
# MAIN BUILD (index + sort + seek)
# ===========================================================================


def build_cross_discipline_features(input_path: Path, output_path: Path, logger) -> int:
    """Build cross-discipline features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets (not full records).
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process per-horse,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Cross Discipline Builder (index + sort + seek) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    index: list[tuple[str, str, int, int]] = []  # (date, course_uid, num_pmu, offset)
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

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process record by record, streaming output --
    t2 = time.time()
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "crd_nb_disciplines_tried",
        "crd_is_specialist",
        "crd_discipline_switch",
        "crd_current_discipline_wr",
        "crd_best_discipline",
        "crd_is_best_discipline",
        "crd_switch_success_rate",
        "crd_discipline_experience_ratio",
    ]
    fill_counts = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

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

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Snapshot BEFORE update (temporal integrity) --
            snapshots: list[tuple[Optional[str], Optional[str], bool]] = []
            # Store (cheval, discipline, is_winner) for deferred update

            for rec in course_records:
                cheval = rec.get("nom_cheval") or ""
                partant_uid = rec.get("partant_uid") or ""
                course_uid_rec = rec.get("course_uid") or ""
                date_iso = rec.get("date_reunion_iso") or ""

                # Extract discipline
                raw_disc = rec.get("discipline") or rec.get("specialite") or ""
                discipline = _norm_discipline(raw_disc)

                is_winner = bool(rec.get("is_gagnant"))

                if not cheval:
                    # No horse name => emit empty features
                    out_rec: dict[str, Any] = {
                        "partant_uid": partant_uid,
                        "course_uid": course_uid_rec,
                        "date_reunion_iso": date_iso,
                    }
                    for fn in feature_names:
                        out_rec[fn] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                    snapshots.append((None, None, False))
                    continue

                state = horse_states[cheval]

                # Compute features from PRE-RACE state
                feats = _compute_features(state, discipline)

                # Write output
                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_rec,
                    "date_reunion_iso": date_iso,
                }
                for fn in feature_names:
                    val = feats.get(fn)
                    out_rec[fn] = val
                    if val is not None:
                        fill_counts[fn] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Store info needed for state update
                snapshots.append((cheval, discipline, is_winner))

            # -- Update states AFTER all snapshots for this course --
            for cheval, discipline, is_winner in snapshots:
                if cheval is None:
                    continue
                state = horse_states[cheval]
                _update_state(state, discipline, is_winner)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Cross discipline build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
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
        description="Cross discipline: features changement de discipline"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/cross_discipline/)",
    )
    args = parser.parse_args()

    logger = setup_logging("cross_discipline_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "cross_discipline.jsonl"
    build_cross_discipline_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
