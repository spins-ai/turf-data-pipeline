#!/usr/bin/env python3
"""
feature_builders.musique_discipline_breakdown_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Parse the ``musique`` field per-discipline to extract discipline-specific
performance features.

Temporal integrity: musique is a point-in-time snapshot provided by the
source at race-time -- it describes past performance only, no future leakage.
Single-pass streaming: no temporal state needed.

Produces:
  - musique_discipline_breakdown_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/
       musique_discipline_breakdown/

Features per partant (10):
  - mdb_current_disc_count   : nb of recent races in the CURRENT discipline
  - mdb_current_disc_win_rate: win rate in the current discipline from musique
  - mdb_current_disc_avg_pos : average finishing position in current discipline
  - mdb_cross_disc_count     : nb of different disciplines seen in musique
  - mdb_discipline_switch    : 1 if last race was in a different discipline
  - mdb_plat_count           : nb of flat (p) races in musique
  - mdb_trot_count           : nb of trot (a+m) races in musique
  - mdb_obstacle_count       : nb of obstacle (o+c+h) races in musique
  - mdb_best_disc_vs_current : 1 if current discipline is the horse's best (by win rate)
  - mdb_disc_consistency     : proportion of musique races in the dominant discipline

Musique discipline letters:
  a = attelé  (trot harness)
  m = monté   (trot mounted)
  p = plat    (flat gallop)
  o = obstacle
  c = cross/steeple
  h = haies   (hurdles)

Usage:
    python feature_builders/musique_discipline_breakdown_builder.py
    python feature_builders/musique_discipline_breakdown_builder.py \\
        --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_DEFAULT_INPUT = Path(
    "D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"
)
_DEFAULT_OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/"
    "musique_discipline_breakdown"
)
_OUTPUT_FILENAME = "musique_discipline_breakdown_features.jsonl"

_LOG_EVERY = 500_000

# Discipline letter groupings
_DISC_TROT = {"a", "m"}
_DISC_PLAT = {"p"}
_DISC_OBSTACLE = {"o", "c", "h"}
_ALL_DISC_LETTERS = _DISC_TROT | _DISC_PLAT | _DISC_OBSTACLE

# Characters that represent non-finishing outcomes (DNF-like)
_DNF_CHARS = {"D", "T", "A", "R"}

# Disciplines sourced from specialite / discipline fields mapped to musique letters
_SPECIALITE_TO_DISC: dict[str, str] = {
    # PMU specialite codes
    "ATTELE": "a",
    "MONTE": "m",
    "PLAT": "p",
    "OBSTACLE": "o",
    "HAIES": "h",
    "STEEPLE": "c",
    "CROSS": "c",
    # Lowercase variants
    "attele": "a",
    "monte": "m",
    "plat": "p",
    "obstacle": "o",
    "haies": "h",
    "steeple": "c",
    "cross": "c",
    # Short codes
    "A": "a",
    "M": "m",
    "P": "p",
    "O": "o",
    "H": "h",
    "C": "c",
}


# ===========================================================================
# MUSIQUE PARSING
# ===========================================================================


def _parse_musique_with_disc(
    musique: Optional[str],
) -> list[tuple[Optional[int], str]]:
    """Parse a musique string into a list of (position, discipline_letter) tuples.

    Most-recent race is index 0.

    Position encoding:
      - Digit(s) followed by a discipline letter  -> (int_pos, disc_letter)
        Special case: '0' means unplaced (mapped to None, position >= 10)
      - DNF char (D/T/A/R) followed by a discipline letter -> (None, disc_letter)
      - Digit(s) with NO discipline letter -> (int_pos, '') -- older format

    Returns list of up to 20 tokens (most recent first).
    """
    if not musique or not isinstance(musique, str):
        return []

    results: list[tuple[Optional[int], str]] = []
    i = 0
    n = len(musique)

    while i < n and len(results) < 20:
        ch = musique[i]

        # DNF character
        if ch.upper() in _DNF_CHARS:
            i += 1
            # peek at discipline letter
            disc = ""
            if i < n and musique[i].lower() in _ALL_DISC_LETTERS:
                disc = musique[i].lower()
                i += 1
            results.append((None, disc))
            continue

        # Digit(s) -- collect full number
        if ch.isdigit():
            j = i
            while j < n and musique[j].isdigit():
                j += 1
            num_str = musique[i:j]
            i = j
            # peek at discipline letter
            disc = ""
            if i < n and musique[i].lower() in _ALL_DISC_LETTERS:
                disc = musique[i].lower()
                i += 1
            try:
                pos = int(num_str)
                # 0 means unplaced (10+)
                pos_out: Optional[int] = pos if pos > 0 else None
            except ValueError:
                pos_out = None
            results.append((pos_out, disc))
            continue

        # Any other character -- skip
        i += 1

    return results


def _resolve_current_disc(rec: dict) -> Optional[str]:
    """Determine the discipline letter for the current race from the record.

    Checks: specialite, discipline, type_course fields.
    Returns a single lowercase letter or None.
    """
    for field in ("specialite", "discipline", "type_course", "type_pari"):
        val = rec.get(field)
        if val and isinstance(val, str):
            mapped = _SPECIALITE_TO_DISC.get(val.strip())
            if mapped:
                return mapped
            # Try upper
            mapped = _SPECIALITE_TO_DISC.get(val.strip().upper())
            if mapped:
                return mapped
    return None


# ===========================================================================
# FEATURE COMPUTATION (single-pass, per-record)
# ===========================================================================

_FEATURE_KEYS = (
    "mdb_current_disc_count",
    "mdb_current_disc_win_rate",
    "mdb_current_disc_avg_pos",
    "mdb_cross_disc_count",
    "mdb_discipline_switch",
    "mdb_plat_count",
    "mdb_trot_count",
    "mdb_obstacle_count",
    "mdb_best_disc_vs_current",
    "mdb_disc_consistency",
)

_NULL_FEATURES: dict[str, Any] = {k: None for k in _FEATURE_KEYS}


def _compute_features(
    musique: Optional[str],
    current_disc: Optional[str],
) -> dict[str, Any]:
    """Compute all 10 mdb_* features from musique and current discipline."""

    tokens = _parse_musique_with_disc(musique)

    if not tokens:
        return dict(_NULL_FEATURES)

    # ------------------------------------------------------------------
    # Raw counts per discipline group
    # ------------------------------------------------------------------
    plat_count = 0
    trot_count = 0
    obstacle_count = 0

    # Per-discipline-letter stats: wins and numeric positions
    disc_wins: dict[str, int] = {}
    disc_positions: dict[str, list[int]] = {}
    disc_counts: dict[str, int] = {}

    for pos, disc in tokens:
        if disc in _DISC_PLAT:
            plat_count += 1
        elif disc in _DISC_TROT:
            trot_count += 1
        elif disc in _DISC_OBSTACLE:
            obstacle_count += 1

        if disc:
            disc_counts[disc] = disc_counts.get(disc, 0) + 1
            if disc not in disc_wins:
                disc_wins[disc] = 0
                disc_positions[disc] = []
            if pos == 1:
                disc_wins[disc] += 1
            if pos is not None:
                disc_positions[disc].append(pos)

    # Total races
    total_races = len(tokens)

    # ------------------------------------------------------------------
    # Current discipline stats
    # ------------------------------------------------------------------
    current_disc_count: Optional[int] = None
    current_disc_win_rate: Optional[float] = None
    current_disc_avg_pos: Optional[float] = None

    if current_disc:
        c_count = disc_counts.get(current_disc, 0)
        current_disc_count = c_count
        if c_count > 0:
            wins = disc_wins.get(current_disc, 0)
            current_disc_win_rate = round(wins / c_count, 4)
            positions = disc_positions.get(current_disc, [])
            if positions:
                current_disc_avg_pos = round(sum(positions) / len(positions), 3)

    # ------------------------------------------------------------------
    # Cross-discipline count (number of distinct discipline letters seen)
    # ------------------------------------------------------------------
    cross_disc_count = len(disc_counts) if disc_counts else None

    # ------------------------------------------------------------------
    # Discipline switch: last race in a different discipline than current
    # ------------------------------------------------------------------
    discipline_switch: Optional[int] = None
    if current_disc and tokens:
        last_disc = tokens[0][1]  # most recent
        if last_disc:
            discipline_switch = 0 if last_disc == current_disc else 1

    # ------------------------------------------------------------------
    # Best discipline by win rate (min 1 race)
    # ------------------------------------------------------------------
    best_disc_vs_current: Optional[int] = None
    if disc_counts:
        best_disc = None
        best_wr = -1.0
        for d, cnt in disc_counts.items():
            if cnt > 0:
                wr = disc_wins.get(d, 0) / cnt
                if wr > best_wr:
                    best_wr = wr
                    best_disc = d
        if best_disc is not None and current_disc:
            best_disc_vs_current = 1 if best_disc == current_disc else 0

    # ------------------------------------------------------------------
    # Discipline consistency: proportion of races in dominant discipline
    # ------------------------------------------------------------------
    disc_consistency: Optional[float] = None
    if total_races > 0 and disc_counts:
        max_count = max(disc_counts.values())
        disc_consistency = round(max_count / total_races, 4)

    return {
        "mdb_current_disc_count": current_disc_count,
        "mdb_current_disc_win_rate": current_disc_win_rate,
        "mdb_current_disc_avg_pos": current_disc_avg_pos,
        "mdb_cross_disc_count": cross_disc_count,
        "mdb_discipline_switch": discipline_switch,
        "mdb_plat_count": plat_count if plat_count > 0 or tokens else None,
        "mdb_trot_count": trot_count if trot_count > 0 or tokens else None,
        "mdb_obstacle_count": obstacle_count if obstacle_count > 0 or tokens else None,
        "mdb_best_disc_vs_current": best_disc_vs_current,
        "mdb_disc_consistency": disc_consistency,
    }


# ===========================================================================
# HELPERS
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
# MAIN BUILD
# ===========================================================================


def build_musique_discipline_breakdown_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build musique discipline breakdown features from partants_master.jsonl.

    Single-pass streaming: musique is a point-in-time field, so no temporal
    state accumulation is required.
    """
    logger.info("=== Musique Discipline Breakdown Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_no_musique = 0
    n_no_disc = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1

        if n_read % _LOG_EVERY == 0:
            logger.info(
                "  Lu %d records... (sans musique: %d, sans discipline: %d)",
                n_read, n_no_musique, n_no_disc,
            )
            gc.collect()

        musique = rec.get("musique")
        current_disc = _resolve_current_disc(rec)

        if not musique:
            n_no_musique += 1
        if not current_disc:
            n_no_disc += 1

        feats = _compute_features(musique, current_disc)
        feats["partant_uid"] = rec.get("partant_uid")
        feats["course_uid"] = rec.get("course_uid")
        feats["date_reunion_iso"] = rec.get("date_reunion_iso")

        results.append(feats)

    elapsed = time.time() - t0
    logger.info(
        "Musique discipline breakdown build termine: %d features en %.1fs",
        len(results), elapsed,
    )
    logger.info(
        "  Records sans musique: %d / %d (%.1f%%)",
        n_no_musique, n_read, 100 * n_no_musique / max(n_read, 1),
    )
    logger.info(
        "  Records sans discipline: %d / %d (%.1f%%)",
        n_no_disc, n_read, 100 * n_no_disc / max(n_read, 1),
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
    if _DEFAULT_INPUT.exists():
        return _DEFAULT_INPUT
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {_DEFAULT_INPUT}"
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features musique discipline breakdown "
            "a partir de partants_master"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help=(
            "Chemin vers partants_master.jsonl "
            f"(defaut: {_DEFAULT_INPUT})"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help=(
            "Repertoire de sortie "
            f"(defaut: {_DEFAULT_OUTPUT_DIR})"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("musique_discipline_breakdown_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else _DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_musique_discipline_breakdown_features(input_path, logger)

    out_path = output_dir / _OUTPUT_FILENAME
    save_jsonl(results, out_path, logger)

    # Fill rates
    if results:
        feature_keys = [
            k for k in results[0]
            if k not in {"partant_uid", "course_uid", "date_reunion_iso"}
        ]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %s: %d/%d (%.1f%%)",
                k, v, total_count, 100 * v / total_count,
            )

    logger.info("Termine.")


if __name__ == "__main__":
    main()
