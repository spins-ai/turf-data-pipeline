#!/usr/bin/env python3
"""
feature_builders.musique_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Advanced musique (form string) parsing features -- deep extraction from the
musique field.

The musique field encodes a horse's recent race history as a compact string
like "1p3p2p6p0p" or "Da1p4a5a0a" where:
  - Digits  = finishing positions (1=won, 0=unplaced/10+)
  - Letters after digits = discipline (p=plat, a=attele, m=monte, h=haies,
    s=steeple, c=cross)
  - D = disqualified, A = arrete, T = tombe, R = refuse

Temporal integrity: musique is a snapshot provided by the source at race-time,
so it describes past performance only -- no future leakage.

Produces:
  - musique_advanced.jsonl  in builder_outputs/musique_advanced/

Features per partant (10):
  - mus_nb_results             : number of results in the musique string
  - mus_nb_wins                : count of '1' positions
  - mus_nb_places              : count of positions 1, 2, 3
  - mus_nb_unplaced            : count of '0' (unplaced)
  - mus_nb_disqualified        : count of D/A/T/R incidents
  - mus_avg_position           : average of numeric positions (0 treated as 10)
  - mus_recent_3_avg           : average of first 3 positions (most recent)
  - mus_discipline_consistency : fraction of results in same discipline as
                                 current race
  - mus_improving_trend        : 1 if first-3 avg < last-3 avg (improving),
                                 -1 if worsening, 0 otherwise
  - mus_last_result            : most recent position (D/A/T/R encoded as ints)

Usage:
    python feature_builders/musique_advanced_builder.py
    python feature_builders/musique_advanced_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import re
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
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/musique_advanced")

_LOG_EVERY = 500_000

# ---------------------------------------------------------------------------
# Regex: each token is an optional status letter (D/A/T/R) OR a digit,
# followed by an optional discipline letter (p/a/m/h/s/c).
# Examples:  "1p"  "Da"  "0p"  "3a"  "T"  "12p"
# ---------------------------------------------------------------------------
_TOKEN_RE = re.compile(r"([DATR]|\d+)([pamhsc])?", re.IGNORECASE)

_DNF_CHARS = {"D", "A", "T", "R"}

# Encoding for non-numeric last results
_SPECIAL_ENCODING: dict[str, int] = {
    "D": 11,  # disqualified
    "A": 12,  # arrete (stopped)
    "T": 13,  # tombe (fallen)
    "R": 14,  # refuse (refused)
}

# Discipline mapping -- normalise to canonical lowercase letter
_DISCIPLINE_MAP = {
    "plat": "p",
    "attele": "a",
    "attele": "a",
    "monte": "m",
    "haies": "h",
    "steeple": "s",
    "steeplechase": "s",
    "cross": "c",
    "cross-country": "c",
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


def _parse_musique(musique: Optional[str]) -> list[tuple[Optional[int], Optional[str], str]]:
    """Parse a musique string into a list of (position, discipline, raw_status).

    Returns list of tuples, most recent first:
      - position: int (1-9) or 10 for '0', or None for D/A/T/R
      - discipline: lowercase letter or None
      - raw_status: the raw value token (e.g. '1', 'D', '0')
    """
    if not musique or not isinstance(musique, str):
        return []

    tokens = _TOKEN_RE.findall(musique)
    results: list[tuple[Optional[int], Optional[str], str]] = []

    for val, disc in tokens:
        upper_val = val.upper()
        disc_lower = disc.lower() if disc else None

        if upper_val in _DNF_CHARS:
            results.append((None, disc_lower, upper_val))
        else:
            try:
                pos = int(val)
                # 0 means unplaced (10th or worse)
                results.append((10 if pos == 0 else pos, disc_lower, val))
            except ValueError:
                results.append((None, disc_lower, upper_val))

    return results


def _guess_current_discipline(rec: dict) -> Optional[str]:
    """Try to determine the discipline of the current race from the record."""
    # Try explicit discipline field
    disc = rec.get("discipline") or rec.get("discipline_course") or ""
    if isinstance(disc, str):
        disc_lower = disc.strip().lower()
        if disc_lower in _DISCIPLINE_MAP:
            return _DISCIPLINE_MAP[disc_lower]
        # Single-letter already?
        if len(disc_lower) == 1 and disc_lower in "pamhsc":
            return disc_lower

    # Try specialite field
    spec = rec.get("specialite") or ""
    if isinstance(spec, str):
        spec_lower = spec.strip().lower()
        if spec_lower in _DISCIPLINE_MAP:
            return _DISCIPLINE_MAP[spec_lower]
        if len(spec_lower) == 1 and spec_lower in "pamhsc":
            return spec_lower

    return None


def _compute_features(musique: Optional[str], rec: dict) -> dict[str, Any]:
    """Compute all 10 musique advanced features from a raw musique string."""
    parsed = _parse_musique(musique)

    feats: dict[str, Any] = {}

    # 1. mus_nb_results
    feats["mus_nb_results"] = len(parsed) if parsed else None

    if not parsed:
        feats["mus_nb_wins"] = None
        feats["mus_nb_places"] = None
        feats["mus_nb_unplaced"] = None
        feats["mus_nb_disqualified"] = None
        feats["mus_avg_position"] = None
        feats["mus_recent_3_avg"] = None
        feats["mus_discipline_consistency"] = None
        feats["mus_improving_trend"] = None
        feats["mus_last_result"] = None
        return feats

    positions = [p for p, _, _ in parsed if p is not None]

    # 2. mus_nb_wins
    feats["mus_nb_wins"] = sum(1 for p in positions if p == 1)

    # 3. mus_nb_places (1, 2, or 3)
    feats["mus_nb_places"] = sum(1 for p in positions if p <= 3)

    # 4. mus_nb_unplaced (original 0 => stored as 10)
    feats["mus_nb_unplaced"] = sum(1 for _, _, raw in parsed if raw == "0")

    # 5. mus_nb_disqualified (D, A, T, R)
    feats["mus_nb_disqualified"] = sum(
        1 for _, _, raw in parsed if raw.upper() in _DNF_CHARS
    )

    # 6. mus_avg_position (0 treated as 10, D/A/T/R excluded)
    if positions:
        feats["mus_avg_position"] = round(sum(positions) / len(positions), 3)
    else:
        feats["mus_avg_position"] = None

    # 7. mus_recent_3_avg (first 3 = most recent)
    recent_3 = [p for p, _, _ in parsed[:3] if p is not None]
    if recent_3:
        feats["mus_recent_3_avg"] = round(sum(recent_3) / len(recent_3), 3)
    else:
        feats["mus_recent_3_avg"] = None

    # 8. mus_discipline_consistency
    current_disc = _guess_current_discipline(rec)
    disciplines = [d for _, d, _ in parsed if d is not None]
    if current_disc and disciplines:
        matching = sum(1 for d in disciplines if d == current_disc)
        feats["mus_discipline_consistency"] = round(matching / len(disciplines), 3)
    else:
        feats["mus_discipline_consistency"] = None

    # 9. mus_improving_trend
    #    Compare first 3 (most recent) vs last 3 (oldest) numeric positions.
    #    Lower position = better.  improving = recent avg < older avg.
    if len(positions) >= 6:
        first_3 = [p for p, _, _ in parsed[:3] if p is not None]
        last_3 = [p for p, _, _ in parsed[-3:] if p is not None]
        if first_3 and last_3:
            recent_avg = sum(first_3) / len(first_3)
            older_avg = sum(last_3) / len(last_3)
            if recent_avg < older_avg - 0.5:
                feats["mus_improving_trend"] = 1
            elif recent_avg > older_avg + 0.5:
                feats["mus_improving_trend"] = -1
            else:
                feats["mus_improving_trend"] = 0
        else:
            feats["mus_improving_trend"] = None
    else:
        feats["mus_improving_trend"] = None

    # 10. mus_last_result: most recent position encoded as int
    first_pos, _, first_raw = parsed[0]
    if first_pos is not None:
        feats["mus_last_result"] = first_pos
    else:
        feats["mus_last_result"] = _SPECIAL_ENCODING.get(first_raw.upper())

    return feats


# ===========================================================================
# MAIN BUILD (single-pass streaming, .tmp then rename)
# ===========================================================================


def build_musique_advanced_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build musique advanced features from partants_master.jsonl.

    Single-pass streaming: reads input line by line, writes output
    to a .tmp file then atomically renames.

    Returns total number of feature records written.
    """
    logger.info("=== Musique Advanced Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    feature_keys = [
        "mus_nb_results",
        "mus_nb_wins",
        "mus_nb_places",
        "mus_nb_unplaced",
        "mus_nb_disqualified",
        "mus_avg_position",
        "mus_recent_3_avg",
        "mus_discipline_consistency",
        "mus_improving_trend",
        "mus_last_result",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    n_written = 0

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            musique = rec.get("musique")
            feats = _compute_features(musique, rec)

            out_rec = {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid"),
                "date_reunion_iso": rec.get("date_reunion_iso"),
            }
            for k in feature_keys:
                v = feats.get(k)
                out_rec[k] = v
                if v is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            n_written += 1

            if n_written % _LOG_EVERY == 0:
                logger.info("  Ecrit %d records...", n_written)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Musique advanced build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
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
        description="Construction des features musique avancees a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/musique_advanced/)",
    )
    args = parser.parse_args()

    logger = setup_logging("musique_advanced_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "musique_advanced.jsonl"
    build_musique_advanced_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
