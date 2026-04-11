#!/usr/bin/env python3
"""
feature_builders.musique_decoder_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Parse the ``musique`` field (e.g. "1a2a3aDa0p") into 10 features.

Temporal integrity: musique is a snapshot provided by the source at race-time,
so it describes past performance only -- no future leakage.

Produces:
  - musique_decoder.jsonl  in output/musique_decoder/

Features per partant:
  - musique_pos_1 .. musique_pos_5 : positions of the last 5 races (1=most recent)
  - musique_nb_victoires_5         : number of "1" in last 5
  - musique_nb_places_5            : number of "1","2","3" in last 5
  - musique_nb_dnf_5               : number of "D","T","A","R" in last 5
  - musique_trend                  : improving (+1) or declining (-1) or 0
  - musique_consistency            : std of numeric positions in last 5

Usage:
    python feature_builders/musique_decoder_builder.py
"""

from __future__ import annotations

import argparse
import json
import math
import re
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "musique_decoder"

_LOG_EVERY = 500_000

# Pattern: one or more digits OR a letter, followed by an optional lowercase letter (allure)
_MUSIQUE_TOKEN_RE = re.compile(r"(\d+|[A-Z])([a-z])?", re.IGNORECASE)

_DNF_CHARS = {"D", "T", "A", "R"}


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


def _parse_musique(musique: Optional[str]) -> list[Optional[int]]:
    """Parse a musique string into a list of positions (most recent first).

    Letters D/T/A/R -> None (DNF), digits -> int position, 0 -> None (unplaced).
    Returns up to the first 10 tokens.
    """
    if not musique or not isinstance(musique, str):
        return []
    tokens = _MUSIQUE_TOKEN_RE.findall(musique)
    positions: list[Optional[int]] = []
    for val, _allure in tokens:
        if len(positions) >= 10:
            break
        upper = val.upper()
        if upper in _DNF_CHARS:
            positions.append(None)  # DNF
        else:
            try:
                p = int(val)
                positions.append(p if p > 0 else None)
            except ValueError:
                positions.append(None)
    return positions


def _compute_features(musique: Optional[str]) -> dict[str, Any]:
    """Compute all 10 musique features from a raw musique string."""
    positions = _parse_musique(musique)
    last5 = positions[:5]  # Most recent 5

    # Positions 1-5 (None if not enough history)
    feats: dict[str, Any] = {}
    for i in range(5):
        feats[f"musique_pos_{i + 1}"] = last5[i] if i < len(last5) else None

    if not last5:
        feats["musique_nb_victoires_5"] = None
        feats["musique_nb_places_5"] = None
        feats["musique_nb_dnf_5"] = None
        feats["musique_trend"] = None
        feats["musique_consistency"] = None
        return feats

    # Count DNFs from original string tokens (None in our list = DNF or 0/unplaced)
    # Re-parse to distinguish DNF from 0
    raw_tokens = _MUSIQUE_TOKEN_RE.findall(musique or "")[:5]
    nb_dnf = sum(1 for val, _ in raw_tokens if val.upper() in _DNF_CHARS)

    numeric = [p for p in last5 if p is not None]
    feats["musique_nb_victoires_5"] = sum(1 for p in numeric if p == 1)
    feats["musique_nb_places_5"] = sum(1 for p in numeric if p <= 3)
    feats["musique_nb_dnf_5"] = nb_dnf

    # Trend: compare first half vs second half of last 5 numeric positions
    if len(numeric) >= 3:
        mid = len(numeric) // 2
        recent_avg = sum(numeric[:mid]) / mid  # more recent (lower index)
        older_avg = sum(numeric[mid:]) / (len(numeric) - mid)
        if recent_avg < older_avg - 0.5:
            feats["musique_trend"] = 1  # improving (lower position = better)
        elif recent_avg > older_avg + 0.5:
            feats["musique_trend"] = -1  # declining
        else:
            feats["musique_trend"] = 0
    else:
        feats["musique_trend"] = None

    # Consistency: std of numeric positions
    if len(numeric) >= 2:
        mean = sum(numeric) / len(numeric)
        variance = sum((p - mean) ** 2 for p in numeric) / len(numeric)
        feats["musique_consistency"] = round(math.sqrt(variance), 3)
    else:
        feats["musique_consistency"] = None

    return feats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_musique_decoder_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build musique decoder features from partants_master.jsonl."""
    logger.info("=== Musique Decoder Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        musique = rec.get("musique")
        feats = _compute_features(musique)
        feats["partant_uid"] = rec.get("partant_uid")
        results.append(feats)

    elapsed = time.time() - t0
    logger.info(
        "Musique decoder build termine: %d features en %.1fs",
        len(results), elapsed,
    )
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
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
        description="Construction des features musique decoder a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("musique_decoder_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_musique_decoder_features(input_path, logger)

    out_path = output_dir / "musique_decoder.jsonl"
    save_jsonl(results, out_path, logger)

    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
