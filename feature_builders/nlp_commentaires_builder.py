#!/usr/bin/env python3
"""
feature_builders.nlp_commentaires_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
NLP-based features extracted from text fields in partants_master.

Single-pass streaming: reads each partants_master record, computes features
from commentaire_apres_course, musique, avis_entraineur, and
cnd_conditions_texte_original, then writes output.

No external NLP library -- pure regex/keyword matching.

Produces:
  - nlp_commentaires_features.jsonl  in builder_outputs/nlp_commentaires/

Features per partant (6):
  - nlp_comment_length         : length of commentaire_apres_course (0 if empty)
  - nlp_comment_sentiment      : keyword-based sentiment score from commentaire
  - nlp_musique_score          : weighted score parsed from musique string
  - nlp_avis_present           : 1 if avis_entraineur is non-empty, else 0
  - nlp_conditions_handicap    : 1 if "handicap" in conditions text
  - nlp_conditions_listed      : 1 if "liste"/"groupe"/"group" in conditions text

Usage:
    python feature_builders/nlp_commentaires_builder.py
    python feature_builders/nlp_commentaires_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/nlp_commentaires")
OUTPUT_FILE = OUTPUT_DIR / "nlp_commentaires_features.jsonl"

# Progress log every N records
_LOG_EVERY = 500_000
# gc.collect every N records
_GC_EVERY = 500_000

# ===========================================================================
# SENTIMENT KEYWORDS
# ===========================================================================

_POSITIVE_KW = {
    "bien", "bonne", "facile", "dominant", "remarquable",
    "excellent", "brillant", "impressionnant", "regulier",
}
_NEGATIVE_KW = {
    "decevant", "fatigue", "mal", "boiteux", "irregulier",
    "faible", "decrochant", "gene",
}

# ===========================================================================
# MUSIQUE PARSING
# ===========================================================================

# Pattern: one or more digits followed by zero or more letters (surface code)
# Also matches "D" (disqualified) possibly followed by surface letters
# e.g. "1P2P0P3P4PDPDP" or "DM3M122A6A2A0A3A0A0A"
_MUSIQUE_TOKEN_RE = re.compile(r"(\d+|D)([A-Za-z]*)")


def _parse_musique(musique: str) -> dict:
    """Parse musique string and return summary stats.

    Returns dict with:
      - positions: list of int positions (most recent first)
      - n_disqualified: count of 'D' tokens
      - n_unplaced: count of 0 positions
      - n_wins: count of position == 1
      - weighted_avg: weighted average position (recent = higher weight)
    """
    if not musique:
        return None

    tokens = _MUSIQUE_TOKEN_RE.findall(musique)
    if not tokens:
        return None

    positions = []
    n_disqualified = 0

    for value, _surface in tokens:
        if value == "D":
            n_disqualified += 1
            positions.append(10)  # treat D as worst position for averaging
        else:
            pos = int(value)
            if pos == 0:
                positions.append(10)  # unplaced = 10 for averaging
            else:
                positions.append(pos)

    if not positions:
        return None

    n_unplaced = sum(1 for v, _ in tokens if v != "D" and int(v) == 0)
    n_wins = sum(1 for v, _ in tokens if v != "D" and int(v) == 1)

    # Weighted average: most recent position gets weight = len, next = len-1, etc.
    n = len(positions)
    weights = list(range(n, 0, -1))  # [n, n-1, ..., 1]
    total_weight = sum(weights)
    weighted_avg = sum(p * w for p, w in zip(positions, weights)) / total_weight

    return {
        "weighted_avg": weighted_avg,
        "n_disqualified": n_disqualified,
        "n_unplaced": n_unplaced,
        "n_wins": n_wins,
        "n_races": n,
    }


def _musique_score(parsed: dict) -> float:
    """Compute a composite musique score from parsed data.

    Higher = better recent form.
    Score = (10 - weighted_avg) / 9 * 0.5
           + win_rate * 0.3
           - disqualification_rate * 0.1
           - unplaced_rate * 0.1

    Result in roughly [-0.2, 1.0] range.
    """
    n = parsed["n_races"]
    if n == 0:
        return 0.0

    # Position component: 10 = worst, 1 = best -> normalize to [0, 1]
    pos_score = (10 - parsed["weighted_avg"]) / 9.0

    win_rate = parsed["n_wins"] / n
    disq_rate = parsed["n_disqualified"] / n
    unplaced_rate = parsed["n_unplaced"] / n

    score = pos_score * 0.5 + win_rate * 0.3 - disq_rate * 0.1 - unplaced_rate * 0.1
    return round(score, 4)


# ===========================================================================
# SENTIMENT SCORING
# ===========================================================================

def _normalize_text(text: str) -> str:
    """Lowercase and strip accents for keyword matching."""
    # Simple accent removal for French text
    text = text.lower()
    for src, dst in [
        ("\xe9", "e"), ("\xe8", "e"), ("\xea", "e"), ("\xeb", "e"),
        ("\xe0", "a"), ("\xe2", "a"), ("\xe4", "a"),
        ("\xf4", "o"), ("\xf6", "o"),
        ("\xf9", "u"), ("\xfb", "u"), ("\xfc", "u"),
        ("\xee", "i"), ("\xef", "i"),
        ("\xe7", "c"),
    ]:
        text = text.replace(src, dst)
    return text


def _sentiment_score(comment: str) -> float:
    """Keyword-based sentiment: (pos - neg) / (pos + neg + 1)."""
    normalized = _normalize_text(comment)
    words = set(re.findall(r"[a-z]+", normalized))

    nb_pos = len(words & _POSITIVE_KW)
    nb_neg = len(words & _NEGATIVE_KW)

    return round((nb_pos - nb_neg) / (nb_pos + nb_neg + 1), 4)


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
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# MAIN BUILD
# ===========================================================================

def _resolve_input(cli_input: Optional[str]) -> Path:
    """Find input file from CLI arg or default candidates."""
    if cli_input:
        p = Path(cli_input)
        if p.exists():
            return p
        raise FileNotFoundError(f"Input not found: {p}")

    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "partants_master.jsonl not found. Tried:\n"
        + "\n".join(f"  - {c}" for c in INPUT_CANDIDATES)
    )


def build(logger, input_path: Optional[Path] = None):
    """Main build function."""
    if input_path is None:
        input_path = _resolve_input(None)

    logger.info("=== NLP Commentaires Feature Builder ===")
    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", OUTPUT_FILE)

    output_path = OUTPUT_FILE
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    t0 = time.time()
    n_read = 0
    n_written = 0

    feature_names = [
        "nlp_comment_length",
        "nlp_comment_sentiment",
        "nlp_musique_score",
        "nlp_avis_present",
        "nlp_conditions_handicap",
        "nlp_conditions_listed",
    ]
    fill = {k: 0 for k in feature_names}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_read += 1

            if n_read % _LOG_EVERY == 0:
                elapsed = time.time() - t0
                logger.info(
                    "  Progres: %d records lus (%.1fs, %.0f rec/s)",
                    n_read, elapsed, n_read / elapsed if elapsed > 0 else 0,
                )

            if n_read % _GC_EVERY == 0:
                gc.collect()

            # Extract identifiers
            partant_uid = rec.get("partant_uid")
            course_uid = rec.get("course_uid")
            date_iso = rec.get("date_reunion_iso", "")

            if not partant_uid:
                continue

            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_iso,
            }

            # --- Feature 1: nlp_comment_length ---
            comment = rec.get("commentaire_apres_course") or ""
            comment = str(comment).strip()
            out["nlp_comment_length"] = len(comment)
            if len(comment) > 0:
                fill["nlp_comment_length"] += 1

            # --- Feature 2: nlp_comment_sentiment ---
            if comment:
                out["nlp_comment_sentiment"] = _sentiment_score(comment)
                fill["nlp_comment_sentiment"] += 1
            else:
                out["nlp_comment_sentiment"] = None

            # --- Feature 3: nlp_musique_score ---
            musique = rec.get("musique") or ""
            musique = str(musique).strip()
            parsed = _parse_musique(musique)
            if parsed is not None:
                out["nlp_musique_score"] = _musique_score(parsed)
                fill["nlp_musique_score"] += 1
            else:
                out["nlp_musique_score"] = None

            # --- Feature 4: nlp_avis_present ---
            avis = rec.get("avis_entraineur") or ""
            avis = str(avis).strip()
            out["nlp_avis_present"] = 1 if avis else 0
            if avis:
                fill["nlp_avis_present"] += 1

            # --- Feature 5: nlp_conditions_handicap ---
            conditions = rec.get("cnd_conditions_texte_original") or ""
            conditions = str(conditions).strip()
            cond_lower = conditions.lower()
            is_handicap = 1 if "handicap" in cond_lower else 0
            out["nlp_conditions_handicap"] = is_handicap
            if is_handicap:
                fill["nlp_conditions_handicap"] += 1

            # --- Feature 6: nlp_conditions_listed ---
            is_listed = 1 if ("liste" in cond_lower or "groupe" in cond_lower or "group" in cond_lower) else 0
            out["nlp_conditions_listed"] = is_listed
            if is_listed:
                fill["nlp_conditions_listed"] += 1

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d records ecrits en %.1fs", n_written, elapsed)
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %-30s: %7d / %d (%.1f%%)", k, v, n_written, pct)


def main():
    parser = argparse.ArgumentParser(description="NLP commentaires feature builder")
    parser.add_argument("--input", type=str, default=None, help="Path to partants_master.jsonl")
    args = parser.parse_args()

    logger = setup_logging("nlp_commentaires_builder")

    input_path = None
    if args.input:
        input_path = Path(args.input)

    build(logger, input_path=input_path)


if __name__ == "__main__":
    main()
