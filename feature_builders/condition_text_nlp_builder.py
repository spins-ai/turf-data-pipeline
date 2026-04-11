#!/usr/bin/env python3
"""
feature_builders.condition_text_nlp_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
NLP features from the race conditions text (cnd_conditions_texte_original).

This field contains rich info about race type, restrictions, and prizes.
Pure regex approach -- NO external NLP libraries.

Temporal integrity: conditions text is published before the race,
so these features are known at race-time -- no future leakage.

Produces:
  - condition_text_nlp_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/condition_text_nlp/

Features per partant (10):
  - ctn_allocation_euros         : euro amount extracted from conditions text
  - ctn_is_reclamer              : 1 if claiming race
  - ctn_is_course_a_conditions   : 1 if conditions race
  - ctn_is_handicap_text         : 1 if "handicap" in text
  - ctn_is_apprenti              : 1 if apprentice/lads race
  - ctn_sex_restriction          : 1 if mare/filly only
  - ctn_age_restriction          : extracted min or max age from text
  - ctn_is_national              : 1 if national-level race
  - ctn_is_international         : 1 if international race
  - ctn_prize_level              : log(extracted_allocation + 1)

Usage:
    python feature_builders/condition_text_nlp_builder.py
    python feature_builders/condition_text_nlp_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/condition_text_nlp")

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# ===========================================================================
# REGEX PATTERNS (compiled once)
# ===========================================================================

# Euro amounts: "15.000 euros", "15 000€", "15000 Euros", etc.
_RE_EUROS = re.compile(
    r"(\d[\d\s.,]*\d)\s*(?:euros?|€)",
    re.IGNORECASE,
)

# Age patterns: "4 ans", "3 ans et au-dessus", "de 3 a 5 ans", "3-5 ans"
_RE_AGE = re.compile(
    r"(\d)\s*(?:a|-)\s*(\d)\s*ans",
    re.IGNORECASE,
)
_RE_AGE_SINGLE = re.compile(
    r"(\d)\s*ans",
    re.IGNORECASE,
)


# ===========================================================================
# TEXT NORMALISATION
# ===========================================================================


def _strip_accents(text: str) -> str:
    """Remove accents/diacritics from text."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalise(text: Optional[str]) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    if not text or not isinstance(text, str):
        return ""
    t = text.lower().strip()
    t = _strip_accents(t)
    t = re.sub(r"\s+", " ", t)
    return t


# ===========================================================================
# EURO EXTRACTION
# ===========================================================================


def _parse_euro_amount(text: str) -> Optional[float]:
    """Extract the first euro amount from normalised text. Returns float or None."""
    m = _RE_EUROS.search(text)
    if not m:
        return None
    raw = m.group(1)
    # Remove spaces and dots used as thousand separators, keep comma as decimal
    # "15.000" -> "15000", "15 000" -> "15000", "1.500,50" -> "1500.50"
    cleaned = raw.replace(" ", "").replace("\u00a0", "")
    # If both dot and comma present, dot is thousands sep
    if "." in cleaned and "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "." in cleaned:
        # Decide: "15.000" is 15000, "15.50" is 15.50
        parts = cleaned.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            cleaned = cleaned.replace(".", "")  # thousands separator
        # else keep as decimal
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


# ===========================================================================
# AGE EXTRACTION
# ===========================================================================


def _extract_age(text: str) -> Optional[int]:
    """Extract a representative age value from text.

    For ranges like "3 a 5 ans" returns the midpoint (4).
    For single "4 ans" returns 4.
    Returns None if no age found.
    """
    m = _RE_AGE.search(text)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        if 2 <= lo <= 10 and 2 <= hi <= 10:
            return (lo + hi) // 2
    m2 = _RE_AGE_SINGLE.search(text)
    if m2:
        age = int(m2.group(1))
        if 2 <= age <= 10:
            return age
    return None


# ===========================================================================
# FEATURE COMPUTATION
# ===========================================================================

_FEATURE_KEYS = [
    "ctn_allocation_euros",
    "ctn_is_reclamer",
    "ctn_is_course_a_conditions",
    "ctn_is_handicap_text",
    "ctn_is_apprenti",
    "ctn_sex_restriction",
    "ctn_age_restriction",
    "ctn_is_national",
    "ctn_is_international",
    "ctn_prize_level",
]


def _compute_features(raw_text: Optional[str]) -> dict[str, Any]:
    """Compute all 10 NLP features from the conditions text."""
    text = _normalise(raw_text)

    feats: dict[str, Any] = {}

    if not text:
        for k in _FEATURE_KEYS:
            feats[k] = None
        return feats

    # 1. Allocation euros
    euros = _parse_euro_amount(text)
    feats["ctn_allocation_euros"] = round(euros, 2) if euros is not None else None

    # 2. Claiming race
    feats["ctn_is_reclamer"] = 1 if ("reclamer" in text or "a reclamer" in text) else 0

    # 3. Conditions race
    feats["ctn_is_course_a_conditions"] = 1 if "conditions" in text else 0

    # 4. Handicap from text
    feats["ctn_is_handicap_text"] = 1 if "handicap" in text else 0

    # 5. Apprentice race
    feats["ctn_is_apprenti"] = 1 if ("apprenti" in text or "lads" in text) else 0

    # 6. Sex restriction
    feats["ctn_sex_restriction"] = 1 if (
        "femelles" in text or "juments" in text or "pouliches" in text
    ) else 0

    # 7. Age restriction
    feats["ctn_age_restriction"] = _extract_age(text)

    # 8. National (but not "international")
    has_international = "international" in text
    feats["ctn_is_national"] = 1 if (
        "national" in text and not has_international
    ) else 0

    # 9. International
    feats["ctn_is_international"] = 1 if has_international else 0

    # 10. Prize level = log(allocation + 1)
    if euros is not None and euros > 0:
        feats["ctn_prize_level"] = round(math.log(euros + 1), 4)
    else:
        feats["ctn_prize_level"] = None

    return feats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build(input_path: Path, logger):
    """Single-pass streaming build of condition text NLP features."""
    logger.info("=== Condition Text NLP Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "condition_text_nlp_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_read = 0
    n_with_text = 0
    fill = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  %d records traites...", n_read)
            if n_read % _GC_EVERY == 0:
                gc.collect()

            raw_text = rec.get("cnd_conditions_texte_original")
            if raw_text:
                n_with_text += 1

            feats = _compute_features(raw_text)

            out = {
                "partant_uid": rec.get("partant_uid", ""),
                "course_uid": rec.get("course_uid", ""),
                "date_reunion_iso": rec.get("date_reunion_iso", ""),
            }
            out.update(feats)

            # Track fill rates
            for k in _FEATURE_KEYS:
                if out.get(k) is not None:
                    fill[k] += 1

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    # Atomic rename
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Termine: %d records (%d avec texte conditions, %.1f%%) en %.1fs",
        n_read, n_with_text,
        n_with_text / n_read * 100 if n_read else 0,
        elapsed,
    )
    logger.info("Output: %s", output_path)

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill.items():
        pct = v / n_read * 100 if n_read > 0 else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_read, pct)


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="NLP features from race conditions text (cnd_conditions_texte_original)"
    )
    parser.add_argument("--input", type=str, default=None,
                        help="Path to partants_master.jsonl")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Override output directory")
    args = parser.parse_args()

    logger = setup_logging("condition_text_nlp_builder")

    input_path = _find_input(args.input)

    if args.output_dir:
        global OUTPUT_DIR
        OUTPUT_DIR = Path(args.output_dir)

    build(input_path, logger)


if __name__ == "__main__":
    main()
