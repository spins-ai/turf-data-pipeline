#!/usr/bin/env python3
"""
feature_builders.race_conditions_encoded_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Encodes race condition strings into numeric features.

Reads partants_master.jsonl in single-pass streaming mode.  Each record is
processed independently (no temporal state required).

Parses the ``conditions`` field plus supplementary fields (``type_course``,
``discipline``, ``specialite``) to extract structured numeric features that
capture the regulatory category and restrictions of each race.

Produces:
  - race_conditions_encoded.jsonl   in output/race_conditions_encoded/

Features per partant (10):
  - rce_is_handicap      : 1 if race conditions mention "handicap"
  - rce_is_claiming      : 1 if conditions mention "reclamer" or "claiming"
  - rce_is_listed        : 1 if conditions mention "listed" or "listee"
  - rce_is_group_race    : 1 if conditions mention "groupe" or "group" (I/II/III)
  - rce_group_level      : 0=not group, 1=Group III, 2=Group II, 3=Group I
  - rce_is_maiden        : 1 if conditions mention "debut" or horse has 0 wins
  - rce_age_restricted   : 1 if conditions mention specific age restrictions
  - rce_sex_restricted   : 1 if conditions mention "pouliches" or "femelles"
  - rce_conditions_class : numeric class (0=claiming, 1=maiden, 2=handicap,
                           3=conditions, 4=listed, 5=group)
  - rce_race_type_code   : discipline encoded (0=plat, 1=trot_attele,
                           2=trot_monte, 3=obstacle)

Usage:
    python feature_builders/race_conditions_encoded_builder.py
    python feature_builders/race_conditions_encoded_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_conditions_encoded")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Fallback candidates (local project paths) when primary INPUT_PARTANTS is missing
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# ---------------------------------------------------------------------------
# Pre-compiled regexes
# ---------------------------------------------------------------------------

# Handicap
_RE_HANDICAP = re.compile(r"\bhandicap\b", re.IGNORECASE)

# Claiming / reclamer
_RE_CLAIMING = re.compile(r"\b(reclam(?:er|able)?|claiming)\b", re.IGNORECASE)

# Listed
_RE_LISTED = re.compile(r"\b(listed|list[ée]e?)\b", re.IGNORECASE)

# Group race — generic presence (word "groupe" or "group" or "gr." near numeral)
_RE_GROUP = re.compile(
    r"\b(group[e]?\s+[iii123]+|gr\.?\s*[123])\b", re.IGNORECASE
)

# Group level patterns — match "groupe I/II/III" or "gr. 1/2/3"
# Ordered from most specific (I) to least (III) to avoid false overlaps.
_RE_GROUP_I = re.compile(
    r"\bgroup[e]?\s+i(?!\s*[i2-9])\b|\bgr\.?\s*1(?![0-9])\b", re.IGNORECASE
)
_RE_GROUP_II = re.compile(
    r"\bgroup[e]?\s+ii(?!\s*[i3-9])\b|\bgr\.?\s*2(?![0-9])\b", re.IGNORECASE
)
_RE_GROUP_III = re.compile(
    r"\bgroup[e]?\s+iii\b|\bgr\.?\s*3(?![0-9])\b", re.IGNORECASE
)

# Maiden / debut
_RE_MAIDEN = re.compile(r"\b(d[ée]but(?:ant)?s?|maiden)\b", re.IGNORECASE)

# Age restrictions — "3 ans", "2 ans", "4 ans et plus", etc.
_RE_AGE = re.compile(r"\b([2-9]\s*ans?)\b", re.IGNORECASE)

# Sex restrictions — female only
_RE_SEX = re.compile(r"\b(pouliche[s]?|femelle[s]?|juments?)\b", re.IGNORECASE)

# Conditions race (catch-all label used by PMU/letrot)
_RE_CONDITIONS = re.compile(r"\b(conditions?|cond\.)\b", re.IGNORECASE)


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield parsed dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning(
                        "Ligne JSON invalide ignoree (erreur #%d)", errors
                    )
    logger.info(
        "Lecture terminee: %d records, %d erreurs JSON", count, errors
    )


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _norm_text(*fields) -> str:
    """Concatenate non-None string fields, lower-cased, for regex matching."""
    parts = []
    for f in fields:
        if f and isinstance(f, str):
            parts.append(f.lower())
    return " ".join(parts)


# ===========================================================================
# DISCIPLINE ENCODING
# ===========================================================================

# Maps known discipline / type_course / specialite values to numeric codes.
# 0 = plat (flat), 1 = trot attelé, 2 = trot monté, 3 = obstacle (hurdles/steeplechase)

_DISCIPLINE_MAP: dict[str, int] = {
    # Flat
    "plat": 0,
    "flat": 0,
    "course_plat": 0,
    "galop_plat": 0,
    # Trot attelé
    "trot_attele": 1,
    "trot attele": 1,
    "trot_attelé": 1,
    "attelé": 1,
    "attele": 1,
    "trot": 1,          # default trot → attelé (most common)
    # Trot monté
    "trot_monte": 2,
    "trot monté": 2,
    "monte": 2,
    "monté": 2,
    # Obstacle
    "haies": 3,
    "hurdle": 3,
    "steeple": 3,
    "steeplechase": 3,
    "cross": 3,
    "obstacle": 3,
    "chase": 3,
    "clôture": 3,
    "cloture": 3,
}


def _encode_discipline(discipline: Any, type_course: Any, specialite: Any) -> Optional[int]:
    """Return numeric race-type code from discipline / type_course / specialite."""
    for raw in (discipline, type_course, specialite):
        if not raw or not isinstance(raw, str):
            continue
        key = raw.strip().lower().replace("-", "_").replace(" ", "_")
        if key in _DISCIPLINE_MAP:
            return _DISCIPLINE_MAP[key]
        # Partial match for compound strings
        for token, code in _DISCIPLINE_MAP.items():
            if token in key:
                return code
    return None


# ===========================================================================
# SINGLE-RECORD FEATURE EXTRACTION
# ===========================================================================


def _extract_features(rec: dict[str, Any]) -> dict[str, Any]:
    """Extract race-conditions encoded features from a single partant record."""

    # --- Source fields ---
    conditions  = rec.get("conditions") or ""
    type_course = rec.get("type_course") or ""
    discipline  = rec.get("discipline") or ""
    specialite  = rec.get("specialite") or ""
    nb_victoires = _safe_int(rec.get("nb_victoires_carriere"))

    # Concatenated text for regex matching
    text = _norm_text(conditions, type_course, discipline, specialite)

    # ------------------------------------------------------------------
    # 1. rce_is_handicap
    # ------------------------------------------------------------------
    is_handicap = 1 if _RE_HANDICAP.search(text) else 0

    # ------------------------------------------------------------------
    # 2. rce_is_claiming
    # ------------------------------------------------------------------
    is_claiming = 1 if _RE_CLAIMING.search(text) else 0

    # ------------------------------------------------------------------
    # 3. rce_is_listed
    # ------------------------------------------------------------------
    is_listed = 1 if _RE_LISTED.search(text) else 0

    # ------------------------------------------------------------------
    # 4. rce_is_group_race  &  5. rce_group_level
    # Check specific levels first (self-sufficient patterns); fall back to
    # generic _RE_GROUP for unclassified mentions → assume level III.
    # ------------------------------------------------------------------
    group_level = 0

    if _RE_GROUP_I.search(text):
        group_level = 3   # Group I (highest)
    elif _RE_GROUP_II.search(text):
        group_level = 2   # Group II
    elif _RE_GROUP_III.search(text):
        group_level = 1   # Group III
    elif _RE_GROUP.search(text):
        group_level = 1   # Generic group mention → assume III (lowest)

    is_group_race = 1 if group_level > 0 else 0

    # ------------------------------------------------------------------
    # 6. rce_is_maiden
    # Falls back to nb_victoires_carriere == 0 when no text signal.
    # ------------------------------------------------------------------
    if _RE_MAIDEN.search(text):
        is_maiden = 1
    elif nb_victoires is not None and nb_victoires == 0:
        is_maiden = 1
    else:
        is_maiden = 0

    # ------------------------------------------------------------------
    # 7. rce_age_restricted
    # ------------------------------------------------------------------
    is_age_restricted = 1 if _RE_AGE.search(text) else 0

    # ------------------------------------------------------------------
    # 8. rce_sex_restricted
    # ------------------------------------------------------------------
    is_sex_restricted = 1 if _RE_SEX.search(text) else 0

    # ------------------------------------------------------------------
    # 9. rce_conditions_class
    # Hierarchical: claiming < maiden < handicap < conditions < listed < group
    # ------------------------------------------------------------------
    if is_claiming:
        conditions_class = 0
    elif is_maiden:
        conditions_class = 1
    elif is_handicap:
        conditions_class = 2
    elif _RE_CONDITIONS.search(text):
        conditions_class = 3
    elif is_listed:
        conditions_class = 4
    elif is_group_race:
        conditions_class = 5
    else:
        # Default: plain conditions race (most common fallback)
        conditions_class = 3

    # ------------------------------------------------------------------
    # 10. rce_race_type_code
    # ------------------------------------------------------------------
    race_type_code = _encode_discipline(discipline, type_course, specialite)

    # ------------------------------------------------------------------
    # Build output record
    # ------------------------------------------------------------------
    return {
        "partant_uid":          rec.get("partant_uid"),
        "course_uid":           rec.get("course_uid"),
        "date_reunion_iso":     rec.get("date_reunion_iso"),
        "rce_is_handicap":      is_handicap,
        "rce_is_claiming":      is_claiming,
        "rce_is_listed":        is_listed,
        "rce_is_group_race":    is_group_race,
        "rce_group_level":      group_level,
        "rce_is_maiden":        is_maiden,
        "rce_age_restricted":   is_age_restricted,
        "rce_sex_restricted":   is_sex_restricted,
        "rce_conditions_class": conditions_class,
        "rce_race_type_code":   race_type_code,
    }


# ===========================================================================
# MAIN BUILD (single-pass streaming)
# ===========================================================================


def build_race_conditions_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Stream partants_master.jsonl and encode race conditions in one pass."""

    logger.info("=== Race Conditions Encoded Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_errors = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Traite %d records... (%.1fs)", n_read, time.time() - t0)

        try:
            feat = _extract_features(rec)
            results.append(feat)
        except Exception as exc:  # noqa: BLE001
            n_errors += 1
            if n_errors <= 10:
                logger.warning(
                    "Erreur feature extraction record %d: %s", n_read, exc
                )

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features, %d erreurs, en %.1fs",
        len(results), n_errors, elapsed,
    )

    gc.collect()
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file, checking CLI arg then fallback candidates."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve. Candidats: "
        + ", ".join(str(c) for c in INPUT_CANDIDATES)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Encode race conditions into numeric features from partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_conditions_encoded/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_conditions_encoded_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_race_conditions_features(input_path, logger)

    # Save
    out_path = output_dir / "race_conditions_encoded.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [
            k for k in results[0]
            if k not in ("partant_uid", "course_uid", "date_reunion_iso")
        ]
        total = len(results)
        logger.info("=== Fill rates ===")
        for key in feature_keys:
            filled = sum(1 for r in results if r.get(key) is not None)
            logger.info(
                "  %s: %d/%d (%.1f%%)", key, filled, total,
                100.0 * filled / total if total else 0.0,
            )

    logger.info("Termine.")


if __name__ == "__main__":
    main()
