#!/usr/bin/env python3
"""
feature_builders.course_context_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
6 course-context features derived from race conditions, allocation, and field size.

Reads partants_master.jsonl in streaming mode, groups records by course_uid,
and computes per-partant course-context features.

No temporal concern here: these features describe the race itself (not past
history), so they are the same for all partants in a course and carry no
future-leakage risk.

Produces:
  - course_context.jsonl   in output/course_context/

Features per partant:
  - course_prestige          : 1-5 scale (Handicap/other=1, Listed=2, Gr3=3, Gr2=4, Gr1=5)
  - is_course_phare          : 1 if highest-allocation course in its reunion
  - type_paris_level         : 1-5 based on bet types (simple=1 .. quinte=5)
  - nb_partants_normalized   : nombre_partants / 20
  - allocation_per_partant   : allocation_totale / nb_partants
  - is_handicap              : 1 if handicap race

Usage:
    python feature_builders/course_context_builder.py
    python feature_builders/course_context_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "course_context"

_LOG_EVERY = 500_000

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
# HELPERS
# ===========================================================================

_HANDICAP_RE = re.compile(r"handicap", re.IGNORECASE)
_GROUPE1_RE = re.compile(r"groupe?\s*[iI1]\b", re.IGNORECASE)
_GROUPE2_RE = re.compile(r"groupe?\s*(?:II|2)\b", re.IGNORECASE)
_GROUPE3_RE = re.compile(r"groupe?\s*(?:III|3)\b", re.IGNORECASE)
_LISTED_RE = re.compile(r"list[eé]e?|listed", re.IGNORECASE)


def _detect_prestige(conditions_text: str) -> int:
    """Return prestige level 1-5 from conditions text.

    Groupe 1 = 5, Groupe 2 = 4, Groupe 3 = 3, Listed = 2, other = 1.
    We check from most specific to least to avoid false matches.
    """
    if not conditions_text:
        return 1
    # Check Groupe 3 before Groupe 1 because "III" contains "I"
    if _GROUPE1_RE.search(conditions_text):
        # Make sure it's not Groupe III or Groupe II
        # "Groupe I " but not "Groupe II" or "Groupe III"
        # The regex already handles word boundary, but double-check
        if not _GROUPE2_RE.search(conditions_text) and not _GROUPE3_RE.search(conditions_text):
            return 5
    if _GROUPE2_RE.search(conditions_text):
        if not _GROUPE3_RE.search(conditions_text):
            return 4
    if _GROUPE3_RE.search(conditions_text):
        return 3
    if _LISTED_RE.search(conditions_text):
        return 2
    return 1


def _detect_handicap(conditions_text: str) -> int:
    """Return 1 if the race is a handicap, 0 otherwise."""
    if not conditions_text:
        return 0
    return 1 if _HANDICAP_RE.search(conditions_text) else 0


def _parse_allocation_from_conditions(conditions_text: str) -> Optional[float]:
    """Try to extract allocation totale from conditions text.

    The text typically contains a pattern like '80.000.' or '110.000.'
    representing the total allocation in euros.
    """
    if not conditions_text:
        return None
    # Match patterns like "80.000." or "110.000." (French number format)
    m = re.search(r'\b(\d{1,3}(?:\.\d{3})+)\.\s', conditions_text)
    if m:
        try:
            return float(m.group(1).replace(".", ""))
        except ValueError:
            pass
    # Also try plain number pattern like "80000"
    m = re.search(r'\b(\d{4,})\b', conditions_text)
    if m:
        val = float(m.group(1))
        if val >= 5000:  # reasonable minimum for a race allocation
            return val
    return None


def _type_paris_level(rec: dict) -> int:
    """Determine bet type level from available fields.

    simple=1, +couple=2, +tierce=3, +quarte=4, +quinte=5.
    We infer from rap_type_pari, mch_type_pari, and cnd_ flags.
    """
    rap_type = (rec.get("rap_type_pari") or "").lower()
    mch_type = (rec.get("mch_type_pari") or "").lower()
    combined = rap_type + " " + mch_type

    # Check from highest to lowest
    if "quinte" in combined or rec.get("cnd_cond_is_quinte"):
        return 5
    if "quarte" in combined:
        return 4
    if "tierce" in combined or "trio" in combined or rec.get("cnd_cond_is_tierce"):
        return 3
    if "couple" in combined or "multi" in combined or "pick" in combined:
        return 2
    return 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_course_context_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build 6 course-context features from partants_master.jsonl.

    Single-pass approach:
      1. Stream all records, keep only the fields we need.
      2. Group by course_uid to compute per-course features.
      3. Group by reunion_uid to find highest-allocation course (is_course_phare).
      4. Emit one feature dict per partant_uid.
    """
    logger.info("=== Course Context Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields into memory ──
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        conditions = rec.get("cnd_conditions_texte_original") or ""

        # Try to get allocation from dedicated field, fallback to parsing conditions
        allocation = rec.get("allocation_totale")
        if allocation is None:
            allocation = _parse_allocation_from_conditions(conditions)

        nb_partants = rec.get("nombre_partants") or 0
        try:
            nb_partants = int(nb_partants)
        except (ValueError, TypeError):
            nb_partants = 0

        slim = {
            "uid": rec.get("partant_uid"),
            "course": rec.get("course_uid", ""),
            "reunion": rec.get("reunion_uid", ""),
            "conditions": conditions,
            "allocation": allocation,
            "nb_partants": nb_partants,
            "rap_type_pari": rec.get("rap_type_pari"),
            "mch_type_pari": rec.get("mch_type_pari"),
            "cnd_cond_is_quinte": rec.get("cnd_cond_is_quinte"),
            "cnd_cond_is_tierce": rec.get("cnd_cond_is_tierce"),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0
    )

    # ── Phase 2: Compute per-course allocation (max across runners) ──
    # Several runners share the same course; allocation should be constant per course
    # but in case of inconsistency, take the max.
    course_alloc: dict[str, float] = {}
    course_nb_partants: dict[str, int] = {}
    course_reunion: dict[str, str] = {}
    course_conditions: dict[str, str] = {}
    course_paris_level: dict[str, int] = {}

    for rec in slim_records:
        cuid = rec["course"]
        if not cuid:
            continue

        # Allocation: keep max across records for this course
        alloc = rec["allocation"]
        if alloc is not None:
            try:
                alloc = float(alloc)
                if cuid not in course_alloc or alloc > course_alloc[cuid]:
                    course_alloc[cuid] = alloc
            except (ValueError, TypeError):
                pass

        # nb_partants: keep max
        nb = rec["nb_partants"]
        if nb > 0:
            if cuid not in course_nb_partants or nb > course_nb_partants[cuid]:
                course_nb_partants[cuid] = nb

        # Reunion mapping
        if rec["reunion"]:
            course_reunion[cuid] = rec["reunion"]

        # Conditions: keep longest (most informative)
        cond = rec["conditions"]
        if cond and (cuid not in course_conditions or len(cond) > len(course_conditions[cuid])):
            course_conditions[cuid] = cond

        # Paris level: keep max
        pl = _type_paris_level(rec)
        if cuid not in course_paris_level or pl > course_paris_level[cuid]:
            course_paris_level[cuid] = pl

    logger.info(
        "Courses uniques: %d, avec allocation: %d",
        len(course_nb_partants), len(course_alloc),
    )

    # ── Phase 3: Determine is_course_phare per reunion ──
    # Group courses by reunion, find the one with highest allocation
    reunion_courses: dict[str, list[str]] = defaultdict(list)
    for cuid, ruid in course_reunion.items():
        reunion_courses[ruid].append(cuid)

    phare_courses: set[str] = set()
    for ruid, cuids in reunion_courses.items():
        best_cuid = None
        best_alloc = -1.0
        for cuid in cuids:
            alloc = course_alloc.get(cuid, 0.0)
            if alloc > best_alloc:
                best_alloc = alloc
                best_cuid = cuid
        if best_cuid and best_alloc > 0:
            phare_courses.add(best_cuid)

    logger.info("Courses phares: %d", len(phare_courses))

    # ── Phase 4: Emit features ──
    results: list[dict[str, Any]] = []

    for rec in slim_records:
        cuid = rec["course"]
        alloc = course_alloc.get(cuid)
        nb = course_nb_partants.get(cuid, 0)
        conditions = course_conditions.get(cuid, "")

        prestige = _detect_prestige(conditions)
        is_phare = 1 if cuid in phare_courses else 0
        paris_level = course_paris_level.get(cuid, 1)
        nb_norm = round(nb / 20.0, 4) if nb > 0 else None
        alloc_per = round(alloc / nb, 2) if alloc and nb > 0 else None
        is_handi = _detect_handicap(conditions)

        results.append({
            "partant_uid": rec["uid"],
            "course_prestige": prestige,
            "is_course_phare": is_phare,
            "type_paris_level": paris_level,
            "nb_partants_normalized": nb_norm,
            "allocation_per_partant": alloc_per,
            "is_handicap": is_handi,
        })

    elapsed = time.time() - t0
    logger.info(
        "Course context build termine: %d features en %.1fs",
        len(results), elapsed,
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features course-context a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/course_context/)",
    )
    args = parser.parse_args()

    logger = setup_logging("course_context_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_course_context_features(input_path, logger)

    # Save
    out_path = output_dir / "course_context.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
