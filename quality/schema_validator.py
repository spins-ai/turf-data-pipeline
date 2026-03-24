#!/usr/bin/env python3
"""
quality/schema_validator.py
===========================
Validation de schema pour partants_master.jsonl.

Echantillonne 1 000 enregistrements (reservoir sampling), puis verifie :
  1. Champs obligatoires presents (partant_uid, course_uid, date_reunion_iso,
     nom_cheval, discipline)
  2. Types attendus (numeriques, dates YYYY-MM-DD)
  3. Plages de valeurs (position 1-20, distance 800-10000, cote > 1.0)
  4. Rapport de violations par champ

Streaming ligne par ligne, RAM < 2 Go.

Usage :
    python quality/schema_validator.py
    python quality/schema_validator.py --input path/to/partants_master.jsonl
    python quality/schema_validator.py --sample-size 5000
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

DEFAULT_INPUT = _PROJECT_ROOT / "data_master" / "partants_master.jsonl"
DEFAULT_OUTPUT = _PROJECT_ROOT / "quality" / "schema_validation_report.md"
DEFAULT_SAMPLE_SIZE = 1_000
RESERVOIR_SEED = 42

log = logging.getLogger(__name__)

# ===========================================================================
# SCHEMA DEFINITION
# ===========================================================================

REQUIRED_FIELDS = [
    "partant_uid",
    "course_uid",
    "date_reunion_iso",
    "nom_cheval",
    "discipline",
]

# Fields expected to be numeric (int or float)
NUMERIC_FIELDS = [
    "position",
    "distance",
    "distance_course",
    "dist",
    "cote",
    "cote_probable",
    "cote_depart",
    "cote_actuelle",
    "poids",
    "poids_jockey",
    "handicap_poids",
    "age",
    "numero",
    "numPmu",
    "nb_partants",
    "allocation_totale",
    "allocation",
    "dotation",
    "gain",
    "gains",
    "gains_carriere",
    "gain_annee",
    "nb_courses",
    "nb_victoires",
    "nb_places",
    "rapport_simple",
    "rapport_couple",
    "reduction_km",
    "temps",
    "temps_km",
]

# Fields expected to match YYYY-MM-DD
DATE_FIELDS = [
    "date_reunion_iso",
    "date_naissance",
    "date_course",
]

# Value range rules: field -> (min, max) inclusive, applied when field is present
# and numeric.  None means no bound.
RANGE_RULES: dict[str, tuple[float | None, float | None]] = {
    "position": (1.0, 20.0),
    "distance": (800.0, 10000.0),
    "distance_course": (800.0, 10000.0),
    "dist": (800.0, 10000.0),
    "cote": (1.0, None),
    "cote_probable": (1.0, None),
    "cote_depart": (1.0, None),
    "cote_actuelle": (1.0, None),
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ===========================================================================
# HELPERS
# ===========================================================================


def _is_numeric(value: Any) -> float | None:
    """Return float if value is numeric, else None."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return None


def _reservoir_sample(filepath: Path, k: int, seed: int) -> list[dict]:
    """Reservoir-sample *k* JSON records from a JSONL file (streaming)."""
    rng = random.Random(seed)
    reservoir: list[dict] = []
    n = 0

    with open(filepath, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            n += 1
            if n <= k:
                reservoir.append(rec)
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    reservoir[j] = rec
    return reservoir


# ===========================================================================
# VALIDATORS
# ===========================================================================


class SchemaValidator:
    """Accumulates per-field violation counts."""

    def __init__(self) -> None:
        self.missing_field: dict[str, int] = defaultdict(int)
        self.bad_type: dict[str, int] = defaultdict(int)
        self.bad_range: dict[str, int] = defaultdict(int)
        self.bad_date_format: dict[str, int] = defaultdict(int)
        self.total = 0
        self.examples: dict[str, list[str]] = defaultdict(list)

    # ---- individual checks -------------------------------------------------

    def check_required(self, rec: dict) -> None:
        for field in REQUIRED_FIELDS:
            val = rec.get(field)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                self.missing_field[field] += 1

    def check_numeric_types(self, rec: dict) -> None:
        for field in NUMERIC_FIELDS:
            val = rec.get(field)
            if val is None:
                continue
            if _is_numeric(val) is None:
                self.bad_type[field] += 1
                examples = self.examples[f"type:{field}"]
                if len(examples) < 3:
                    examples.append(repr(val))

    def check_date_formats(self, rec: dict) -> None:
        for field in DATE_FIELDS:
            val = rec.get(field)
            if val is None:
                continue
            if not isinstance(val, str) or not _DATE_RE.match(val):
                self.bad_date_format[field] += 1
                examples = self.examples[f"date:{field}"]
                if len(examples) < 3:
                    examples.append(repr(val))

    def check_ranges(self, rec: dict) -> None:
        for field, (lo, hi) in RANGE_RULES.items():
            val = rec.get(field)
            if val is None:
                continue
            num = _is_numeric(val)
            if num is None:
                continue
            if lo is not None and num < lo:
                self.bad_range[field] += 1
                examples = self.examples[f"range:{field}"]
                if len(examples) < 3:
                    examples.append(str(val))
            elif hi is not None and num > hi:
                self.bad_range[field] += 1
                examples = self.examples[f"range:{field}"]
                if len(examples) < 3:
                    examples.append(str(val))

    # ---- orchestrate -------------------------------------------------------

    def validate(self, rec: dict) -> None:
        self.total += 1
        self.check_required(rec)
        self.check_numeric_types(rec)
        self.check_date_formats(rec)
        self.check_ranges(rec)


# ===========================================================================
# REPORT
# ===========================================================================


def _pct(count: int, total: int) -> str:
    if total == 0:
        return "N/A"
    return f"{count / total * 100:.2f}%"


def build_report(v: SchemaValidator, input_path: Path) -> str:
    lines: list[str] = []
    lines.append("# Schema Validation Report")
    lines.append(f"\nSource: `{input_path}`")
    lines.append(f"Sample size: **{v.total}** records\n")

    # Required fields
    lines.append("## Champs obligatoires manquants\n")
    lines.append("| Champ | Manquants | % |")
    lines.append("|-------|-----------|---|")
    for field in REQUIRED_FIELDS:
        cnt = v.missing_field.get(field, 0)
        lines.append(f"| {field} | {cnt} | {_pct(cnt, v.total)} |")

    # Type violations
    lines.append("\n## Violations de type (champs numeriques)\n")
    lines.append("| Champ | Non-numeriques | % | Exemples |")
    lines.append("|-------|---------------|---|----------|")
    for field in NUMERIC_FIELDS:
        cnt = v.bad_type.get(field, 0)
        if cnt == 0:
            continue
        exs = ", ".join(v.examples.get(f"type:{field}", []))
        lines.append(f"| {field} | {cnt} | {_pct(cnt, v.total)} | {exs} |")

    # Date format violations
    lines.append("\n## Violations de format date (attendu YYYY-MM-DD)\n")
    lines.append("| Champ | Invalides | % | Exemples |")
    lines.append("|-------|-----------|---|----------|")
    for field in DATE_FIELDS:
        cnt = v.bad_date_format.get(field, 0)
        if cnt == 0:
            continue
        exs = ", ".join(v.examples.get(f"date:{field}", []))
        lines.append(f"| {field} | {cnt} | {_pct(cnt, v.total)} | {exs} |")

    # Range violations
    lines.append("\n## Violations de plage de valeurs\n")
    lines.append("| Champ | Hors plage | % | Plage attendue | Exemples |")
    lines.append("|-------|-----------|---|----------------|----------|")
    for field, (lo, hi) in RANGE_RULES.items():
        cnt = v.bad_range.get(field, 0)
        if cnt == 0:
            continue
        lo_s = str(lo) if lo is not None else "-inf"
        hi_s = str(hi) if hi is not None else "+inf"
        exs = ", ".join(v.examples.get(f"range:{field}", []))
        lines.append(
            f"| {field} | {cnt} | {_pct(cnt, v.total)} | [{lo_s}, {hi_s}] | {exs} |"
        )

    # Summary
    total_violations = (
        sum(v.missing_field.values())
        + sum(v.bad_type.values())
        + sum(v.bad_date_format.values())
        + sum(v.bad_range.values())
    )
    lines.append(f"\n## Resume\n")
    lines.append(f"- Total violations : **{total_violations}**")
    lines.append(f"- Enregistrements verifies : **{v.total}**")
    status = "PASS" if total_violations == 0 else "WARN"
    lines.append(f"- Statut : **{status}**")

    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Schema validator for partants_master.jsonl"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to write the report (Markdown)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of records to sample (default: 1000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=RESERVOIR_SEED,
        help="Random seed for reservoir sampling",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    input_path: Path = args.input
    if not input_path.exists():
        log.error("Input file not found: %s", input_path)
        return 1

    log.info("Sampling %d records from %s ...", args.sample_size, input_path)
    t0 = time.perf_counter()
    sample = _reservoir_sample(input_path, args.sample_size, args.seed)
    elapsed = time.perf_counter() - t0
    log.info("Sampled %d records in %.1fs", len(sample), elapsed)

    if not sample:
        log.error("No records found in %s", input_path)
        return 1

    validator = SchemaValidator()
    for rec in sample:
        validator.validate(rec)

    report = build_report(validator, input_path)

    output_path: Path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    log.info("Report written to %s", output_path)

    # Print summary to stdout
    total_violations = (
        sum(validator.missing_field.values())
        + sum(validator.bad_type.values())
        + sum(validator.bad_date_format.values())
        + sum(validator.bad_range.values())
    )
    status = "PASS" if total_violations == 0 else "WARN"
    print(f"\n[schema_validator] {status} -- {total_violations} violation(s) "
          f"on {validator.total} records")

    return 0 if total_violations == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
