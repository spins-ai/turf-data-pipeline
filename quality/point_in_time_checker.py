#!/usr/bin/env python3
"""
quality/point_in_time_checker.py
================================
Verifies temporal (point-in-time) correctness of features in partants_master.jsonl.

Checks performed:
  1. Sample 1000 records via reservoir sampling.
  2. For each record, verify that computed features are consistent with
     "before race" semantics (e.g., nb_courses_carriere > 0 required for
     non-null momentum / rolling stats).
  3. Cross-check: if nb_courses_carriere == 0 then momentum_3 / rolling
     features should be None / null.
  4. Verify global date ordering of the JSONL (records must be sorted by
     date_reunion_iso ascending).
  5. Report all violations with counts and examples.

No API calls -- 100% local processing.

Usage:
    python3 quality/point_in_time_checker.py
    python3 quality/point_in_time_checker.py --input path/to/partants_master.jsonl
    python3 quality/point_in_time_checker.py --sample-size 5000
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.logging_setup import setup_logging

DEFAULT_INPUT = _PROJECT_ROOT / "data_master" / "partants_master.jsonl"
OUTPUT_DIR = _PROJECT_ROOT / "output" / "quality"
SAMPLE_SIZE = 1000
RESERVOIR_SEED = 42

DATE_FIELD = "date_reunion_iso"

# ---------------------------------------------------------------------------
# Features that MUST be null / absent when nb_courses_carriere == 0
# (first-time starters cannot have rolling performance stats).
# ---------------------------------------------------------------------------
REQUIRES_PRIOR_RACES: list[str] = [
    "momentum_3",
    "momentum_5",
    "hist_victoires_5",
    "hist_victoires_10",
    "hist_victoires_20",
    "hist_places_5",
    "hist_places_10",
    "hist_places_20",
    "hist_taux_vic_5",
    "hist_taux_vic_10",
    "hist_taux_vic_20",
    "hist_taux_place_5",
    "hist_taux_place_10",
    "hist_taux_place_20",
    "hist_gains_moy_5",
    "hist_gains_moy_10",
    "hist_gains_moy_20",
    "hist_classement_moy_5",
    "hist_classement_moy_10",
    "hist_regularite_5",
    "hist_regularite_10",
    "hist_progression",
    "hist_streak_victoires",
    "hist_streak_places",
    "hist_meilleur_classement",
    "hist_pire_classement",
    "hist_dnf_5",
    "hist_dnf_10",
    "hist_pct_complete_5",
    "hist_pct_complete_10",
    "win_rate_carriere",
    "place_rate_carriere",
    "seq_nb_victoires_recent_5",
    "seq_nb_places_recent_5",
    "seq_serie_victoires",
    "seq_serie_places",
    "seq_serie_non_places",
    "elo_cheval_delta",
]

# ---------------------------------------------------------------------------
# Numeric features that should be >= 0 when present (sanity bounds).
# ---------------------------------------------------------------------------
NON_NEGATIVE_FEATURES: list[str] = [
    "nb_courses_carriere",
    "nb_victoires_carriere",
    "nb_places_carriere",
    "gains_carriere_euros",
    "gains_annee_euros",
    "elo_cheval",
    "elo_jockey",
    "elo_entraineur",
    "nombre_partants",
]

# ---------------------------------------------------------------------------
# Features whose numeric value should be <= nb_courses_carriere
# (cannot have more wins/places than total starts).
# ---------------------------------------------------------------------------
BOUNDED_BY_CAREER: list[str] = [
    "nb_victoires_carriere",
    "nb_places_carriere",
    "nb_places_2eme",
    "nb_places_3eme",
]


# ===========================================================================
# DATA CLASSES
# ===========================================================================


@dataclass
class Violation:
    """A single point-in-time violation."""

    rule: str
    partant_uid: str
    date: str
    detail: str


@dataclass
class CheckReport:
    """Aggregated report of all checks."""

    total_records_scanned: int = 0
    sample_size: int = 0
    date_ordering_ok: bool = True
    date_ordering_first_violation: Optional[str] = None
    violations: list[Violation] = field(default_factory=list)

    # ---------- helpers ----------
    def add(self, rule: str, rec: dict, detail: str) -> None:
        self.violations.append(
            Violation(
                rule=rule,
                partant_uid=rec.get("partant_uid", "?"),
                date=rec.get(DATE_FIELD, "?"),
                detail=detail,
            )
        )

    def summary_dict(self) -> dict[str, Any]:
        by_rule: dict[str, int] = {}
        for v in self.violations:
            by_rule[v.rule] = by_rule.get(v.rule, 0) + 1
        return {
            "total_records_scanned": self.total_records_scanned,
            "sample_size": self.sample_size,
            "date_ordering_ok": self.date_ordering_ok,
            "date_ordering_first_violation": self.date_ordering_first_violation,
            "total_violations": len(self.violations),
            "violations_by_rule": by_rule,
            "example_violations": [
                {
                    "rule": v.rule,
                    "partant_uid": v.partant_uid,
                    "date": v.date,
                    "detail": v.detail,
                }
                for v in self.violations[:50]
            ],
        }


# ===========================================================================
# RESERVOIR SAMPLING
# ===========================================================================


def reservoir_sample(
    path: Path,
    k: int,
    seed: int = RESERVOIR_SEED,
    logger: Optional[logging.Logger] = None,
) -> tuple[list[dict], int, bool, Optional[str]]:
    """Reservoir-sample *k* records from a JSONL file while also checking
    date ordering on the full stream.

    Returns:
        (sampled_records, total_count, date_ordering_ok, first_violation_detail)
    """
    rng = random.Random(seed)
    reservoir: list[dict] = []
    total = 0
    prev_date: Optional[str] = None
    date_ok = True
    first_violation: Optional[str] = None

    with open(path, "r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                if logger:
                    logger.warning("Ligne %d : JSON invalide, ignoree.", line_no)
                continue

            total += 1

            # -- date ordering check (full stream) --
            cur_date = rec.get(DATE_FIELD, "")
            if prev_date is not None and cur_date < prev_date:
                if date_ok:
                    first_violation = (
                        f"Ligne {line_no}: {cur_date} < precedent {prev_date}"
                    )
                date_ok = False
            prev_date = cur_date

            # -- reservoir sampling --
            if total <= k:
                reservoir.append(rec)
            else:
                j = rng.randint(1, total)
                if j <= k:
                    reservoir[j - 1] = rec

            if total % 500_000 == 0 and logger:
                logger.info("  ... %d lignes lues", total)

    return reservoir, total, date_ok, first_violation


# ===========================================================================
# CHECKS
# ===========================================================================


def check_first_starter_nulls(rec: dict, report: CheckReport) -> None:
    """If nb_courses_carriere == 0, rolling features must be None."""
    nb = rec.get("nb_courses_carriere")
    if nb is None:
        return  # field absent, nothing to check
    try:
        nb = int(nb)
    except (ValueError, TypeError):
        return
    if nb != 0:
        return

    for feat in REQUIRES_PRIOR_RACES:
        val = rec.get(feat)
        if val is not None:
            report.add(
                rule="first_starter_non_null",
                rec=rec,
                detail=f"nb_courses_carriere=0 but {feat}={val!r}",
            )


def check_non_negative(rec: dict, report: CheckReport) -> None:
    """Numeric sanity: certain fields should never be negative."""
    for feat in NON_NEGATIVE_FEATURES:
        val = rec.get(feat)
        if val is None:
            continue
        try:
            num = float(val)
        except (ValueError, TypeError):
            continue
        if num < 0:
            report.add(
                rule="negative_value",
                rec=rec,
                detail=f"{feat}={val}",
            )


def check_bounded_by_career(rec: dict, report: CheckReport) -> None:
    """nb_victoires_carriere etc. cannot exceed nb_courses_carriere."""
    nb = rec.get("nb_courses_carriere")
    if nb is None:
        return
    try:
        nb = int(nb)
    except (ValueError, TypeError):
        return

    for feat in BOUNDED_BY_CAREER:
        val = rec.get(feat)
        if val is None:
            continue
        try:
            fval = int(val)
        except (ValueError, TypeError):
            continue
        if fval > nb:
            report.add(
                rule="exceeds_career_count",
                rec=rec,
                detail=f"{feat}={fval} > nb_courses_carriere={nb}",
            )


def check_win_rate_bounds(rec: dict, report: CheckReport) -> None:
    """Win/place rates must be in [0, 1] when present."""
    rate_fields = [
        "win_rate_carriere",
        "place_rate_carriere",
        "hist_taux_vic_5",
        "hist_taux_vic_10",
        "hist_taux_vic_20",
        "hist_taux_place_5",
        "hist_taux_place_10",
        "hist_taux_place_20",
        "proba_implicite",
    ]
    for feat in rate_fields:
        val = rec.get(feat)
        if val is None:
            continue
        try:
            fval = float(val)
        except (ValueError, TypeError):
            continue
        if fval < 0.0 or fval > 1.0:
            report.add(
                rule="rate_out_of_bounds",
                rec=rec,
                detail=f"{feat}={fval} not in [0,1]",
            )


def check_elo_plausibility(rec: dict, report: CheckReport) -> None:
    """Elo ratings should be within a reasonable range (e.g. 800-2500)."""
    for feat in ("elo_cheval", "elo_jockey", "elo_entraineur", "elo_combined"):
        val = rec.get(feat)
        if val is None:
            continue
        try:
            fval = float(val)
        except (ValueError, TypeError):
            continue
        if fval < 500 or fval > 3000:
            report.add(
                rule="elo_implausible",
                rec=rec,
                detail=f"{feat}={fval} outside [500,3000]",
            )


def check_sequence_consistency(rec: dict, report: CheckReport) -> None:
    """seq_nb_courses_historique should be <= nb_courses_carriere."""
    seq = rec.get("seq_nb_courses_historique")
    nb = rec.get("nb_courses_carriere")
    if seq is None or nb is None:
        return
    try:
        seq_v = int(seq)
        nb_v = int(nb)
    except (ValueError, TypeError):
        return
    if seq_v > nb_v:
        report.add(
            rule="seq_exceeds_career",
            rec=rec,
            detail=f"seq_nb_courses_historique={seq_v} > nb_courses_carriere={nb_v}",
        )


# ===========================================================================
# MAIN
# ===========================================================================


def run(
    input_path: Path,
    sample_size: int,
    seed: int,
    logger: logging.Logger,
) -> CheckReport:
    """Execute all point-in-time checks and return a report."""
    report = CheckReport(sample_size=sample_size)

    logger.info("=== Point-in-Time Correctness Checker ===")
    logger.info("Input : %s", input_path)
    logger.info("Sample: %d records (seed=%d)", sample_size, seed)

    if not input_path.exists():
        logger.error("Fichier introuvable : %s", input_path)
        return report

    # -- Phase 1: reservoir sample + date ordering (single pass) --
    logger.info("Phase 1 : lecture + reservoir sampling + date ordering ...")
    sample, total, date_ok, date_viol = reservoir_sample(
        input_path, sample_size, seed=seed, logger=logger
    )
    report.total_records_scanned = total
    report.date_ordering_ok = date_ok
    report.date_ordering_first_violation = date_viol

    logger.info("  Total lignes   : %d", total)
    logger.info("  Echantillon    : %d", len(sample))
    logger.info("  Date ordering  : %s", "OK" if date_ok else "VIOLATION")
    if date_viol:
        logger.warning("  Premier desordre: %s", date_viol)

    # -- Phase 2: per-record checks on sample --
    logger.info("Phase 2 : verification des %d records echantillonnes ...", len(sample))
    for rec in sample:
        check_first_starter_nulls(rec, report)
        check_non_negative(rec, report)
        check_bounded_by_career(rec, report)
        check_win_rate_bounds(rec, report)
        check_elo_plausibility(rec, report)
        check_sequence_consistency(rec, report)

    # -- Report --
    summary = report.summary_dict()
    logger.info("=== Resultats ===")
    logger.info("  Violations totales : %d", summary["total_violations"])
    for rule, cnt in summary["violations_by_rule"].items():
        logger.info("    %-30s : %d", rule, cnt)

    # -- Write JSON report --
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / "point_in_time_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    logger.info("Rapport ecrit : %s", report_path)

    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Point-in-time correctness checker for partants_master.jsonl"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=SAMPLE_SIZE,
        help="Number of records to sample (default: 1000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=RESERVOIR_SEED,
        help="Random seed for reservoir sampling",
    )
    args = parser.parse_args()

    logger = setup_logging("point_in_time_checker")
    report = run(args.input, args.sample_size, args.seed, logger)

    if report.violations:
        logger.warning(
            "ECHEC : %d violation(s) point-in-time detectee(s).",
            len(report.violations),
        )
        sys.exit(1)
    elif not report.date_ordering_ok:
        logger.warning("ECHEC : ordre chronologique non respecte dans le JSONL.")
        sys.exit(1)
    else:
        logger.info("SUCCES : aucune violation point-in-time.")
        sys.exit(0)


if __name__ == "__main__":
    main()
