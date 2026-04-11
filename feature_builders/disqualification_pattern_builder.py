#!/usr/bin/env python3
"""
feature_builders.disqualification_pattern_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Disqualification and incident pattern features.

Reads partants_master.jsonl in streaming mode, indexes and sorts
chronologically, then streams through records computing per-horse
disqualification/incident history features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.  State is snapshotted
BEFORE the current race updates it.

Produces:
  - disqualification_pattern.jsonl  in builder_outputs/disqualification_pattern/

Features per partant (8):
  - dqp_horse_dq_count      : total disqualifications for this horse
  - dqp_horse_dq_rate       : disqualifications / total races
  - dqp_horse_incident_count: total incidents (incident field non-empty)
  - dqp_horse_incident_rate : incidents / total races
  - dqp_recent_dq           : 1 if horse was disqualified in any of last 3 races
  - dqp_last_race_dq        : 1 if last race was a disqualification
  - dqp_horse_dnf_count     : count of non-finishes (statut != "Partant" or
                               position is null/non-numeric)
  - dqp_is_reliable         : 1 if horse has <5% DQ+incident rate over 10+ races

Usage:
    python feature_builders/disqualification_pattern_builder.py
    python feature_builders/disqualification_pattern_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/disqualification_pattern")

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


def _is_disqualified(rec: dict) -> bool:
    """Check if the record indicates a disqualification."""
    if rec.get("is_disqualifie"):
        return True
    statut = (rec.get("statut") or "").strip().lower()
    if "disq" in statut:
        return True
    return False


def _has_incident(rec: dict) -> bool:
    """Check if the incident field is non-empty."""
    incident = rec.get("incident")
    if incident is None:
        return False
    if isinstance(incident, str) and incident.strip():
        return True
    return False


def _is_dnf(rec: dict) -> bool:
    """Check if the horse did not finish.

    DNF = statut is not a normal runner OR position is null / non-numeric.
    """
    statut = (rec.get("statut") or "").strip().lower()
    if statut and statut not in ("partant", ""):
        return True
    pos = rec.get("position_arrivee")
    if pos is None:
        return True
    try:
        p = int(pos)
        return p <= 0
    except (ValueError, TypeError):
        return True


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Tracks disqualification / incident / DNF history for one horse."""

    __slots__ = ("dq_count", "incident_count", "dnf_count", "total_races", "recent_dqs")

    def __init__(self) -> None:
        self.dq_count: int = 0
        self.incident_count: int = 0
        self.dnf_count: int = 0
        self.total_races: int = 0
        self.recent_dqs: deque = deque(maxlen=3)

    def snapshot(self) -> dict[str, Any]:
        """Return features BEFORE updating with the current race."""
        feats: dict[str, Any] = {}

        feats["dqp_horse_dq_count"] = self.dq_count
        feats["dqp_horse_incident_count"] = self.incident_count
        feats["dqp_horse_dnf_count"] = self.dnf_count

        if self.total_races > 0:
            feats["dqp_horse_dq_rate"] = round(self.dq_count / self.total_races, 4)
            feats["dqp_horse_incident_rate"] = round(self.incident_count / self.total_races, 4)
        else:
            feats["dqp_horse_dq_rate"] = None
            feats["dqp_horse_incident_rate"] = None

        # Recent DQ: 1 if any of last 3 races was a DQ
        if len(self.recent_dqs) > 0:
            feats["dqp_recent_dq"] = 1 if any(self.recent_dqs) else 0
            feats["dqp_last_race_dq"] = 1 if self.recent_dqs[-1] else 0
        else:
            feats["dqp_recent_dq"] = None
            feats["dqp_last_race_dq"] = None

        # Reliability: <5% combined DQ+incident rate over 10+ races
        if self.total_races >= 10:
            combined_rate = (self.dq_count + self.incident_count) / self.total_races
            feats["dqp_is_reliable"] = 1 if combined_rate < 0.05 else 0
        else:
            feats["dqp_is_reliable"] = None

        return feats

    def update(self, is_dq: bool, has_inc: bool, is_dnf_flag: bool) -> None:
        """Update state AFTER snapshotting."""
        self.total_races += 1
        if is_dq:
            self.dq_count += 1
        if has_inc:
            self.incident_count += 1
        if is_dnf_flag:
            self.dnf_count += 1
        self.recent_dqs.append(is_dq)


# ===========================================================================
# MAIN BUILD (two-phase: index+sort, seek-based streaming output)
# ===========================================================================


def build_disqualification_pattern_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build disqualification pattern features.

    Phase 1: read all records, extract minimal fields, build index, sort chrono.
    Phase 2: stream through sorted index, snapshot state before update,
             write features directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Disqualification Pattern Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Index + sort --
    logger.info("Phase 1: indexation et tri chronologique...")

    index: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Phase 1: lu %d records...", n_read)

        index.append({
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
            "is_dq": _is_disqualified(rec),
            "has_inc": _has_incident(rec),
            "is_dnf": _is_dnf(rec),
        })

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    index.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique termine.")

    # -- Phase 2: Seek-based streaming output --
    logger.info("Phase 2: calcul des features par partant...")
    t1 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)
    n_written = 0
    fill_counts: dict[str, int] = {
        "dqp_horse_dq_count": 0,
        "dqp_horse_dq_rate": 0,
        "dqp_horse_incident_count": 0,
        "dqp_horse_incident_rate": 0,
        "dqp_recent_dq": 0,
        "dqp_last_race_dq": 0,
        "dqp_horse_dnf_count": 0,
        "dqp_is_reliable": 0,
    }

    i = 0
    total = len(index)

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        while i < total:
            # Group by course
            course_uid = index[i]["course"]
            course_date = index[i]["date"]
            course_group: list[dict] = []

            while (
                i < total
                and index[i]["course"] == course_uid
                and index[i]["date"] == course_date
            ):
                course_group.append(index[i])
                i += 1

            # -- Snapshot BEFORE update --
            for rec in course_group:
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid:
                    features.update(horse_states[hid].snapshot())
                else:
                    features.update({
                        "dqp_horse_dq_count": None,
                        "dqp_horse_dq_rate": None,
                        "dqp_horse_incident_count": None,
                        "dqp_horse_incident_rate": None,
                        "dqp_recent_dq": None,
                        "dqp_last_race_dq": None,
                        "dqp_horse_dnf_count": None,
                        "dqp_is_reliable": None,
                    })

                # Track fill rates
                for k in fill_counts:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update state AFTER snapshot --
            for rec in course_group:
                hid = rec["horse_id"]
                if hid:
                    horse_states[hid].update(rec["is_dq"], rec["has_inc"], rec["is_dnf"])

            if n_written % _LOG_EVERY == 0 and n_written > 0:
                logger.info("  Phase 2: ecrit %d / %d records...", n_written, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    # Free index
    del index
    gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Disqualification pattern build termine: %d features en %.1fs (chevaux: %d)",
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de disqualification/incident a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/disqualification_pattern/)",
    )
    args = parser.parse_args()

    logger = setup_logging("disqualification_pattern_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "disqualification_pattern.jsonl"
    build_disqualification_pattern_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
