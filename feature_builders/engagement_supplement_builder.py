#!/usr/bin/env python3
"""Engagement/supplement features: race entry patterns, engagement level,
number of declared runners ratio, and race-level metadata features."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/engagement_supplement")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("engagement_supplement_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "engagement_supplement_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        current_course = None
        course_records: list[dict] = []

        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                cuid = rec.get("course_uid", "")

                if cuid != current_course and course_records:
                    _process_course(course_records, fout, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, fills):
    # Race-level features
    n_partants = len(records)
    distance = _safe(records[0].get("distance"))
    allocation = _safe(records[0].get("allocation")) or _safe(records[0].get("allocation_euros"))
    nb_places_pmu = _safe(records[0].get("nb_places_pmu")) or _safe(records[0].get("nombre_places"))

    # Allocation per runner
    alloc_per_runner = None
    if allocation and n_partants > 0:
        alloc_per_runner = allocation / n_partants

    # PMU place ratio
    pmu_ratio = None
    if nb_places_pmu and n_partants > 0:
        pmu_ratio = nb_places_pmu / n_partants

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        numero = _safe(rec.get("numero_partant")) or _safe(rec.get("numero_cheval")) or _safe(rec.get("place_corde")) or _safe(rec.get("numero"))

        # 1. Number in field
        if numero is not None:
            feat["es_numero"] = int(numero)
            fills["es_numero"] += 1

            # Position in draw (normalized)
            if n_partants > 1:
                feat["es_draw_pctl"] = round((numero - 1) / (n_partants - 1), 4)
                fills["es_draw_pctl"] += 1

        # 2. Allocation per runner
        if alloc_per_runner is not None:
            feat["es_alloc_per_runner"] = round(alloc_per_runner, 0)
            fills["es_alloc_per_runner"] += 1

        # 3. PMU place ratio (probability of finishing placed)
        if pmu_ratio is not None:
            feat["es_pmu_place_ratio"] = round(pmu_ratio, 4)
            fills["es_pmu_place_ratio"] += 1

        # 4. Is supplement (late entry)
        supplement = rec.get("supplement") or rec.get("supplement_euros") or rec.get("est_supplement")
        if supplement is not None:
            feat["es_is_supplement"] = 1 if supplement else 0
            fills["es_is_supplement"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
