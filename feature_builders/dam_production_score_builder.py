#!/usr/bin/env python3
"""Dam production score builder - STREAMING. 6 features:
dam_offspring_wr, dam_offspring_pr, dam_offspring_avg_gains,
dam_nb_offspring, dam_best_offspring_wr, dam_consistency.

Tracks all offspring of each dam to build a production score.
No OOM: streaming course-by-course, bounded accumulators per dam."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/dam_production")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _norm(name):
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 else None


def main():
    logger = setup_logging("dam_production_score_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "dam_production_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Accumulators per dam
    dam_stats = defaultdict(lambda: [0, 0, 0, 0.0])  # [total, wins, places, gains]
    # Per dam × offspring: track unique offspring WRs
    dam_offspring = defaultdict(lambda: defaultdict(lambda: [0, 0]))  # dam -> {horse -> [wins, total]}

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        current_course = None
        course_records = []

        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                cuid = rec.get("course_uid", "")

                if cuid != current_course and course_records:
                    _process_course(course_records, fout, dam_stats, dam_offspring, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, dam_stats, dam_offspring, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, dam_stats, dam_offspring, fills):
    # SNAPSHOT features
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        mere = _norm(rec.get("mere") or rec.get("nom_mere") or rec.get("dam"))

        if mere:
            ds = dam_stats.get(mere)
            if ds and ds[0] >= 5:
                total, wins, places, gains = ds

                # 1. Dam offspring WR
                feat["dps_dam_wr"] = round(wins / total, 4)
                fills["dps_dam_wr"] += 1

                # 2. Dam offspring place rate
                feat["dps_dam_pr"] = round(places / total, 4)
                fills["dps_dam_pr"] += 1

                # 3. Dam average gains
                feat["dps_dam_avg_gains"] = round(gains / total, 0)
                fills["dps_dam_avg_gains"] += 1

                # 4. Nb offspring tracked
                offspring = dam_offspring.get(mere, {})
                feat["dps_dam_nb_offspring"] = len(offspring)
                fills["dps_dam_nb_offspring"] += 1

                # 5. Best offspring WR
                if offspring:
                    best_wr = 0
                    for horse, (hw, ht) in offspring.items():
                        if ht >= 3:
                            wr = hw / ht
                            if wr > best_wr:
                                best_wr = wr
                    if best_wr > 0:
                        feat["dps_best_offspring_wr"] = round(best_wr, 4)
                        fills["dps_best_offspring_wr"] += 1

                # 6. Dam consistency (std of offspring WRs)
                if len(offspring) >= 2:
                    wrs = []
                    for horse, (hw, ht) in offspring.items():
                        if ht >= 3:
                            wrs.append(hw / ht)
                    if len(wrs) >= 2:
                        mean_wr = sum(wrs) / len(wrs)
                        var = sum((w - mean_wr) ** 2 for w in wrs) / len(wrs)
                        feat["dps_dam_consistency"] = round(math.sqrt(var), 4)
                        fills["dps_dam_consistency"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        mere = _norm(rec.get("mere") or rec.get("nom_mere") or rec.get("dam"))
        horse = _norm(rec.get("nom_cheval") or rec.get("cheval"))
        is_winner = bool(rec.get("is_gagnant"))
        is_placed = bool(rec.get("is_place"))
        gains = _safe(rec.get("gains_carriere_euros")) or 0

        if mere:
            ds = dam_stats[mere]
            ds[0] += 1
            ds[1] += int(is_winner)
            ds[2] += int(is_placed)
            ds[3] += gains

            if horse:
                oh = dam_offspring[mere][horse]
                oh[0] += int(is_winner)
                oh[1] += 1


if __name__ == "__main__":
    main()
