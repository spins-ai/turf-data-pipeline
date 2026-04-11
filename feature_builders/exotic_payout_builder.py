#!/usr/bin/env python3
"""Exotic payout features: exploit rap_rapport_ columns for quarté, quinté,
multi, 2sur4, couple place payouts. These indicate race predictability,
market efficiency, and payout structure."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/exotic_payout")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("exotic_payout_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "exotic_payout_features.jsonl"
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
    """Race-level exotic payout analysis — shared across all runners."""
    r0 = records[0]

    # Exotic payouts (race-level)
    quarte_ordre = _safe(r0.get("rap_rapport_quarte_ordre"))
    quarte_bonus = _safe(r0.get("rap_rapport_quarte_bonus"))
    quinte_ordre = _safe(r0.get("rap_rapport_quinte_ordre"))
    quinte_bonus3 = _safe(r0.get("rap_rapport_quinte_bonus3"))
    multi4 = _safe(r0.get("rap_rapport_multi_4"))
    multi5 = _safe(r0.get("rap_rapport_multi_5"))
    multi6 = _safe(r0.get("rap_rapport_multi_6"))
    multi7 = _safe(r0.get("rap_rapport_multi_7"))
    dsq_max = _safe(r0.get("rap_rapport_2sur4_max"))
    dsq_min = _safe(r0.get("rap_rapport_2sur4_min"))
    dsq_nb = _safe(r0.get("rap_rapport_2sur4_nb_combinaisons"))
    cp1 = _safe(r0.get("rap_rapport_couple_place_1"))
    cp2 = _safe(r0.get("rap_rapport_couple_place_2"))
    cp3 = _safe(r0.get("rap_rapport_couple_place_3"))

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        # 1. Quarté payouts (race difficulty indicator)
        if quarte_ordre is not None:
            feat["exp_quarte_ordre"] = round(quarte_ordre, 2)
            feat["exp_log_quarte"] = round(math.log1p(quarte_ordre), 3)
            fills["exp_quarte_ordre"] += 1
            fills["exp_log_quarte"] += 1

        if quarte_bonus is not None:
            feat["exp_quarte_bonus"] = round(quarte_bonus, 2)
            fills["exp_quarte_bonus"] += 1

        # Quarté ratio (how much order matters)
        if quarte_ordre is not None and quarte_bonus is not None and quarte_bonus > 0:
            feat["exp_quarte_order_ratio"] = round(quarte_ordre / quarte_bonus, 2)
            fills["exp_quarte_order_ratio"] += 1

        # 2. Quinté payouts
        if quinte_ordre is not None:
            feat["exp_quinte_ordre"] = round(quinte_ordre, 2)
            feat["exp_log_quinte"] = round(math.log1p(quinte_ordre), 3)
            fills["exp_quinte_ordre"] += 1
            fills["exp_log_quinte"] += 1

        if quinte_bonus3 is not None:
            feat["exp_quinte_bonus3"] = round(quinte_bonus3, 2)
            fills["exp_quinte_bonus3"] += 1

        # 3. Multi payouts (graded by difficulty)
        if multi4 is not None:
            feat["exp_multi4"] = round(multi4, 2)
            fills["exp_multi4"] += 1

        if multi5 is not None:
            feat["exp_multi5"] = round(multi5, 2)
            fills["exp_multi5"] += 1

        if multi6 is not None:
            feat["exp_multi6"] = round(multi6, 2)
            fills["exp_multi6"] += 1

        if multi7 is not None:
            feat["exp_multi7"] = round(multi7, 2)
            fills["exp_multi7"] += 1

        # Multi difficulty gradient (how steep is the payout curve)
        if multi4 is not None and multi7 is not None and multi4 > 0:
            feat["exp_multi_gradient"] = round(multi7 / multi4, 2)
            fills["exp_multi_gradient"] += 1

        # 4. 2sur4 (race spread indicator)
        if dsq_max is not None:
            feat["exp_2sur4_max"] = round(dsq_max, 2)
            fills["exp_2sur4_max"] += 1

        if dsq_min is not None:
            feat["exp_2sur4_min"] = round(dsq_min, 2)
            fills["exp_2sur4_min"] += 1

        if dsq_max is not None and dsq_min is not None and dsq_min > 0:
            feat["exp_2sur4_spread"] = round(dsq_max / dsq_min, 2)
            fills["exp_2sur4_spread"] += 1

        if dsq_nb is not None:
            feat["exp_2sur4_nb_combis"] = int(dsq_nb)
            fills["exp_2sur4_nb_combis"] += 1

        # 5. Couple place (top-heavy vs distributed)
        couple_places = [x for x in [cp1, cp2, cp3] if x is not None]
        if couple_places:
            feat["exp_cp_avg"] = round(sum(couple_places) / len(couple_places), 2)
            fills["exp_cp_avg"] += 1

            if len(couple_places) >= 2:
                feat["exp_cp_spread"] = round(max(couple_places) - min(couple_places), 2)
                fills["exp_cp_spread"] += 1

        # 6. Composite race predictability
        # Low payouts = predictable race; high payouts = surprise
        predictability_signals = []
        if quarte_ordre is not None:
            predictability_signals.append(math.log1p(quarte_ordre))
        if multi4 is not None:
            predictability_signals.append(math.log1p(multi4))
        if predictability_signals:
            feat["exp_race_surprise_idx"] = round(
                sum(predictability_signals) / len(predictability_signals), 3)
            fills["exp_race_surprise_idx"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
