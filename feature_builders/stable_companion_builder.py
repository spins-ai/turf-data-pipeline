#!/usr/bin/env python3
"""Stable companion detector - STREAMING. 6 features:
nb_companions (same trainer in race), companion_avg_cote,
is_best_of_stable (lowest odds), trainer_entries_signal,
trainer_multi_entry_wr, companion_cote_ratio.

No OOM: streaming course-by-course, pure within-race features."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/stable_companion")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("stable_companion_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "stable_companion_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Accumulator: trainer multi-entry WR (when trainer has 2+ horses)
    multi_entry_hist = defaultdict(lambda: [0, 0])  # trainer -> [wins, total] in multi-entry races

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
                    _process_course(course_records, fout, multi_entry_hist, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, multi_entry_hist, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, multi_entry_hist, fills):
    # Group by trainer
    trainer_indices = defaultdict(list)
    for i, rec in enumerate(records):
        trainer = (rec.get("entraineur") or "").upper().strip()
        if trainer and len(trainer) >= 3:
            trainer_indices[trainer].append(i)

    # Get odds for each runner
    odds = []
    for rec in records:
        c = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        odds.append(c)

    # SNAPSHOT features
    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        trainer = (rec.get("entraineur") or "").upper().strip()
        if trainer and len(trainer) >= 3:
            companions = trainer_indices.get(trainer, [])
            nb_companions = len(companions)

            # 1. Number of stable companions (including self)
            feat["sc_nb_companions"] = nb_companions
            fills["sc_nb_companions"] += 1

            if nb_companions >= 2:
                # 2. Has companion flag
                feat["sc_has_companion"] = 1
                fills["sc_has_companion"] += 1

                # Companion odds
                comp_odds = [odds[j] for j in companions if odds[j] is not None and odds[j] > 0]
                my_odds = odds[i]

                if comp_odds:
                    avg_comp_odds = sum(comp_odds) / len(comp_odds)
                    feat["sc_companion_avg_cote"] = round(avg_comp_odds, 2)
                    fills["sc_companion_avg_cote"] += 1

                    # 3. Is this the best of the stable? (lowest odds)
                    if my_odds is not None and my_odds > 0:
                        min_odds = min(comp_odds)
                        feat["sc_is_stable_choice"] = 1 if abs(my_odds - min_odds) < 0.01 else 0
                        fills["sc_is_stable_choice"] += 1

                        # 4. Ratio my odds vs companion avg
                        feat["sc_cote_ratio"] = round(my_odds / avg_comp_odds, 4)
                        fills["sc_cote_ratio"] += 1

                # 5. Trainer multi-entry historical WR
                h = multi_entry_hist.get(trainer)
                if h and h[1] >= 10:
                    feat["sc_multi_entry_wr"] = round(h[0] / h[1], 4)
                    fills["sc_multi_entry_wr"] += 1
            else:
                feat["sc_has_companion"] = 0
                fills["sc_has_companion"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for trainer, indices in trainer_indices.items():
        if len(indices) >= 2:
            for idx in indices:
                is_winner = bool(records[idx].get("is_gagnant"))
                multi_entry_hist[trainer][1] += 1
                if is_winner:
                    multi_entry_hist[trainer][0] += 1


if __name__ == "__main__":
    main()
