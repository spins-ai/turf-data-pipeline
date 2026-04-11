#!/usr/bin/env python3
"""Trainer win streak builder - STREAMING. 8 features:
trainer hot/cold streak, rolling WR (10/20/50), jockey-trainer combo WR,
stable form composite.

No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_win_streak")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("trainer_win_streak_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "trainer_win_streak_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Accumulators
    trainer_last = defaultdict(lambda: deque(maxlen=50))  # trainer -> deque of (is_win, is_place)
    jt_last = defaultdict(lambda: deque(maxlen=30))       # jockey|trainer -> deque of is_win

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
                    _process_course(course_records, fout, trainer_last, jt_last, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, trainer_last, jt_last, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, trainer_last, jt_last, fills):
    # SNAPSHOT features
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        trainer = (rec.get("entraineur") or "").upper().strip()
        jockey = (rec.get("jockey_driver") or "").upper().strip()

        if trainer and len(trainer) >= 3:
            hist = trainer_last.get(trainer)
            if hist and len(hist) >= 5:
                results = list(hist)
                n = len(results)

                # 1. Rolling WR last 10
                last10 = results[-min(10, n):]
                wr10 = sum(w for w, _ in last10) / len(last10)
                feat["tws_wr_10"] = round(wr10, 4)
                fills["tws_wr_10"] += 1

                # 2. Rolling WR last 20
                if n >= 10:
                    last20 = results[-min(20, n):]
                    wr20 = sum(w for w, _ in last20) / len(last20)
                    feat["tws_wr_20"] = round(wr20, 4)
                    fills["tws_wr_20"] += 1

                # 3. Rolling WR last 50
                if n >= 20:
                    wr50 = sum(w for w, _ in results) / n
                    feat["tws_wr_50"] = round(wr50, 4)
                    fills["tws_wr_50"] += 1

                # 4. Place rate last 10
                pr10 = sum(p for _, p in last10) / len(last10)
                feat["tws_pr_10"] = round(pr10, 4)
                fills["tws_pr_10"] += 1

                # 5. Current win streak (consecutive wins from end)
                streak = 0
                for w, _ in reversed(results):
                    if w:
                        streak += 1
                    else:
                        break
                feat["tws_win_streak"] = streak
                fills["tws_win_streak"] += 1

                # 6. Current losing streak
                lose_streak = 0
                for w, _ in reversed(results):
                    if not w:
                        lose_streak += 1
                    else:
                        break
                feat["tws_lose_streak"] = lose_streak
                fills["tws_lose_streak"] += 1

                # 7. Momentum (WR last 5 - WR last 20)
                if n >= 10:
                    last5 = results[-min(5, n):]
                    wr5 = sum(w for w, _ in last5) / len(last5)
                    last20 = results[-min(20, n):]
                    wr20 = sum(w for w, _ in last20) / len(last20)
                    feat["tws_momentum"] = round(wr5 - wr20, 4)
                    fills["tws_momentum"] += 1

        # 8. Jockey-trainer combo WR
        if trainer and jockey and len(trainer) >= 3 and len(jockey) >= 3:
            jt_key = f"{jockey}|{trainer}"
            jt_hist = jt_last.get(jt_key)
            if jt_hist and len(jt_hist) >= 3:
                jt_wr = sum(jt_hist) / len(jt_hist)
                feat["tws_jt_combo_wr"] = round(jt_wr, 4)
                feat["tws_jt_combo_n"] = len(jt_hist)
                fills["tws_jt_combo_wr"] += 1
                fills["tws_jt_combo_n"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        trainer = (rec.get("entraineur") or "").upper().strip()
        jockey = (rec.get("jockey_driver") or "").upper().strip()
        is_winner = bool(rec.get("is_gagnant"))
        is_placed = bool(rec.get("is_place"))

        if trainer and len(trainer) >= 3:
            trainer_last[trainer].append((int(is_winner), int(is_placed)))

        if trainer and jockey and len(trainer) >= 3 and len(jockey) >= 3:
            jt_key = f"{jockey}|{trainer}"
            jt_last[jt_key].append(int(is_winner))


if __name__ == "__main__":
    main()
