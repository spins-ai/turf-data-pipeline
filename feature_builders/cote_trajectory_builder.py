#!/usr/bin/env python3
"""Cote trajectory features: how odds evolve over a horse's career,
market confidence trends, and value detection signals."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cote_trajectory")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


class _CoteState:
    __slots__ = ("cotes", "wins_when_fav", "total_when_fav",
                 "wins", "total", "cote_result_pairs")

    def __init__(self):
        self.cotes = deque(maxlen=20)
        self.wins_when_fav = 0
        self.total_when_fav = 0
        self.wins = 0
        self.total = 0
        self.cote_result_pairs = deque(maxlen=20)  # (cote, is_win)


def main():
    logger = setup_logging("cote_trajectory_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "cote_trajectory_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _CoteState] = {}

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
                    _process_course(course_records, fout, horse_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, fills):
    # Determine field favorite
    cotes_field = []
    for rec in records:
        c = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference")) or _safe(rec.get("cote_probable"))
        if c: cotes_field.append(c)
    min_cote = min(cotes_field) if cotes_field else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference")) or _safe(rec.get("cote_probable"))

        if hid and cote:
            st = horse_states.get(hid)
            is_fav = (cote == min_cote) if min_cote else False

            if st and st.total >= 3:
                clist = list(st.cotes)

                # 1. Avg cote historique
                avg_cote = sum(clist) / len(clist)
                feat["ct_avg_cote"] = round(avg_cote, 2)
                fills["ct_avg_cote"] += 1

                # 2. Current vs avg (drifting shorter or longer?)
                feat["ct_cote_vs_avg"] = round(cote - avg_cote, 2)
                fills["ct_cote_vs_avg"] += 1

                # 3. Cote trend (last 3 vs prev 3)
                if len(clist) >= 6:
                    recent = sum(clist[:3]) / 3
                    older = sum(clist[3:6]) / 3
                    if older > 0:
                        feat["ct_cote_trend"] = round((recent - older) / older, 4)
                        fills["ct_cote_trend"] += 1

                # 4. Market confidence: when favorite, win rate
                if st.total_when_fav >= 3:
                    feat["ct_fav_wr"] = round(st.wins_when_fav / st.total_when_fav, 4)
                    fills["ct_fav_wr"] += 1

                # 5. ROI proxy: sum(cote * win) / total
                pairs = list(st.cote_result_pairs)
                if len(pairs) >= 5:
                    roi = sum(c * w for c, w in pairs) / len(pairs) - 1
                    feat["ct_roi_proxy"] = round(roi, 4)
                    fills["ct_roi_proxy"] += 1

                # 6. Cote volatility
                if len(clist) >= 3:
                    mean_c = sum(clist) / len(clist)
                    if mean_c > 0:
                        std_c = math.sqrt(sum((c - mean_c) ** 2 for c in clist) / len(clist))
                        feat["ct_cote_cv"] = round(std_c / mean_c, 4)
                        fills["ct_cote_cv"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference")) or _safe(rec.get("cote_probable"))
        is_winner = bool(rec.get("is_gagnant"))

        if hid and cote:
            if hid not in horse_states:
                horse_states[hid] = _CoteState()
            st = horse_states[hid]
            st.cotes.appendleft(cote)  # most recent first
            st.wins += int(is_winner)
            st.total += 1
            st.cote_result_pairs.append((cote, int(is_winner)))

            is_fav = (cote == min_cote) if min_cote else False
            if is_fav:
                st.wins_when_fav += int(is_winner)
                st.total_when_fav += 1


if __name__ == "__main__":
    main()
