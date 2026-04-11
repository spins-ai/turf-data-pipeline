#!/usr/bin/env python3
"""Leak-free target features: running averages of the TARGET variable (position, win, place)
computed STRICTLY from past data. These are the #1 most important features for GBM models.
Uses leave-one-out encoding for the current race to avoid target leakage."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/target_leakfree")
_LOG_EVERY = 500_000
_SMOOTH = 20  # Smoothing factor for LOO encoding


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _EntityStats:
    __slots__ = ("wins", "places", "total", "pos_sum", "recent_wins", "recent_total")

    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0
        self.pos_sum = 0.0  # sum of normalized positions
        self.recent_wins = deque(maxlen=20)
        self.recent_total = deque(maxlen=20)


def _loo_rate(wins, total, global_rate):
    """Leave-one-out smoothed rate."""
    if total == 0:
        return None
    return (wins + _SMOOTH * global_rate) / (total + _SMOOTH)


def main():
    logger = setup_logging("target_leakfree_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "target_leakfree_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Entity trackers
    horse_stats: dict[str, _EntityStats] = {}
    jockey_stats: dict[str, _EntityStats] = {}
    trainer_stats: dict[str, _EntityStats] = {}
    sire_stats: dict[str, _EntityStats] = {}
    hippo_stats: dict[str, _EntityStats] = {}
    jt_combo_stats: dict[str, _EntityStats] = {}

    global_wins = 0
    global_places = 0
    global_total = 0
    global_pos_sum = 0.0

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
                    global_wins, global_places, global_total, global_pos_sum = _process_course(
                        course_records, fout,
                        horse_stats, jockey_stats, trainer_stats, sire_stats,
                        hippo_stats, jt_combo_stats,
                        global_wins, global_places, global_total, global_pos_sum,
                        fills,
                    )
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            global_wins, global_places, global_total, global_pos_sum = _process_course(
                course_records, fout,
                horse_stats, jockey_stats, trainer_stats, sire_stats,
                hippo_stats, jt_combo_stats,
                global_wins, global_places, global_total, global_pos_sum,
                fills,
            )
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout,
                    horse_st, jockey_st, trainer_st, sire_st,
                    hippo_st, jt_st,
                    gw, gp, gt, gps, fills):
    gr_win = gw / gt if gt > 0 else 0.08
    gr_place = gp / gt if gt > 0 else 0.25
    gr_pos = gps / gt if gt > 0 else 0.5
    hippo = records[0].get("hippodrome_normalise", "")

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = rec.get("jockey_driver") or rec.get("nom_jockey") or ""
        trainer = rec.get("entraineur") or rec.get("nom_entraineur") or ""
        sire = rec.get("pere") or ""
        jt_key = f"{jockey}|{trainer}" if jockey and trainer else ""

        # Horse target stats
        hs = horse_st.get(hid)
        if hs and hs.total > 0:
            feat["tlf_horse_wr"] = round(_loo_rate(hs.wins, hs.total, gr_win), 5)
            feat["tlf_horse_place_rate"] = round(_loo_rate(hs.places, hs.total, gr_place), 5)
            feat["tlf_horse_avg_pos"] = round(hs.pos_sum / hs.total, 4)
            fills["tlf_horse_wr"] += 1
            fills["tlf_horse_place_rate"] += 1
            fills["tlf_horse_avg_pos"] += 1

            # Recent form (last 20)
            rw = list(hs.recent_wins)
            rt = list(hs.recent_total)
            if len(rw) >= 3:
                feat["tlf_horse_recent_wr"] = round(sum(rw) / len(rw), 5)
                fills["tlf_horse_recent_wr"] += 1

        # Jockey target stats
        js = jockey_st.get(jockey)
        if js and js.total >= 10:
            feat["tlf_jockey_wr"] = round(_loo_rate(js.wins, js.total, gr_win), 5)
            feat["tlf_jockey_place_rate"] = round(_loo_rate(js.places, js.total, gr_place), 5)
            fills["tlf_jockey_wr"] += 1
            fills["tlf_jockey_place_rate"] += 1

        # Trainer target stats
        ts = trainer_st.get(trainer)
        if ts and ts.total >= 10:
            feat["tlf_trainer_wr"] = round(_loo_rate(ts.wins, ts.total, gr_win), 5)
            feat["tlf_trainer_place_rate"] = round(_loo_rate(ts.places, ts.total, gr_place), 5)
            fills["tlf_trainer_wr"] += 1
            fills["tlf_trainer_place_rate"] += 1

        # Sire target stats
        ss = sire_st.get(sire)
        if ss and ss.total >= 20:
            feat["tlf_sire_wr"] = round(_loo_rate(ss.wins, ss.total, gr_win), 5)
            fills["tlf_sire_wr"] += 1

        # Hippodrome target stats
        hps = hippo_st.get(hippo)
        if hps and hps.total >= 50:
            feat["tlf_hippo_avg_pos"] = round(hps.pos_sum / hps.total, 4)
            fills["tlf_hippo_avg_pos"] += 1

        # Jockey-Trainer combo
        jts = jt_st.get(jt_key) if jt_key else None
        if jts and jts.total >= 5:
            feat["tlf_jt_combo_wr"] = round(_loo_rate(jts.wins, jts.total, gr_win), 5)
            fills["tlf_jt_combo_wr"] += 1

        # Composite: weighted combination of all target stats
        weights = []
        if hs and hs.total > 0:
            weights.append((_loo_rate(hs.wins, hs.total, gr_win), 3.0))  # Horse most important
        if js and js.total >= 10:
            weights.append((_loo_rate(js.wins, js.total, gr_win), 1.5))
        if ts and ts.total >= 10:
            weights.append((_loo_rate(ts.wins, ts.total, gr_win), 1.0))
        if ss and ss.total >= 20:
            weights.append((_loo_rate(ss.wins, ss.total, gr_win), 0.5))

        if weights:
            wsum = sum(v * w for v, w in weights)
            wtotal = sum(w for _, w in weights)
            feat["tlf_composite_wr"] = round(wsum / wtotal, 5)
            fills["tlf_composite_wr"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = rec.get("jockey_driver") or rec.get("nom_jockey") or ""
        trainer = rec.get("entraineur") or rec.get("nom_entraineur") or ""
        sire = rec.get("pere") or ""
        jt_key = f"{jockey}|{trainer}" if jockey and trainer else ""

        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe(rec.get("position_arrivee"))
        field = _safe(rec.get("nombre_partants"))
        is_placed = False
        try:
            is_placed = int(rec.get("position_arrivee", 99)) <= 3
        except (TypeError, ValueError):
            pass

        norm_pos = pos / field if pos is not None and field and field > 0 else None

        gw += int(is_winner)
        gp += int(is_placed)
        gt += 1
        if norm_pos is not None:
            gps += norm_pos

        for entity_key, stats_dict in [
            (hid, horse_st), (jockey, jockey_st), (trainer, trainer_st),
            (sire, sire_st), (hippo, hippo_st), (jt_key, jt_st),
        ]:
            if not entity_key:
                continue
            if entity_key not in stats_dict:
                stats_dict[entity_key] = _EntityStats()
            s = stats_dict[entity_key]
            s.wins += int(is_winner)
            s.places += int(is_placed)
            s.total += 1
            if norm_pos is not None:
                s.pos_sum += norm_pos
            s.recent_wins.append(1 if is_winner else 0)

    return gw, gp, gt, gps


if __name__ == "__main__":
    main()
