#!/usr/bin/env python3
"""Combination deep features: horse×jockey×trainer×distance multi-way interactions,
unique combo tracking, and combo win rate estimation."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/combinaison_marche_deep")
_LOG_EVERY = 500_000

# Bayesian shrinkage prior
_K = 10  # shrinkage strength


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _dist_bucket(d):
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "stay"
    return "long"


class _ComboStats:
    __slots__ = ("wins", "total")

    def __init__(self):
        self.wins = 0
        self.total = 0


def main():
    logger = setup_logging("combinaison_marche_deep_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "combinaison_marche_deep_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Pair/triple stats
    hj_stats: dict[str, _ComboStats] = {}   # horse×jockey
    ht_stats: dict[str, _ComboStats] = {}   # horse×trainer
    hd_stats: dict[str, _ComboStats] = {}   # horse×distance
    jd_stats: dict[str, _ComboStats] = {}   # jockey×distance
    hjt_stats: dict[str, _ComboStats] = {}  # horse×jockey×trainer
    global_wr_num = 0
    global_wr_den = 0

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
                    gwr = global_wr_num / global_wr_den if global_wr_den > 100 else 0.1
                    _process_course(course_records, fout, hj_stats, ht_stats,
                                    hd_stats, jd_stats, hjt_stats, gwr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            gwr = global_wr_num / global_wr_den if global_wr_den > 100 else 0.1
            _process_course(course_records, fout, hj_stats, ht_stats,
                            hd_stats, jd_stats, hjt_stats, gwr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _shrunk_wr(stats, gwr):
    if stats is None or stats.total < 2:
        return None
    return (stats.total * (stats.wins / stats.total) + _K * gwr) / (stats.total + _K)


def _process_course(records, fout, hj_stats, ht_stats, hd_stats,
                    jd_stats, hjt_stats, gwr, fills):
    distance = _safe(records[0].get("distance"))
    dist_b = _dist_bucket(distance)

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()

        if hid:
            # Horse × Jockey
            if jockey:
                hj_key = f"{hid}|{jockey}"
                hj = hj_stats.get(hj_key)
                wr = _shrunk_wr(hj, gwr)
                if wr is not None:
                    feat["cm_hj_wr"] = round(wr, 5)
                    feat["cm_hj_runs"] = hj.total
                    fills["cm_hj_wr"] += 1
                    fills["cm_hj_runs"] += 1

                # Is first time this combo?
                feat["cm_hj_first"] = 1 if hj is None else 0
                fills["cm_hj_first"] += 1

            # Horse × Trainer
            if trainer:
                ht_key = f"{hid}|{trainer}"
                ht = ht_stats.get(ht_key)
                wr = _shrunk_wr(ht, gwr)
                if wr is not None:
                    feat["cm_ht_wr"] = round(wr, 5)
                    fills["cm_ht_wr"] += 1

            # Horse × Distance
            if dist_b:
                hd_key = f"{hid}|{dist_b}"
                hd = hd_stats.get(hd_key)
                wr = _shrunk_wr(hd, gwr)
                if wr is not None:
                    feat["cm_hd_wr"] = round(wr, 5)
                    feat["cm_hd_runs"] = hd.total
                    fills["cm_hd_wr"] += 1
                    fills["cm_hd_runs"] += 1

                feat["cm_hd_first"] = 1 if hd is None else 0
                fills["cm_hd_first"] += 1

            # Jockey × Distance
            if jockey and dist_b:
                jd_key = f"{jockey}|{dist_b}"
                jd = jd_stats.get(jd_key)
                wr = _shrunk_wr(jd, gwr)
                if wr is not None:
                    feat["cm_jd_wr"] = round(wr, 5)
                    fills["cm_jd_wr"] += 1

            # Triple: Horse × Jockey × Trainer
            if jockey and trainer:
                hjt_key = f"{hid}|{jockey}|{trainer}"
                hjt = hjt_stats.get(hjt_key)
                if hjt and hjt.total >= 3:
                    feat["cm_hjt_wr"] = round(hjt.wins / hjt.total, 5)
                    feat["cm_hjt_runs"] = hjt.total
                    fills["cm_hjt_wr"] += 1
                    fills["cm_hjt_runs"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))

        if not hid:
            continue

        if jockey:
            hj_key = f"{hid}|{jockey}"
            if hj_key not in hj_stats:
                hj_stats[hj_key] = _ComboStats()
            hj_stats[hj_key].wins += int(is_winner)
            hj_stats[hj_key].total += 1

        if trainer:
            ht_key = f"{hid}|{trainer}"
            if ht_key not in ht_stats:
                ht_stats[ht_key] = _ComboStats()
            ht_stats[ht_key].wins += int(is_winner)
            ht_stats[ht_key].total += 1

        if dist_b:
            hd_key = f"{hid}|{dist_b}"
            if hd_key not in hd_stats:
                hd_stats[hd_key] = _ComboStats()
            hd_stats[hd_key].wins += int(is_winner)
            hd_stats[hd_key].total += 1

        if jockey and dist_b:
            jd_key = f"{jockey}|{dist_b}"
            if jd_key not in jd_stats:
                jd_stats[jd_key] = _ComboStats()
            jd_stats[jd_key].wins += int(is_winner)
            jd_stats[jd_key].total += 1

        if jockey and trainer:
            hjt_key = f"{hid}|{jockey}|{trainer}"
            if hjt_key not in hjt_stats:
                hjt_stats[hjt_key] = _ComboStats()
            hjt_stats[hjt_key].wins += int(is_winner)
            hjt_stats[hjt_key].total += 1


if __name__ == "__main__":
    main()
