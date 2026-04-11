#!/usr/bin/env python3
"""Track bias detector - STREAMING VERSION. 5 features:
stall/corde bias, front-runner bias, terrain bias, fav-distance bias.
Rolling 365-day lookback per hippodrome.

No OOM: streaming course-by-course from partants_master.jsonl."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/track_bias")
_LOG_EVERY = 500_000
LOOKBACK_DAYS = 365


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    v = _safe(val)
    return int(v) if v is not None else None


def _dist_cat(distance):
    d = _safe(distance)
    if d is None: return None
    if d <= 1200: return "sprint"
    if d <= 1600: return "mile"
    if d <= 2200: return "inter"
    if d <= 3000: return "long"
    return "marathon"


def _corde_bin(place):
    if place is None: return None
    if place <= 4: return "inner"
    if place <= 8: return "middle"
    return "outer"


class _HippoHistory:
    """Lightweight rolling history per hippodrome."""
    __slots__ = ("corde_wins", "corde_total", "terrain_wins", "terrain_total",
                 "front_wins", "front_total", "total_races",
                 "fav_dist_wins", "fav_dist_total")

    def __init__(self):
        # corde -> {wins, total}
        self.corde_wins = defaultdict(int)
        self.corde_total = defaultdict(int)
        # terrain -> {wins, total}
        self.terrain_wins = defaultdict(int)
        self.terrain_total = defaultdict(int)
        # Front-runner (corde <= 4) wins
        self.front_wins = 0
        self.front_total = 0
        self.total_races = 0
        # Distance category -> {fav_wins, total}
        self.fav_dist_wins = defaultdict(int)
        self.fav_dist_total = defaultdict(int)


def main():
    logger = setup_logging("track_bias_detector_v2")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "track_bias_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Per-hippodrome rolling history
    hippo_hist: dict[str, _HippoHistory] = {}

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
                    _process_course(course_records, fout, hippo_hist, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, hippo_hist, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, hippo_hist, fills):
    r0 = records[0]
    hippo = (r0.get("hippodrome_normalise") or "").lower().strip()
    terrain = (r0.get("terrain") or r0.get("etat_terrain") or r0.get("type_piste") or "").lower().strip()
    distance = _safe(r0.get("distance"))
    dc = _dist_cat(distance)

    # Find favourite
    fav_idx = None
    fav_cote = float("inf")
    for i, rec in enumerate(records):
        c = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        if c is not None and c > 0 and c < fav_cote:
            fav_cote = c
            fav_idx = i

    hh = hippo_hist.get(hippo) if hippo else None

    # SNAPSHOT features
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        place_corde = _safe_int(rec.get("place_corde"))

        if hh and hh.total_races >= 20:
            # 1. Corde bias
            if place_corde is not None:
                ct = hh.corde_total.get(place_corde, 0)
                if ct >= 5:
                    cw = hh.corde_wins.get(place_corde, 0)
                    feat["tb_corde_wr"] = round(cw / ct, 4)
                    fills["tb_corde_wr"] += 1

                # Corde bin bias
                cb = _corde_bin(place_corde)
                if cb:
                    ct2 = hh.corde_total.get(cb, 0)
                    if ct2 >= 10:
                        cw2 = hh.corde_wins.get(cb, 0)
                        feat["tb_corde_bin_wr"] = round(cw2 / ct2, 4)
                        fills["tb_corde_bin_wr"] += 1

            # 2. Front-runner bias
            if hh.front_total >= 10:
                feat["tb_front_bias"] = round(hh.front_wins / hh.front_total, 4)
                fills["tb_front_bias"] += 1

            # 3. Terrain bias
            if terrain:
                tt = hh.terrain_total.get(terrain, 0)
                if tt >= 10:
                    tw = hh.terrain_wins.get(terrain, 0)
                    feat["tb_terrain_wr"] = round(tw / tt, 4)
                    fills["tb_terrain_wr"] += 1

            # 4. Fav × distance bias
            if dc:
                fdt = hh.fav_dist_total.get(dc, 0)
                if fdt >= 10:
                    fdw = hh.fav_dist_wins.get(dc, 0)
                    feat["tb_fav_dist_wr"] = round(fdw / fdt, 4)
                    fills["tb_fav_dist_wr"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE history
    if not hippo:
        return

    if hippo not in hippo_hist:
        hippo_hist[hippo] = _HippoHistory()
    hh = hippo_hist[hippo]
    hh.total_races += 1

    fav_won = False
    for i, rec in enumerate(records):
        is_winner = bool(rec.get("is_gagnant"))
        place_corde = _safe_int(rec.get("place_corde"))

        if place_corde is not None:
            hh.corde_total[place_corde] += 1
            if is_winner:
                hh.corde_wins[place_corde] += 1

            cb = _corde_bin(place_corde)
            if cb:
                hh.corde_total[cb] += 1
                if is_winner:
                    hh.corde_wins[cb] += 1

            if place_corde <= 4:
                hh.front_total += 1
                if is_winner:
                    hh.front_wins += 1

        if terrain:
            hh.terrain_total[terrain] += 1
            if is_winner:
                hh.terrain_wins[terrain] += 1

        if i == fav_idx and is_winner:
            fav_won = True

    if dc and fav_idx is not None:
        hh.fav_dist_total[dc] += 1
        if fav_won:
            hh.fav_dist_wins[dc] += 1


if __name__ == "__main__":
    main()
