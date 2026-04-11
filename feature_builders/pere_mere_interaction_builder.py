#!/usr/bin/env python3
"""Sire/Dam interaction features: performance stats by father (pere) and mother (mere),
sire×distance affinity, dam×surface, and sire progeny win rates."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pere_mere_interaction")
_LOG_EVERY = 500_000
_K = 15  # shrinkage


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


class _SireStats:
    __slots__ = ("wins", "places", "total", "wins_by_dist", "total_by_dist")

    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0
        self.wins_by_dist = defaultdict(int)
        self.total_by_dist = defaultdict(int)


def main():
    logger = setup_logging("pere_mere_interaction_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "pere_mere_interaction_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    sire_stats: dict[str, _SireStats] = {}
    dam_stats: dict[str, _SireStats] = {}
    sire_dam_wins: dict[str, int] = defaultdict(int)
    sire_dam_total: dict[str, int] = defaultdict(int)
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
                    _process_course(course_records, fout, sire_stats, dam_stats,
                                    sire_dam_wins, sire_dam_total, gwr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            gwr = global_wr_num / global_wr_den if global_wr_den > 100 else 0.1
            _process_course(course_records, fout, sire_stats, dam_stats,
                            sire_dam_wins, sire_dam_total, gwr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, sire_stats, dam_stats,
                    sire_dam_wins, sire_dam_total, gwr, fills):
    distance = _safe(records[0].get("distance"))
    dist_b = _dist_bucket(distance)

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        pere = (rec.get("pere") or rec.get("pgr_pere") or "").strip()
        mere = (rec.get("mere") or rec.get("pgr_mere") or "").strip()

        # Sire features
        if pere:
            sst = sire_stats.get(pere)
            if sst and sst.total >= 20:
                raw_wr = sst.wins / sst.total
                shrunk_wr = (sst.total * raw_wr + _K * gwr) / (sst.total + _K)
                feat["pm_sire_wr"] = round(shrunk_wr, 5)
                feat["pm_sire_pr"] = round(sst.places / sst.total, 5)
                feat["pm_sire_runs"] = sst.total
                fills["pm_sire_wr"] += 1
                fills["pm_sire_pr"] += 1
                fills["pm_sire_runs"] += 1

                # Sire × distance
                if dist_b and sst.total_by_dist.get(dist_b, 0) >= 10:
                    feat["pm_sire_dist_wr"] = round(
                        sst.wins_by_dist[dist_b] / sst.total_by_dist[dist_b], 5)
                    fills["pm_sire_dist_wr"] += 1

        # Dam features
        if mere:
            dst = dam_stats.get(mere)
            if dst and dst.total >= 5:
                feat["pm_dam_wr"] = round(dst.wins / dst.total, 5)
                feat["pm_dam_runs"] = dst.total
                fills["pm_dam_wr"] += 1
                fills["pm_dam_runs"] += 1

        # Sire × Dam cross
        if pere and mere:
            sd_key = f"{pere}|{mere}"
            sd_t = sire_dam_total.get(sd_key, 0)
            if sd_t >= 3:
                feat["pm_sire_dam_wr"] = round(sire_dam_wins.get(sd_key, 0) / sd_t, 5)
                feat["pm_sire_dam_runs"] = sd_t
                fills["pm_sire_dam_wr"] += 1
                fills["pm_sire_dam_runs"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        pere = (rec.get("pere") or rec.get("pgr_pere") or "").strip()
        mere = (rec.get("mere") or rec.get("pgr_mere") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe(rec.get("place_arrivee"))
        is_place = int(pos is not None and pos <= 3)

        if pere:
            if pere not in sire_stats:
                sire_stats[pere] = _SireStats()
            sst = sire_stats[pere]
            sst.wins += int(is_winner)
            sst.places += is_place
            sst.total += 1
            if dist_b:
                sst.wins_by_dist[dist_b] += int(is_winner)
                sst.total_by_dist[dist_b] += 1

        if mere:
            if mere not in dam_stats:
                dam_stats[mere] = _SireStats()
            dst = dam_stats[mere]
            dst.wins += int(is_winner)
            dst.places += is_place
            dst.total += 1

        if pere and mere:
            sd_key = f"{pere}|{mere}"
            sire_dam_wins[sd_key] += int(is_winner)
            sire_dam_total[sd_key] += 1


if __name__ == "__main__":
    main()
