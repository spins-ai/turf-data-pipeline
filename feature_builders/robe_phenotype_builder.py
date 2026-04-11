#!/usr/bin/env python3
"""Robe/phenotype features: coat color performance stats, sex-based stats,
and robe×distance/surface interactions."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/robe_phenotype")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _dist_bucket(d):
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "stay"
    return "long"


class _RobeStats:
    __slots__ = ("wins", "total", "wins_by_dist", "total_by_dist")

    def __init__(self):
        self.wins = 0
        self.total = 0
        self.wins_by_dist = defaultdict(int)
        self.total_by_dist = defaultdict(int)


class _SexStats:
    __slots__ = ("wins", "total")

    def __init__(self):
        self.wins = 0
        self.total = 0


def main():
    logger = setup_logging("robe_phenotype_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "robe_phenotype_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    robe_stats: dict[str, _RobeStats] = {}
    sex_stats: dict[str, _SexStats] = {}
    robe_sex_wins: dict[str, int] = defaultdict(int)
    robe_sex_total: dict[str, int] = defaultdict(int)

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
                    _process_course(course_records, fout, robe_stats, sex_stats,
                                    robe_sex_wins, robe_sex_total, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, robe_stats, sex_stats,
                            robe_sex_wins, robe_sex_total, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, robe_stats, sex_stats,
                    robe_sex_wins, robe_sex_total, fills):
    distance = _safe(records[0].get("distance"))
    dist_b = _dist_bucket(distance)

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        robe = (rec.get("robe") or rec.get("pgr_robe") or "").strip().lower()
        sexe = (rec.get("sexe") or rec.get("pgr_sexe") or "").strip().lower()

        # Robe features
        if robe:
            rst = robe_stats.get(robe)
            if rst and rst.total >= 50:
                feat["rp_robe_wr"] = round(rst.wins / rst.total, 5)
                fills["rp_robe_wr"] += 1

                # Robe × distance
                if dist_b and rst.total_by_dist.get(dist_b, 0) >= 20:
                    feat["rp_robe_dist_wr"] = round(
                        rst.wins_by_dist[dist_b] / rst.total_by_dist[dist_b], 5)
                    fills["rp_robe_dist_wr"] += 1

        # Sex features
        if sexe:
            sst = sex_stats.get(sexe)
            if sst and sst.total >= 100:
                feat["rp_sex_wr"] = round(sst.wins / sst.total, 5)
                fills["rp_sex_wr"] += 1

        # Robe × sex interaction
        if robe and sexe:
            rs_key = f"{robe}|{sexe}"
            rs_t = robe_sex_total.get(rs_key, 0)
            if rs_t >= 30:
                feat["rp_robe_sex_wr"] = round(robe_sex_wins.get(rs_key, 0) / rs_t, 5)
                fills["rp_robe_sex_wr"] += 1

        # Sex encoding
        if sexe:
            sex_map = {"male": 0, "males": 0, "m": 0,
                       "femelle": 1, "femelles": 1, "f": 1,
                       "hongre": 2, "hongres": 2, "h": 2}
            code = sex_map.get(sexe)
            if code is not None:
                feat["rp_sex_code"] = code
                feat["rp_is_hongre"] = 1 if code == 2 else 0
                feat["rp_is_female"] = 1 if code == 1 else 0
                fills["rp_sex_code"] += 1
                fills["rp_is_hongre"] += 1
                fills["rp_is_female"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        robe = (rec.get("robe") or rec.get("pgr_robe") or "").strip().lower()
        sexe = (rec.get("sexe") or rec.get("pgr_sexe") or "").strip().lower()
        is_winner = bool(rec.get("is_gagnant"))

        if robe:
            if robe not in robe_stats:
                robe_stats[robe] = _RobeStats()
            rst = robe_stats[robe]
            rst.wins += int(is_winner)
            rst.total += 1
            if dist_b:
                rst.wins_by_dist[dist_b] += int(is_winner)
                rst.total_by_dist[dist_b] += 1

        if sexe:
            if sexe not in sex_stats:
                sex_stats[sexe] = _SexStats()
            sst = sex_stats[sexe]
            sst.wins += int(is_winner)
            sst.total += 1

        if robe and sexe:
            rs_key = f"{robe}|{sexe}"
            robe_sex_wins[rs_key] += int(is_winner)
            robe_sex_total[rs_key] += 1


if __name__ == "__main__":
    main()
