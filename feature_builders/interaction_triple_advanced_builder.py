#!/usr/bin/env python3
"""Advanced triple interaction features: jockey×hippo, trainer×hippo,
jockey×distance×surface, horse×hippo combinations with shrunk win rates."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/interaction_triple_advanced")
_LOG_EVERY = 500_000
_K = 10  # shrinkage


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


class _PairStats:
    __slots__ = ("wins", "places", "total")
    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0


def main():
    logger = setup_logging("interaction_triple_advanced_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "interaction_triple_advanced_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    jh_stats: dict[str, _PairStats] = {}   # jockey×hippo
    th_stats: dict[str, _PairStats] = {}   # trainer×hippo
    hh_stats: dict[str, _PairStats] = {}   # horse×hippo
    jds_stats: dict[str, _PairStats] = {}  # jockey×dist×surface
    tds_stats: dict[str, _PairStats] = {}  # trainer×dist
    global_wins = 0
    global_total = 0

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
                    gwr = global_wins / global_total if global_total > 100 else 0.1
                    _process_course(course_records, fout, jh_stats, th_stats,
                                    hh_stats, jds_stats, tds_stats, gwr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            gwr = global_wins / global_total if global_total > 100 else 0.1
            _process_course(course_records, fout, jh_stats, th_stats,
                            hh_stats, jds_stats, tds_stats, gwr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _shrunk(st, gwr):
    if st is None or st.total < 3:
        return None
    raw = st.wins / st.total
    return (st.total * raw + _K * gwr) / (st.total + _K)


def _process_course(records, fout, jh_stats, th_stats, hh_stats,
                    jds_stats, tds_stats, gwr, fills):
    hippo = (records[0].get("hippodrome_normalise") or records[0].get("hippodrome") or records[0].get("nom_hippodrome") or "").strip()
    distance = _safe(records[0].get("distance"))
    dist_b = _dist_bucket(distance)
    surface = (records[0].get("surface") or records[0].get("type_piste") or "").strip().lower()

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()

        # Jockey × Hippo
        if jockey and hippo:
            key = f"{jockey}|{hippo}"
            wr = _shrunk(jh_stats.get(key), gwr)
            if wr is not None:
                feat["ita_jockey_hippo_wr"] = round(wr, 5)
                feat["ita_jockey_hippo_runs"] = jh_stats[key].total
                fills["ita_jockey_hippo_wr"] += 1
                fills["ita_jockey_hippo_runs"] += 1

        # Trainer × Hippo
        if trainer and hippo:
            key = f"{trainer}|{hippo}"
            wr = _shrunk(th_stats.get(key), gwr)
            if wr is not None:
                feat["ita_trainer_hippo_wr"] = round(wr, 5)
                feat["ita_trainer_hippo_runs"] = th_stats[key].total
                fills["ita_trainer_hippo_wr"] += 1
                fills["ita_trainer_hippo_runs"] += 1

        # Horse × Hippo
        if hid and hippo:
            key = f"{hid}|{hippo}"
            hh = hh_stats.get(key)
            if hh and hh.total >= 2:
                feat["ita_horse_hippo_runs"] = hh.total
                feat["ita_horse_hippo_pr"] = round(hh.places / hh.total, 4)
                fills["ita_horse_hippo_runs"] += 1
                fills["ita_horse_hippo_pr"] += 1

        # Jockey × Distance × Surface
        if jockey and dist_b and surface:
            key = f"{jockey}|{dist_b}|{surface}"
            wr = _shrunk(jds_stats.get(key), gwr)
            if wr is not None:
                feat["ita_jockey_dist_surf_wr"] = round(wr, 5)
                fills["ita_jockey_dist_surf_wr"] += 1

        # Trainer × Distance
        if trainer and dist_b:
            key = f"{trainer}|{dist_b}"
            wr = _shrunk(tds_stats.get(key), gwr)
            if wr is not None:
                feat["ita_trainer_dist_wr"] = round(wr, 5)
                fills["ita_trainer_dist_wr"] += 1

        # Combo novelty score (how many combos are new)
        novelty = 0
        checks = 0
        if jockey and hippo:
            checks += 1
            if f"{jockey}|{hippo}" not in jh_stats: novelty += 1
        if hid and hippo:
            checks += 1
            if f"{hid}|{hippo}" not in hh_stats: novelty += 1
        if jockey and dist_b and surface:
            checks += 1
            if f"{jockey}|{dist_b}|{surface}" not in jds_stats: novelty += 1
        if checks > 0:
            feat["ita_combo_novelty"] = round(novelty / checks, 3)
            fills["ita_combo_novelty"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe(rec.get("place_arrivee"))
        is_place = int(pos is not None and pos <= 3)

        def _update(d, key):
            if key not in d:
                d[key] = _PairStats()
            d[key].wins += int(is_winner)
            d[key].places += is_place
            d[key].total += 1

        if jockey and hippo: _update(jh_stats, f"{jockey}|{hippo}")
        if trainer and hippo: _update(th_stats, f"{trainer}|{hippo}")
        if hid and hippo: _update(hh_stats, f"{hid}|{hippo}")
        if jockey and dist_b and surface: _update(jds_stats, f"{jockey}|{dist_b}|{surface}")
        if trainer and dist_b: _update(tds_stats, f"{trainer}|{dist_b}")


if __name__ == "__main__":
    main()
