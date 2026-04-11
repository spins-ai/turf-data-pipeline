#!/usr/bin/env python3
"""Market inefficiency features - STREAMING VERSION.
12 features: odds calibration edge, hippo predictability, steam×odds interaction,
overbet/underbet, field-size adjustment, longshot bias.

No OOM: streaming course-by-course with lightweight accumulators."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_inefficiency")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _odds_bucket(cote):
    if cote is None:
        return None
    if cote < 2: return 0
    if cote < 3: return 1
    if cote < 5: return 2
    if cote < 8: return 3
    if cote < 12: return 4
    if cote < 20: return 5
    if cote < 35: return 6
    if cote < 60: return 7
    return 8


def _drift_dir(opening, final):
    if opening is None or final is None or opening <= 0:
        return None
    pct = (final - opening) / opening * 100
    if pct < -10: return -1
    if pct > 10: return 1
    return 0


def _odds_level(cote):
    if cote is None: return None
    if cote < 5: return "short"
    if cote < 15: return "mid"
    return "long"


def _field_size_cat(nb):
    if nb is None: return None
    if nb < 8: return "small"
    if nb < 12: return "medium"
    if nb < 16: return "large"
    return "xlarge"


def main():
    logger = setup_logging("market_inefficiency_builder_v2")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "market_inefficiency_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Lightweight accumulators (bounded keys: 9 buckets, ~200 hippos, ~12 combos)
    odds_cal = defaultdict(lambda: [0, 0, 0.0])  # [wins, total, implied_sum]
    hippo_fav = defaultdict(lambda: [0, 0])  # [fav_wins, total_courses]
    steam_odds = defaultdict(lambda: [0, 0])  # [wins, total]
    field_fav = defaultdict(lambda: [0, 0])  # [fav_wins, total]

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
                    _process_course(course_records, fout, odds_cal, hippo_fav,
                                    steam_odds, field_fav, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, odds_cal, hippo_fav,
                            steam_odds, field_fav, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, odds_cal, hippo_fav, steam_odds, field_fav, fills):
    nb = len(records)
    fs_cat = _field_size_cat(nb)

    # Find favourite (lowest cote)
    fav_idx = None
    fav_cote = float("inf")
    for i, rec in enumerate(records):
        c = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        if c is not None and c < fav_cote:
            fav_cote = c
            fav_idx = i

    # SNAPSHOT features (read from PAST accumulators)
    features_list = []
    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        opening = _safe(rec.get("cote_reference"))
        hippo = (rec.get("hippodrome_normalise") or "").lower().strip()
        bucket = _odds_bucket(cote)
        dd = _drift_dir(opening, cote)
        ol = _odds_level(cote)
        implied = 1.0 / cote if cote and cote > 1 else None
        so_key = f"{ol}|{dd}" if ol and dd is not None else None

        # 1. Odds bucket
        if bucket is not None:
            feat["mi_odds_bucket"] = bucket
            fills["mi_odds_bucket"] += 1

        # 2. Odds calibration edge
        if bucket is not None:
            h = odds_cal[bucket]
            if h[1] >= 10:
                actual = h[0] / h[1]
                avg_implied = h[2] / h[1]
                feat["mi_odds_cal_edge"] = round(actual - avg_implied, 4)
                fills["mi_odds_cal_edge"] += 1

        # 3-4. Hippo predictability
        if hippo:
            hf = hippo_fav.get(hippo)
            if hf and hf[1] >= 20:
                fav_wr = hf[0] / hf[1]
                feat["mi_hippo_fav_wr"] = round(fav_wr, 4)
                feat["mi_hippo_predictability"] = round(
                    max(0.0, min(1.0, (fav_wr - 0.15) / 0.35)), 4)
                fills["mi_hippo_fav_wr"] += 1
                fills["mi_hippo_predictability"] += 1

        # 5-6. Steam × odds interaction
        feat["mi_drift_dir"] = dd
        if dd is not None:
            fills["mi_drift_dir"] += 1
        if so_key:
            h = steam_odds.get(so_key)
            if h and h[1] >= 10:
                feat["mi_steam_odds_wr"] = round(h[0] / h[1], 4)
                fills["mi_steam_odds_wr"] += 1

        # 7. Overbet score
        if bucket is not None:
            h = odds_cal[bucket]
            if h[1] >= 20:
                actual = h[0] / h[1]
                avg_implied = h[2] / h[1]
                feat["mi_overbet_score"] = round(actual - avg_implied, 4)
                fills["mi_overbet_score"] += 1

        # 8. Field-adjusted implied prob
        if implied is not None and fs_cat:
            ff = field_fav.get(fs_cat)
            if ff and ff[1] >= 20:
                fav_wr = ff[0] / ff[1]
                adj = fav_wr / 0.33 if fav_wr > 0 else 1.0
                feat["mi_field_adj_prob"] = round(implied * adj, 4)
                fills["mi_field_adj_prob"] += 1

        # 9. Is value zone
        if bucket is not None:
            h = odds_cal[bucket]
            if h[1] >= 30:
                actual = h[0] / h[1]
                avg_implied = h[2] / h[1]
                feat["mi_is_value_zone"] = 1 if actual > avg_implied else 0
                fills["mi_is_value_zone"] += 1

        # 10. Is favourite
        feat["mi_is_fav"] = 1 if i == fav_idx else 0
        fills["mi_is_fav"] += 1

        # 11. Fav edge vs field size
        if implied is not None and fs_cat:
            ff = field_fav.get(fs_cat)
            if ff and ff[1] >= 20:
                fav_wr = ff[0] / ff[1]
                feat["mi_fav_edge_field"] = round(fav_wr - implied, 4)
                fills["mi_fav_edge_field"] += 1

        # 12. Longshot bias
        if bucket is not None and implied is not None:
            h = odds_cal[bucket]
            if h[1] >= 20:
                actual = h[0] / h[1]
                feat["mi_longshot_bias"] = round(implied - actual, 4)
                fills["mi_longshot_bias"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators AFTER writing
    fav_won = False
    for i, rec in enumerate(records):
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        opening = _safe(rec.get("cote_reference"))
        is_winner = bool(rec.get("is_gagnant"))
        bucket = _odds_bucket(cote)
        dd = _drift_dir(opening, cote)
        ol = _odds_level(cote)
        so_key = f"{ol}|{dd}" if ol and dd is not None else None

        if bucket is not None and cote and cote > 1:
            odds_cal[bucket][1] += 1
            odds_cal[bucket][2] += 1.0 / cote
            if is_winner:
                odds_cal[bucket][0] += 1

        if so_key:
            steam_odds[so_key][1] += 1
            if is_winner:
                steam_odds[so_key][0] += 1

        if i == fav_idx and is_winner:
            fav_won = True

    # Update hippo and field fav stats
    if fav_idx is not None:
        hippo = (records[fav_idx].get("hippodrome_normalise") or "").lower().strip()
        if hippo:
            hippo_fav[hippo][1] += 1
            if fav_won:
                hippo_fav[hippo][0] += 1
        if fs_cat:
            field_fav[fs_cat][1] += 1
            if fav_won:
                field_fav[fs_cat][0] += 1


if __name__ == "__main__":
    main()
