#!/usr/bin/env python3
"""Market exotic features: advanced odds analysis - implied probability,
overround, odds compression, favorite strength, and market consensus."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_exotic")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("market_exotic_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "market_exotic_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

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
                    _process_course(course_records, fout, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, fills):
    # Collect all odds in the field
    odds_list = []
    for rec in records:
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference")) or _safe(rec.get("cote_probable"))
        if cote is not None:
            odds_list.append(cote)

    # Implied probabilities and overround
    implied_probs = {}
    overround = None
    if odds_list:
        total_ip = sum(1.0 / o for o in odds_list if o > 0)
        if total_ip > 0:
            overround = total_ip  # >1 means bookmaker margin
            for i, rec in enumerate(records):
                cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference")) or _safe(rec.get("cote_probable"))
                if cote and cote > 0:
                    implied_probs[rec.get("partant_uid", "")] = (1.0 / cote) / total_ip

    # Sorted odds for ranking
    sorted_odds = sorted(odds_list) if odds_list else []
    fav_cote = sorted_odds[0] if sorted_odds else None
    second_fav = sorted_odds[1] if len(sorted_odds) >= 2 else None

    # Odds stats
    odds_mean = sum(odds_list) / len(odds_list) if odds_list else None
    odds_std = None
    if odds_list and len(odds_list) >= 3 and odds_mean:
        odds_std = math.sqrt(sum((o - odds_mean) ** 2 for o in odds_list) / len(odds_list))

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference")) or _safe(rec.get("cote_probable"))
        puid = rec.get("partant_uid", "")

        if cote is not None:
            # 1. Implied probability (normalized)
            if puid in implied_probs:
                feat["me_implied_prob"] = round(implied_probs[puid], 5)
                fills["me_implied_prob"] += 1

            # 2. Raw implied prob
            feat["me_raw_implied_prob"] = round(1.0 / cote, 5)
            fills["me_raw_implied_prob"] += 1

            # 3. Log odds
            feat["me_log_odds"] = round(math.log(cote), 4)
            fills["me_log_odds"] += 1

            # 4. Odds rank
            if sorted_odds:
                rank = sorted(range(len(sorted_odds)), key=lambda i: abs(sorted_odds[i] - cote))[0] + 1
                # More accurate: count how many have lower odds
                rank = sum(1 for o in odds_list if o < cote) + 1
                feat["me_odds_rank"] = rank
                feat["me_is_favorite"] = 1 if rank == 1 else 0
                feat["me_is_top3_market"] = 1 if rank <= 3 else 0
                fills["me_odds_rank"] += 1
                fills["me_is_favorite"] += 1
                fills["me_is_top3_market"] += 1

            # 5. Odds vs field mean
            if odds_mean is not None:
                feat["me_odds_vs_mean"] = round(cote - odds_mean, 2)
                fills["me_odds_vs_mean"] += 1

            # 6. Odds z-score
            if odds_std is not None and odds_std > 0:
                feat["me_odds_zscore"] = round((cote - odds_mean) / odds_std, 3)
                fills["me_odds_zscore"] += 1

            # 7. Favorite strength (gap between fav and 2nd)
            if fav_cote is not None and second_fav is not None and fav_cote > 0:
                feat["me_fav_gap"] = round(second_fav - fav_cote, 2)
                feat["me_fav_dominance"] = round(second_fav / fav_cote, 3)
                fills["me_fav_gap"] += 1
                fills["me_fav_dominance"] += 1

            # 8. Overround
            if overround is not None:
                feat["me_overround"] = round(overround, 4)
                fills["me_overround"] += 1

        # 9. Field size
        feat["me_field_size"] = len(records)
        fills["me_field_size"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
