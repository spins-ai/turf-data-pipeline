#!/usr/bin/env python3
"""Rapport structure aggregator: exploit the 143 rap_ri_e_ columns containing
granular payout data per bet type. Aggregate into meaningful features:
- Number of bet types available per race
- Average/max dividends across bet types
- Payout skew (how top-heavy is the distribution)
- Exotic bet efficiency indicators
- Per-runner: was this horse in winning combis?"""
from __future__ import annotations
import gc, json, math, re, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rap_structure")
_LOG_EVERY = 500_000

# Bet types to scan
_BET_TYPES = [
    "simple_gagnant", "simple_place", "couple_gagnant", "couple_ordre",
    "couple_place", "tierce", "trio", "trio_ordre", "quarte_plus",
    "quinte_plus", "deux_sur_quatre", "multi", "mini_multi",
    "super_quatre", "pick5"
]


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("rap_structure_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "rap_structure_features.jsonl"
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


def _extract_race_payouts(rec):
    """Extract all dividends from rap_ri_e_ columns for a record."""
    all_divs = []
    bet_type_divs = defaultdict(list)

    for key, val in rec.items():
        if not key.startswith("rap_ri_e_"):
            continue
        if "_dividende" not in key:
            continue
        d = _safe(val)
        if d is not None and d > 0:
            all_divs.append(d)
            # Extract bet type
            for bt in _BET_TYPES:
                if bt in key:
                    bet_type_divs[bt].append(d)
                    break

    return all_divs, bet_type_divs


def _extract_horse_in_combis(rec, horse_num):
    """Check if this horse number appears in winning combinations."""
    if not horse_num:
        return 0, 0

    h_str = str(int(horse_num)) if isinstance(horse_num, (int, float)) else str(horse_num)
    in_count = 0
    total_combis = 0

    for key, val in rec.items():
        if not key.startswith("rap_ri_e_"):
            continue
        if "_combinaison" not in key:
            continue
        if val is None:
            continue
        total_combis += 1
        combi_str = str(val)
        # Horse number in combination (separated by - or spaces)
        nums = re.split(r'[\s\-/,]+', combi_str)
        if h_str in nums:
            in_count += 1

    return in_count, total_combis


def _process_course(records, fout, fills):
    # Race-level: extract from first record (race-level fields are shared)
    r0 = records[0]
    all_divs, bet_type_divs = _extract_race_payouts(r0)

    # Race-level features
    race_feats = {}

    # 1. Number of bet types with payouts
    n_bet_types = len(bet_type_divs)
    if n_bet_types > 0:
        race_feats["rs_n_bet_types"] = n_bet_types

    # 2. Total number of payout lines
    if all_divs:
        race_feats["rs_n_payouts"] = len(all_divs)

        # 3. Average dividend
        avg_div = sum(all_divs) / len(all_divs)
        race_feats["rs_avg_dividend"] = round(avg_div, 2)

        # 4. Max dividend (biggest surprise)
        race_feats["rs_max_dividend"] = round(max(all_divs), 2)

        # 5. Min dividend
        race_feats["rs_min_dividend"] = round(min(all_divs), 2)

        # 6. Dividend spread
        if len(all_divs) >= 2:
            race_feats["rs_div_spread"] = round(max(all_divs) - min(all_divs), 2)

        # 7. Log average (tames outliers)
        race_feats["rs_log_avg_div"] = round(math.log1p(avg_div), 3)

        # 8. Payout skew (top-heavy?)
        if len(all_divs) >= 3:
            sorted_divs = sorted(all_divs, reverse=True)
            top_share = sorted_divs[0] / sum(sorted_divs) if sum(sorted_divs) > 0 else 0
            race_feats["rs_top_payout_share"] = round(top_share, 4)

    # 9. Per-bet-type max dividends
    for bt in ["simple_gagnant", "simple_place", "tierce", "quarte_plus", "quinte_plus"]:
        divs = bet_type_divs.get(bt, [])
        if divs:
            race_feats[f"rs_{bt}_max"] = round(max(divs), 2)

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        feat.update(race_feats)

        # Per-runner: horse in combis
        horse_num = rec.get("numero_partant") or rec.get("place_corde")
        in_combis, total_combis = _extract_horse_in_combis(r0, horse_num)

        if total_combis > 0:
            feat["rs_in_combis"] = in_combis
            feat["rs_combi_rate"] = round(in_combis / total_combis, 4)
            fills["rs_in_combis"] += 1
            fills["rs_combi_rate"] += 1

        for k in race_feats:
            fills[k] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
