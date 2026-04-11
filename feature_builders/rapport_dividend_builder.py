#!/usr/bin/env python3
from __future__ import annotations
import argparse, gc, json, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rapport_dividend")
_LOG_EVERY = 500_000

def _safe(val):
    try: return float(val)
    except: return None

def main():
    logger = setup_logging("rapport_dividend_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "rapport_dividend_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # State tracking dictionaries
    horse_win_dividends = defaultdict(lambda: deque(maxlen=50))
    jockey_win_dividends = defaultdict(lambda: deque(maxlen=100))
    trainer_win_dividends = defaultdict(lambda: deque(maxlen=100))
    hippo_dividends = defaultdict(lambda: deque(maxlen=200))
    discipline_dividends = defaultdict(lambda: deque(maxlen=500))
    hippo_market_conc = defaultdict(lambda: deque(maxlen=100))
    horse_recent_div = defaultdict(lambda: deque(maxlen=10))

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
                    _process_course(course_records, fout, horse_win_dividends, jockey_win_dividends,
                                   trainer_win_dividends, hippo_dividends, discipline_dividends,
                                   hippo_market_conc, horse_recent_div, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,} lines, written {written:,}")
                    gc.collect()

        # Last course
        if course_records:
            _process_course(course_records, fout, horse_win_dividends, jockey_win_dividends,
                           trainer_win_dividends, hippo_dividends, discipline_dividends,
                           hippo_market_conc, horse_recent_div, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v/total*100:.1f}%")


def _process_course(records, fout, horse_win_div, jockey_win_div, trainer_win_div,
                    hippo_div, disc_div, hippo_mc, horse_recent, fills):
    """Snapshot features BEFORE updating state (temporal integrity)."""
    hippo = records[0].get("hippodrome_normalise", "")
    disc = records[0].get("discipline", "")

    features_list = []
    for rec in records:
        hid = rec.get("horse_id", "")
        jockey = rec.get("jockey_driver", "")
        trainer = rec.get("entraineur", "")

        feat = {"partant_uid": rec.get("partant_uid", "")}

        # Horse historical win dividend stats
        hw = list(horse_win_div.get(hid, []))
        if hw:
            feat["rdiv_horse_avg_win_dividend"] = sum(hw) / len(hw)
            feat["rdiv_horse_max_win_dividend"] = max(hw)
            feat["rdiv_horse_win_count"] = len(hw)
            fills["rdiv_horse_avg_win_dividend"] += 1

        # Jockey historical win dividend
        jw = list(jockey_win_div.get(jockey, []))
        if jw:
            feat["rdiv_jockey_avg_win_dividend"] = sum(jw) / len(jw)
            feat["rdiv_jockey_median_win_dividend"] = sorted(jw)[len(jw)//2]
            fills["rdiv_jockey_avg_win_dividend"] += 1

        # Trainer historical win dividend
        tw = list(trainer_win_div.get(trainer, []))
        if tw:
            feat["rdiv_trainer_avg_win_dividend"] = sum(tw) / len(tw)
            fills["rdiv_trainer_avg_win_dividend"] += 1

        # Hippodrome dividend level (track predictability)
        hd = list(hippo_div.get(hippo, []))
        if hd:
            hd_avg = sum(hd) / len(hd)
            feat["rdiv_hippo_avg_dividend"] = hd_avg
            feat["rdiv_hippo_dividend_std"] = (sum((x - hd_avg)**2 for x in hd) / len(hd)) ** 0.5
            fills["rdiv_hippo_avg_dividend"] += 1

        # Discipline dividend level
        dd = list(disc_div.get(disc, []))
        if dd:
            feat["rdiv_discipline_avg_dividend"] = sum(dd) / len(dd)
            fills["rdiv_discipline_avg_dividend"] += 1

        # Market concentration at this hippodrome
        mc = list(hippo_mc.get(hippo, []))
        if mc:
            feat["rdiv_hippo_market_concentration_avg"] = sum(mc) / len(mc)
            fills["rdiv_hippo_market_concentration_avg"] += 1

        # Horse recent dividend environment (last races)
        hr = list(horse_recent.get(hid, []))
        if hr:
            feat["rdiv_horse_recent_avg_race_dividend"] = sum(hr) / len(hr)
            fills["rdiv_horse_recent_avg_race_dividend"] += 1

        # Upset factor: horse avg win dividend vs discipline avg
        if hw and dd:
            disc_avg = sum(dd) / len(dd)
            if disc_avg > 0:
                feat["rdiv_horse_upset_factor"] = (sum(hw) / len(hw)) / disc_avg
                fills["rdiv_horse_upset_factor"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # NOW update state (after features emitted)
    for rec in records:
        hid = rec.get("horse_id", "")
        jockey = rec.get("jockey_driver", "")
        trainer = rec.get("entraineur", "")
        is_winner = rec.get("is_gagnant", False)

        rsg = _safe(rec.get("rap_rapport_simple_gagnant"))
        div_moy = _safe(rec.get("rap_dividend_moyen"))
        mkt_conc = _safe(rec.get("rap_market_concentration"))

        if is_winner and rsg is not None:
            horse_win_div[hid].append(rsg)
            jockey_win_div[jockey].append(rsg)
            trainer_win_div[trainer].append(rsg)

        if rsg is not None:
            hippo_div[hippo].append(rsg)
            disc_div[disc].append(rsg)

        if mkt_conc is not None:
            hippo_mc[hippo].append(mkt_conc)

        if div_moy is not None:
            horse_recent[hid].append(div_moy)


if __name__ == "__main__":
    main()
