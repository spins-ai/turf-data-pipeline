#!/usr/bin/env python3
"""Rapport/payout features: exploit the rich rap_ columns containing
actual race payouts, dividends, and market concentration data.
Cross payouts with odds for calibration and value detection."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rapport_payout")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _HorsePayoutState:
    __slots__ = ("dividends", "total", "wins")

    def __init__(self):
        self.dividends = deque(maxlen=20)
        self.total = 0
        self.wins = 0


def main():
    logger = setup_logging("rapport_payout_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "rapport_payout_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorsePayoutState] = {}

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
                    _process_course(course_records, fout, horse_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, fills):
    # Race-level payout data (from any record, they share race-level fields)
    r0 = records[0]
    dividend_moy = _safe(r0.get("rap_dividend_moyen"))
    market_conc = _safe(r0.get("rap_market_concentration"))
    audience = (r0.get("rap_audience") or "").strip()

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        dividende = _safe(rec.get("rap_dividende")) or _safe(rec.get("rap_dividende_euros"))
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))

        # 1. Average dividend of the race (race quality indicator)
        if dividend_moy is not None:
            feat["rp_dividend_moyen"] = round(dividend_moy, 2)
            feat["rp_log_dividend"] = round(math.log1p(dividend_moy), 3)
            fills["rp_dividend_moyen"] += 1
            fills["rp_log_dividend"] += 1

        # 2. Market concentration (how predictable was the race)
        if market_conc is not None:
            feat["rp_market_concentration"] = round(market_conc, 4)
            fills["rp_market_concentration"] += 1

        # 3. Audience (race prestige)
        if audience:
            is_national = 1 if audience.upper() == "NATIONAL" else 0
            feat["rp_is_national"] = is_national
            fills["rp_is_national"] += 1

        # 4. Horse payout history
        if hid:
            st = horse_states.get(hid)
            if st and st.total >= 3 and st.dividends:
                divs = list(st.dividends)
                avg_div = sum(divs) / len(divs)
                feat["rp_avg_past_dividend"] = round(avg_div, 2)
                fills["rp_avg_past_dividend"] += 1

                # High payout history (longshot history)
                high_payouts = sum(1 for d in divs if d > 50)
                feat["rp_high_payout_count"] = high_payouts
                fills["rp_high_payout_count"] += 1

        # 5. Cote vs dividend calibration
        if cote and dividende:
            # If dividend/cote ratio > 1, the race was predictable
            feat["rp_cote_div_ratio"] = round(dividende / cote, 3)
            fills["rp_cote_div_ratio"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        is_winner = bool(rec.get("is_gagnant"))
        dividende = _safe(rec.get("rap_dividende")) or _safe(rec.get("rap_dividende_euros"))

        if hid:
            if hid not in horse_states:
                horse_states[hid] = _HorsePayoutState()
            st = horse_states[hid]
            st.total += 1
            if is_winner and dividende:
                st.dividends.append(dividende)
            st.wins += int(is_winner)


if __name__ == "__main__":
    main()
