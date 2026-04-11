#!/usr/bin/env python3
"""Photo finish frequency builder - STREAMING. 5 features:
horse_close_finish_pct, horse_narrow_wins, horse_narrow_losses,
race_competitiveness_score, horse_clutch_rating.

Horses that frequently finish in tight margins may be more competitive.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, re, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/photo_finish")
_LOG_EVERY = 500_000

_RE_LENGTHS = re.compile(r"(\d+)\s*L?\s*(1/[24]|3/4)?", re.IGNORECASE)
_FRAC_MAP = {"1/4": 0.25, "1/2": 0.5, "3/4": 0.75}
_SPECIAL = {"nez": 0.05, "courte tete": 0.1, "ct": 0.1, "courte encolure": 0.2, "ce": 0.2, "tete": 0.15, "encolure": 0.25}


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _parse_ecart(raw):
    if not raw:
        return None
    s = str(raw).lower().strip()
    if not s: return None
    for key, val in _SPECIAL.items():
        if key in s: return val
    m = _RE_LENGTHS.search(s)
    if m:
        val = int(m.group(1))
        frac = _FRAC_MAP.get(m.group(2), 0.0) if m.group(2) else 0.0
        return val + frac
    try:
        return float(s)
    except ValueError:
        return None


def main():
    logger = setup_logging("photo_finish_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "photo_finish_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # horse -> deque of (ecart, position, is_winner)
    horse_finishes = defaultdict(lambda: deque(maxlen=30))

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
                    _process_course(course_records, fout, horse_finishes, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_finishes, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_finishes, fills):
    # Parse all ecarts
    ecarts = [_parse_ecart(rec.get("ecart_precedent")) for rec in records]
    valid_ecarts = [e for e in ecarts if e is not None]

    # Race-level competitiveness
    race_comp = None
    if len(valid_ecarts) >= 3:
        close_count = sum(1 for e in valid_ecarts if e < 1.0)
        race_comp = close_count / len(valid_ecarts)

    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        is_winner = bool(rec.get("is_gagnant"))

        # 1. Race competitiveness score
        if race_comp is not None:
            feat["pf_race_competitiveness"] = round(race_comp, 4)
            fills["pf_race_competitiveness"] += 1

        # Horse history
        if horse and len(horse) >= 2:
            hist = horse_finishes.get(horse)
            if hist and len(hist) >= 5:
                hist_list = list(hist)
                n = len(hist_list)

                # Count close finishes (ecart < 1 length or winner)
                close = sum(1 for e, pos, w in hist_list if (e is not None and e < 1.0) or w)
                feat["pf_close_finish_pct"] = round(close / n, 4)
                fills["pf_close_finish_pct"] += 1

                # 2. Narrow wins (won by < 1 length)
                narrow_wins = sum(1 for e, pos, w in hist_list if w)
                total_close = sum(1 for e, pos, w in hist_list if e is not None and e < 1.0)

                feat["pf_narrow_wins"] = narrow_wins
                fills["pf_narrow_wins"] += 1

                # 3. Narrow losses (lost by < 1 length)
                narrow_losses = sum(1 for e, pos, w in hist_list if not w and e is not None and e < 1.0)
                feat["pf_narrow_losses"] = narrow_losses
                fills["pf_narrow_losses"] += 1

                # 4. Clutch rating: wins / (wins + narrow losses)
                if narrow_wins + narrow_losses > 0:
                    feat["pf_clutch_rating"] = round(narrow_wins / (narrow_wins + narrow_losses), 4)
                    fills["pf_clutch_rating"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE
    for i, rec in enumerate(records):
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe(rec.get("position_arrivee"))
        e = ecarts[i] if i < len(ecarts) else None

        if horse and len(horse) >= 2:
            horse_finishes[horse].append((e if not is_winner else 0.0, pos, is_winner))


if __name__ == "__main__":
    main()
