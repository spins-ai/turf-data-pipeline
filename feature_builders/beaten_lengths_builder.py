#!/usr/bin/env python3
"""Beaten lengths normalizer - STREAMING. 8 features:
ecart_lengths, ecart_normalized_by_distance, ecart_category,
horse_avg_ecart, horse_ecart_trend, ecart_vs_field_avg,
horse_close_finish_rate, race_tightness.

Parses ecart_precedent (e.g. "4 L", "NEZ", "1 L 1/2") into numeric lengths.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, re, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/beaten_lengths")
_LOG_EVERY = 500_000

# Patterns for parsing ecart
_RE_LENGTHS = re.compile(r"(\d+)\s*L?\s*(1/[24]|3/4)?", re.IGNORECASE)
_FRAC_MAP = {"1/4": 0.25, "1/2": 0.5, "3/4": 0.75}
_SPECIAL = {
    "nez": 0.05, "courte tete": 0.1, "ct": 0.1,
    "courte encolure": 0.2, "ce": 0.2,
    "tete": 0.15, "encolure": 0.25,
}


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _parse_ecart(raw):
    """Parse ecart_precedent string into numeric lengths."""
    if not raw:
        return None
    s = str(raw).lower().strip()
    if not s:
        return None

    # Special cases
    for key, val in _SPECIAL.items():
        if key in s:
            return val

    # Try numeric pattern: "4 L", "1 L 1/2", "12"
    m = _RE_LENGTHS.search(s)
    if m:
        val = int(m.group(1))
        frac = _FRAC_MAP.get(m.group(2), 0.0) if m.group(2) else 0.0
        return val + frac

    # Try pure number
    try:
        return float(s)
    except ValueError:
        return None


def main():
    logger = setup_logging("beaten_lengths_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "beaten_lengths_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Horse ecart history (last 20 races)
    horse_ecarts = defaultdict(lambda: deque(maxlen=20))

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
                    _process_course(course_records, fout, horse_ecarts, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_ecarts, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_ecarts, fills):
    distance = _safe(records[0].get("distance"))

    # Parse ecarts for all runners
    ecarts = []
    for rec in records:
        e = _parse_ecart(rec.get("ecart_precedent"))
        ecarts.append(e)

    # Race-level stats
    valid_ecarts = [e for e in ecarts if e is not None]
    race_avg_ecart = sum(valid_ecarts) / len(valid_ecarts) if valid_ecarts else None
    race_max_ecart = max(valid_ecarts) if valid_ecarts else None

    # SNAPSHOT features
    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        e = ecarts[i]
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        is_winner = bool(rec.get("is_gagnant"))

        if e is not None:
            # 1. Raw ecart in lengths
            feat["bl_ecart_lengths"] = round(e, 2)
            fills["bl_ecart_lengths"] += 1

            # 2. Ecart normalized by distance (lengths per km)
            if distance and distance > 0:
                feat["bl_ecart_per_km"] = round(e / (distance / 1000), 4)
                fills["bl_ecart_per_km"] += 1

            # 3. Ecart vs field average
            if race_avg_ecart is not None and race_avg_ecart > 0:
                feat["bl_ecart_vs_avg"] = round(e / race_avg_ecart, 4)
                fills["bl_ecart_vs_avg"] += 1

        elif is_winner:
            feat["bl_ecart_lengths"] = 0.0
            fills["bl_ecart_lengths"] += 1

        # 4. Race tightness (how close was the finish)
        if race_max_ecart is not None:
            feat["bl_race_tightness"] = round(race_max_ecart, 2)
            fills["bl_race_tightness"] += 1

        # Horse history features
        if horse and len(horse) >= 2:
            hist = horse_ecarts.get(horse)
            if hist and len(hist) >= 3:
                hist_list = list(hist)
                n = len(hist_list)

                # 5. Average ecart over last races
                avg = sum(hist_list) / n
                feat["bl_horse_avg_ecart"] = round(avg, 4)
                fills["bl_horse_avg_ecart"] += 1

                # 6. Ecart trend (last 3 vs previous)
                if n >= 6:
                    recent = sum(hist_list[-3:]) / 3
                    older = sum(hist_list[-6:-3]) / 3
                    feat["bl_ecart_trend"] = round(older - recent, 4)  # positive = improving
                    fills["bl_ecart_trend"] += 1

                # 7. Close finish rate (ecart < 1 length)
                close = sum(1 for x in hist_list if x < 1.0)
                feat["bl_close_finish_rate"] = round(close / n, 4)
                fills["bl_close_finish_rate"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE horse ecart history
    for i, rec in enumerate(records):
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        e = ecarts[i]
        is_winner = bool(rec.get("is_gagnant"))

        if horse and len(horse) >= 2:
            if e is not None:
                horse_ecarts[horse].append(e)
            elif is_winner:
                horse_ecarts[horse].append(0.0)


if __name__ == "__main__":
    main()
