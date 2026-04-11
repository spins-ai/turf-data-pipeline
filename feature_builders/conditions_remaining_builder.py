#!/usr/bin/env python3
"""Exploit remaining underused columns:
- cnd_cond_depart (autostart vs volte)
- cnd_cond_sexe (sex-restricted races)
- met_nebulosite, met_vent_direction (cloud cover, wind dir)
- mkt_public_overbet (public betting bias)
- allocation_partant (prize money per runner)
- mto_precipitation_mm (alt precip source)
Cross with race context for richer features."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/conditions_remaining")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


# Wind direction encoding (headwind/tailwind proxy using compass degrees)
_WIND_DIR_MAP = {
    "n": 0, "nne": 22.5, "ne": 45, "ene": 67.5,
    "e": 90, "ese": 112.5, "se": 135, "sse": 157.5,
    "s": 180, "ssw": 202.5, "sw": 225, "wsw": 247.5,
    "w": 270, "wnw": 292.5, "nw": 315, "nnw": 337.5,
}


class _HorseStartState:
    __slots__ = ("autostart_wins", "autostart_total",
                 "volte_wins", "volte_total", "total")

    def __init__(self):
        self.autostart_wins = 0
        self.autostart_total = 0
        self.volte_wins = 0
        self.volte_total = 0
        self.total = 0


def main():
    logger = setup_logging("conditions_remaining_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "conditions_remaining_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseStartState] = {}

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
    r0 = records[0]

    # Race-level conditions
    depart = (r0.get("cnd_cond_depart") or "").strip().lower()
    sexe_cond = (r0.get("cnd_cond_sexe") or "").strip().lower()
    nebulosite = _safe(r0.get("met_nebulosite"))
    vent_dir = (r0.get("met_vent_direction") or "").strip().lower()
    precipitation = _safe(r0.get("mto_precipitation_mm"))

    # Field-level allocation stats
    field_alloc = []
    for rec in records:
        a = _safe(rec.get("allocation_partant"))
        if a is not None and a > 0:
            field_alloc.append(a)

    avg_alloc = sum(field_alloc) / len(field_alloc) if field_alloc else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""

        # 1. Start type (autostart vs volte — critical for trot)
        if depart:
            is_auto = "auto" in depart
            is_volte = "volt" in depart
            feat["cr_is_autostart"] = 1 if is_auto else 0
            feat["cr_is_volte"] = 1 if is_volte else 0
            fills["cr_is_autostart"] += 1
            fills["cr_is_volte"] += 1

            # Horse start type history
            if hid:
                st = horse_states.get(hid)
                if st and st.total >= 5:
                    if is_auto and st.autostart_total >= 3:
                        feat["cr_horse_autostart_wr"] = round(
                            st.autostart_wins / st.autostart_total, 5)
                        fills["cr_horse_autostart_wr"] += 1
                    if is_volte and st.volte_total >= 3:
                        feat["cr_horse_volte_wr"] = round(
                            st.volte_wins / st.volte_total, 5)
                        fills["cr_horse_volte_wr"] += 1

                    # Preference ratio
                    if st.autostart_total >= 3 and st.volte_total >= 3:
                        auto_wr = st.autostart_wins / st.autostart_total
                        volte_wr = st.volte_wins / st.volte_total
                        feat["cr_start_pref"] = round(auto_wr - volte_wr, 5)
                        fills["cr_start_pref"] += 1

        # 2. Sex-restricted race
        if sexe_cond:
            feat["cr_sex_restricted"] = 1
            feat["cr_females_only"] = 1 if "femelle" in sexe_cond or "jument" in sexe_cond else 0
            fills["cr_sex_restricted"] += 1
            fills["cr_females_only"] += 1
        else:
            feat["cr_sex_restricted"] = 0
            fills["cr_sex_restricted"] += 1

        # 3. Cloud cover
        if nebulosite is not None:
            feat["cr_cloud_cover"] = round(nebulosite, 1)
            fills["cr_cloud_cover"] += 1
            feat["cr_is_overcast"] = 1 if nebulosite >= 7 else 0
            fills["cr_is_overcast"] += 1

        # 4. Wind direction (encode as sin/cos for cyclical nature)
        if vent_dir and vent_dir in _WIND_DIR_MAP:
            deg = _WIND_DIR_MAP[vent_dir]
            rad = math.radians(deg)
            feat["cr_wind_sin"] = round(math.sin(rad), 4)
            feat["cr_wind_cos"] = round(math.cos(rad), 4)
            fills["cr_wind_sin"] += 1
            fills["cr_wind_cos"] += 1

        # 5. Alternative precipitation source
        if precipitation is not None:
            feat["cr_precip_mm"] = round(precipitation, 1)
            fills["cr_precip_mm"] += 1

        # 6. Public overbet signal
        overbet = _safe(rec.get("mkt_public_overbet"))
        if overbet is not None:
            feat["cr_public_overbet"] = round(overbet, 3)
            fills["cr_public_overbet"] += 1

        # 7. Allocation per runner (class indicator)
        alloc = _safe(rec.get("allocation_partant"))
        if alloc is not None and alloc > 0:
            feat["cr_allocation"] = round(alloc, 0)
            feat["cr_log_allocation"] = round(math.log1p(alloc), 3)
            fills["cr_allocation"] += 1
            fills["cr_log_allocation"] += 1

            if avg_alloc is not None and avg_alloc > 0:
                feat["cr_alloc_vs_field"] = round(alloc / avg_alloc, 3)
                fills["cr_alloc_vs_field"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        is_winner = bool(rec.get("is_gagnant"))

        if hid and depart:
            if hid not in horse_states:
                horse_states[hid] = _HorseStartState()
            st = horse_states[hid]
            st.total += 1
            if "auto" in depart:
                st.autostart_wins += int(is_winner)
                st.autostart_total += 1
            elif "volt" in depart:
                st.volte_wins += int(is_winner)
                st.volte_total += 1


if __name__ == "__main__":
    main()
