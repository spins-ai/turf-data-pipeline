#!/usr/bin/env python3
"""Meteo × performance cross features: how weather/terrain conditions
interact with horse history, speed, and equipment. Temperature × distance,
rain × terrain specialist, wind × position."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/meteo_cross_performance")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _terrain_code(t):
    if not t:
        return None
    t = t.lower()
    if "lourd" in t or "tres" in t:
        return 4
    if "souple" in t or "collant" in t:
        return 3
    if "bon" in t and "souple" in t:
        return 2
    if "bon" in t or "standard" in t:
        return 1
    if "sec" in t or "leger" in t:
        return 0
    return 1  # default


class _HorseMeteoState:
    __slots__ = ("wins_by_terrain", "total_by_terrain",
                 "wins_rain", "total_rain", "wins_dry", "total_dry",
                 "total")

    def __init__(self):
        self.wins_by_terrain = defaultdict(int)
        self.total_by_terrain = defaultdict(int)
        self.wins_rain = 0
        self.total_rain = 0
        self.wins_dry = 0
        self.total_dry = 0
        self.total = 0


def main():
    logger = setup_logging("meteo_cross_performance_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "meteo_cross_performance_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseMeteoState] = {}

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
    # Race-level meteo
    r0 = records[0]
    terrain = (r0.get("met_terrain_predit") or "").strip()
    terrain_code = _terrain_code(terrain)
    temp = _safe(r0.get("met_temperature"))
    rain = _safe(r0.get("met_meteo_pluie_mm")) or _safe(r0.get("mto_precipitation_mm"))
    wind = _safe(r0.get("met_vent_vitesse"))
    distance = _safe(r0.get("distance"))
    is_rainy = rain is not None and rain > 1.0

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""

        # Met horse specialist fields
        nb_terrain = _safe(rec.get("met_cheval_nb_courses_terrain"))
        taux_vic_terrain = _safe(rec.get("met_cheval_taux_vic_terrain"))
        taux_place_terrain = _safe(rec.get("met_cheval_taux_place_terrain"))
        taux_vic_pluie = _safe(rec.get("met_cheval_taux_vic_pluie"))
        specialist = rec.get("met_cheval_specialist_terrain")
        impact = _safe(rec.get("met_impact_meteo_score"))

        # 1. Terrain code (numeric)
        if terrain_code is not None:
            feat["mcp_terrain_code"] = terrain_code
            fills["mcp_terrain_code"] += 1

        # 2. Temperature × distance interaction
        if temp is not None and distance:
            # Hot + long distance = harder
            feat["mcp_temp_x_dist"] = round(temp * distance / 1000, 1)
            fills["mcp_temp_x_dist"] += 1

            # Temperature buckets
            if temp < 5:
                feat["mcp_is_cold"] = 1
            elif temp > 25:
                feat["mcp_is_hot"] = 1
            else:
                feat["mcp_is_cold"] = 0
                feat["mcp_is_hot"] = 0
            fills["mcp_is_cold"] += 1
            fills["mcp_is_hot"] += 1

        # 3. Rain intensity
        if rain is not None:
            feat["mcp_rain_mm"] = round(rain, 1)
            feat["mcp_is_rainy"] = 1 if is_rainy else 0
            fills["mcp_rain_mm"] += 1
            fills["mcp_is_rainy"] += 1

        # 4. Wind × distance
        if wind is not None:
            feat["mcp_wind_speed"] = wind
            fills["mcp_wind_speed"] += 1
            if distance:
                feat["mcp_wind_x_dist"] = round(wind * distance / 1000, 1)
                fills["mcp_wind_x_dist"] += 1

        # 5. Horse terrain specialist cross
        if nb_terrain is not None and nb_terrain >= 3 and taux_vic_terrain is not None:
            feat["mcp_terrain_specialist_wr"] = round(taux_vic_terrain, 4)
            feat["mcp_terrain_specialist_exp"] = int(nb_terrain)
            fills["mcp_terrain_specialist_wr"] += 1
            fills["mcp_terrain_specialist_exp"] += 1

        if taux_place_terrain is not None:
            feat["mcp_terrain_place_rate"] = round(taux_place_terrain, 4)
            fills["mcp_terrain_place_rate"] += 1

        # 6. Rain specialist
        if taux_vic_pluie is not None:
            feat["mcp_rain_specialist_wr"] = round(taux_vic_pluie, 4)
            fills["mcp_rain_specialist_wr"] += 1

            # Rain specialist × is rainy
            if is_rainy:
                feat["mcp_rain_specialist_active"] = round(taux_vic_pluie, 4)
                fills["mcp_rain_specialist_active"] += 1

        # 7. Impact meteo score
        if impact is not None:
            feat["mcp_meteo_impact"] = round(impact, 3)
            fills["mcp_meteo_impact"] += 1

        # 8. Horse historical terrain performance
        if hid:
            st = horse_states.get(hid)
            if st and st.total >= 5:
                # Win rate on this terrain code
                if terrain_code is not None:
                    tc_total = st.total_by_terrain.get(terrain_code, 0)
                    if tc_total >= 3:
                        feat["mcp_horse_terrain_wr"] = round(
                            st.wins_by_terrain[terrain_code] / tc_total, 5)
                        fills["mcp_horse_terrain_wr"] += 1

                # Win rate rain vs dry
                if is_rainy and st.total_rain >= 3:
                    feat["mcp_horse_rain_wr"] = round(st.wins_rain / st.total_rain, 5)
                    fills["mcp_horse_rain_wr"] += 1
                elif not is_rainy and st.total_dry >= 3:
                    feat["mcp_horse_dry_wr"] = round(st.wins_dry / st.total_dry, 5)
                    fills["mcp_horse_dry_wr"] += 1

                # Rain/dry advantage
                if st.total_rain >= 3 and st.total_dry >= 3:
                    rain_wr = st.wins_rain / st.total_rain
                    dry_wr = st.wins_dry / st.total_dry
                    feat["mcp_rain_advantage"] = round(rain_wr - dry_wr, 5)
                    fills["mcp_rain_advantage"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseMeteoState()
        st = horse_states[hid]
        st.total += 1

        if terrain_code is not None:
            st.wins_by_terrain[terrain_code] += int(is_winner)
            st.total_by_terrain[terrain_code] += 1

        if is_rainy:
            st.wins_rain += int(is_winner)
            st.total_rain += 1
        else:
            st.wins_dry += int(is_winner)
            st.total_dry += 1


if __name__ == "__main__":
    main()
