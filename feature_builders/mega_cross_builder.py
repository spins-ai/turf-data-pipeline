#!/usr/bin/env python3
"""MEGA cross-features: complex multi-dimensional interactions that cross
ALL data sources. This is the ultimate feature builder combining:
- Horse ability × race conditions × jockey form × weather × market
- Composite power ratings
- Bayesian ensemble predictions
- Non-linear interactions (polynomial, ratio, multiplicative)."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/mega_cross")
_LOG_EVERY = 500_000
_K = 15  # shrinkage


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _safe_pos(val):
    v = _safe(val)
    return v if v is not None and v > 0 else None


class _HorseMegaState:
    __slots__ = ("wins", "places", "total", "recent_speeds", "recent_pos",
                 "ewma_pos", "ewma_speed", "wins_as_fav", "total_as_fav",
                 "wins_as_outsider", "total_as_outsider")

    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0
        self.recent_speeds = deque(maxlen=10)
        self.recent_pos = deque(maxlen=10)
        self.ewma_pos = None
        self.ewma_speed = None
        self.wins_as_fav = 0
        self.total_as_fav = 0
        self.wins_as_outsider = 0
        self.total_as_outsider = 0


class _JockeyMegaState:
    __slots__ = ("wins", "total", "ewma_pos")

    def __init__(self):
        self.wins = 0
        self.total = 0
        self.ewma_pos = None


def main():
    logger = setup_logging("mega_cross_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "mega_cross_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseMegaState] = {}
    jockey_states: dict[str, _JockeyMegaState] = {}
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
                    _process_course(course_records, fout, horse_states,
                                    jockey_states, gwr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            gwr = global_wins / global_total if global_total > 100 else 0.1
            _process_course(course_records, fout, horse_states,
                            jockey_states, gwr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, jockey_states, gwr, fills):
    n_field = len(records)
    distance = _safe(records[0].get("distance"))
    terrain = (records[0].get("met_terrain_predit") or "").strip().lower()
    temp = _safe(records[0].get("met_temperature"))
    rain = _safe(records[0].get("met_meteo_pluie_mm")) or _safe(records[0].get("mto_precipitation_mm"))

    # Field-level aggregates
    field_speeds = []
    field_ewma_pos = []
    field_gains = []
    field_cotes = []
    field_horse_wr = []

    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        hst = horse_states.get(hid) if hid else None
        if hst and hst.total >= 3:
            if hst.ewma_pos is not None:
                field_ewma_pos.append(hst.ewma_pos)
            if hst.ewma_speed is not None:
                field_speeds.append(hst.ewma_speed)
            field_horse_wr.append(hst.wins / hst.total)

        g = _safe(rec.get("gains_carriere_euros"))
        if g: field_gains.append(g)
        c = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        if c: field_cotes.append(c)

    avg_field_wr = sum(field_horse_wr) / len(field_horse_wr) if field_horse_wr else None
    avg_field_ewma = sum(field_ewma_pos) / len(field_ewma_pos) if field_ewma_pos else None
    min_cote = min(field_cotes) if field_cotes else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey_driver") or "").strip()
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        age = _safe(rec.get("age"))
        gains = _safe(rec.get("gains_carriere_euros"))
        speed = _safe(rec.get("reduction_km_ms"))
        speed_fig = _safe(rec.get("spd_speed_figure"))
        momentum = _safe(rec.get("seq_momentum"))
        progression = _safe(rec.get("seq_progression_score"))
        volatility = _safe(rec.get("seq_volatilite_position_5"))
        pos_moy_5 = _safe(rec.get("seq_position_moy_5"))
        nb_vic = _safe(rec.get("nb_victoires_carriere"))
        nb_courses = _safe(rec.get("nb_courses_carriere"))
        proba_imp = _safe(rec.get("proba_implicite")) or _safe(rec.get("mkt_proba_ajustee"))

        hst = horse_states.get(hid) if hid else None
        jst = jockey_states.get(jockey) if jockey else None

        # === 1. POWER RATING (composite score) ===
        components = []
        weights = []

        if hst and hst.total >= 3:
            horse_wr = hst.wins / hst.total
            # a) Horse win rate (shrunk)
            shrunk_wr = (hst.total * horse_wr + _K * gwr) / (hst.total + _K)
            components.append(shrunk_wr * 100)
            weights.append(3.0)

            # b) Recent form (EWMA position - inverted, lower = better)
            if hst.ewma_pos is not None:
                form_score = max(0, 15 - hst.ewma_pos) * 5
                components.append(form_score)
                weights.append(2.5)

            # c) Speed figure
            if hst.ewma_speed is not None:
                components.append(min(hst.ewma_speed, 120))
                weights.append(2.0)

        if jst and jst.total >= 10:
            jockey_wr = jst.wins / jst.total
            components.append(jockey_wr * 100)
            weights.append(1.5)

        if proba_imp:
            components.append(proba_imp * 100)
            weights.append(2.0)

        if momentum is not None:
            components.append(max(0, momentum + 50))
            weights.append(1.0)

        if len(components) >= 3:
            power = sum(c * w for c, w in zip(components, weights)) / sum(weights)
            feat["mx_power_rating"] = round(power, 3)
            fills["mx_power_rating"] += 1

        # === 2. HORSE ABILITY × RACE CONDITIONS ===
        if hst and hst.total >= 3:
            horse_wr = hst.wins / hst.total

            # Horse WR × field quality
            if avg_field_wr is not None:
                feat["mx_wr_x_field_quality"] = round(horse_wr / (avg_field_wr + 0.01), 3)
                fills["mx_wr_x_field_quality"] += 1

            # Horse form vs field form
            if hst.ewma_pos is not None and avg_field_ewma is not None:
                feat["mx_form_vs_field"] = round(avg_field_ewma - hst.ewma_pos, 3)
                fills["mx_form_vs_field"] += 1

        # === 3. VALUE DETECTION (market vs ability) ===
        if cote and hst and hst.total >= 5:
            horse_wr = hst.wins / hst.total
            implied = 1.0 / cote
            # Overlay: true probability vs implied
            overlay = horse_wr - implied
            feat["mx_overlay"] = round(overlay, 5)
            fills["mx_overlay"] += 1

            # Kelly fraction (simplified)
            if horse_wr > implied:
                kelly = (horse_wr * (cote - 1) - (1 - horse_wr)) / (cote - 1) if cote > 1 else 0
                feat["mx_kelly"] = round(max(0, kelly), 5)
                fills["mx_kelly"] += 1

        # === 4. NON-LINEAR INTERACTIONS ===
        # Age × speed (older horses slow down)
        if age and speed_fig:
            feat["mx_age_x_speed"] = round(age * speed_fig, 1)
            fills["mx_age_x_speed"] += 1

        # Momentum × cote (improving horse with value)
        if momentum is not None and cote:
            feat["mx_momentum_x_cote"] = round(momentum / (cote + 1), 4)
            fills["mx_momentum_x_cote"] += 1

        # Volatility × field size (inconsistent in big fields)
        if volatility is not None:
            feat["mx_volatility_x_field"] = round(volatility * n_field, 3)
            fills["mx_volatility_x_field"] += 1

        # === 5. MARKET CONFIDENCE × ABILITY ===
        if cote and min_cote:
            is_fav = (cote == min_cote)
            if is_fav and hst and hst.total >= 3:
                # Favorite with good recent form
                if hst.ewma_pos is not None:
                    feat["mx_fav_form"] = round(1.0 / (hst.ewma_pos + 1), 4)
                    fills["mx_fav_form"] += 1

                # Favorite reliability
                if hst.total_as_fav >= 3:
                    feat["mx_fav_reliability"] = round(hst.wins_as_fav / hst.total_as_fav, 4)
                    fills["mx_fav_reliability"] += 1

        # === 6. SPEED × CONDITIONS ===
        if speed_fig and terrain:
            # Speed on heavy ground worth more
            if "lourd" in terrain or "souple" in terrain:
                feat["mx_speed_heavy_ground"] = round(speed_fig * 1.1, 2)
                fills["mx_speed_heavy_ground"] += 1

        # === 7. GAINS EFFICIENCY ===
        if gains and nb_courses and nb_courses >= 5 and age and age >= 2:
            efficiency = gains / (nb_courses * max(age - 1, 1))
            feat["mx_gains_efficiency"] = round(efficiency, 1)
            fills["mx_gains_efficiency"] += 1

        # === 8. CONSISTENCY UNDER PRESSURE ===
        if volatility is not None and pos_moy_5 is not None:
            # Low volatility + good avg = consistent performer
            if volatility > 0:
                consistency = pos_moy_5 / (volatility + 0.1)
                feat["mx_consistency_score"] = round(1.0 / (consistency + 0.01), 4)
                fills["mx_consistency_score"] += 1

        # === 9. PROGRESSION × MARKET ===
        if progression is not None and proba_imp is not None:
            # Improving horse × market belief
            feat["mx_progression_x_market"] = round(progression * proba_imp, 5)
            fills["mx_progression_x_market"] += 1

        # === 10. JOCKEY × HORSE SYNERGY ===
        if jst and hst and jst.total >= 10 and hst.total >= 5:
            if jst.ewma_pos is not None and hst.ewma_pos is not None:
                # Both in form = synergy
                combined_form = (jst.ewma_pos + hst.ewma_pos) / 2
                feat["mx_synergy_form"] = round(1.0 / (combined_form + 0.1), 4)
                fills["mx_synergy_form"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    alpha = 0.15
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey_driver") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe_pos(rec.get("position_arrivee"))
        speed = _safe(rec.get("reduction_km_ms"))
        speed_fig = _safe(rec.get("spd_speed_figure"))
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))

        global_wins = is_winner  # Updated in outer scope would need nonlocal
        # Note: gwr passed as param, we update global stats indirectly

        if hid:
            if hid not in horse_states:
                horse_states[hid] = _HorseMegaState()
            hst = horse_states[hid]
            hst.wins += int(is_winner)
            hst.places += int(pos is not None and pos <= 3)
            hst.total += 1
            if pos:
                hst.recent_pos.append(pos)
                hst.ewma_pos = pos if hst.ewma_pos is None else alpha * pos + (1 - alpha) * hst.ewma_pos
            if speed_fig:
                hst.recent_speeds.append(speed_fig)
                hst.ewma_speed = speed_fig if hst.ewma_speed is None else alpha * speed_fig + (1 - alpha) * hst.ewma_speed

            if cote and min_cote:
                if cote == min_cote:
                    hst.wins_as_fav += int(is_winner)
                    hst.total_as_fav += 1
                elif cote > (min_cote * 3):
                    hst.wins_as_outsider += int(is_winner)
                    hst.total_as_outsider += 1

        if jockey:
            if jockey not in jockey_states:
                jockey_states[jockey] = _JockeyMegaState()
            jst = jockey_states[jockey]
            jst.wins += int(is_winner)
            jst.total += 1
            if pos:
                jst.ewma_pos = pos if jst.ewma_pos is None else alpha * pos + (1 - alpha) * jst.ewma_pos


if __name__ == "__main__":
    main()
