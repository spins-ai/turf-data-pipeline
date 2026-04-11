#!/usr/bin/env python3
"""Performance stability features: how consistent/variable is a horse's performance.
Critical for risk assessment in betting models and uncertainty quantification."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/performance_stability")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _HorseStability:
    __slots__ = (
        "positions", "norm_positions", "cotes", "speeds",
        "wins", "total", "outcomes",
    )

    def __init__(self):
        self.positions = deque(maxlen=30)       # raw position
        self.norm_positions = deque(maxlen=30)  # position / field_size
        self.cotes = deque(maxlen=30)           # odds
        self.speeds = deque(maxlen=30)          # reduction_km_ms
        self.wins = 0
        self.total = 0
        self.outcomes = deque(maxlen=30)        # 1=win, 0=loss


def _std(values):
    if len(values) < 2:
        return None
    m = sum(values) / len(values)
    var = sum((x - m) ** 2 for x in values) / len(values)
    return math.sqrt(var)


def _cv(values):
    """Coefficient of variation."""
    if len(values) < 2:
        return None
    m = sum(values) / len(values)
    if m == 0:
        return None
    s = _std(values)
    return s / abs(m) if s is not None else None


def _entropy(outcomes):
    """Shannon entropy of win/loss outcomes."""
    if len(outcomes) < 5:
        return None
    n = len(outcomes)
    p_win = sum(outcomes) / n
    p_loss = 1 - p_win
    e = 0.0
    if p_win > 0:
        e -= p_win * math.log2(p_win)
    if p_loss > 0:
        e -= p_loss * math.log2(p_loss)
    return e


def _trend_slope(values):
    """Simple linear regression slope."""
    n = len(values)
    if n < 3:
        return None
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return None
    return num / den


def main():
    logger = setup_logging("performance_stability_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "performance_stability_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseStability] = {}

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
    features_list = []

    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        feat = {"partant_uid": rec.get("partant_uid", "")}

        if not hid:
            features_list.append(feat)
            continue

        st = horse_states.get(hid)

        if st is not None and st.total >= 3:
            nps = list(st.norm_positions)
            if not nps:
                features_list.append(feat)
                continue

            # 1. Position volatility (std dev of normalized positions)
            s = _std(nps)
            if s is not None:
                feat["ps_position_volatility"] = round(s, 4)
                fills["ps_position_volatility"] += 1

            # 2. Position CV (coefficient of variation)
            c = _cv(nps)
            if c is not None:
                feat["ps_position_cv"] = round(c, 4)
                fills["ps_position_cv"] += 1

            # 3. Performance range (best - worst normalized position in recent races)
            feat["ps_perf_range"] = round(max(nps) - min(nps), 4)
            fills["ps_perf_range"] += 1

            # 4. Performance IQR (interquartile range)
            snps = sorted(nps)
            n = len(snps)
            q1 = snps[n // 4]
            q3 = snps[3 * n // 4]
            feat["ps_perf_iqr"] = round(q3 - q1, 4)
            fills["ps_perf_iqr"] += 1

            # 5. Outcome entropy (predictability of win/loss pattern)
            ent = _entropy(list(st.outcomes))
            if ent is not None:
                feat["ps_outcome_entropy"] = round(ent, 4)
                fills["ps_outcome_entropy"] += 1

            # 6. Performance trend slope (improving/declining)
            slope = _trend_slope(nps)
            if slope is not None:
                feat["ps_perf_trend"] = round(slope, 6)
                fills["ps_perf_trend"] += 1

            # 7. Sharpe ratio equivalent: (mean win rate - baseline) / volatility
            if s is not None and s > 0:
                mean_np = sum(nps) / len(nps)
                # Lower norm position = better, so invert
                feat["ps_sharpe_proxy"] = round((0.5 - mean_np) / s, 4)
                fills["ps_sharpe_proxy"] += 1

            # 8. Max drawdown: worst consecutive decline in normalized position
            if len(nps) >= 3:
                max_dd = 0.0
                peak = nps[0]  # Lower is better, so track min
                for v in nps[1:]:
                    if v < peak:
                        peak = v
                    dd = v - peak  # Higher = worse
                    if dd > max_dd:
                        max_dd = dd
                feat["ps_max_drawdown"] = round(max_dd, 4)
                fills["ps_max_drawdown"] += 1

            # 9. Consistency score: % of races finishing in top 40% of field
            top_pct = sum(1 for p in nps if p <= 0.4) / len(nps)
            feat["ps_consistency_top40"] = round(top_pct, 4)
            fills["ps_consistency_top40"] += 1

            # 10. Odds vs performance correlation proxy
            if len(st.cotes) >= 5:
                cotes_l = list(st.cotes)[-len(nps):]
                if len(cotes_l) == len(nps):
                    # Spearman-like: rank correlation
                    n = len(nps)
                    pos_ranks = _rank(nps)
                    cote_ranks = _rank(cotes_l)
                    if pos_ranks and cote_ranks and n >= 2:
                        d_sq = sum((p - c) ** 2 for p, c in zip(pos_ranks, cote_ranks))
                        denom = n * (n * n - 1)
                        if denom > 0:
                            rho = 1 - 6 * d_sq / denom
                        feat["ps_odds_perf_correlation"] = round(rho, 4)
                        fills["ps_odds_perf_correlation"] += 1

            # 11. Speed consistency
            speeds_l = list(st.speeds)
            if len(speeds_l) >= 3:
                scv = _cv(speeds_l)
                if scv is not None:
                    feat["ps_speed_consistency"] = round(scv, 4)
                    fills["ps_speed_consistency"] += 1

            # 12. Recent vs career stability
            if len(nps) >= 10:
                recent_std = _std(nps[-5:])
                career_std = _std(nps)
                if recent_std is not None and career_std is not None and career_std > 0:
                    feat["ps_stability_trend"] = round(recent_std / career_std, 4)
                    fills["ps_stability_trend"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue

        pos = _safe(rec.get("position_arrivee"))
        field = _safe(rec.get("nombre_partants"))
        cote = _safe(rec.get("cote_finale"))
        speed = _safe(rec.get("reduction_km_ms"))
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseStability()
        st = horse_states[hid]

        if pos is not None:
            st.positions.append(pos)
        if pos is not None and field is not None and field > 0:
            st.norm_positions.append(pos / field)
        if cote is not None:
            st.cotes.append(cote)
        if speed is not None:
            st.speeds.append(speed)
        st.outcomes.append(1 if is_winner else 0)
        st.wins += int(is_winner)
        st.total += 1


def _rank(values):
    """Simple ranking (1-based, ascending)."""
    if not values:
        return None
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0] * len(values)
    for rank, (idx, _) in enumerate(indexed, 1):
        ranks[idx] = rank
    return ranks


if __name__ == "__main__":
    main()
