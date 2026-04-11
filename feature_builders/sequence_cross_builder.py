#!/usr/bin/env python3
"""Sequence cross-features: exploit seq_seq_* arrays to compute advanced
time-series features. Cross position sequences with cote sequences, distance
sequences with rest patterns, and compute trend/volatility/autocorrelation."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/sequence_cross")
_LOG_EVERY = 500_000


def _safe_list(val):
    """Extract numeric list from field."""
    if isinstance(val, list):
        return [float(x) for x in val if x is not None and x != '']
    return []


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _slope(vals):
    """Linear regression slope over indices 0..n-1."""
    n = len(vals)
    if n < 3:
        return None
    x_mean = (n - 1) / 2
    y_mean = sum(vals) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(vals))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den > 0 else None


def _autocorr(vals, lag=1):
    """Lag-1 autocorrelation."""
    n = len(vals)
    if n < lag + 3:
        return None
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals)
    if var == 0:
        return None
    cov = sum((vals[i] - mean) * (vals[i - lag] - mean) for i in range(lag, n))
    return cov / var


def _entropy(vals, bins=5):
    """Shannon entropy of a sequence (discretized)."""
    if len(vals) < 5:
        return None
    mn, mx = min(vals), max(vals)
    if mx == mn:
        return 0.0
    counts = [0] * bins
    for v in vals:
        idx = min(int((v - mn) / (mx - mn) * bins), bins - 1)
        counts[idx] += 1
    n = len(vals)
    h = 0.0
    for c in counts:
        if c > 0:
            p = c / n
            h -= p * math.log2(p)
    return h


def main():
    logger = setup_logging("sequence_cross_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "sequence_cross_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                feat = {"partant_uid": rec.get("partant_uid", "")}

                positions = _safe_list(rec.get("seq_seq_positions"))
                cotes = _safe_list(rec.get("seq_seq_cotes"))
                distances = _safe_list(rec.get("seq_seq_distances"))
                repos = _safe_list(rec.get("seq_seq_jours_repos"))
                current_dist = _safe(rec.get("distance"))

                # ========= POSITION SEQUENCE FEATURES =========
                if len(positions) >= 3:
                    # 1. Linear trend (slope) of positions
                    slope = _slope(positions)
                    if slope is not None:
                        feat["sc_pos_slope"] = round(slope, 4)
                        fills["sc_pos_slope"] += 1

                    # 2. Autocorrelation (consistency)
                    ac = _autocorr(positions)
                    if ac is not None:
                        feat["sc_pos_autocorr"] = round(ac, 4)
                        fills["sc_pos_autocorr"] += 1

                    # 3. Entropy (unpredictability)
                    if len(positions) >= 5:
                        h = _entropy(positions)
                        if h is not None:
                            feat["sc_pos_entropy"] = round(h, 4)
                            fills["sc_pos_entropy"] += 1

                    # 4. Min/max/range in last 5
                    last5 = positions[:5]
                    feat["sc_pos_min5"] = min(last5)
                    feat["sc_pos_max5"] = max(last5)
                    feat["sc_pos_range5"] = max(last5) - min(last5)
                    fills["sc_pos_min5"] += 1
                    fills["sc_pos_max5"] += 1
                    fills["sc_pos_range5"] += 1

                    # 5. Weighted recent (exponential)
                    alpha = 0.3
                    ewma = positions[0]
                    for p in positions[1:5]:
                        ewma = alpha * p + (1 - alpha) * ewma
                    feat["sc_pos_ewma"] = round(ewma, 3)
                    fills["sc_pos_ewma"] += 1

                # ========= COTE × POSITION CROSS =========
                if len(cotes) >= 3 and len(positions) >= 3:
                    n = min(len(cotes), len(positions))
                    # 6. Correlation between odds and positions
                    c_slice = cotes[:n]
                    p_slice = positions[:n]
                    if n >= 4:
                        c_mean = sum(c_slice) / n
                        p_mean = sum(p_slice) / n
                        cov = sum((c_slice[i] - c_mean) * (p_slice[i] - p_mean) for i in range(n))
                        c_std = math.sqrt(sum((c - c_mean) ** 2 for c in c_slice))
                        p_std = math.sqrt(sum((p - p_mean) ** 2 for p in p_slice))
                        if c_std > 0 and p_std > 0:
                            corr = cov / (c_std * p_std)
                            feat["sc_cote_pos_corr"] = round(corr, 4)
                            fills["sc_cote_pos_corr"] += 1

                    # 7. Times when beaten odds (position better than implied)
                    beats = 0
                    for i in range(min(5, n)):
                        implied_rank = sum(1 for c in cotes[:n] if c <= cotes[i])
                        if positions[i] < implied_rank:
                            beats += 1
                    feat["sc_beats_market_5"] = beats
                    fills["sc_beats_market_5"] += 1

                    # 8. Average value: cote when winning/placing
                    good_cotes = [cotes[i] for i in range(min(10, n)) if positions[i] <= 3]
                    if good_cotes:
                        feat["sc_avg_cote_when_placed"] = round(sum(good_cotes) / len(good_cotes), 2)
                        fills["sc_avg_cote_when_placed"] += 1

                # ========= DISTANCE SEQUENCE × POSITION =========
                if len(distances) >= 3 and len(positions) >= 3 and current_dist:
                    n = min(len(distances), len(positions))
                    # 9. Performance at similar distances
                    similar = [(positions[i], distances[i]) for i in range(n)
                               if abs(distances[i] - current_dist) < 300]
                    if len(similar) >= 2:
                        avg_pos_similar = sum(p for p, d in similar) / len(similar)
                        feat["sc_pos_at_similar_dist"] = round(avg_pos_similar, 2)
                        feat["sc_n_similar_dist"] = len(similar)
                        fills["sc_pos_at_similar_dist"] += 1
                        fills["sc_n_similar_dist"] += 1

                    # 10. Distance change impact
                    if len(distances) >= 2:
                        dist_changes = []
                        pos_changes = []
                        for i in range(min(5, n) - 1):
                            dc = distances[i] - distances[i + 1]
                            pc = positions[i] - positions[i + 1]
                            dist_changes.append(dc)
                            pos_changes.append(pc)
                        if len(dist_changes) >= 2:
                            # Does horse improve when distance increases?
                            up_dist = [pos_changes[i] for i in range(len(dist_changes))
                                       if dist_changes[i] > 100]
                            down_dist = [pos_changes[i] for i in range(len(dist_changes))
                                         if dist_changes[i] < -100]
                            if up_dist:
                                feat["sc_pos_change_dist_up"] = round(sum(up_dist) / len(up_dist), 2)
                                fills["sc_pos_change_dist_up"] += 1
                            if down_dist:
                                feat["sc_pos_change_dist_down"] = round(sum(down_dist) / len(down_dist), 2)
                                fills["sc_pos_change_dist_down"] += 1

                # ========= REST × PERFORMANCE =========
                if len(repos) >= 3 and len(positions) >= 3:
                    n = min(len(repos), len(positions))
                    # 11. Performance after short vs long rest
                    short_rest_pos = [positions[i] for i in range(min(n, 10))
                                      if i < len(repos) and repos[i] < 14]
                    long_rest_pos = [positions[i] for i in range(min(n, 10))
                                     if i < len(repos) and repos[i] > 30]
                    if len(short_rest_pos) >= 2:
                        feat["sc_pos_after_short_rest"] = round(sum(short_rest_pos) / len(short_rest_pos), 2)
                        fills["sc_pos_after_short_rest"] += 1
                    if len(long_rest_pos) >= 2:
                        feat["sc_pos_after_long_rest"] = round(sum(long_rest_pos) / len(long_rest_pos), 2)
                        fills["sc_pos_after_long_rest"] += 1

                    # 12. Rest regularity (CV of repos)
                    r_mean = sum(repos) / len(repos)
                    if r_mean > 0:
                        r_std = math.sqrt(sum((r - r_mean) ** 2 for r in repos) / len(repos))
                        feat["sc_rest_cv"] = round(r_std / r_mean, 4)
                        fills["sc_rest_cv"] += 1

                    # 13. Optimal rest window (best avg pos by rest bucket)
                    rest_buckets = {}  # bucket -> [positions]
                    for i in range(min(n, len(repos))):
                        r = repos[i]
                        if r < 10:
                            b = "veryshort"
                        elif r < 21:
                            b = "short"
                        elif r < 42:
                            b = "medium"
                        else:
                            b = "long"
                        if b not in rest_buckets:
                            rest_buckets[b] = []
                        rest_buckets[b].append(positions[i])

                    if len(rest_buckets) >= 2:
                        best_b = min(rest_buckets.keys(),
                                     key=lambda b: sum(rest_buckets[b]) / len(rest_buckets[b])
                                     if rest_buckets[b] else 99)
                        # Is current rest in optimal bucket?
                        if repos:
                            curr_rest = repos[0]
                            if curr_rest < 10: curr_b = "veryshort"
                            elif curr_rest < 21: curr_b = "short"
                            elif curr_rest < 42: curr_b = "medium"
                            else: curr_b = "long"
                            feat["sc_optimal_rest_match"] = 1 if curr_b == best_b else 0
                            fills["sc_optimal_rest_match"] += 1

                # ========= COTE SEQUENCE FEATURES =========
                if len(cotes) >= 3:
                    # 14. Cote trend
                    c_slope = _slope(cotes[:8])
                    if c_slope is not None:
                        feat["sc_cote_slope"] = round(c_slope, 4)
                        fills["sc_cote_slope"] += 1

                    # 15. Cote volatility
                    c_mean = sum(cotes[:8]) / min(8, len(cotes))
                    if c_mean > 0:
                        c_std = math.sqrt(sum((c - c_mean) ** 2 for c in cotes[:8]) / min(8, len(cotes)))
                        feat["sc_cote_cv"] = round(c_std / c_mean, 4)
                        fills["sc_cote_cv"] += 1

                fout.write(json.dumps(feat, ensure_ascii=False) + "\n")
                written += 1

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Processed {lineno:,}")
                    gc.collect()

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


if __name__ == "__main__":
    main()
