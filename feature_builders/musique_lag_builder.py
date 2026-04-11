#!/usr/bin/env python3
"""Musique lag features: parse the 'musique' string to extract individual race results.
Musique format: "DM3M122A6A2A0A3A0A0A" where each char/group = one past race result.
Digits = finish position, letters = discipline (A=attelé, M=monté) or status (D=disqualifié,
T=tombé, R=rétrogradé, 0=non-placé). Extract lag features N-1 to N-5 individually."""
from __future__ import annotations
import gc, json, math, re, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/musique_lag")
_LOG_EVERY = 500_000

# Pattern: each race is a position (digit or 0) followed by a discipline letter
# Common patterns: "1A" = 1er attelé, "DM" = disqualifié monté, "0A" = non-placé attelé
_RACE_PATTERN = re.compile(r'([0-9DTRA]{1,2})([AaMm])?')

_STATUS_MAP = {'D': 'disqualifie', 'T': 'tombe', 'R': 'retrograde', 'A': 'arrete'}
_DISC_MAP = {'A': 'attele', 'a': 'attele', 'M': 'monte', 'm': 'monte'}


def _parse_musique(musique: str) -> list[dict]:
    """Parse musique string into list of past race dicts (most recent first)."""
    if not musique or not isinstance(musique, str):
        return []

    races = []
    i = 0
    while i < len(musique):
        c = musique[i]

        # Status letter (D, T, R) followed by discipline
        if c in 'DTRA' and i + 1 < len(musique) and musique[i + 1] in 'AaMm':
            races.append({
                'position': None,
                'status': _STATUS_MAP.get(c, c),
                'discipline': _DISC_MAP.get(musique[i + 1], ''),
                'is_dnf': True,
            })
            i += 2
            continue

        # Digit(s) followed by discipline letter
        if c.isdigit():
            pos_str = c
            # Check for two-digit position (10+)
            if i + 1 < len(musique) and musique[i + 1].isdigit():
                pos_str += musique[i + 1]
                i += 1

            pos = int(pos_str)
            disc = ''
            if i + 1 < len(musique) and musique[i + 1] in 'AaMm':
                disc = _DISC_MAP.get(musique[i + 1], '')
                i += 1

            races.append({
                'position': pos if pos > 0 else None,  # 0 = non-placé
                'status': 'finished' if pos > 0 else 'non_place',
                'discipline': disc,
                'is_dnf': False,
            })
            i += 1
            continue

        # Discipline letter alone or unknown char — skip
        i += 1

    return races


def main():
    logger = setup_logging("musique_lag_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "musique_lag_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                feat = {"partant_uid": rec.get("partant_uid", "")}

                musique = rec.get("musique", "")
                races = _parse_musique(musique)

                if races:
                    feat["ml_nb_races_in_musique"] = len(races)
                    fills["ml_nb_races_in_musique"] += 1

                    # Lag features N-1 to N-5
                    for lag in range(min(5, len(races))):
                        r = races[lag]
                        prefix = f"ml_lag{lag+1}"

                        # Position (None if DNF or non-placé)
                        if r['position'] is not None:
                            feat[f"{prefix}_pos"] = r['position']
                            fills[f"{prefix}_pos"] += 1

                        # Is DNF?
                        feat[f"{prefix}_is_dnf"] = 1 if r['is_dnf'] else 0
                        fills[f"{prefix}_is_dnf"] += 1

                        # Is win?
                        feat[f"{prefix}_is_win"] = 1 if r['position'] == 1 else 0
                        fills[f"{prefix}_is_win"] += 1

                        # Is top 3?
                        feat[f"{prefix}_is_place"] = 1 if r['position'] is not None and r['position'] <= 3 else 0
                        fills[f"{prefix}_is_place"] += 1

                    # Aggregates over musique
                    positions = [r['position'] for r in races if r['position'] is not None]

                    if positions:
                        # Wins in last 5 / last 10
                        last5 = positions[:5]
                        last10 = positions[:10]
                        feat["ml_wins_last5"] = sum(1 for p in last5 if p == 1)
                        feat["ml_wins_last10"] = sum(1 for p in last10 if p == 1)
                        feat["ml_places_last5"] = sum(1 for p in last5 if p <= 3)
                        feat["ml_places_last10"] = sum(1 for p in last10 if p <= 3)
                        fills["ml_wins_last5"] += 1
                        fills["ml_wins_last10"] += 1
                        fills["ml_places_last5"] += 1
                        fills["ml_places_last10"] += 1

                        # Average position last 5
                        if len(last5) >= 2:
                            feat["ml_avg_pos_last5"] = round(sum(last5) / len(last5), 2)
                            fills["ml_avg_pos_last5"] += 1

                        # Trend: avg pos last 3 vs avg pos 4-6 (lower = improving)
                        if len(positions) >= 6:
                            recent3 = sum(positions[:3]) / 3
                            older3 = sum(positions[3:6]) / 3
                            feat["ml_pos_trend_3v3"] = round(recent3 - older3, 2)
                            fills["ml_pos_trend_3v3"] += 1

                        # Best position in musique
                        feat["ml_best_pos"] = min(positions)
                        fills["ml_best_pos"] += 1

                    # DNF count
                    dnf_count = sum(1 for r in races if r['is_dnf'])
                    feat["ml_dnf_count"] = dnf_count
                    fills["ml_dnf_count"] += 1

                    if len(races) >= 3:
                        feat["ml_dnf_rate"] = round(dnf_count / len(races), 4)
                        fills["ml_dnf_rate"] += 1

                    # Discipline switches in musique
                    discs = [r['discipline'] for r in races if r['discipline']]
                    if len(discs) >= 2:
                        switches = sum(1 for i in range(1, len(discs)) if discs[i] != discs[i-1])
                        feat["ml_disc_switches"] = switches
                        fills["ml_disc_switches"] += 1

                    # Current discipline from musique (most recent)
                    current_disc = next((r['discipline'] for r in races if r['discipline']), '')
                    race_disc = rec.get("discipline", "")
                    if current_disc and race_disc:
                        is_same = 1 if (
                            ('attele' in current_disc and 'attele' in race_disc) or
                            ('monte' in current_disc and 'monte' in race_disc)
                        ) else 0
                        feat["ml_same_discipline"] = is_same
                        fills["ml_same_discipline"] += 1

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
