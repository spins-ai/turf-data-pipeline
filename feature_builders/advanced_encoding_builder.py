#!/usr/bin/env python3
"""
feature_builders.advanced_encoding_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Advanced encoding features for categorical variables and temporal cycles.

Produces:
  - advanced_encodings.jsonl   in output/advanced_encodings/

Features per partant (13):
  - freq_enc_hippodrome     : nb past races at this hippodrome (all horses)
  - freq_enc_jockey_global  : total career races of this jockey
  - freq_enc_trainer_global : total career races of this trainer
  - woe_hippodrome          : Weight of Evidence for hippodrome (log odds)
  - woe_discipline          : Weight of Evidence for discipline
  - sin_month               : sin(2*pi*month/12) cyclical month encoding
  - cos_month               : cos(2*pi*month/12)
  - sin_dow                 : sin(2*pi*day_of_week/7) cyclical DOW
  - cos_dow                 : cos(2*pi*day_of_week/7)
  - sin_hour                : sin(2*pi*hour/24) cyclical hour
  - cos_hour                : cos(2*pi*hour/24)
  - position_encoding_seq   : sinusoidal position encoding for sequence index
  - advanced_combo_poly     : polynomial combo of top features (cote * nb_partants)

Usage:
    python feature_builders/advanced_encoding_builder.py
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "advanced_encodings"
_LOG_EVERY = 500_000


def _iter_jsonl(path: Path, logger):
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


def _safe_float(v, default=None):
    if v is None:
        return default
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (ValueError, TypeError):
        return default


def build_advanced_encodings(input_path: Path, logger) -> list[dict[str, Any]]:
    """Two-pass builder: pass 1 = gather stats, pass 2 = compute features."""
    logger.info("=== Advanced Encoding Builder ===")
    t0 = time.time()

    # Pass 1: gather global stats for WoE and frequency
    hippo_stats: dict[str, dict] = defaultdict(lambda: {"wins": 0, "total": 0})
    disc_stats: dict[str, dict] = defaultdict(lambda: {"wins": 0, "total": 0})
    hippo_global_count: dict[str, int] = defaultdict(int)
    jockey_global_count: dict[str, int] = defaultdict(int)
    trainer_global_count: dict[str, int] = defaultdict(int)

    logger.info("Pass 1: gathering global stats from %s", input_path)
    n1 = 0
    for rec in _iter_jsonl(input_path, logger):
        n1 += 1
        hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
        disc = (rec.get("discipline") or "").lower()
        jockey = rec.get("jockey") or rec.get("driver") or ""
        trainer = rec.get("entraineur") or ""
        is_win = bool(rec.get("is_gagnant"))

        if hippo:
            hippo_stats[hippo]["total"] += 1
            hippo_global_count[hippo] += 1
            if is_win:
                hippo_stats[hippo]["wins"] += 1
        if disc:
            disc_stats[disc]["total"] += 1
            if is_win:
                disc_stats[disc]["wins"] += 1
        if jockey:
            jockey_global_count[jockey] += 1
        if trainer:
            trainer_global_count[trainer] += 1

    total_wins = sum(v["wins"] for v in hippo_stats.values())
    total_all = sum(v["total"] for v in hippo_stats.values())
    global_win_rate = total_wins / max(total_all, 1)

    logger.info("Pass 1 done: %d records, global_win_rate=%.4f", n1, global_win_rate)

    # Compute WoE values
    def woe(wins, total, prior_wins, prior_total):
        """Weight of Evidence with Laplace smoothing."""
        p_event = (wins + 1) / (prior_wins + 2)
        p_nonevent = (total - wins + 1) / (prior_total - prior_wins + 2)
        if p_nonevent <= 0:
            return 0.0
        return math.log(max(p_event, 1e-10) / max(p_nonevent, 1e-10))

    hippo_woe = {}
    for h, s in hippo_stats.items():
        hippo_woe[h] = woe(s["wins"], s["total"], total_wins, total_all)

    disc_woe = {}
    for d, s in disc_stats.items():
        disc_woe[d] = woe(s["wins"], s["total"], total_wins, total_all)

    # Pass 2: compute features
    logger.info("Pass 2: computing features")
    results = []
    n2 = 0

    # Track horse sequence index per horse
    horse_seq: dict[str, int] = defaultdict(int)

    for rec in _iter_jsonl(input_path, logger):
        n2 += 1
        if n2 % _LOG_EVERY == 0:
            logger.info("  Traite %d records...", n2)

        uid = rec.get("partant_uid")
        hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
        disc = (rec.get("discipline") or "").lower()
        jockey = rec.get("jockey") or rec.get("driver") or ""
        trainer = rec.get("entraineur") or ""
        cheval = rec.get("nom_cheval") or ""
        date_str = rec.get("date_reunion_iso", "")
        heure = rec.get("heure_depart", "")
        cote = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
        nb_partants = _safe_float(rec.get("nombre_partants"))

        # Temporal cyclical features
        sin_m = cos_m = sin_d = cos_d = sin_h = cos_h = None
        if date_str and len(date_str) >= 10:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(date_str[:10])
                month = dt.month
                dow = dt.weekday()
                sin_m = math.sin(2 * math.pi * month / 12)
                cos_m = math.cos(2 * math.pi * month / 12)
                sin_d = math.sin(2 * math.pi * dow / 7)
                cos_d = math.cos(2 * math.pi * dow / 7)
            except Exception:
                pass

        if heure:
            try:
                h_val = int(heure.split(":")[0]) if ":" in heure else int(heure[:2])
                sin_h = math.sin(2 * math.pi * h_val / 24)
                cos_h = math.cos(2 * math.pi * h_val / 24)
            except Exception:
                pass

        # Sequence position encoding (sinusoidal)
        seq_idx = horse_seq[cheval] if cheval else 0
        pos_enc = math.sin(seq_idx / (10000 ** (0 / 16)))  # simplified pos encoding
        if cheval:
            horse_seq[cheval] += 1

        # Polynomial combo
        poly_combo = None
        if cote and nb_partants and nb_partants > 0:
            poly_combo = round(cote * nb_partants, 2)

        results.append({
            "partant_uid": uid,
            "freq_enc_hippodrome": hippo_global_count.get(hippo, 0),
            "freq_enc_jockey_global": jockey_global_count.get(jockey, 0),
            "freq_enc_trainer_global": trainer_global_count.get(trainer, 0),
            "woe_hippodrome": round(hippo_woe.get(hippo, 0.0), 6),
            "woe_discipline": round(disc_woe.get(disc, 0.0), 6),
            "sin_month": round(sin_m, 6) if sin_m is not None else None,
            "cos_month": round(cos_m, 6) if cos_m is not None else None,
            "sin_dow": round(sin_d, 6) if sin_d is not None else None,
            "cos_dow": round(cos_d, 6) if cos_d is not None else None,
            "sin_hour": round(sin_h, 6) if sin_h is not None else None,
            "cos_hour": round(cos_h, 6) if cos_h is not None else None,
            "position_encoding_seq": round(pos_enc, 6),
            "advanced_combo_poly": poly_combo,
        })

    dt = time.time() - t0
    logger.info("Terminé: %d features en %.1fs", len(results), dt)
    return results


def main():
    logger = setup_logging("advanced_encoding_builder")
    parser = argparse.ArgumentParser(description="Advanced Encoding Builder")
    parser.add_argument("--input", type=str, help="Path to partants_master.jsonl")
    args = parser.parse_args()

    input_path = None
    if args.input:
        input_path = Path(args.input)
    else:
        for c in INPUT_CANDIDATES:
            if c.exists():
                input_path = c
                break

    if not input_path or not input_path.exists():
        logger.error("Aucun fichier partants_master.jsonl trouve.")
        sys.exit(1)

    results = build_advanced_encodings(input_path, logger)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "advanced_encodings.jsonl"
    save_jsonl(results, out_path, logger)
    logger.info("Sauvegarde: %s (%d records)", out_path, len(results))


if __name__ == "__main__":
    main()
