#!/usr/bin/env python3
"""
feature_builders.musique_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
22 features from PMU "musique" string.

Musique format: "1a2a3a0a5m4a..." where digit = position, letter = discipline/surface.
- a = attele (trot), m = monte (trot monte), p = plat (galop)
- h = haies, s = steeple, c = cross
- 0 = tombe/arrete/disqualifie, D/T/A/R = non-finish codes

Usage:
    python feature_builders/musique_features.py
    python feature_builders/musique_features.py --input output/02_liste_courses/partants_normalises.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_DEFAULT = os.path.join("output", "02_liste_courses", "partants_normalises.jsonl")
OUTPUT_DIR_DEFAULT = os.path.join("output", "musique_features")
LOG_DIR = os.path.join("logs")

_POSITION_PATTERN = re.compile(r'(\d+|[DTAR])([amphsc])', re.IGNORECASE)

_DISC_MAP = {
    "attele": "a", "monte": "m", "plat": "p", "haies": "h",
    "steeple": "s", "cross": "c", "trot attele": "a", "trot monte": "m",
}

# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("musique_features")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOG_DIR, "musique_features.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

# ===========================================================================
# HELPERS
# ===========================================================================

def _decode_musique(musique: str | None) -> list[dict]:
    """Decode musique string into list of {position, discipline} dicts.
    Most recent result is first in the list.
    """
    if not musique:
        return []
    results = []
    for m in _POSITION_PATTERN.finditer(musique):
        pos_str, disc = m.group(1), m.group(2).lower()
        if pos_str.isdigit():
            pos = int(pos_str)
        else:
            pos = None  # D, T, A, R = non-finish
        results.append({"position": pos, "discipline": disc, "raw": m.group(0)})
    return results

# ===========================================================================
# LOAD
# ===========================================================================

def load_jsonl(path: str, logger: logging.Logger) -> list:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    logger.info("Charge %d enregistrements depuis %s", len(records), path)
    return records


def load_json_or_jsonl(path: str, logger: logging.Logger) -> list:
    if path.endswith(".jsonl"):
        return load_jsonl(path, logger)
    jsonl_path = path.replace(".json", ".jsonl")
    if os.path.exists(jsonl_path):
        return load_jsonl(jsonl_path, logger)
    if os.path.exists(path):
        logger.info("Chargement JSON: %s", path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("  %d entrees chargees", len(data))
        return data
    logger.error("Fichier introuvable: %s", path)
    sys.exit(1)

# ===========================================================================
# BUILDER
# ===========================================================================

def build_musique_features(partants: list, logger: logging.Logger = None) -> list:
    """Build 22 features from decoded musique string."""
    if logger is None:
        logger = logging.getLogger(__name__)
    results = []
    enriched = 0

    for idx, p in enumerate(partants):
        decoded = _decode_musique(p.get("musique"))

        feat = {}

        if decoded:
            enriched += 1
            nb = len(decoded)
            positions = [d["position"] for d in decoded if d["position"] is not None]
            valid_positions = [pos for pos in positions if pos > 0]

            feat["musique_nb_courses"] = nb
            feat["musique_nb_victoires"] = sum(1 for pos in positions if pos == 1)
            feat["musique_nb_places"] = sum(1 for pos in positions if pos <= 3)
            feat["musique_nb_2eme"] = sum(1 for pos in positions if pos == 2)
            feat["musique_nb_3eme"] = sum(1 for pos in positions if pos == 3)
            feat["musique_nb_dnf"] = sum(
                1 for d in decoded if d["position"] is None or d["position"] == 0
            )
            feat["musique_nb_zero"] = sum(1 for d in decoded if d["position"] == 0)
            feat["musique_nb_disqualifications"] = sum(
                1 for d in decoded
                if d["position"] is None and d["raw"][0].upper() in ("D", "A", "T")
            )

            feat["musique_taux_victoire"] = round(feat["musique_nb_victoires"] / nb, 3) if nb else None
            feat["musique_taux_place"] = round(feat["musique_nb_places"] / nb, 3) if nb else None

            # Recent positions
            feat["musique_derniere_pos"] = decoded[0]["position"] if decoded else None
            feat["musique_avant_derniere_pos"] = decoded[1]["position"] if len(decoded) > 1 else None

            # Average position last N
            valid_5 = valid_positions[:5]
            valid_10 = valid_positions[:10]
            feat["musique_avg_pos_5"] = round(sum(valid_5) / len(valid_5), 2) if valid_5 else None
            feat["musique_avg_pos_10"] = round(sum(valid_10) / len(valid_10), 2) if valid_10 else None

            # Last 5 positions (as list for direct model access)
            feat["musique_last_5_positions"] = [d["position"] for d in decoded[:5]]

            # Trend: compare recent 3 vs previous 3
            recent_3 = valid_positions[:3]
            prev_3 = valid_positions[3:6]
            if len(recent_3) >= 2 and len(prev_3) >= 2:
                avg_recent = sum(recent_3) / len(recent_3)
                avg_prev = sum(prev_3) / len(prev_3)
                trend_val = round(avg_prev - avg_recent, 2)
                feat["musique_trend"] = trend_val
                if trend_val > 0.5:
                    feat["musique_trend_label"] = 1
                elif trend_val < -0.5:
                    feat["musique_trend_label"] = -1
                else:
                    feat["musique_trend_label"] = 0
            else:
                feat["musique_trend"] = None
                feat["musique_trend_label"] = None

            # Discipline diversity
            disciplines = set(d["discipline"] for d in decoded)
            feat["musique_nb_disciplines"] = len(disciplines)

            # Percentage same discipline as current race
            current_disc = (p.get("discipline") or "").lower()
            current_code = _DISC_MAP.get(current_disc, "")
            if current_code and nb > 0:
                same = sum(1 for d in decoded if d["discipline"] == current_code)
                feat["musique_pct_meme_discipline"] = round(same / nb, 3)
            else:
                feat["musique_pct_meme_discipline"] = None

            # Consecutive streaks
            consec_places = 0
            for d in decoded:
                if d["position"] is not None and 1 <= d["position"] <= 3:
                    consec_places += 1
                else:
                    break
            feat["musique_consecutive_places"] = consec_places

            consec_hors = 0
            for d in decoded:
                if d["position"] is None or d["position"] == 0 or d["position"] > 3:
                    consec_hors += 1
                else:
                    break
            feat["musique_consecutive_hors_places"] = consec_hors

            # Surface/discipline changes count
            disc_list = [d["discipline"] for d in decoded]
            surface_changes = 0
            for i_d in range(1, len(disc_list)):
                if disc_list[i_d] != disc_list[i_d - 1]:
                    surface_changes += 1
            feat["musique_surface_changes"] = surface_changes
        else:
            for k in ("musique_nb_courses", "musique_nb_victoires", "musique_nb_places",
                       "musique_nb_dnf", "musique_nb_zero", "musique_nb_disqualifications",
                       "musique_nb_2eme", "musique_nb_3eme",
                       "musique_taux_victoire", "musique_taux_place",
                       "musique_derniere_pos", "musique_avant_derniere_pos",
                       "musique_avg_pos_5", "musique_avg_pos_10",
                       "musique_last_5_positions",
                       "musique_trend", "musique_trend_label",
                       "musique_nb_disciplines", "musique_pct_meme_discipline",
                       "musique_consecutive_places", "musique_consecutive_hors_places",
                       "musique_surface_changes"):
                feat[k] = None

        p.update(feat)
        results.append(p)

        if (idx + 1) % 200000 == 0:
            logger.info("  %d/%d traites, %d enrichis", idx + 1, len(partants), enriched)

    logger.info("Features musique: %d/%d enrichis (%.1f%%)",
                enriched, len(results), 100 * enriched / max(len(results), 1))
    return results

# ===========================================================================
# EXPORT
# ===========================================================================

def save_jsonl(records: list, path: str, logger: logging.Logger):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    logger.info("Sauve JSONL: %s (%d)", path, len(records))

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="22 features from musique string")
    parser.add_argument("--input", default=PARTANTS_DEFAULT, help="Partants JSONL/JSON")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("musique_features.py")
    logger.info("=" * 70)

    partants = load_json_or_jsonl(args.input, logger)
    results = build_musique_features(partants, logger)

    out_path = os.path.join(args.output_dir, "musique_features.jsonl")
    save_jsonl(results, out_path, logger)
    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
