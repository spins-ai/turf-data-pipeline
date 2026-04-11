#!/usr/bin/env python3
"""Race conditions text mining: extract structured features from race condition
text fields (conditions, libelle_course, type_course, etc.)."""
from __future__ import annotations
import gc, json, math, re, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/conditions_texte_mining")
_LOG_EVERY = 500_000


def _extract_features(rec):
    """Extract structured features from text condition fields."""
    feat = {}

    # Type de course
    type_c = (rec.get("type_course") or rec.get("discipline") or "").lower()
    if type_c:
        feat["ctm_is_attele"] = 1 if "attel" in type_c else 0
        feat["ctm_is_monte"] = 1 if "mont" in type_c else 0
        feat["ctm_is_plat"] = 1 if "plat" in type_c else 0
        feat["ctm_is_obstacle"] = 1 if any(x in type_c for x in ("haie", "steeple", "cross", "obstacle")) else 0
        feat["ctm_is_trot"] = 1 if "trot" in type_c else 0

    # Conditions text
    conditions = (rec.get("conditions") or rec.get("conditions_course") or
                  rec.get("libelle_course") or "").lower()

    if conditions:
        # Age restrictions
        feat["ctm_is_age_restricted"] = 1 if re.search(r'\b[23] ans\b', conditions) else 0

        # Handicap
        feat["ctm_is_handicap"] = 1 if "handicap" in conditions else 0

        # Claiming (à réclamer)
        feat["ctm_is_claimer"] = 1 if any(x in conditions for x in ("réclamer", "reclamer", "claim")) else 0

        # Group/Listed
        feat["ctm_is_groupe"] = 1 if re.search(r'group[e]?\s*[123i]', conditions) else 0
        feat["ctm_is_listed"] = 1 if "listed" in conditions or "listé" in conditions else 0

        # Apprenti/lads
        feat["ctm_is_apprenti"] = 1 if "apprenti" in conditions else 0

        # Sexe restriction (femelles, juments)
        feat["ctm_is_female_only"] = 1 if any(x in conditions for x in ("femelle", "jument", "pouliche")) else 0

        # Distance keywords
        feat["ctm_mentions_sprint"] = 1 if "sprint" in conditions else 0

    # Surface
    surface = (rec.get("surface") or rec.get("type_piste") or "").lower()
    if surface:
        feat["ctm_is_herbe"] = 1 if any(x in surface for x in ("herbe", "gazon", "turf")) else 0
        feat["ctm_is_psf"] = 1 if any(x in surface for x in ("psf", "fibresand", "polytrack", "sable")) else 0

    # Corde
    corde = (rec.get("corde") or "").lower()
    if corde:
        feat["ctm_corde_droite"] = 1 if "droit" in corde else 0
        feat["ctm_corde_gauche"] = 1 if "gauch" in corde else 0

    return feat


def main():
    logger = setup_logging("conditions_texte_mining_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "conditions_texte_mining_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                feat = {"partant_uid": rec.get("partant_uid", "")}

                extracted = _extract_features(rec)
                feat.update(extracted)
                for k in extracted:
                    fills[k] += 1

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
