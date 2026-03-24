#!/usr/bin/env python3
"""
renormaliser.py — Re-normalise les brutes existantes sans re-scraper.

Charge reunions_brut.json, reconstruit les dataclasses ReunionBrute,
applique normalisation + fusion inter-sources, et sauvegarde reunions_normalisees.*.

Usage :
    python3 renormaliser.py
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

# On importe tout depuis le pipeline principal
from importlib import import_module

# Import dynamique pour gérer le nom de fichier avec 01_
mod = import_module("01_calendrier_reunions")

ReunionBrute = mod.ReunionBrute
normaliser_reunion = mod.normaliser_reunion
fusion_inter_sources = mod.fusion_inter_sources
build_reunion_references = mod.build_reunion_references
deduplication_intra_source = mod.deduplication_intra_source
default_source_configs = mod.default_source_configs
Sauvegarder = mod.Sauvegarder

OUTPUT_DIR = Path(__file__).resolve().parent / "../../output" / "01_calendrier_reunions"
BRUTES_PATH = OUTPUT_DIR / "reunions_brut.json"


from utils.logging_setup import setup_logging


def main():
    logger = setup_logging("renormaliser")
    logger.info("=" * 60)
    logger.info("RE-NORMALISATION DES BRUTES")
    logger.info("=" * 60)

    if not BRUTES_PATH.exists():
        logger.error("Fichier brutes introuvable: %s", BRUTES_PATH)
        sys.exit(1)

    with open(BRUTES_PATH, "r", encoding="utf-8") as f:
        brutes_raw = json.load(f)
    logger.info("Chargées: %d brutes", len(brutes_raw))

    # Reconvertir les dicts en dataclasses ReunionBrute
    sources_config = default_source_configs()
    brutes = []
    for raw in brutes_raw:
        # Filtrer les champs valides pour ReunionBrute
        valid_fields = {f.name for f in ReunionBrute.__dataclass_fields__.values()}
        filtered = {k: v for k, v in raw.items() if k in valid_fields}
        # Gérer extras qui peut être None
        if filtered.get("extras") is None:
            filtered["extras"] = {}
        try:
            brutes.append(ReunionBrute(**filtered))
        except TypeError as e:
            logger.warning("Brute ignorée: %s", e)

    logger.info("Dataclasses créées: %d", len(brutes))

    # Déduplication
    brutes = deduplication_intra_source(brutes, logger)

    # Normalisation
    normalisees = [normaliser_reunion(r, sources_config) for r in brutes]
    logger.info("Normalisées: %d", len(normalisees))

    # Fusion inter-sources
    nb_avant = len(normalisees)
    normalisees = fusion_inter_sources(normalisees, sources_config, logger)
    logger.info("Après fusion: %d (-%d doublons)", len(normalisees), nb_avant - len(normalisees))

    # Sauvegarde
    sauv = Sauvegarder(OUTPUT_DIR, logger)
    norm_dicts = [asdict(r) for r in normalisees]
    sauv.sauver_json(norm_dicts, "reunions_normalisees.json")

    try:
        sauv.sauver_parquet(norm_dicts, "reunions_normalisees.parquet")
    except Exception as e:
        logger.warning("Parquet ignoré: %s", e)

    try:
        sauv.sauver_csv(norm_dicts, "reunions_normalisees.csv")
    except Exception as e:
        logger.warning("CSV ignoré: %s", e)

    # Table de références
    refs = build_reunion_references(normalisees, brutes)
    sauv.sauver_json(refs, "reunions_references_02.json")

    logger.info("=" * 60)
    logger.info("RE-NORMALISATION TERMINÉE: %d normalisées", len(normalisees))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
