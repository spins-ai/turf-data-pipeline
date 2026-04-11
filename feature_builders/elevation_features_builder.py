#!/usr/bin/env python3
"""
feature_builders.elevation_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Elevation / terrain-derived features using hippodrome characteristics
and metadata.

Single-pass streaming: all features are derived from the current record
fields (corde, type_piste, hippodrome_normalise, distance) -- no temporal
state, no future leakage.

Produces:
  - elevation_features.jsonl   in builder_outputs/elevation_features/

Features per partant (8):
  - elv_is_left_handed         : 1 if corde contains "gauche" or "G"
  - elv_is_right_handed        : 1 if corde contains "droite" or "D"
  - elv_corde_encoded          : G=0, D=1, eight-shaped/both=2, None
  - elv_track_type_encoded     : herbe=0, sable=1, psf=2, fibresand=3,
                                 cendree=4, other=5
  - elv_is_turf                : 1 if type_piste is turf/herbe
  - elv_is_all_weather         : 1 if psf or sable-based surface
  - elv_circuit_size_proxy     : large=2, medium=1, small=0 based on
                                 hippodrome lookup (top 20)
  - elv_distance_x_surface     : distance * (1 if turf else 0.95)

Usage:
    python feature_builders/elevation_features_builder.py
    python feature_builders/elevation_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/elevation_features")

_LOG_EVERY = 500_000

# ===========================================================================
# STATIC LOOKUPS
# ===========================================================================

# Top 20 hippodromes by typical circuit size.
# large = major venues with large fields, medium = regional, small = provincial.
_HIPPODROME_SIZE: dict[str, int] = {
    "vincennes": 2,
    "longchamp": 2,
    "auteuil": 2,
    "chantilly": 2,
    "deauville": 2,
    "saint-cloud": 2,
    "enghien": 2,
    "paris-vincennes": 2,
    "maisons-laffitte": 1,
    "compiegne": 1,
    "fontainebleau": 1,
    "lyon-parilly": 1,
    "toulouse": 1,
    "bordeaux": 1,
    "marseille-borely": 1,
    "cagnes-sur-mer": 1,
    "angers": 0,
    "le-croise-laroche": 0,
    "vichy": 0,
    "cabourg": 0,
    "laval": 0,
    "graignes": 0,
    "meslay-du-maine": 0,
    "nantes": 1,
    "lyon-la-soie": 1,
    "pau": 1,
    "strasbourg": 0,
    "le-mans": 0,
    "mauquenchy": 0,
    "amiens": 0,
}

# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
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
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _normalise_corde(raw: Any) -> Optional[str]:
    """Normalise corde string to a lowercase canonical form."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    return s if s else None


def _normalise_piste(raw: Any) -> Optional[str]:
    """Normalise type_piste to lowercase."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    return s if s else None


def _normalise_hippo(raw: Any) -> Optional[str]:
    """Normalise hippodrome name to lowercase, stripped."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    return s if s else None


# ---------------------------------------------------------------------------
# Feature computation helpers
# ---------------------------------------------------------------------------


def _compute_is_left_handed(corde: Optional[str]) -> Optional[int]:
    if corde is None:
        return None
    if "gauche" in corde or corde == "g":
        return 1
    if "droite" in corde or corde == "d":
        return 0
    return None


def _compute_is_right_handed(corde: Optional[str]) -> Optional[int]:
    if corde is None:
        return None
    if "droite" in corde or corde == "d":
        return 1
    if "gauche" in corde or corde == "g":
        return 0
    return None


def _compute_corde_encoded(corde: Optional[str]) -> Optional[int]:
    """G=0, D=1, eight-shaped/both=2, None if missing."""
    if corde is None:
        return None
    if "gauche" in corde or corde == "g":
        return 0
    if "droite" in corde or corde == "d":
        return 1
    # eight-shaped, both, mixte, etc.
    if corde:
        return 2
    return None


def _compute_track_type_encoded(piste: Optional[str], is_psf: Any) -> Optional[int]:
    """herbe=0, sable=1, psf=2, fibresand=3, cendree=4, other=5."""
    if piste is None and not is_psf:
        return None
    # Check PSF flag first
    if is_psf:
        return 2
    if piste is None:
        return None
    if "herbe" in piste or "turf" in piste or "gazon" in piste:
        return 0
    if "sable" in piste:
        return 1
    if "psf" in piste or "polytrack" in piste:
        return 2
    if "fibresand" in piste or "fibre" in piste:
        return 3
    if "cendr" in piste:
        return 4
    # Anything else that's non-empty
    if piste:
        return 5
    return None


def _compute_is_turf(piste: Optional[str]) -> Optional[int]:
    if piste is None:
        return None
    if "herbe" in piste or "turf" in piste or "gazon" in piste:
        return 1
    return 0


def _compute_is_all_weather(piste: Optional[str], is_psf: Any) -> Optional[int]:
    if is_psf:
        return 1
    if piste is None:
        return None
    if "psf" in piste or "polytrack" in piste or "sable" in piste or "fibresand" in piste or "fibre" in piste:
        return 1
    return 0


def _compute_circuit_size_proxy(hippo: Optional[str]) -> Optional[int]:
    """Lookup hippodrome circuit size. large=2, medium=1, small=0."""
    if hippo is None:
        return None
    # Direct lookup
    val = _HIPPODROME_SIZE.get(hippo)
    if val is not None:
        return val
    # Try partial match for known large hippodromes
    for key, size in _HIPPODROME_SIZE.items():
        if key in hippo or hippo in key:
            return size
    # Unknown hippodrome defaults to small
    return 0


def _compute_distance_x_surface(
    distance: Optional[float], is_turf: Optional[int]
) -> Optional[float]:
    """distance * (1 if turf else 0.95)."""
    if distance is None:
        return None
    factor = 1.0 if (is_turf == 1) else 0.95
    return round(distance * factor, 1)


# ===========================================================================
# MAIN BUILD -- SINGLE-PASS STREAMING
# ===========================================================================


def _build_streaming(input_path: Path, output_path: Path, logger) -> int:
    """Stream partants, compute features, write directly to disk.

    Returns total records written.
    """
    logger.info("=== Elevation Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0

    fill_counts: dict[str, int] = {
        "elv_is_left_handed": 0,
        "elv_is_right_handed": 0,
        "elv_corde_encoded": 0,
        "elv_track_type_encoded": 0,
        "elv_is_turf": 0,
        "elv_is_all_weather": 0,
        "elv_circuit_size_proxy": 0,
        "elv_distance_x_surface": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for rec in _iter_jsonl(input_path, logger):
            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_processed)
                gc.collect()

            partant_uid = rec.get("partant_uid")
            if not partant_uid:
                continue

            # Extract and normalise raw fields
            corde = _normalise_corde(rec.get("corde"))
            piste = _normalise_piste(rec.get("type_piste"))
            hippo = _normalise_hippo(rec.get("hippodrome_normalise"))
            distance = _safe_float(rec.get("distance"))
            is_psf = rec.get("met_is_psf")

            # Compute features
            is_left = _compute_is_left_handed(corde)
            is_right = _compute_is_right_handed(corde)
            corde_enc = _compute_corde_encoded(corde)
            track_enc = _compute_track_type_encoded(piste, is_psf)
            is_turf = _compute_is_turf(piste)
            is_aw = _compute_is_all_weather(piste, is_psf)
            circuit_sz = _compute_circuit_size_proxy(hippo)
            dist_x_surf = _compute_distance_x_surface(distance, is_turf)

            features: dict[str, Any] = {
                "partant_uid": partant_uid,
                "elv_is_left_handed": is_left,
                "elv_is_right_handed": is_right,
                "elv_corde_encoded": corde_enc,
                "elv_track_type_encoded": track_enc,
                "elv_is_turf": is_turf,
                "elv_is_all_weather": is_aw,
                "elv_circuit_size_proxy": circuit_sz,
                "elv_distance_x_surface": dist_x_surf,
            }

            # Track fill rates
            for k in fill_counts:
                if features[k] is not None:
                    fill_counts[k] += 1

            fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
            n_written += 1

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Elevation features build termine: %d features ecrites en %.1fs",
        n_written, elapsed,
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

    return n_written


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features elevation/terrain a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/elevation_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("elevation_features_builder")
    logger.info("=" * 70)
    logger.info("elevation_features_builder.py -- Elevation/terrain features")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "elevation_features.jsonl"

    n_written = _build_streaming(input_path, out_path, logger)

    logger.info("Termine -- %d partants traites", n_written)


if __name__ == "__main__":
    main()
