#!/usr/bin/env python3
"""
feature_builders.categorical_hash_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Hashes high-cardinality categorical features into fixed-size numeric features
for ML using the hashing trick.

Reads partants_master.jsonl in single-pass streaming mode, computes per-partant
hash-encoded categorical features, and writes the results to a JSONL file.

Hashing trick: hash(string) % n_buckets encodes each string into one of
n_buckets integer values, giving fixed-size numeric features suitable for
embedding layers or gradient boosting trees without a vocabulary lookup.

hashlib.md5 is used for consistency across runs and platforms. The first 8
bytes of the digest are interpreted as a big-endian uint64, then taken modulo
n_buckets to produce a stable integer in [0, n_buckets - 1].

Features per partant (10):
  - ch_horse_hash_16    : hash of horse_id into 16 buckets (embedding-ready)
  - ch_horse_hash_64    : hash of horse_id into 64 buckets
  - ch_jockey_hash_32   : hash of jockey name into 32 buckets
  - ch_trainer_hash_32  : hash of trainer/entraineur name into 32 buckets
  - ch_hippo_hash_16    : hash of hippodrome into 16 buckets
  - ch_owner_hash_16    : hash of proprietaire into 16 buckets
  - ch_sire_hash_32     : hash of nom_pere (sire) into 32 buckets
  - ch_dam_hash_16      : hash of nom_mere (dam) into 16 buckets
  - ch_horse_jockey_hash_32  : hash of (horse_id + jockey) combo into 32 buckets
  - ch_triple_hash_64        : hash of (horse + jockey + trainer) combo into 64 buckets

Usage:
    python feature_builders/categorical_hash_builder.py
    python feature_builders/categorical_hash_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/categorical_hash_builder.py --output-dir /path/to/output
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/categorical_hash")

_LOG_EVERY = 500_000

# ===========================================================================
# HASHING UTILITY
# ===========================================================================


def _hash_str(value: str, n_buckets: int) -> int:
    """Hash a string into an integer bucket in [0, n_buckets - 1].

    Uses MD5 for consistency across runs and platforms. Interprets the first
    8 bytes of the digest as a big-endian unsigned 64-bit integer, then takes
    the result modulo n_buckets.

    Args:
        value:     The string to hash. Must be a non-empty string.
        n_buckets: Number of hash buckets (output range: [0, n_buckets - 1]).

    Returns:
        Integer in [0, n_buckets - 1].
    """
    digest = hashlib.md5(value.encode("utf-8"), usedforsecurity=False).digest()
    uint64 = int.from_bytes(digest[:8], byteorder="big")
    return uint64 % n_buckets


def _safe_hash(value: Any, n_buckets: int) -> Optional[int]:
    """Return hash bucket for *value*, or None if value is absent/empty.

    Coerces the value to a stripped lowercase string before hashing so that
    minor capitalisation differences collapse to the same bucket.
    """
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    return _hash_str(s, n_buckets)


def _combo_hash(parts: list[Any], n_buckets: int) -> Optional[int]:
    """Hash the concatenation of multiple values separated by '|'.

    Returns None if any component is absent or empty, ensuring that unknown
    combos do not silently collapse to a valid bucket.
    """
    pieces: list[str] = []
    for p in parts:
        if p is None:
            return None
        s = str(p).strip().lower()
        if not s:
            return None
        pieces.append(s)
    return _hash_str("|".join(pieces), n_buckets)


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
                    logger.warning(
                        "Ligne JSON invalide ignoree (erreur %d)", errors
                    )
    logger.info(
        "Lecture terminee: %d records, %d erreurs JSON", count, errors
    )


# ===========================================================================
# FEATURE EXTRACTION HELPERS
# ===========================================================================


def _resolve_horse_id(rec: dict[str, Any]) -> Optional[str]:
    """Return the best available horse identifier from a record."""
    return rec.get("horse_id") or rec.get("nom_cheval")


def _resolve_jockey(rec: dict[str, Any]) -> Optional[str]:
    """Return the best available jockey identifier from a record."""
    return rec.get("jockey") or rec.get("nom_jockey")


def _resolve_trainer(rec: dict[str, Any]) -> Optional[str]:
    """Return the best available trainer identifier from a record."""
    return rec.get("entraineur") or rec.get("nom_entraineur")


# ===========================================================================
# MAIN BUILD (SINGLE-PASS STREAMING)
# ===========================================================================


def build_categorical_hash_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build categorical hash features in a single streaming pass.

    No state is accumulated between records -- each record is processed
    independently, making this builder truly O(1) memory per record.

    Args:
        input_path: Path to the partants_master.jsonl input file.
        logger:     Logger instance.

    Returns:
        List of dicts, one per input record, each containing:
          - partant_uid (str or None)
          - course_uid  (str or None)
          - date_reunion_iso (str or None)
          - 10 hash features (int or None)
    """
    logger.info("=== Categorical Hash Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    results: list[dict[str, Any]] = []
    n_read = 0
    n_null_uid = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Traite %d records...", n_read)

        # -- Resolve key fields --
        partant_uid = rec.get("partant_uid")
        course_uid = rec.get("course_uid")
        date_iso = rec.get("date_reunion_iso")

        horse = _resolve_horse_id(rec)
        jockey = _resolve_jockey(rec)
        trainer = _resolve_trainer(rec)
        hippodrome = rec.get("hippodrome")
        proprietaire = rec.get("proprietaire")
        nom_pere = rec.get("nom_pere")
        nom_mere = rec.get("nom_mere")

        if partant_uid is None:
            n_null_uid += 1

        # -- Compute hash features --
        ch_horse_hash_16 = _safe_hash(horse, 16)
        ch_horse_hash_64 = _safe_hash(horse, 64)
        ch_jockey_hash_32 = _safe_hash(jockey, 32)
        ch_trainer_hash_32 = _safe_hash(trainer, 32)
        ch_hippo_hash_16 = _safe_hash(hippodrome, 16)
        ch_owner_hash_16 = _safe_hash(proprietaire, 16)
        ch_sire_hash_32 = _safe_hash(nom_pere, 32)
        ch_dam_hash_16 = _safe_hash(nom_mere, 16)

        # Combo: horse_id + jockey
        ch_horse_jockey_hash_32 = _combo_hash([horse, jockey], 32)

        # Combo: horse + jockey + trainer
        ch_triple_hash_64 = _combo_hash([horse, jockey, trainer], 64)

        results.append({
            "partant_uid": partant_uid,
            "course_uid": course_uid,
            "date_reunion_iso": date_iso,
            # Individual entity hashes
            "ch_horse_hash_16": ch_horse_hash_16,
            "ch_horse_hash_64": ch_horse_hash_64,
            "ch_jockey_hash_32": ch_jockey_hash_32,
            "ch_trainer_hash_32": ch_trainer_hash_32,
            "ch_hippo_hash_16": ch_hippo_hash_16,
            "ch_owner_hash_16": ch_owner_hash_16,
            "ch_sire_hash_32": ch_sire_hash_32,
            "ch_dam_hash_16": ch_dam_hash_16,
            # Interaction hashes
            "ch_horse_jockey_hash_32": ch_horse_jockey_hash_32,
            "ch_triple_hash_64": ch_triple_hash_64,
        })

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs", len(results), elapsed
    )
    if n_null_uid:
        logger.warning(
            "%d records sans partant_uid (ils restent dans le fichier de sortie)",
            n_null_uid,
        )

    # Explicit GC after building to free JSONL lines still in memory
    gc.collect()

    return results


# ===========================================================================
# INPUT RESOLUTION & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI argument or auto-detection."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve parmi: "
        + str([str(c) for c in INPUT_CANDIDATES])
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features de hashage categoriel "
            "a partir de partants_master"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help=(
            "Repertoire de sortie "
            "(defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/categorical_hash)"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("categorical_hash_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_categorical_hash_features(input_path, logger)

    # -- Save output --
    out_path = output_dir / "categorical_hash_features.jsonl"
    save_jsonl(results, out_path, logger)

    # -- Fill-rate summary --
    if results:
        feature_keys = [
            k for k in results[0] if k not in {"partant_uid", "course_uid", "date_reunion_iso"}
        ]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100.0 * v / total)

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
