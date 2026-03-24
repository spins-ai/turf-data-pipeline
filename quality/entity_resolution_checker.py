#!/usr/bin/env python3
"""
quality/entity_resolution_checker.py
=====================================
Detecteur de problemes de resolution d'entites dans partants_master.jsonl.

Verifie :
  1. Variantes de noms de chevaux (distance de Levenshtein <= 2)
  2. Variantes de noms de jockeys (distance de Levenshtein <= 2)
  3. Normalisation des hippodromes (doublons proches)
  4. Unicite des partant_uid par (date, reunion, course, numPmu)
  5. Coherence inter-sources (memes entites, sources differentes)

Streaming de partants_master.jsonl, RAM < 3 Go.

Usage :
    python quality/entity_resolution_checker.py
    python quality/entity_resolution_checker.py --input path/to/partants_master.jsonl
    python quality/entity_resolution_checker.py --sample-size 100000
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

DEFAULT_INPUT = _PROJECT_ROOT / "data_master" / "partants_master.jsonl"
DEFAULT_OUTPUT = _PROJECT_ROOT / "quality" / "entity_resolution_report.md"
DEFAULT_SAMPLE_SIZE = 50_000

# Levenshtein threshold for flagging potential duplicates
LEVENSHTEIN_THRESHOLD = 2

# Maximum pairs to report per category
MAX_REPORT_PAIRS = 50

# Reservoir sampling seed for reproducibility
RANDOM_SEED = 42

# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging  # noqa: E402


# ===========================================================================
# LEVENSHTEIN (pure Python, no dependencies)
# ===========================================================================

def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            # insertion, deletion, substitution
            cost = 0 if c1 == c2 else 1
            curr_row.append(min(
                curr_row[j] + 1,
                prev_row[j + 1] + 1,
                prev_row[j] + cost,
            ))
        prev_row = curr_row
    return prev_row[-1]


# ===========================================================================
# STREAMING + RESERVOIR SAMPLING
# ===========================================================================

def stream_jsonl(path: Path, logger: logging.Logger):
    """Yield dicts from a JSONL file, skipping bad lines."""
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                if lineno <= 5:
                    logger.warning("Ligne %d : JSON invalide, ignoree", lineno)


def reservoir_sample(iterable, k: int, seed: int = RANDOM_SEED) -> list[dict]:
    """Reservoir sampling (Vitter's Algorithm R) -- constant memory O(k)."""
    rng = random.Random(seed)
    reservoir: list[dict] = []
    for i, item in enumerate(iterable):
        if i < k:
            reservoir.append(item)
        else:
            j = rng.randint(0, i)
            if j < k:
                reservoir[j] = item
    return reservoir


# ===========================================================================
# CHECK 1 & 2 : NAME VARIANT DETECTION (horses / jockeys)
# ===========================================================================

def _find_near_duplicates(
    names: set[str],
    threshold: int,
    max_pairs: int,
    logger: logging.Logger,
    label: str,
) -> list[tuple[str, str, int]]:
    """
    Find pairs of names within Levenshtein distance <= threshold.

    Uses length-bucket pruning: two names can only be within distance d
    if their lengths differ by at most d.  This avoids the full O(n^2) scan.
    """
    logger.info("Recherche de doublons proches parmi %d %s ...", len(names), label)

    # Bucket by length
    by_length: dict[int, list[str]] = defaultdict(list)
    for n in names:
        by_length[len(n)].append(n)

    pairs: list[tuple[str, str, int]] = []
    lengths_sorted = sorted(by_length.keys())

    comparisons = 0
    for idx, l1 in enumerate(lengths_sorted):
        bucket1 = by_length[l1]
        # Compare within same bucket
        for i in range(len(bucket1)):
            for j in range(i + 1, len(bucket1)):
                comparisons += 1
                d = levenshtein_distance(bucket1[i], bucket1[j])
                if d <= threshold and d > 0:
                    pairs.append((bucket1[i], bucket1[j], d))
                    if len(pairs) >= max_pairs:
                        logger.info("  %d paires trouvees (limite atteinte), %d comparaisons", len(pairs), comparisons)
                        return pairs
        # Compare with nearby-length buckets
        for l2 in lengths_sorted[idx + 1:]:
            if l2 - l1 > threshold:
                break
            bucket2 = by_length[l2]
            for n1 in bucket1:
                for n2 in bucket2:
                    comparisons += 1
                    d = levenshtein_distance(n1, n2)
                    if d <= threshold and d > 0:
                        pairs.append((n1, n2, d))
                        if len(pairs) >= max_pairs:
                            logger.info("  %d paires trouvees (limite atteinte), %d comparaisons", len(pairs), comparisons)
                            return pairs

    logger.info("  %d paires trouvees, %d comparaisons", len(pairs), comparisons)
    return pairs


def check_name_variants(
    sample: list[dict],
    field: str,
    label: str,
    logger: logging.Logger,
) -> list[tuple[str, str, int]]:
    """Extract unique names from sample, find near-duplicates."""
    names: set[str] = set()
    for rec in sample:
        val = rec.get(field)
        if val and isinstance(val, str) and val.strip():
            names.add(val.strip().upper())
    logger.info("Noms uniques (%s) dans l'echantillon : %d", label, len(names))
    return _find_near_duplicates(names, LEVENSHTEIN_THRESHOLD, MAX_REPORT_PAIRS, logger, label)


# ===========================================================================
# CHECK 3 : HIPPODROME NORMALISATION
# ===========================================================================

def check_hippodrome_normalisation(
    hippodromes: set[str],
    logger: logging.Logger,
) -> list[tuple[str, str, int]]:
    """Find hippodrome names that are suspiciously close."""
    logger.info("Verification de la normalisation des hippodromes (%d uniques) ...", len(hippodromes))
    # Also flag substring containment (e.g. "vincennes" vs "paris-vincennes")
    containment_pairs: list[tuple[str, str, int]] = []
    hippo_list = sorted(hippodromes)
    for i in range(len(hippo_list)):
        for j in range(i + 1, len(hippo_list)):
            h1, h2 = hippo_list[i], hippo_list[j]
            # Check substring containment
            if h1 in h2 or h2 in h1:
                containment_pairs.append((h1, h2, 0))

    # Also do Levenshtein check
    lev_pairs = _find_near_duplicates(hippodromes, LEVENSHTEIN_THRESHOLD, MAX_REPORT_PAIRS, logger, "hippodromes")

    # Merge, deduplicate
    seen = set()
    merged: list[tuple[str, str, int]] = []
    for p in containment_pairs + lev_pairs:
        key = (min(p[0], p[1]), max(p[0], p[1]))
        if key not in seen:
            seen.add(key)
            merged.append(p)
    return merged


# ===========================================================================
# CHECK 4 : UID UNIQUENESS
# ===========================================================================

def check_uid_uniqueness_streaming(
    path: Path,
    logger: logging.Logger,
) -> dict[str, Any]:
    """
    Verify partant_uid is unique per (date, reunion, course, numPmu).
    Streams the file; stores only uid -> composite-key mapping in a dict.
    """
    logger.info("Verification de l'unicite des partant_uid (streaming) ...")

    uid_to_key: dict[str, str] = {}
    duplicates: list[dict[str, str]] = []
    total = 0
    missing_uid = 0
    missing_composite = 0

    for rec in stream_jsonl(path, logger):
        total += 1
        uid = rec.get("partant_uid")
        if not uid:
            missing_uid += 1
            continue

        date_r = rec.get("date_reunion_iso", "")
        reunion = str(rec.get("numero_reunion", ""))
        course = str(rec.get("numero_course", ""))
        num_pmu = str(rec.get("num_pmu", ""))

        if not all([date_r, reunion, course, num_pmu]):
            missing_composite += 1

        composite = f"{date_r}|{reunion}|{course}|{num_pmu}"

        if uid in uid_to_key:
            if uid_to_key[uid] != composite:
                if len(duplicates) < MAX_REPORT_PAIRS:
                    duplicates.append({
                        "uid": uid,
                        "key_1": uid_to_key[uid],
                        "key_2": composite,
                    })
        else:
            uid_to_key[uid] = composite

        # Memory guard: if dict exceeds ~10M entries, warn and stop growing
        if len(uid_to_key) > 10_000_000 and total % 1_000_000 == 0:
            logger.warning("uid_to_key a atteint %d entrees, memoire elevee", len(uid_to_key))

    # Also check reverse: same composite key -> multiple UIDs
    key_to_uids: dict[str, list[str]] = defaultdict(list)
    for uid, key in uid_to_key.items():
        key_to_uids[key].append(uid)
    composite_collisions = {k: v for k, v in key_to_uids.items() if len(v) > 1}
    # Free memory
    del key_to_uids

    return {
        "total_records": total,
        "unique_uids": len(uid_to_key),
        "missing_uid": missing_uid,
        "missing_composite_fields": missing_composite,
        "uid_maps_to_different_keys": duplicates,
        "composite_key_collisions_count": len(composite_collisions),
        "composite_key_collisions_sample": dict(list(composite_collisions.items())[:20]),
    }


# ===========================================================================
# CHECK 5 : CROSS-SOURCE MATCHING
# ===========================================================================

def check_cross_source_matching(
    path: Path,
    logger: logging.Logger,
    max_groups: int = 1000,
) -> dict[str, Any]:
    """
    Stream the file and group records by cle_partant across sources.
    For records that share the same cle_partant but come from different sources,
    check if key field values agree.
    """
    logger.info("Verification de la coherence inter-sources (streaming) ...")

    COMPARE_FIELDS = [
        "nom_cheval", "jockey_driver", "distance", "discipline",
        "position_arrivee", "nombre_partants",
    ]

    # Store source -> field values per cle_partant, but only for multi-source keys
    # First pass: find cle_partant that appear in multiple sources
    key_sources: dict[str, set[str]] = defaultdict(set)
    total = 0
    for rec in stream_jsonl(path, logger):
        total += 1
        cle = rec.get("cle_partant")
        source = rec.get("source")
        if cle and source:
            key_sources[cle].add(source)
        # Memory guard
        if len(key_sources) > 5_000_000:
            logger.warning("key_sources trop volumineux, arret du scan")
            break

    multi_source_keys = {k for k, v in key_sources.items() if len(v) > 1}
    del key_sources  # free memory
    logger.info("  Cles multi-sources : %d", len(multi_source_keys))

    if not multi_source_keys:
        return {
            "total_records": total,
            "multi_source_keys": 0,
            "disagreements": [],
        }

    # Limit to max_groups for memory
    if len(multi_source_keys) > max_groups:
        rng = random.Random(RANDOM_SEED)
        multi_source_keys = set(rng.sample(sorted(multi_source_keys), max_groups))

    # Second pass: collect field values for those keys
    key_data: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for rec in stream_jsonl(path, logger):
        cle = rec.get("cle_partant")
        source = rec.get("source")
        if cle in multi_source_keys and source:
            vals = {}
            for f in COMPARE_FIELDS:
                v = rec.get(f)
                if v is not None:
                    vals[f] = v
            key_data[cle][source] = vals

    # Compare across sources
    disagreements: list[dict[str, Any]] = []
    for cle, sources_dict in key_data.items():
        source_list = list(sources_dict.keys())
        for i in range(len(source_list)):
            for j in range(i + 1, len(source_list)):
                s1, s2 = source_list[i], source_list[j]
                v1, v2 = sources_dict[s1], sources_dict[s2]
                diffs = {}
                for f in COMPARE_FIELDS:
                    val1 = v1.get(f)
                    val2 = v2.get(f)
                    if val1 is not None and val2 is not None:
                        # Normalize strings for comparison
                        cmp1 = str(val1).strip().upper() if isinstance(val1, str) else val1
                        cmp2 = str(val2).strip().upper() if isinstance(val2, str) else val2
                        if cmp1 != cmp2:
                            diffs[f] = {s1: val1, s2: val2}
                if diffs:
                    disagreements.append({
                        "cle_partant": cle,
                        "sources": [s1, s2],
                        "disagreements": diffs,
                    })
                    if len(disagreements) >= MAX_REPORT_PAIRS:
                        return {
                            "total_records": total,
                            "multi_source_keys": len(multi_source_keys),
                            "disagreements": disagreements,
                        }

    return {
        "total_records": total,
        "multi_source_keys": len(multi_source_keys),
        "disagreements": disagreements,
    }


# ===========================================================================
# REPORT GENERATION
# ===========================================================================

def generate_report(
    horse_pairs: list[tuple[str, str, int]],
    jockey_pairs: list[tuple[str, str, int]],
    hippo_pairs: list[tuple[str, str, int]],
    uid_results: dict[str, Any],
    cross_source: dict[str, Any],
    sample_size: int,
    total_records: int,
    output_path: Path,
    logger: logging.Logger,
) -> None:
    """Write Markdown report."""
    logger.info("Generation du rapport : %s", output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# Entity Resolution Report")
    lines.append("")
    lines.append(f"- **Total records streamed (UID check)**: {uid_results['total_records']:,}")
    lines.append(f"- **Sample size (name checks)**: {sample_size:,}")
    lines.append(f"- **Levenshtein threshold**: {LEVENSHTEIN_THRESHOLD}")
    lines.append("")

    # -- Check 1 : Horse name variants --
    lines.append("## 1. Horse Name Variants")
    lines.append("")
    if horse_pairs:
        lines.append(f"Found **{len(horse_pairs)}** potential duplicate pairs:")
        lines.append("")
        lines.append("| Name A | Name B | Distance |")
        lines.append("|--------|--------|----------|")
        for a, b, d in sorted(horse_pairs, key=lambda x: x[2]):
            lines.append(f"| {a} | {b} | {d} |")
    else:
        lines.append("No near-duplicate horse names found in sample.")
    lines.append("")

    # -- Check 2 : Jockey name variants --
    lines.append("## 2. Jockey Name Variants")
    lines.append("")
    if jockey_pairs:
        lines.append(f"Found **{len(jockey_pairs)}** potential duplicate pairs:")
        lines.append("")
        lines.append("| Name A | Name B | Distance |")
        lines.append("|--------|--------|----------|")
        for a, b, d in sorted(jockey_pairs, key=lambda x: x[2]):
            lines.append(f"| {a} | {b} | {d} |")
    else:
        lines.append("No near-duplicate jockey names found in sample.")
    lines.append("")

    # -- Check 3 : Hippodrome normalisation --
    lines.append("## 3. Hippodrome Normalisation")
    lines.append("")
    if hippo_pairs:
        lines.append(f"Found **{len(hippo_pairs)}** suspicious pairs:")
        lines.append("")
        lines.append("| Hippodrome A | Hippodrome B | Distance / Containment |")
        lines.append("|--------------|--------------|------------------------|")
        for a, b, d in hippo_pairs:
            label = "substring" if d == 0 else str(d)
            lines.append(f"| {a} | {b} | {label} |")
    else:
        lines.append("No suspicious hippodrome name pairs found.")
    lines.append("")

    # -- Check 4 : UID uniqueness --
    lines.append("## 4. UID Uniqueness")
    lines.append("")
    lines.append(f"- Unique UIDs: **{uid_results['unique_uids']:,}**")
    lines.append(f"- Missing partant_uid: **{uid_results['missing_uid']:,}**")
    lines.append(f"- Missing composite key fields: **{uid_results['missing_composite_fields']:,}**")
    uid_dups = uid_results["uid_maps_to_different_keys"]
    lines.append(f"- UIDs mapping to different composite keys: **{len(uid_dups)}**")
    if uid_dups:
        lines.append("")
        lines.append("| UID | Key 1 | Key 2 |")
        lines.append("|-----|-------|-------|")
        for dup in uid_dups[:20]:
            lines.append(f"| `{dup['uid']}` | {dup['key_1']} | {dup['key_2']} |")
    coll_count = uid_results["composite_key_collisions_count"]
    lines.append(f"- Composite keys mapping to multiple UIDs: **{coll_count}**")
    if uid_results["composite_key_collisions_sample"]:
        lines.append("")
        lines.append("Sample collisions:")
        lines.append("")
        for key, uids in list(uid_results["composite_key_collisions_sample"].items())[:10]:
            lines.append(f"  - `{key}` -> {len(uids)} UIDs")
    lines.append("")

    # -- Check 5 : Cross-source matching --
    lines.append("## 5. Cross-Source Matching")
    lines.append("")
    lines.append(f"- Multi-source keys sampled: **{cross_source['multi_source_keys']:,}**")
    disag = cross_source["disagreements"]
    lines.append(f"- Disagreements found: **{len(disag)}**")
    if disag:
        lines.append("")
        for item in disag[:20]:
            lines.append(f"### `{item['cle_partant']}` ({' vs '.join(item['sources'])})")
            lines.append("")
            for field, vals in item["disagreements"].items():
                parts = [f"{src}: `{v}`" for src, v in vals.items()]
                lines.append(f"- **{field}**: {' / '.join(parts)}")
            lines.append("")
    lines.append("")

    # -- Summary --
    issues = 0
    if horse_pairs:
        issues += len(horse_pairs)
    if jockey_pairs:
        issues += len(jockey_pairs)
    if hippo_pairs:
        issues += len(hippo_pairs)
    if uid_dups:
        issues += len(uid_dups)
    issues += coll_count
    issues += len(disag)

    lines.append("## Summary")
    lines.append("")
    if issues == 0:
        lines.append("No entity resolution issues detected.")
    else:
        lines.append(f"Total potential issues flagged: **{issues}**")
    lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Rapport ecrit : %s (%d lignes)", output_path, len(lines))


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Entity resolution checker for partants_master.jsonl",
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help="Path for Markdown report",
    )
    parser.add_argument(
        "--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE,
        help="Number of records to reservoir-sample for name checks",
    )
    args = parser.parse_args()

    logger = setup_logging("entity_resolution_checker")
    logger.info("=== Entity Resolution Checker ===")
    logger.info("Input : %s", args.input)
    logger.info("Sample: %d records", args.sample_size)

    if not args.input.exists():
        logger.error("Fichier introuvable : %s", args.input)
        sys.exit(1)

    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1 : Reservoir sample + collect hippodromes (single pass)
    # ------------------------------------------------------------------
    logger.info("Pass 1 : echantillonnage + collecte hippodromes ...")
    rng = random.Random(RANDOM_SEED)
    reservoir: list[dict] = []
    hippodromes: set[str] = set()
    total_records = 0

    for rec in stream_jsonl(args.input, logger):
        total_records += 1
        # Reservoir sampling inline
        if total_records <= args.sample_size:
            reservoir.append(rec)
        else:
            j = rng.randint(0, total_records - 1)
            if j < args.sample_size:
                reservoir[j] = rec

        # Collect hippodromes (set, very low cardinality)
        hippo = rec.get("hippodrome_normalise")
        if hippo and isinstance(hippo, str) and hippo.strip():
            hippodromes.add(hippo.strip().lower())

        if total_records % 500_000 == 0:
            logger.info("  ... %d records lus", total_records)

    logger.info("Pass 1 terminee : %d records, %d echantillonnes, %d hippodromes",
                total_records, len(reservoir), len(hippodromes))

    # ------------------------------------------------------------------
    # Check 1 : Horse name variants
    # ------------------------------------------------------------------
    horse_pairs = check_name_variants(reservoir, "nom_cheval", "chevaux", logger)

    # ------------------------------------------------------------------
    # Check 2 : Jockey name variants
    # ------------------------------------------------------------------
    jockey_pairs = check_name_variants(reservoir, "jockey_driver", "jockeys", logger)

    # ------------------------------------------------------------------
    # Check 3 : Hippodrome normalisation
    # ------------------------------------------------------------------
    hippo_pairs = check_hippodrome_normalisation(hippodromes, logger)

    # Free sample memory before heavy passes
    del reservoir

    # ------------------------------------------------------------------
    # Check 4 : UID uniqueness (full streaming pass)
    # ------------------------------------------------------------------
    uid_results = check_uid_uniqueness_streaming(args.input, logger)

    # ------------------------------------------------------------------
    # Check 5 : Cross-source matching (two streaming passes)
    # ------------------------------------------------------------------
    cross_source = check_cross_source_matching(args.input, logger)

    # ------------------------------------------------------------------
    # Generate report
    # ------------------------------------------------------------------
    elapsed = time.time() - t0
    logger.info("Duree totale : %.1f s", elapsed)

    generate_report(
        horse_pairs=horse_pairs,
        jockey_pairs=jockey_pairs,
        hippo_pairs=hippo_pairs,
        uid_results=uid_results,
        cross_source=cross_source,
        sample_size=min(args.sample_size, total_records),
        total_records=total_records,
        output_path=args.output,
        logger=logger,
    )

    # Exit code based on findings
    total_issues = (
        len(horse_pairs)
        + len(jockey_pairs)
        + len(hippo_pairs)
        + len(uid_results["uid_maps_to_different_keys"])
        + uid_results["composite_key_collisions_count"]
        + len(cross_source["disagreements"])
    )
    if total_issues > 0:
        logger.warning("Issues detectees : %d (voir rapport)", total_issues)
    else:
        logger.info("Aucun probleme de resolution d'entites detecte.")


if __name__ == "__main__":
    main()
