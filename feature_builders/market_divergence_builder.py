#!/usr/bin/env python3
"""
feature_builders.market_divergence_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Market divergence feature comparing ZeTurf cotes with PMU cotes.

Reads ZeTurf data (output/51_zeturf/zeturf_data.jsonl) which may contain
per-partant ``cote_zeturf`` values, and compares them with ``cote_finale``
(PMU final odds) from partants_master to compute a divergence signal.

When per-partant ZeTurf cotes are unavailable (course-level data only),
the builder uses the ``zeturf_prono_rang`` field (already merged into
partants_master) to derive a proxy implied-odds rank divergence.

Temporal integrity: both ZeTurf cotes and PMU cotes are observed at race
time -- no future leakage.

Produces:
  - market_divergence.jsonl  in output/market_divergence/

Features per partant:
  - market_divergence        : abs(cote_zeturf - cote_pmu) / cote_pmu
                               when per-partant zeturf cotes are available.
  - market_divergence_proxy  : abs(zeturf_rank_implied - pmu_rank) / nb_partants
                               proxy when only pronostic ranks are available.
  - zeturf_pmu_ratio         : cote_zeturf / cote_finale
                               raw ratio (>1 = ZeTurf thinks less likely).

Usage:
    python feature_builders/market_divergence_builder.py
    python feature_builders/market_divergence_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
ZETURF_PATH = _PROJECT_ROOT / "output" / "51_zeturf" / "zeturf_data.jsonl"
OUTPUT_DIR = _PROJECT_ROOT / "output" / "market_divergence"

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _find_input() -> Path:
    """Find the first existing input file."""
    for p in INPUT_CANDIDATES:
        if p.exists():
            return p
    return INPUT_CANDIDATES[0]


def _load_zeturf_cotes(zeturf_path: Path, logger: Any) -> dict[str, float]:
    """Load per-partant ZeTurf cotes into a lookup dict.

    Returns a dict keyed by a composite key (date|horse_name or date|num)
    mapping to cote_zeturf float values.  If the file doesn't exist or
    contains no per-partant cotes, returns an empty dict.
    """
    lookup: dict[str, float] = {}
    if not zeturf_path.exists():
        logger.info("ZeTurf file not found at %s -- skipping direct cotes", zeturf_path)
        return lookup

    count = 0
    with open(zeturf_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            cote = _safe_float(rec.get("cote_zeturf"))
            if cote is None or cote <= 0:
                continue

            # Build lookup key from available identifiers
            date_str = str(rec.get("date", ""))[:10]
            # Try horse name or number
            horse = rec.get("cheval", rec.get("nom", "")).strip().upper()
            num = rec.get("numero", rec.get("num", ""))

            if date_str and horse:
                lookup[f"{date_str}|{horse}"] = cote
                count += 1
            if date_str and num:
                lookup[f"{date_str}|{num}"] = cote

    logger.info("Loaded %d ZeTurf per-partant cotes", count)
    return lookup


# ===========================================================================
# BUILDER
# ===========================================================================


def build_market_divergence(
    partants: list[dict],
    zeturf_cotes: dict[str, float],
    logger: Any = None,
) -> list[dict]:
    """Compute market divergence features for each partant.

    Parameters
    ----------
    partants : list[dict]
        Records from partants_master.
    zeturf_cotes : dict[str, float]
        Lookup of ZeTurf per-partant cotes (may be empty).
    logger : Logger, optional

    Returns
    -------
    list[dict]
        Input records augmented with market divergence features.
    """
    import logging
    if logger is None:
        logger = logging.getLogger(__name__)

    # Group partants by course_uid for rank-based proxy
    course_groups: dict[str, list[int]] = defaultdict(list)
    for idx, row in enumerate(partants):
        cuid = row.get("course_uid")
        if cuid:
            course_groups[cuid].append(idx)

    # Pre-compute PMU odds rank within each course
    pmu_rank: dict[int, Optional[float]] = {}
    for cuid, indices in course_groups.items():
        scored = []
        for i in indices:
            cote = _safe_float(partants[i].get("cote_finale"))
            if cote is not None and cote > 0:
                scored.append((i, cote))
        if len(scored) < 2:
            for i in indices:
                pmu_rank[i] = None
            continue
        # Sort ascending by cote (lowest odds = rank 1 = favourite)
        scored.sort(key=lambda x: x[1])
        for rank_pos, (i, _) in enumerate(scored):
            pmu_rank[i] = rank_pos + 1
        scored_set = {i for i, _ in scored}
        for i in indices:
            if i not in scored_set:
                pmu_rank[i] = None

    enriched_direct = 0
    enriched_proxy = 0
    results: list[dict] = []

    for idx, row in enumerate(partants):
        feat: dict[str, Optional[float]] = {
            "market_divergence": None,
            "market_divergence_proxy": None,
            "zeturf_pmu_ratio": None,
        }

        cote_pmu = _safe_float(row.get("cote_finale"))

        # --- Try direct ZeTurf cote matching ---
        cote_zt: Optional[float] = None
        if zeturf_cotes:
            date_str = str(row.get("date_reunion_iso", ""))[:10]
            horse = str(row.get("nom_cheval", "")).strip().upper()
            num = row.get("num_pmu", "")

            # Try horse name key first, then number
            cote_zt = zeturf_cotes.get(f"{date_str}|{horse}")
            if cote_zt is None and num:
                cote_zt = zeturf_cotes.get(f"{date_str}|{num}")

        if cote_zt is not None and cote_pmu is not None and cote_pmu > 0:
            feat["market_divergence"] = round(abs(cote_zt - cote_pmu) / cote_pmu, 4)
            feat["zeturf_pmu_ratio"] = round(cote_zt / cote_pmu, 4)
            enriched_direct += 1

        # --- Proxy: rank-based divergence using zeturf_prono_rang ---
        zt_rank = _safe_float(row.get("zeturf_prono_rang"))
        pr = pmu_rank.get(idx)
        if zt_rank is not None and pr is not None:
            cuid = row.get("course_uid")
            nb = len(course_groups.get(cuid, []))
            if nb > 1:
                feat["market_divergence_proxy"] = round(
                    abs(zt_rank - pr) / nb, 4
                )
                enriched_proxy += 1

        row.update(feat)
        results.append(row)

        if (idx + 1) % _LOG_EVERY == 0:
            logger.info(
                "  %d/%d traites, %d direct, %d proxy",
                idx + 1, len(partants), enriched_direct, enriched_proxy,
            )

    logger.info(
        "Market divergence: %d direct, %d proxy out of %d partants",
        enriched_direct, enriched_proxy, len(results),
    )
    return results


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market divergence: ZeTurf vs PMU cotes"
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Partants JSONL/JSON file (auto-detected if omitted)",
    )
    parser.add_argument(
        "--zeturf",
        default=str(ZETURF_PATH),
        help="ZeTurf JSONL file",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Output directory",
    )
    args = parser.parse_args()

    logger = setup_logging("market_divergence")
    logger.info("=" * 70)
    logger.info("market_divergence_builder.py")
    logger.info("=" * 70)

    # Load input
    input_path = Path(args.input) if args.input else _find_input()
    logger.info("Input: %s", input_path)

    partants: list[dict] = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                partants.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    logger.info("Loaded %d partants", len(partants))

    # Load ZeTurf cotes
    zeturf_cotes = _load_zeturf_cotes(Path(args.zeturf), logger)

    # Build features
    results = build_market_divergence(partants, zeturf_cotes, logger)

    # Save
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "market_divergence.jsonl"
    save_jsonl(results, str(out_path), logger)
    logger.info("Done -- %d partants written to %s", len(results), out_path)


if __name__ == "__main__":
    main()
