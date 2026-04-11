#!/usr/bin/env python3
"""Market divergence: ZeTurf vs PMU cotes comparison.
STREAMING VERSION - no OOM, processes course-by-course.

Features:
  - md_divergence       : abs(cote_zeturf - cote_pmu) / cote_pmu
  - md_divergence_proxy : abs(zeturf_rank - pmu_rank) / nb_partants
  - md_zeturf_pmu_ratio : cote_zeturf / cote_finale
"""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
ZETURF_PATH = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/51_zeturf/zeturf_data.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_divergence")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _load_zeturf_cotes(path: Path, logger):
    """Load ZeTurf per-partant cotes into a lookup dict."""
    lookup = {}
    if not path.exists():
        logger.info("ZeTurf file not found: %s", path)
        return lookup

    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            cote = _safe(rec.get("cote_zeturf"))
            if cote is None:
                continue
            date_str = str(rec.get("date", ""))[:10]
            horse = (rec.get("cheval") or rec.get("nom") or "").strip().upper()
            num = rec.get("numero") or rec.get("num") or ""
            if date_str and horse:
                lookup[f"{date_str}|{horse}"] = cote
                count += 1
            if date_str and num:
                lookup[f"{date_str}|{num}"] = cote

    logger.info("Loaded %d ZeTurf cotes", count)
    return lookup


def main():
    logger = setup_logging("market_divergence_builder_v2")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "market_divergence_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    zeturf_cotes = _load_zeturf_cotes(ZETURF_PATH, logger)

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        current_course = None
        course_records = []

        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                cuid = rec.get("course_uid", "")

                if cuid != current_course and course_records:
                    _process_course(course_records, fout, zeturf_cotes, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, zeturf_cotes, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, zeturf_cotes, fills):
    """Process one course: compute PMU rank, match ZeTurf, write features."""

    # Compute PMU cote rank within course
    scored = []
    for i, rec in enumerate(records):
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        if cote is not None:
            scored.append((i, cote))

    scored.sort(key=lambda x: x[1])
    pmu_rank = {}
    for rank, (i, _) in enumerate(scored, 1):
        pmu_rank[i] = rank
    nb = len(records)

    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        cote_pmu = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))

        # Try direct ZeTurf cote match
        cote_zt = None
        if zeturf_cotes:
            date_str = str(rec.get("date_reunion_iso", ""))[:10]
            horse = str(rec.get("nom_cheval", "")).strip().upper()
            num = rec.get("num_pmu", "")
            cote_zt = zeturf_cotes.get(f"{date_str}|{horse}")
            if cote_zt is None and num:
                cote_zt = zeturf_cotes.get(f"{date_str}|{num}")

        # 1. Direct divergence
        if cote_zt is not None and cote_pmu is not None:
            feat["md_divergence"] = round(abs(cote_zt - cote_pmu) / cote_pmu, 4)
            feat["md_zeturf_pmu_ratio"] = round(cote_zt / cote_pmu, 4)
            fills["md_divergence"] += 1
            fills["md_zeturf_pmu_ratio"] += 1

        # 2. Proxy rank divergence
        zt_rank = _safe(rec.get("zeturf_prono_rang"))
        pr = pmu_rank.get(i)
        if zt_rank is not None and pr is not None and nb > 1:
            feat["md_divergence_proxy"] = round(abs(zt_rank - pr) / nb, 4)
            fills["md_divergence_proxy"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
