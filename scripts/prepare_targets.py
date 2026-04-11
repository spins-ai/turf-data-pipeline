#!/usr/bin/env python3
"""Prepare ML target variables from partants_master.jsonl.

Extracts and computes:
1. is_gagnant (binary: won the race)
2. is_place (binary: finished in top 3)
3. rapport_simple_gagnant (payout if won)
4. roi_simple (= rapport_simple_gagnant - 1 if won, else -1)
5. position_arrivee (ordinal)
6. nb_partants (field size, for weighting)

Output: JSONL + Parquet to D:/turf-data-pipeline/04_FEATURES/targets/
"""
import gc
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path

INPUT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/04_FEATURES/targets")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _safe_float(v):
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _safe_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_jsonl = OUTPUT_DIR / "targets.jsonl"
    tmp = out_jsonl.with_suffix(".tmp")

    t0 = time.perf_counter()
    written = 0
    stats = defaultdict(int)

    with open(tmp, "w", encoding="utf-8") as fout:
        with open(INPUT, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)

                uid = rec.get("partant_uid", "")
                pos = _safe_int(rec.get("position_arrivee"))
                is_gagnant = 1 if rec.get("is_gagnant") else (1 if pos == 1 else 0)
                is_place = 1 if pos is not None and 1 <= pos <= 3 else 0
                rapport = _safe_float(rec.get("rapport_simple_gagnant"))
                nb_partants = _safe_int(rec.get("nb_partants"))

                # ROI: if bet 1 unit on this horse to win
                if is_gagnant and rapport and rapport > 0:
                    roi_simple = round(rapport - 1, 2)
                else:
                    roi_simple = -1.0

                # Compute place payout ROI if available
                rapport_place = _safe_float(rec.get("rapport_simple_place"))
                if is_place and rapport_place and rapport_place > 0:
                    roi_place = round(rapport_place - 1, 2)
                else:
                    roi_place = -1.0

                target = {
                    "partant_uid": uid,
                    "is_gagnant": is_gagnant,
                    "is_place": is_place,
                    "position_arrivee": pos,
                    "nb_partants": nb_partants,
                    "rapport_simple_gagnant": rapport,
                    "rapport_simple_place": rapport_place,
                    "roi_simple": roi_simple,
                    "roi_place": roi_place,
                }

                fout.write(json.dumps(target, ensure_ascii=False) + "\n")
                written += 1

                # Stats
                if is_gagnant:
                    stats["winners"] += 1
                if is_place:
                    stats["placed"] += 1
                if pos is not None:
                    stats["has_position"] += 1
                if rapport is not None:
                    stats["has_rapport"] += 1

                if lineno % 500_000 == 0:
                    print(f"  {lineno:,} lines processed...", file=sys.stderr)
                    gc.collect()

    tmp.rename(out_jsonl)
    elapsed = time.perf_counter() - t0

    print(f"\n{'='*60}")
    print(f"TARGET VARIABLES PREPARED")
    print(f"{'='*60}")
    print(f"Total records: {written:,}")
    print(f"Winners: {stats['winners']:,} ({stats['winners']/written*100:.1f}%)")
    print(f"Placed (top 3): {stats['placed']:,} ({stats['placed']/written*100:.1f}%)")
    print(f"Has position: {stats['has_position']:,} ({stats['has_position']/written*100:.1f}%)")
    print(f"Has rapport: {stats['has_rapport']:,} ({stats['has_rapport']/written*100:.1f}%)")
    print(f"Time: {elapsed:.0f}s")
    print(f"Output: {out_jsonl}")


if __name__ == "__main__":
    main()
