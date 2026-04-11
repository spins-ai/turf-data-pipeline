"""
update_version_manifest.py
Updates VERSION_MANIFEST.json with current pipeline stats.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import duckdb

# Paths
MANIFEST_PATH = Path("D:/turf-data-pipeline/VERSION_MANIFEST.json")
FEATURES_DIR = Path("D:/turf-data-pipeline/04_FEATURES")

CONSOLIDATED_PARQUET = FEATURES_DIR / "consolidated_features.parquet"
MASTER_PARQUET = FEATURES_DIR / "master_features.parquet"
TARGETS_PARQUET = FEATURES_DIR / "targets.parquet"
SPLITS_DIR = FEATURES_DIR / "splits"


def file_size_mb(path: Path) -> float | None:
    if path.exists():
        return round(path.stat().st_size / (1024 ** 2), 2)
    return None


def count_lines(path: Path) -> int | None:
    if not path.exists():
        return None
    with open(path, "r") as f:
        return sum(1 for line in f if line.strip())


def get_parquet_stats(path: Path, con: duckdb.DuckDBPyConnection) -> dict:
    if not path.exists():
        return {"path": str(path), "exists": False}

    result = con.execute(
        f"SELECT COUNT(*) as rows, COUNT(COLUMNS(*)) as cols FROM read_parquet('{path.as_posix()}')"
    ).fetchone()

    rows = result[0] if result else None

    # Get column count separately (COUNT(COLUMNS(*)) doesn't work in all versions)
    try:
        cols_result = con.execute(
            f"SELECT * FROM read_parquet('{path.as_posix()}') LIMIT 0"
        ).description
        cols = len(cols_result) if cols_result else None
    except Exception:
        cols = None

    return {
        "path": str(path),
        "exists": True,
        "size_mb": file_size_mb(path),
        "rows": rows,
        "columns": cols,
    }


def get_parquet_stats_with_date(path: Path, con: duckdb.DuckDBPyConnection) -> dict:
    stats = get_parquet_stats(path, con)
    if stats.get("exists"):
        mtime = path.stat().st_mtime
        stats["creation_date"] = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
    return stats


def load_existing_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def main() -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading existing manifest...")
    manifest = load_existing_manifest()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to DuckDB...")
    con = duckdb.connect(config={"memory_limit": "2GB"})

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading consolidated_parquet stats...")
    consolidated_stats = get_parquet_stats_with_date(CONSOLIDATED_PARQUET, con)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading master_parquet stats...")
    master_stats = get_parquet_stats(MASTER_PARQUET, con)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading targets stats...")
    targets_stats = get_parquet_stats(TARGETS_PARQUET, con)
    if targets_stats.get("exists"):
        targets_stats["size_mb"] = file_size_mb(TARGETS_PARQUET)

    con.close()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Counting split UIDs...")
    splits = {
        "train": count_lines(SPLITS_DIR / "train_uids.txt"),
        "val": count_lines(SPLITS_DIR / "val_uids.txt"),
        "test": count_lines(SPLITS_DIR / "test_uids.txt"),
    }

    # Preserve existing pipeline_steps_completed and append if needed
    existing_steps: list = manifest.get("pipeline_steps_completed", [])

    manifest.update({
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "consolidated_parquet": consolidated_stats,
        "master_parquet": master_stats,
        "targets": {
            "path": str(TARGETS_PARQUET),
            "exists": targets_stats.get("exists", False),
            "size_mb": targets_stats.get("size_mb"),
            "rows": targets_stats.get("rows"),
        },
        "splits": splits,
        "pipeline_steps_completed": existing_steps,
    })

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Manifest written to {MANIFEST_PATH}")
    print(f"  consolidated_parquet: {consolidated_stats.get('rows')} rows, {consolidated_stats.get('columns')} cols")
    print(f"  master_parquet:       {master_stats.get('rows')} rows, {master_stats.get('columns')} cols")
    print(f"  targets:              {targets_stats.get('rows')} rows")
    print(f"  splits:               train={splits['train']} / val={splits['val']} / test={splits['test']}")


if __name__ == "__main__":
    main()
