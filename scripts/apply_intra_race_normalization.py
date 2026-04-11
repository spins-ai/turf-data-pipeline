#!/usr/bin/env python3
"""
scripts/apply_intra_race_normalization.py — Intra-race feature normalization
=============================================================================
For each runner (partant_uid), computes within-race z-scores and rank
for a curated set of ~200-300 key features, using DuckDB window functions.

Input:
    D:/turf-data-pipeline/04_FEATURES/features_capped.parquet
    D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet  (→ course_uid)

Output:
    D:/turf-data-pipeline/04_FEATURES/features_normalized.parquet

Processing strategy:
  - Join capped features with master to attach course_uid.
  - Filter to ~200-300 key feature columns (elo, speed, odds, position, etc.).
  - Process in batches of 50 features via DuckDB window functions.
  - For each feature col:
      {col}__zscore = (col - AVG(col) OVER (PARTITION BY course_uid))
                      / NULLIF(STDDEV(col) OVER (PARTITION BY course_uid), 0)
      {col}__rank   = RANK() OVER (PARTITION BY course_uid ORDER BY col DESC)
  - Keep original columns alongside new ones.
  - Write final Parquet.

Memory safety: DuckDB memory_limit=8GB, threads=2, temp on D: drive.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FEATURES_CAPPED = Path("D:/turf-data-pipeline/04_FEATURES/features_capped.parquet")
MASTER_PARQUET   = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_PARQUET   = Path("D:/turf-data-pipeline/04_FEATURES/features_normalized.parquet")
TMP_DIR          = Path("D:/turf-data-pipeline/tmp")

# ---------------------------------------------------------------------------
# DuckDB settings
# ---------------------------------------------------------------------------
DUCKDB_MEMORY_LIMIT = "8GB"
DUCKDB_THREADS      = 2

# ---------------------------------------------------------------------------
# Batch size for window-function processing (features per query)
# Keeps individual DuckDB queries manageable in memory.
# ---------------------------------------------------------------------------
BATCH_SIZE = 50

# ---------------------------------------------------------------------------
# Keywords that identify "key" features worth normalizing.
# Any column whose name contains at least one of these tokens (case-insensitive)
# will be included in intra-race normalization.
# Adjust this list to widen / narrow coverage.
# ---------------------------------------------------------------------------
KEY_FEATURE_KEYWORDS: list[str] = [
    # Ratings / performance
    "elo",
    "speed",
    "rating",
    "score",
    "index",
    "figure",
    "class",
    "perf",
    # Odds / market
    "odds",
    "cote",
    "implied",
    "prob",
    "probability",
    "market",
    "edge",
    "value",
    "overround",
    "movement",
    "drift",
    "steam",
    "closing",
    "opening",
    "betfair",
    "exchange",
    # Position / result
    "position",
    "place",
    "rank",
    "finish",
    "ordre",
    "rang",
    "arrivee",
    # Form / momentum
    "form",
    "momentum",
    "streak",
    "run",
    "recent",
    "win_rate",
    "win_pct",
    "place_rate",
    "place_pct",
    "roi",
    # Distance / pace
    "distance",
    "pace",
    "speed_fig",
    "sectional",
    "time",
    # Weight / draw
    "weight",
    "poids",
    "draw",
    "stall",
    "numero",
    "corde",
    # Field-level
    "field",
    "field_size",
    "rivals",
    "competition",
    "strength",
    # Trainer / jockey (entity ratings only)
    "jockey_win",
    "trainer_win",
    "jockey_roi",
    "trainer_roi",
    "jockey_score",
    "trainer_score",
    # Bayesian / target encoded
    "bayesian",
    "target_enc",
    "encoded",
    # Recency / decay
    "recency",
    "decay",
    "weighted",
    # Uncertainty / variance
    "variance",
    "std",
    "uncertainty",
    "confidence",
    # Signal aggregates
    "signal",
    "consensus",
    "aggregate",
    "composite",
]

# Hard maximum on the number of key features to normalize.
# If more than this many columns match the keywords we keep the first MAX_KEY_FEATURES.
MAX_KEY_FEATURES = 300


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def connect_duckdb() -> duckdb.DuckDBPyConnection:
    """Create and configure a DuckDB in-memory connection."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads={DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{TMP_DIR.as_posix()}'")
    logger.info(
        f"DuckDB configured: memory_limit={DUCKDB_MEMORY_LIMIT}, "
        f"threads={DUCKDB_THREADS}, temp={TMP_DIR}"
    )
    return con


def select_key_features(all_columns: list[str]) -> list[str]:
    """
    Return the subset of columns that match at least one KEY_FEATURE_KEYWORDS token.
    Excludes 'partant_uid' and 'course_uid' (metadata, not features).
    """
    keywords_lower = [kw.lower() for kw in KEY_FEATURE_KEYWORDS]
    selected: list[str] = []
    for col in all_columns:
        if col in ("partant_uid", "course_uid"):
            continue
        col_lower = col.lower()
        if any(kw in col_lower for kw in keywords_lower):
            selected.append(col)
        if len(selected) >= MAX_KEY_FEATURES:
            break
    return selected


def batched(lst: list, size: int):
    """Yield successive chunks of `size` from `lst`."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t_start = time.time()

    # -----------------------------------------------------------------------
    # Validate inputs
    # -----------------------------------------------------------------------
    for p in (FEATURES_CAPPED, MASTER_PARQUET):
        if not p.exists():
            logger.error(f"Input file not found: {p}")
            sys.exit(1)

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Connect DuckDB
    # -----------------------------------------------------------------------
    con = connect_duckdb()

    # -----------------------------------------------------------------------
    # Register source Parquet files as views
    # -----------------------------------------------------------------------
    logger.info("Registering source Parquet files as DuckDB views...")

    con.execute(
        f"CREATE OR REPLACE VIEW features_capped AS "
        f"SELECT * FROM read_parquet('{FEATURES_CAPPED.as_posix()}')"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW master AS "
        f"SELECT partant_uid, course_uid FROM read_parquet('{MASTER_PARQUET.as_posix()}')"
    )

    # -----------------------------------------------------------------------
    # Discover all columns in features_capped
    # -----------------------------------------------------------------------
    logger.info("Fetching column list from features_capped...")
    col_info = con.execute("DESCRIBE features_capped").fetchall()
    # col_info rows: (col_name, col_type, ...)
    all_cols = [row[0] for row in col_info]
    logger.info(f"Total columns in features_capped: {len(all_cols)}")

    # -----------------------------------------------------------------------
    # Identify numeric feature columns
    # -----------------------------------------------------------------------
    numeric_types = {
        "INTEGER", "BIGINT", "HUGEINT", "SMALLINT", "TINYINT",
        "FLOAT", "DOUBLE", "DECIMAL", "REAL",
        "INT", "INT4", "INT8", "INT2", "INT1",
        "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT",
    }
    numeric_cols = [
        row[0] for row in col_info
        if row[1].split("(")[0].upper() in numeric_types
        and row[0] not in ("partant_uid", "course_uid")
    ]
    logger.info(f"Numeric feature columns: {len(numeric_cols)}")

    # -----------------------------------------------------------------------
    # Select key features
    # -----------------------------------------------------------------------
    key_features = select_key_features(numeric_cols)
    logger.info(
        f"Key features selected for normalization: {len(key_features)} "
        f"(limit={MAX_KEY_FEATURES})"
    )
    if not key_features:
        logger.error(
            "No key features matched the keyword list. "
            "Check your column naming conventions."
        )
        sys.exit(1)

    # Log a sample
    sample = key_features[:20]
    logger.info(f"Sample key features (first 20): {sample}")

    # -----------------------------------------------------------------------
    # Build joined base table in DuckDB (features + course_uid)
    # We create a persistent table in-memory to avoid re-reading Parquet
    # for every batch.
    # -----------------------------------------------------------------------
    logger.info("Building joined base table (features + course_uid)...")

    # Only pull the columns we actually need: partant_uid + key features
    key_cols_sql = ", ".join(f'f."{c}"' for c in key_features)
    con.execute(f"""
        CREATE OR REPLACE TABLE base AS
        SELECT
            f.partant_uid,
            m.course_uid,
            {key_cols_sql}
        FROM features_capped f
        INNER JOIN master m USING (partant_uid)
    """)

    row_count = con.execute("SELECT COUNT(*) FROM base").fetchone()[0]
    logger.info(f"Base table rows: {row_count:,}")

    # -----------------------------------------------------------------------
    # Process batches: compute z-score + rank for each feature batch
    # and accumulate into an output table.
    # -----------------------------------------------------------------------
    logger.info(
        f"Processing {len(key_features)} features in batches of {BATCH_SIZE}..."
    )

    batches = list(batched(key_features, BATCH_SIZE))
    total_batches = len(batches)

    # Create output table with identity columns (partant_uid + all originals)
    all_orig_cols_sql = ", ".join(f'"{c}"' for c in key_features)
    con.execute(f"""
        CREATE OR REPLACE TABLE output AS
        SELECT partant_uid, course_uid, {all_orig_cols_sql}
        FROM base
    """)

    for batch_idx, batch_cols in enumerate(batches, start=1):
        t_batch = time.time()
        logger.info(
            f"  Batch {batch_idx}/{total_batches}: "
            f"{len(batch_cols)} features ({batch_cols[0]} … {batch_cols[-1]})"
        )

        # Build SELECT expressions for this batch
        # z-score: (col - AVG(col) OVER w) / NULLIF(STDDEV(col) OVER w, 0)
        # rank:    RANK() OVER (PARTITION BY course_uid ORDER BY col DESC)
        #          Higher value → rank 1 (best). Ties get same rank.
        #          NULL values land last (NULLS LAST).
        expr_parts: list[str] = []
        for col in batch_cols:
            safe_col = f'"{col}"'
            zscore_col = f'"{col}__zscore"'
            rank_col   = f'"{col}__rank"'
            expr_parts.append(
                f"({safe_col} - AVG({safe_col}) OVER (PARTITION BY course_uid)) "
                f"/ NULLIF(STDDEV({safe_col}) OVER (PARTITION BY course_uid), 0) "
                f"AS {zscore_col}"
            )
            expr_parts.append(
                f"RANK() OVER ("
                f"PARTITION BY course_uid ORDER BY {safe_col} DESC NULLS LAST"
                f") AS {rank_col}"
            )

        exprs_sql = ",\n            ".join(expr_parts)

        # Add new columns to output table via ALTER TABLE + UPDATE
        # (DuckDB supports adding columns and updating them efficiently)
        for col in batch_cols:
            zscore_col = f"{col}__zscore"
            rank_col   = f"{col}__rank"
            con.execute(f'ALTER TABLE output ADD COLUMN IF NOT EXISTS "{zscore_col}" DOUBLE')
            con.execute(f'ALTER TABLE output ADD COLUMN IF NOT EXISTS "{rank_col}" BIGINT')

        # Compute batch results in a temp table, then UPDATE output.
        # Include course_uid so the UPDATE can join on both keys safely.
        con.execute(f"""
            CREATE OR REPLACE TABLE batch_result AS
            SELECT
                partant_uid,
                course_uid,
                {exprs_sql}
            FROM base
        """)

        # Build UPDATE SET clause
        # Join on both partant_uid AND course_uid to be safe in case partant_uid
        # is not globally unique (i.e. same runner_id used across races in some datasets).
        set_clauses: list[str] = []
        for col in batch_cols:
            set_clauses.append(f'"{col}__zscore" = br."{col}__zscore"')
            set_clauses.append(f'"{col}__rank" = br."{col}__rank"')
        set_sql = ",\n                ".join(set_clauses)

        con.execute(f"""
            UPDATE output
            SET
                {set_sql}
            FROM batch_result br
            WHERE output.partant_uid = br.partant_uid
              AND output.course_uid  = br.course_uid
        """)

        # Drop temp table to free memory
        con.execute("DROP TABLE IF EXISTS batch_result")

        elapsed_batch = time.time() - t_batch
        logger.info(f"    Batch {batch_idx} done in {elapsed_batch:.1f}s")

    # -----------------------------------------------------------------------
    # Verify column count
    # -----------------------------------------------------------------------
    out_col_info = con.execute("DESCRIBE output").fetchall()
    out_cols = [row[0] for row in out_col_info]
    expected_new_cols = len(key_features) * 2  # zscore + rank per feature
    logger.info(
        f"Output table columns: {len(out_cols)} "
        f"(original {len(key_features)} + new {expected_new_cols} + metadata 2)"
    )

    # -----------------------------------------------------------------------
    # Write output Parquet
    # -----------------------------------------------------------------------
    logger.info(f"Writing output Parquet: {OUTPUT_PARQUET} ...")
    con.execute(f"""
        COPY output TO '{OUTPUT_PARQUET.as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)

    # -----------------------------------------------------------------------
    # Quick sanity check
    # -----------------------------------------------------------------------
    out_row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OUTPUT_PARQUET.as_posix()}')"
    ).fetchone()[0]
    logger.info(f"Output Parquet rows: {out_row_count:,}")

    if out_row_count != row_count:
        logger.warning(
            f"Row count mismatch: input={row_count:,}, output={out_row_count:,}. "
            "Check for duplicate partant_uid in master."
        )
    else:
        logger.info("Row count matches input. Sanity check passed.")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    elapsed_total = time.time() - t_start
    logger.info(
        f"\n{'='*60}\n"
        f"Done in {elapsed_total:.1f}s\n"
        f"  Input rows        : {row_count:,}\n"
        f"  Key features      : {len(key_features)}\n"
        f"  New columns added : {expected_new_cols} (zscore + rank)\n"
        f"  Total output cols : {len(out_cols)}\n"
        f"  Output            : {OUTPUT_PARQUET}\n"
        f"{'='*60}"
    )

    con.close()


if __name__ == "__main__":
    main()
