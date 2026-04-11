"""
Build performances_master.parquet from perf_detaillees_enriched.jsonl.

Streams the JSONL in chunks of 100K lines, flattens the 'place' dict,
and writes incrementally with PyArrow ParquetWriter.
"""

import json
import time
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

INPUT_PATH = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/22_performances_detaillees/perf_detaillees_enriched.jsonl")
OUTPUT_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/performances_master.parquet")
CHUNK_SIZE = 100_000

# Expected columns (flat)
COLUMNS = [
    "source_file", "date_course", "reunion", "course", "allure",
    "numPmu", "nomCheval", "perf_index", "perf_date", "perf_hippodrome",
    "perf_discipline", "perf_distance", "perf_nbParticipants",
    "perf_tempsDuPremier", "perf_allocation", "tempsDuPremier",
    "reductionKilometrique", "distanceAvecPrecedent",
    "place_position", "place_raw", "place_status",
    "nomJockey", "poidsJockey", "corde", "distanceParcourue", "oeillere"
]


def flatten_record(record: dict) -> dict:
    """Flatten the place dict into separate columns."""
    place = record.pop("place", None)
    if isinstance(place, dict):
        record["place_position"] = place.get("place")
        record["place_raw"] = place.get("rawValue")
        record["place_status"] = place.get("statusArrivee")
    elif isinstance(place, str):
        # Sometimes place is just a string value
        record["place_position"] = place
        record["place_raw"] = place
        record["place_status"] = None
    else:
        record["place_position"] = None
        record["place_raw"] = None
        record["place_status"] = None
    return record


def build_chunk_table(records: list[dict]) -> pa.Table:
    """Convert a list of flat dicts to a PyArrow table with consistent columns."""
    # Ensure all columns exist in each record
    for rec in records:
        for col in COLUMNS:
            if col not in rec:
                rec[col] = None
    # Build column arrays - convert everything to string to handle mixed types
    arrays = {}
    for col in COLUMNS:
        values = []
        for rec in records:
            v = rec.get(col)
            if v is None:
                values.append(None)
            else:
                values.append(str(v))
        arrays[col] = pa.array(values, type=pa.string())
    return pa.table(arrays)


def main():
    print(f"[INFO] Reading: {INPUT_PATH}")
    print(f"[INFO] Output:  {OUTPUT_PATH}")
    print(f"[INFO] Chunk size: {CHUNK_SIZE:,} lines")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    start = time.time()
    total_rows = 0
    chunk_count = 0
    skipped = 0
    writer = None

    try:
        with open(INPUT_PATH, "r", encoding="utf-8", errors="replace") as f:
            buffer = []
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    record = flatten_record(record)
                    buffer.append(record)
                except (json.JSONDecodeError, ValueError):
                    skipped += 1
                    if skipped <= 10:
                        print(f"  [WARN] Skipped malformed line {line_num}")
                    continue

                if len(buffer) >= CHUNK_SIZE:
                    table = build_chunk_table(buffer)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            str(OUTPUT_PATH),
                            table.schema,
                            compression="snappy"
                        )
                    writer.write_table(table)
                    total_rows += len(buffer)
                    chunk_count += 1
                    elapsed = time.time() - start
                    print(f"  [CHUNK {chunk_count}] Written {total_rows:,} rows ({elapsed:.1f}s)")
                    buffer = []

            # Write remaining records
            if buffer:
                table = build_chunk_table(buffer)
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(OUTPUT_PATH),
                        table.schema,
                        compression="snappy"
                    )
                writer.write_table(table)
                total_rows += len(buffer)
                chunk_count += 1

    finally:
        if writer is not None:
            writer.close()

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"[DONE] Total rows written: {total_rows:,}")
    print(f"[DONE] Skipped lines: {skipped:,}")
    print(f"[DONE] Chunks: {chunk_count}")
    print(f"[DONE] Time: {elapsed:.1f}s")
    print(f"[DONE] Output: {OUTPUT_PATH}")

    # Verify output
    print(f"\n{'='*60}")
    print("[VERIFY] Reading back parquet metadata...")
    pf = pq.ParquetFile(str(OUTPUT_PATH))
    print(f"  Row count: {pf.metadata.num_rows:,}")
    print(f"  Columns ({len(pf.schema_arrow)}): {pf.schema_arrow.names}")
    print(f"  File size: {OUTPUT_PATH.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
