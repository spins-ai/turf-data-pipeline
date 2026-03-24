"""Generate a data catalog of all files in data_master/.

Scans data_master/ recursively and produces data_master/data_catalog.json
with metadata for each file: name, size, format, record count, last modified.

Record counting uses streaming for large files to stay memory-friendly.

Usage:
    python scripts/generate_data_catalog.py
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data_master"
OUTPUT = DATA_DIR / "data_catalog.json"

# Formats we catalogue
FORMAT_MAP = {
    ".json": "json",
    ".jsonl": "jsonl",
    ".csv": "csv",
    ".parquet": "parquet",
}

# Directories to skip
SKIP_DIRS = {"indexes", "__pycache__", "cache", ".git"}


def _detect_format(path: Path) -> str | None:
    """Return format string or None if not a catalogued type."""
    return FORMAT_MAP.get(path.suffix.lower())


def _count_json(path: Path) -> int:
    """Count records in a JSON file (streaming for arrays)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            # Peek at first non-whitespace char
            first = ""
            while True:
                ch = f.read(1)
                if not ch:
                    return 0
                if not ch.isspace():
                    first = ch
                    break

            # Rewind and parse
            f.seek(0)
            data = json.load(f)
            if isinstance(data, list):
                return len(data)
            elif isinstance(data, dict):
                # Single object = 1 record, or check for common wrapper keys
                for key in ("data", "records", "results", "items", "courses",
                            "partants", "chevaux", "jockeys", "entraineurs"):
                    if key in data and isinstance(data[key], list):
                        return len(data[key])
                return 1
            return 1
    except (json.JSONDecodeError, UnicodeDecodeError, MemoryError):
        # Fall back: count top-level array elements by streaming
        return _count_json_streaming(path)


def _count_json_streaming(path: Path) -> int:
    """Rough streaming count for large JSON arrays."""
    count = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            depth = 0
            in_string = False
            escape = False
            for line in f:
                for ch in line:
                    if escape:
                        escape = False
                        continue
                    if ch == "\\":
                        if in_string:
                            escape = True
                        continue
                    if ch == '"':
                        in_string = not in_string
                        continue
                    if in_string:
                        continue
                    if ch == "[" or ch == "{":
                        depth += 1
                    elif ch == "]" or ch == "}":
                        depth -= 1
                        if depth == 1 and ch == "}":
                            count += 1
    except Exception:
        pass
    return count


def _count_jsonl(path: Path) -> int:
    """Count lines in a JSONL file (streaming)."""
    count = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                stripped = line.strip()
                if stripped and not stripped.startswith("//"):
                    count += 1
    except Exception:
        pass
    return count


def _count_csv(path: Path) -> int:
    """Count data rows in a CSV file (streaming, excludes header)."""
    count = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            # Skip header
            try:
                next(reader)
            except StopIteration:
                return 0
            for _ in reader:
                count += 1
    except Exception:
        pass
    return count


def _count_parquet(path: Path) -> int:
    """Count rows in a Parquet file using metadata (no full read)."""
    try:
        import pyarrow.parquet as pq
        metadata = pq.read_metadata(str(path))
        return metadata.num_rows
    except ImportError:
        # pyarrow not installed — try reading footer manually
        return _count_parquet_fallback(path)
    except Exception:
        return -1


def _count_parquet_fallback(path: Path) -> int:
    """Fallback parquet row count by reading the footer magic bytes."""
    try:
        size = path.stat().st_size
        if size < 12:
            return -1
        with open(path, "rb") as f:
            # Parquet footer length is stored in last 8 bytes (4-byte len + 4-byte magic)
            f.seek(size - 8)
            footer_data = f.read(8)
            if footer_data[4:] != b"PAR1":
                return -1
            footer_len = int.from_bytes(footer_data[:4], "little")
            # We can't easily parse the thrift footer without a library
            return -1
    except Exception:
        return -1


def count_records(path: Path, fmt: str) -> int:
    """Count records in a file based on format. Returns -1 if unknown."""
    if fmt == "json":
        return _count_json(path)
    elif fmt == "jsonl":
        return _count_jsonl(path)
    elif fmt == "csv":
        return _count_csv(path)
    elif fmt == "parquet":
        return _count_parquet(path)
    return -1


def human_size(nbytes: int) -> str:
    """Return a human-readable file size string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def generate_catalog() -> dict:
    """Scan data_master/ and build the catalog."""
    if not DATA_DIR.is_dir():
        print(f"ERROR: data directory not found: {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    entries = []
    total_size = 0

    for root, dirs, files in os.walk(DATA_DIR):
        # Prune directories we don't want
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]

        rel_root = Path(root).relative_to(DATA_DIR)

        for name in sorted(files):
            filepath = Path(root) / name
            fmt = _detect_format(filepath)
            if fmt is None:
                continue

            # Skip the catalog itself
            if filepath == OUTPUT:
                continue

            stat = filepath.stat()
            size = stat.st_size
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)

            print(f"  Cataloguing {rel_root / name} ({human_size(size)})...",
                  end="", flush=True)

            rec_count = count_records(filepath, fmt)

            print(f" {rec_count} records" if rec_count >= 0 else " (count unavailable)")

            entry = {
                "name": name,
                "path": str((rel_root / name)).replace("\\", "/"),
                "format": fmt,
                "size_bytes": size,
                "size_human": human_size(size),
                "record_count": rec_count,
                "last_modified": mtime.isoformat(),
            }
            entries.append(entry)
            total_size += size

    catalog = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_directory": str(DATA_DIR),
        "total_files": len(entries),
        "total_size_bytes": total_size,
        "total_size_human": human_size(total_size),
        "files": entries,
    }
    return catalog


def main() -> None:
    print(f"Scanning {DATA_DIR} ...\n")
    catalog = generate_catalog()

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"\nCatalog written to {OUTPUT}")
    print(f"  Total files: {catalog['total_files']}")
    print(f"  Total size : {catalog['total_size_human']}")


if __name__ == "__main__":
    main()
