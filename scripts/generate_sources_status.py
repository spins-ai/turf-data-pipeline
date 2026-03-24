"""Generate sources_status.json for all scrapers (00-145).

For each scraper script found in the project root, checks:
- Whether an output directory exists
- Whether it contains data files
- Whether a checkpoint file exists
- Last collection date (most recent file modification)
- Status: active / partial / blocked / new

Usage:
    python scripts/generate_sources_status.py
"""
import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
DATA_DIR = ROOT / "data_master"
OUTPUT_FILE = DATA_DIR / "sources_status.json"

# Data file extensions
DATA_EXTS = {".json", ".jsonl", ".csv", ".parquet"}

# Checkpoint patterns
CHECKPOINT_PATTERNS = [
    "checkpoint*", "*.checkpoint", "progress*", "*.progress",
    "state.json", "last_run*", "*.state",
]

# How many days back counts as "recent" for active status
ACTIVE_DAYS = 30


def find_scrapers() -> list[dict]:
    """Find all numbered scraper scripts (00-145) in the project root."""
    scrapers = []
    pattern = re.compile(r"^(\d+b?)_(.+)\.py$")

    for f in sorted(ROOT.iterdir()):
        if not f.is_file() or not f.suffix == ".py":
            continue
        m = pattern.match(f.name)
        if not m:
            continue

        num_str = m.group(1)
        label = m.group(2)

        # Determine numeric value for sorting (02b -> 2.5)
        if num_str.endswith("b"):
            sort_key = int(num_str[:-1]) + 0.5
        else:
            sort_key = int(num_str)

        scrapers.append({
            "id": num_str,
            "label": label,
            "script": f.name,
            "sort_key": sort_key,
        })

    scrapers.sort(key=lambda s: s["sort_key"])
    return scrapers


def _guess_output_dir(scraper_id: str, label: str) -> Path | None:
    """Guess the output directory for a scraper."""
    if not OUTPUT_DIR.is_dir():
        return None

    # Try exact prefix match
    for d in OUTPUT_DIR.iterdir():
        if d.is_dir() and d.name.startswith(f"{scraper_id}_"):
            return d

    # Try matching by label keywords
    label_parts = label.lower().replace("_scraper", "").replace("_", " ").split()
    for d in OUTPUT_DIR.iterdir():
        if d.is_dir():
            dname = d.name.lower()
            if all(part in dname for part in label_parts[:2]):
                return d

    return None


def _count_data_files(directory: Path) -> tuple[int, int, datetime | None]:
    """Count data files and total size in a directory. Returns (count, total_bytes, newest_mtime)."""
    count = 0
    total = 0
    newest = None

    for root, _dirs, files in os.walk(directory):
        for name in files:
            fp = Path(root) / name
            if fp.suffix.lower() in DATA_EXTS:
                count += 1
                stat = fp.stat()
                total += stat.st_size
                mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                if newest is None or mtime > newest:
                    newest = mtime

    return count, total, newest


def _has_checkpoint(directory: Path) -> bool:
    """Check if directory contains checkpoint/progress files."""
    if not directory.is_dir():
        return False

    for root, _dirs, files in os.walk(directory):
        for name in files:
            name_lower = name.lower()
            if any(kw in name_lower for kw in
                   ("checkpoint", "progress", "state.json", "last_run", ".state")):
                return True
        # Only check top level and one level deep
        rel = Path(root).relative_to(directory)
        if len(rel.parts) > 1:
            break

    return False


def _find_newest_file(directory: Path) -> datetime | None:
    """Find the most recently modified file in a directory."""
    newest = None
    for root, _dirs, files in os.walk(directory):
        for name in files:
            fp = Path(root) / name
            try:
                mtime = datetime.fromtimestamp(fp.stat().st_mtime, tz=timezone.utc)
                if newest is None or mtime > newest:
                    newest = mtime
            except OSError:
                continue
    return newest


def determine_status(data_count: int, newest: datetime | None) -> str:
    """Determine scraper status based on data files and recency.

    - active: has data files with recent modifications (within ACTIVE_DAYS)
    - partial: has some data files but not recent
    - blocked: output dir exists but 0 data files
    - new: no output dir found (never run)
    """
    if data_count == 0:
        return "new"

    if newest is None:
        return "partial"

    cutoff = datetime.now(timezone.utc) - timedelta(days=ACTIVE_DAYS)
    if newest >= cutoff:
        return "active"
    else:
        return "partial"


def human_size(nbytes: int) -> str:
    """Return a human-readable file size string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def generate_status() -> dict:
    """Build sources status for all scrapers."""
    scrapers = find_scrapers()
    print(f"Found {len(scrapers)} scraper scripts\n")

    entries = []
    status_counts = {"active": 0, "partial": 0, "blocked": 0, "new": 0}

    for sc in scrapers:
        sid = sc["id"]
        label = sc["label"]
        script = sc["script"]

        out_dir = _guess_output_dir(sid, label)
        has_dir = out_dir is not None and out_dir.is_dir()

        if has_dir:
            data_count, data_size, newest = _count_data_files(out_dir)
            has_checkpoint = _has_checkpoint(out_dir)
            last_date = newest.isoformat() if newest else None
            # If dir exists but no data files, check if it just has cache/checkpoint
            if data_count == 0:
                any_file = _find_newest_file(out_dir)
                if any_file:
                    status = "blocked"
                else:
                    status = "new"
            else:
                status = determine_status(data_count, newest)
        else:
            data_count = 0
            data_size = 0
            has_checkpoint = False
            last_date = None
            status = "new"

        status_counts[status] = status_counts.get(status, 0) + 1

        entry = {
            "id": sid,
            "label": label,
            "script": script,
            "output_dir": str(out_dir.relative_to(ROOT)).replace("\\", "/") if has_dir else None,
            "output_dir_exists": has_dir,
            "data_file_count": data_count,
            "data_size_bytes": data_size,
            "data_size_human": human_size(data_size) if data_size > 0 else "0 B",
            "has_checkpoint": has_checkpoint,
            "last_collection_date": last_date,
            "status": status,
        }
        entries.append(entry)

        marker = {"active": "+", "partial": "~", "blocked": "!", "new": " "}
        print(f"  [{marker[status]}] {sid:>4s} {label:<40s} {status:<8s} "
              f"{data_count:>4d} files  {human_size(data_size):>10s}")

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_scrapers": len(entries),
        "status_summary": status_counts,
        "sources": entries,
    }
    return result


def main() -> None:
    print(f"Scanning scrapers and output directories...\n")
    status = generate_status()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(
        json.dumps(status, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"\nStatus written to {OUTPUT_FILE}")
    print(f"  Total scrapers: {status['total_scrapers']}")
    summary = status["status_summary"]
    print(f"  Active: {summary.get('active', 0)}, "
          f"Partial: {summary.get('partial', 0)}, "
          f"Blocked: {summary.get('blocked', 0)}, "
          f"New: {summary.get('new', 0)}")


if __name__ == "__main__":
    main()
