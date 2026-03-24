"""Generate MANIFEST.json listing all files in data_master/ and output/."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIRS_TO_SCAN = ["data_master", "output"]
CACHE_DIRS = {"__pycache__", "cache", ".cache", "node_modules", ".tmp"}
OUTPUT = ROOT / "data_master" / "MANIFEST.json"
ONE_GB = 1 << 30


def human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def count_records(filepath: Path, size: int) -> int | None:
    ext = filepath.suffix.lower()
    if ext == ".jsonl":
        if size > ONE_GB:
            # Estimate: read first 10MB, compute avg line size
            sample_size = 10 * (1 << 20)
            try:
                with open(filepath, "rb") as f:
                    sample = f.read(sample_size)
                lines_in_sample = sample.count(b"\n")
                if lines_in_sample == 0:
                    return 1
                avg_line = sample_size / lines_in_sample
                return int(size / avg_line)
            except Exception:
                return None
        else:
            try:
                count = 0
                with open(filepath, "rb") as f:
                    for _ in f:
                        count += 1
                return count
            except Exception:
                return None
    elif ext == ".json":
        if size > ONE_GB:
            return None  # Too large to parse
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return len(data)
            elif isinstance(data, dict):
                return 1
            return 1
        except Exception:
            return None
    return None


def scan_dir(base: Path) -> list[dict]:
    entries = []
    if not base.exists():
        return entries
    for root, dirs, files in os.walk(base):
        # Prune cache dirs
        dirs[:] = [d for d in dirs if d not in CACHE_DIRS]
        for fname in sorted(files):
            fpath = Path(root) / fname
            # Skip MANIFEST.json itself
            if fpath == OUTPUT:
                continue
            try:
                stat = fpath.stat()
            except OSError:
                continue
            size = stat.st_size
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
            rel = fpath.relative_to(ROOT).as_posix()
            record_count = count_records(fpath, size)
            entry = {
                "filename": rel,
                "size_bytes": size,
                "size_human": human_size(size),
                "last_modified": mtime,
            }
            if record_count is not None:
                entry["record_count"] = record_count
            entries.append(entry)
    return entries


def main():
    all_entries = []
    for dirname in DIRS_TO_SCAN:
        all_entries.extend(scan_dir(ROOT / dirname))

    manifest = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_files": len(all_entries),
        "total_size_bytes": sum(e["size_bytes"] for e in all_entries),
        "total_size_human": human_size(sum(e["size_bytes"] for e in all_entries)),
        "files": sorted(all_entries, key=lambda e: e["filename"]),
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"MANIFEST.json written: {len(all_entries)} files, {manifest['total_size_human']}")


if __name__ == "__main__":
    main()
