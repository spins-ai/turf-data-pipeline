"""Create compressed backups of master data files.

Creates a timestamped backup directory under backups/ with gzip-compressed
copies of data_master/ files. Files over 500 MB get a SHA-256 hash file
only (too expensive to copy). A manifest.json summarises everything.

Usage:
    python scripts/backup_data.py            # full backup
    python scripts/backup_data.py --dry-run  # preview only
"""
import argparse
import gzip
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data_master"
BACKUPS_DIR = ROOT / "backups"

# Extensions we care about
IMPORTANT_EXTS = {".jsonl", ".json", ".parquet", ".csv"}

# Skip patterns
SKIP_NAMES = {"turf_data.duckdb", "turf_data.duckdb.wal"}
SKIP_PREFIXES = ("tmp_", "temp_", ".tmp", ".temp")

# Threshold for "too big to copy"
SIZE_THRESHOLD = 500 * 1024 * 1024  # 500 MB

# Streaming buffer — keeps RAM well under 2 GB
BUFFER_SIZE = 8 * 1024 * 1024  # 8 MB


def human_size(nbytes: int) -> str:
    """Return a human-readable file size string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def sha256_file(filepath: Path) -> str:
    """Compute SHA-256 of a file using streaming reads."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(BUFFER_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def collect_files(data_dir: Path) -> list[dict]:
    """Walk data_master/ and collect files worth backing up."""
    entries = []
    for root, _dirs, files in os.walk(data_dir):
        # Skip index / cache subdirectories
        rel_root = Path(root).relative_to(data_dir)
        if any(part.startswith(".") or part in ("indexes", "__pycache__", "cache")
               for part in rel_root.parts):
            continue

        for name in sorted(files):
            ext = Path(name).suffix.lower()
            if ext not in IMPORTANT_EXTS:
                continue
            if name in SKIP_NAMES:
                continue
            if any(name.lower().startswith(p) for p in SKIP_PREFIXES):
                continue

            full = Path(root) / name
            size = full.stat().st_size
            rel = full.relative_to(data_dir)
            entries.append({
                "rel_path": str(rel).replace("\\", "/"),
                "abs_path": str(full),
                "size": size,
            })
    return entries


def gzip_copy(src: Path, dst: Path) -> int:
    """Stream-compress src into dst.gz.  Returns compressed size."""
    dst_gz = dst.parent / (dst.name + ".gz")
    with open(src, "rb") as f_in, gzip.open(dst_gz, "wb", compresslevel=6) as f_out:
        while True:
            chunk = f_in.read(BUFFER_SIZE)
            if not chunk:
                break
            f_out.write(chunk)
    return dst_gz.stat().st_size


def run_backup(dry_run: bool = False) -> None:
    """Main backup routine."""
    if not DATA_DIR.is_dir():
        print(f"ERROR: data directory not found: {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    entries = collect_files(DATA_DIR)
    if not entries:
        print("No files to back up.")
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = BACKUPS_DIR / f"backup_{stamp}"

    total_original = sum(e["size"] for e in entries)
    small = [e for e in entries if e["size"] <= SIZE_THRESHOLD]
    large = [e for e in entries if e["size"] > SIZE_THRESHOLD]

    # ---- Dry-run report ----
    if dry_run:
        print(f"=== DRY RUN — backup would go to {backup_dir} ===\n")
        print(f"{'File':<55} {'Size':>12}  Action")
        print("-" * 82)
        for e in entries:
            action = "gzip copy" if e["size"] <= SIZE_THRESHOLD else "hash only"
            print(f"{e['rel_path']:<55} {human_size(e['size']):>12}  {action}")
        print("-" * 82)
        print(f"Total original size : {human_size(total_original)}")
        print(f"Files to compress   : {len(small)}")
        print(f"Files hash-only     : {len(large)}")
        return

    # ---- Actual backup ----
    backup_dir.mkdir(parents=True, exist_ok=True)
    manifest_entries = []
    total_backup = 0

    print(f"Backing up {len(entries)} files to {backup_dir}\n")

    for e in entries:
        src = Path(e["abs_path"])
        rel = e["rel_path"]
        size = e["size"]

        print(f"  {rel:<55} {human_size(size):>12}  ", end="", flush=True)

        file_hash = sha256_file(src)

        if size > SIZE_THRESHOLD:
            # Large file — write hash file only
            hash_path = backup_dir / (rel + ".sha256")
            hash_path.parent.mkdir(parents=True, exist_ok=True)
            hash_path.write_text(f"{file_hash}  {rel}\n", encoding="utf-8")
            print("hash only")
            manifest_entries.append({
                "file": rel,
                "original_size": size,
                "backup_type": "hash_only",
                "sha256": file_hash,
            })
        else:
            # Small file — gzip copy
            dst = backup_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            compressed_size = gzip_copy(src, dst)
            total_backup += compressed_size
            ratio = (compressed_size / size * 100) if size else 0
            print(f"gzip  {human_size(compressed_size):>12}  ({ratio:.0f}%)")
            manifest_entries.append({
                "file": rel,
                "original_size": size,
                "compressed_size": compressed_size,
                "backup_type": "gzip",
                "sha256": file_hash,
            })

    # Add hash-only file sizes to total_backup for the hash files themselves
    for mf in manifest_entries:
        if mf["backup_type"] == "hash_only":
            hash_file = backup_dir / (mf["file"] + ".sha256")
            if hash_file.exists():
                total_backup += hash_file.stat().st_size

    # ---- Write manifest ----
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_directory": str(DATA_DIR),
        "backup_directory": str(backup_dir),
        "total_original_size": total_original,
        "total_backup_size": total_backup,
        "file_count": len(manifest_entries),
        "files": manifest_entries,
    }
    manifest_path = backup_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    total_backup += manifest_path.stat().st_size

    # ---- Summary ----
    print()
    print(f"Original size : {human_size(total_original)}")
    print(f"Backup size   : {human_size(total_backup)}")
    if total_original:
        saved = total_original - total_backup
        pct = saved / total_original * 100
        print(f"Space saved   : {human_size(saved)} ({pct:.1f}%)")
    print(f"Manifest      : {manifest_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create compressed backups of data_master/ files."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be backed up without writing anything.",
    )
    args = parser.parse_args()
    run_backup(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
