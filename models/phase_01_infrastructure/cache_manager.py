# -*- coding: utf-8 -*-
"""
Cache Manager
=============
Cache processed datasets to Parquet for fast reload.
Hash-based cache invalidation to detect stale data.
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("cache_manager")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = PROJECT_ROOT / "models" / "data" / ".cache"


class CacheManager:
    """Cache DataFrames to Parquet with hash-based invalidation."""

    def __init__(self, cache_dir: Optional[str] = None, ttl_hours: float = 24.0):
        self.cache_dir = Path(cache_dir) if cache_dir else CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_hours * 3600
        self.manifest_path = self.cache_dir / "_manifest.json"
        self.manifest: Dict[str, dict] = self._load_manifest()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get(self, key: str, source_path: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Return cached DataFrame if valid, else None.

        Parameters
        ----------
        key : cache key (e.g. 'partants_clean')
        source_path : path to source file; if its hash changed, cache is invalid
        """
        cache_file = self.cache_dir / f"{key}.parquet"
        if not cache_file.exists():
            logger.info("  Cache MISS: '%s' (file not found)", key)
            return None

        entry = self.manifest.get(key)
        if entry is None:
            logger.info("  Cache MISS: '%s' (no manifest entry)", key)
            return None

        # Check TTL
        age = time.time() - entry.get("timestamp", 0)
        if age > self.ttl_seconds:
            logger.info("  Cache EXPIRED: '%s' (%.1f hours old)", key, age / 3600)
            return None

        # Check source hash
        if source_path:
            current_hash = self._file_hash(source_path)
            if current_hash != entry.get("source_hash"):
                logger.info("  Cache STALE: '%s' (source changed)", key)
                return None

        df = pd.read_parquet(cache_file)
        logger.info("  Cache HIT: '%s' (%d rows, %.1f hours old)", key, len(df), age / 3600)
        return df

    def put(self, key: str, df: pd.DataFrame, source_path: Optional[str] = None) -> Path:
        """Store DataFrame in cache."""
        cache_file = self.cache_dir / f"{key}.parquet"
        df.to_parquet(cache_file, index=False)

        self.manifest[key] = {
            "timestamp": time.time(),
            "rows": len(df),
            "cols": len(df.columns),
            "source_hash": self._file_hash(source_path) if source_path else None,
            "size_mb": round(cache_file.stat().st_size / 1_048_576, 2),
        }
        self._save_manifest()
        logger.info("  Cache PUT: '%s' (%d rows, %.2f MB)", key, len(df), self.manifest[key]["size_mb"])
        return cache_file

    def invalidate(self, key: str):
        """Remove a specific cache entry."""
        cache_file = self.cache_dir / f"{key}.parquet"
        if cache_file.exists():
            cache_file.unlink()
        self.manifest.pop(key, None)
        self._save_manifest()
        logger.info("  Cache INVALIDATED: '%s'", key)

    def clear_all(self):
        """Remove all cached files."""
        count = 0
        for f in self.cache_dir.glob("*.parquet"):
            f.unlink()
            count += 1
        self.manifest = {}
        self._save_manifest()
        logger.info("  Cache CLEARED: %d files removed", count)

    def status(self) -> pd.DataFrame:
        """Return cache status as a DataFrame."""
        rows = []
        for key, entry in self.manifest.items():
            cache_file = self.cache_dir / f"{key}.parquet"
            age_h = (time.time() - entry.get("timestamp", 0)) / 3600
            rows.append({
                "key": key,
                "rows": entry.get("rows", 0),
                "cols": entry.get("cols", 0),
                "size_mb": entry.get("size_mb", 0),
                "age_hours": round(age_h, 1),
                "valid": cache_file.exists() and age_h < (self.ttl_seconds / 3600),
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Decorator pattern
    # ------------------------------------------------------------------
    def cached(self, key: str, source_path: Optional[str] = None):
        """Decorator to cache a function that returns a DataFrame."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                df = self.get(key, source_path=source_path)
                if df is not None:
                    return df
                df = func(*args, **kwargs)
                self.put(key, df, source_path=source_path)
                return df
            return wrapper
        return decorator

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    @staticmethod
    def _file_hash(path: str, chunk_size: int = 65536) -> str:
        """Compute MD5 hash of a file (first 10 MB for speed)."""
        h = hashlib.md5()
        max_bytes = 10 * 1_048_576
        read = 0
        try:
            with open(path, "rb") as f:
                while read < max_bytes:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    h.update(chunk)
                    read += len(chunk)
        except OSError:
            return ""
        return h.hexdigest()

    def _load_manifest(self) -> Dict[str, dict]:
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def _save_manifest(self):
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(self.manifest, f, indent=2, ensure_ascii=True)


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Cache Manager")
    parser.add_argument("--action", choices=["status", "clear", "invalidate"], default="status")
    parser.add_argument("--key", default=None, help="Cache key (for invalidate)")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args()

    cm = CacheManager(cache_dir=args.cache_dir)

    if args.action == "status":
        status = cm.status()
        if status.empty:
            print("[INFO] Cache is empty.")
        else:
            print(status.to_string(index=False))
    elif args.action == "clear":
        cm.clear_all()
        print("[OK] Cache cleared.")
    elif args.action == "invalidate":
        if not args.key:
            print("[ERROR] --key required for invalidate action.")
        else:
            cm.invalidate(args.key)
            print("[OK] Cache entry '%s' invalidated." % args.key)


if __name__ == "__main__":
    main()
