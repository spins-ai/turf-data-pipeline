#!/usr/bin/env python3
"""
scripts/feature_version_tracker.py — Feature Matrix Versioning
===============================================================
Tracks versions of the features_matrix with metadata:
  - Version tag (v1.0, v1.1, v2.0)
  - Changelog (features added/removed/modified)
  - Timestamp, nb features, nb records
  - SHA256 checksum

Usage:
    python feature_version_tracker.py --tag v1.0 --message "Initial feature matrix"
    python feature_version_tracker.py --list
    python feature_version_tracker.py --diff v1.0 v1.1
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

VERSIONS_FILE = ROOT / "data_master" / "feature_versions.json"
FEATURES_DIR = ROOT / "features"
FEATURES_MATRIX = ROOT / "data_master" / "features_matrix.jsonl"

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _load_versions() -> dict:
    if VERSIONS_FILE.exists():
        return json.loads(VERSIONS_FILE.read_text(encoding="utf-8"))
    return {"versions": []}


def _save_versions(data: dict) -> None:
    VERSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    VERSIONS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _get_feature_names() -> list[str]:
    """Extract feature names from first line of features_matrix.jsonl."""
    if not FEATURES_MATRIX.exists():
        logger.warning(f"Features matrix not found: {FEATURES_MATRIX}")
        return []
    with open(FEATURES_MATRIX, "r", encoding="utf-8") as f:
        line = f.readline().strip()
        if line:
            record = json.loads(line)
            # Exclude non-feature keys
            excluded = {"partant_uid", "course_uid", "date", "hippodrome", "nom_cheval"}
            return sorted(k for k in record.keys() if k not in excluded)
    return []


def _count_records() -> int:
    if not FEATURES_MATRIX.exists():
        return 0
    count = 0
    with open(FEATURES_MATRIX, "r", encoding="utf-8") as f:
        for _ in f:
            count += 1
    return count


def _sha256(path: Path) -> str:
    if not path.exists():
        return "N/A"
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def tag_version(tag: str, message: str) -> None:
    data = _load_versions()
    features = _get_feature_names()
    nb_records = _count_records()

    version_entry = {
        "tag": tag,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "nb_features": len(features),
        "nb_records": nb_records,
        "features": features,
        "sha256": _sha256(FEATURES_MATRIX),
    }

    # Check for changelog vs previous version
    if data["versions"]:
        prev = data["versions"][-1]
        prev_features = set(prev.get("features", []))
        curr_features = set(features)
        added = sorted(curr_features - prev_features)
        removed = sorted(prev_features - curr_features)
        version_entry["changelog"] = {
            "added": added,
            "removed": removed,
            "nb_added": len(added),
            "nb_removed": len(removed),
        }

    data["versions"].append(version_entry)
    _save_versions(data)
    logger.info(f"Tagged feature matrix as {tag}: {len(features)} features, {nb_records} records")


def list_versions() -> None:
    data = _load_versions()
    if not data["versions"]:
        logger.info("No versions recorded yet.")
        return
    for v in data["versions"]:
        cl = v.get("changelog", {})
        added = cl.get("nb_added", 0)
        removed = cl.get("nb_removed", 0)
        logger.info(
            f"  {v['tag']:10s} | {v['nb_features']:4d} features | {v['nb_records']:>10d} records | "
            f"+{added}/-{removed} | {v['timestamp'][:19]} | {v['message']}"
        )


def diff_versions(tag_a: str, tag_b: str) -> None:
    data = _load_versions()
    versions_map = {v["tag"]: v for v in data["versions"]}
    if tag_a not in versions_map or tag_b not in versions_map:
        logger.error(f"Version not found. Available: {list(versions_map.keys())}")
        return
    a_features = set(versions_map[tag_a].get("features", []))
    b_features = set(versions_map[tag_b].get("features", []))
    added = sorted(b_features - a_features)
    removed = sorted(a_features - b_features)
    logger.info(f"Diff {tag_a} -> {tag_b}:")
    logger.info(f"  Added ({len(added)}): {added[:20]}{'...' if len(added) > 20 else ''}")
    logger.info(f"  Removed ({len(removed)}): {removed[:20]}{'...' if len(removed) > 20 else ''}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Feature Matrix Version Tracker")
    parser.add_argument("--tag", help="Version tag (e.g., v1.0)")
    parser.add_argument("--message", "-m", default="", help="Version message")
    parser.add_argument("--list", action="store_true", help="List all versions")
    parser.add_argument("--diff", nargs=2, metavar=("TAG_A", "TAG_B"), help="Diff two versions")
    args = parser.parse_args()

    if args.list:
        list_versions()
    elif args.diff:
        diff_versions(args.diff[0], args.diff[1])
    elif args.tag:
        tag_version(args.tag, args.message)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
