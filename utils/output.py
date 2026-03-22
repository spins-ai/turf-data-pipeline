"""Shared output helpers: save_jsonl, sauver_json, sauver_csv.

These were previously duplicated across 20+ files. All functions accept
a list of dicts, a file path (str or Path), and a logger.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from pathlib import Path
from typing import Any


def save_jsonl(
    records: list[dict[str, Any]],
    filepath: str | os.PathLike,
    logger: logging.Logger,
    *,
    ensure_ascii: bool = False,
) -> None:
    """Write a list of records to a JSONL file (one JSON object per line).

    Uses atomic write (tmp file + replace) to avoid partial writes.
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    tmp = filepath.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=ensure_ascii, default=str) + "\n")
    tmp.replace(filepath)
    logger.info("Sauve JSONL: %s (%d)", filepath, len(records))


def sauver_json(
    data: list[dict[str, Any]],
    filepath: str | os.PathLike,
    logger: logging.Logger,
    *,
    ensure_ascii: bool = False,
    indent: int = 2,
) -> None:
    """Write a list of records as a JSON array to a file.

    Uses atomic write (tmp file + replace) to avoid partial writes.
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    tmp = filepath.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent, default=str)
    tmp.replace(filepath)
    logger.info("Sauve: %s (%d entrees)", filepath.name, len(data))


def sauver_csv(
    data: list[dict[str, Any]],
    filepath: str | os.PathLike,
    logger: logging.Logger,
) -> None:
    """Write a list of records to a CSV file.

    Lists, sets, and frozensets in values are serialised as JSON strings
    so that they survive the round-trip.  Empty data is silently skipped.
    """
    if not data:
        return
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(data[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            flat_row = {}
            for k, v in row.items():
                if isinstance(v, (list, set, frozenset, dict)):
                    flat_row[k] = json.dumps(
                        sorted(v) if isinstance(v, (set, frozenset)) else v,
                        ensure_ascii=False,
                        default=str,
                    )
                else:
                    flat_row[k] = v
            writer.writerow(flat_row)
    logger.info("Sauve: %s", filepath.name)
