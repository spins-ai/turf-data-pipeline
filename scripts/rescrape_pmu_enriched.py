#!/usr/bin/env python3
"""
rescrape_pmu_enriched.py
========================
Re-scrape the PMU participants endpoint (offline API) for all historical
dates already present in the cache, extracting enriched fields that are
only available on PAST races:

    deferre, tempsObtenu, reductionKilometrique, avisEntraineur,
    commentaireApresCourse, oeilleres, handicapValeur, handicapPoids,
    poidsConditionMonte

Reads all existing cache files in output/101_pmu_api/cache/ to discover
(date, reunion, course) combos, then calls the *offline* participants
endpoint and writes enriched records to a JSONL file.

Usage:
    python scripts/rescrape_pmu_enriched.py
    python scripts/rescrape_pmu_enriched.py --max-days 30
    python scripts/rescrape_pmu_enriched.py --reset   # restart from scratch
    python scripts/rescrape_pmu_enriched.py --start 2013-01-01 --end 2019-12-31
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths — relative to repo root (parent of scripts/)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from utils.logging_setup import setup_logging
from utils.scraping import (
    append_jsonl,
    create_session,
    load_checkpoint,
    save_checkpoint,
    smart_pause,
)

log = setup_logging("rescrape_pmu_enriched")

CACHE_DIR = REPO_ROOT / "output" / "101_pmu_api" / "cache"
OUTPUT_DIR = REPO_ROOT / "output" / "101_pmu_api"
OUTPUT_FILE = OUTPUT_DIR / "pmu_participants_enriched.jsonl"
CHECKPOINT_FILE = OUTPUT_DIR / ".checkpoint_enriched.json"

BASE_API = "https://offline.turfinfo.api.pmu.fr/rest/client/1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.pmu.fr/turf/",
}

# Fields to extract per participant
ENRICHED_FIELDS = [
    "deferre",
    "tempsObtenu",
    "reductionKilometrique",
    "avisEntraineur",
    "commentaireApresCourse",
    "oeilleres",
    "handicapValeur",
    "handicapPoids",
    "poidsConditionMonte",
]


# ===================================================================
# Discovery: scan cache to build the list of (date, R, C) combos
# ===================================================================

def discover_combos() -> list[tuple[str, int, int]]:
    """Scan cache dir for course_*.json files and return sorted (date, R, C) tuples."""
    pattern = re.compile(r"^course_(\d{4}-\d{2}-\d{2})_R(\d+)C(\d+)\.json$")
    combos: list[tuple[str, int, int]] = []
    for fname in os.listdir(CACHE_DIR):
        m = pattern.match(fname)
        if m:
            combos.append((m.group(1), int(m.group(2)), int(m.group(3))))
    combos.sort()
    log.info("Discovered %d (date, reunion, course) combos in cache.", len(combos))
    return combos


def discover_combos_from_api(
    session, start_date: str, end_date: str
) -> list[tuple[str, int, int]]:
    """Query the PMU programme API for each date in [start, end] to discover
    (date, R, C) combos.  Used when no cache exists (e.g. 2013-2019)."""
    combos: list[tuple[str, int, int]] = []
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    total_days = (end - current).days + 1
    day_count = 0

    log.info("Discovering combos from API for %s -> %s (%d days)", start_date, end_date, total_days)
    while current <= end:
        date_pmu = current.strftime("%d%m%Y")
        date_iso = current.strftime("%Y-%m-%d")
        url = f"{BASE_API}/programme/{date_pmu}"
        data = api_get(session, url)
        day_count += 1

        if data:
            programme = data.get("programme", data)
            reunions = programme.get("reunions", [])
            for reunion in reunions:
                num_r = reunion.get("numOfficiel", reunion.get("numExterne", 0))
                courses = reunion.get("courses", [])
                for course in courses:
                    num_c = course.get("numOrdre", course.get("numExterne", 0))
                    if num_r and num_c:
                        combos.append((date_iso, int(num_r), int(num_c)))

        if day_count % 50 == 0:
            log.info("Discovery progress: %d/%d days, %d combos found so far", day_count, total_days, len(combos))

        smart_pause(base=0.8, jitter=0.4)
        current += timedelta(days=1)

    combos.sort()
    log.info("Discovered %d combos from API across %d days.", len(combos), total_days)
    return combos


# ===================================================================
# API call
# ===================================================================

def api_get(session, url: str, max_retries: int = 3, timeout: int = 30):
    """GET with retry, returns parsed JSON or None."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 30 * attempt
                log.warning("429 Rate limit on %s — pausing %ds", url, wait)
                time.sleep(wait)
                continue
            if resp.status_code in (400, 404, 420):
                return None
            log.warning("HTTP %d on %s (attempt %d/%d)", resp.status_code, url, attempt, max_retries)
            time.sleep(5 * attempt)
        except Exception as e:
            log.warning("Network error: %s (attempt %d/%d)", e, attempt, max_retries)
            time.sleep(5 * attempt)
    log.error("Failed after %d attempts: %s", max_retries, url)
    return None


# ===================================================================
# Extract enriched fields from one participant
# ===================================================================

def extract_enriched(participant: dict, date_iso: str, num_reunion: int, num_course: int) -> dict:
    """Build a flat record with join keys + enriched fields."""
    record = {
        "date": date_iso,
        "numReunion": num_reunion,
        "numCourse": num_course,
        "numPmu": participant.get("numPmu"),
    }
    for field in ENRICHED_FIELDS:
        value = participant.get(field)
        # commentaireApresCourse can be a dict with a "texte" key
        if field == "commentaireApresCourse" and isinstance(value, dict):
            value = value.get("texte", "")
        record[field] = value
    return record


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Re-scrape PMU participants (offline API) for enriched fields"
    )
    parser.add_argument(
        "--max-days", type=int, default=0,
        help="Max distinct dates to process (0 = unlimited)"
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Ignore checkpoint and restart from scratch"
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Start date (YYYY-MM-DD). When set, discovers combos from PMU API instead of cache."
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End date (YYYY-MM-DD). Required when --start is used."
    )
    args = parser.parse_args()

    # --- Discovery ---
    if args.start:
        if not args.end:
            parser.error("--end is required when --start is specified")
        # Validate date format
        try:
            datetime.strptime(args.start, "%Y-%m-%d")
            datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError:
            parser.error("--start and --end must be YYYY-MM-DD format")

        # For API-based discovery we need a session early
        discovery_session = create_session()
        discovery_session.headers.update(HEADERS)
        combos = discover_combos_from_api(discovery_session, args.start, args.end)
        discovery_session.close()
    else:
        combos = discover_combos()

    if not combos:
        log.info("No combos found — nothing to do.")
        return

    # --- Checkpoint / resume ------------------------------------------------
    checkpoint = {} if args.reset else load_checkpoint(CHECKPOINT_FILE)
    last_processed_date = checkpoint.get("last_processed_date", "")
    total_written = checkpoint.get("total_written", 0)
    total_courses_done = checkpoint.get("total_courses_done", 0)

    if last_processed_date:
        log.info("Resuming after date %s (%d courses, %d records so far)",
                 last_processed_date, total_courses_done, total_written)

    # Group combos by date for day-level progress and checkpoint
    from itertools import groupby
    combos_by_date: list[tuple[str, list[tuple[str, int, int]]]] = []
    for date_key, grp in groupby(combos, key=lambda x: x[0]):
        combos_by_date.append((date_key, list(grp)))

    # Filter out already-done dates
    if last_processed_date:
        combos_by_date = [
            (d, cs) for d, cs in combos_by_date if d > last_processed_date
        ]
    log.info("Dates remaining: %d", len(combos_by_date))

    if args.max_days:
        combos_by_date = combos_by_date[:args.max_days]
        log.info("Limited to %d days by --max-days", args.max_days)

    # --- Session ------------------------------------------------------------
    session = create_session()
    session.headers.update(HEADERS)

    course_counter = 0  # courses within this run (for progress reporting)
    request_counter = 0  # requests within this session (for rotation)

    for day_idx, (date_iso, day_combos) in enumerate(combos_by_date, 1):
        date_pmu = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d%m%Y")

        for combo_date, r, c in day_combos:
            url = f"{BASE_API}/programme/{date_pmu}/R{r}/C{c}/participants"
            data = api_get(session, url)
            request_counter += 1
            course_counter += 1
            total_courses_done += 1

            if data:
                participants = data.get("participants", [])
                for p in participants:
                    record = extract_enriched(p, date_iso, r, c)
                    append_jsonl(OUTPUT_FILE, record)
                    total_written += 1

            # Progress every 100 courses
            if course_counter % 100 == 0:
                log.info(
                    "Progress: %d courses this run | %d total | %d records written | date: %s R%dC%d",
                    course_counter, total_courses_done, total_written, date_iso, r, c,
                )

            # Rate limiting
            smart_pause(base=1.5, jitter=0.8, long_pause_chance=0.05)

            # Rotate session every 500 requests
            if request_counter % 500 == 0:
                log.info("Rotating session after %d requests.", request_counter)
                session.close()
                session = create_session()
                session.headers.update(HEADERS)
                time.sleep(3)

        # Checkpoint after each day
        save_checkpoint(CHECKPOINT_FILE, {
            "last_processed_date": date_iso,
            "total_courses_done": total_courses_done,
            "total_written": total_written,
            "updated_at": datetime.now().isoformat(),
        })

        if day_idx % 10 == 0:
            log.info(
                "=== Day %d/%d done (%s): %d total courses, %d records ===",
                day_idx, len(combos_by_date), date_iso,
                total_courses_done, total_written,
            )

    session.close()

    log.info("=" * 60)
    log.info("DONE — %d courses processed, %d enriched records written", total_courses_done, total_written)
    log.info("Output: %s", OUTPUT_FILE)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
