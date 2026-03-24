#!/usr/bin/env python3
"""
Script 149 — The Racing API Free Tier Scraper (HTTP/REST)
Source : theracingapi.com (free tier)
Collecte : Results, racecards, horse data via REST API
Endpoints :
  GET /v1/results/{date}       -> resultats du jour
  GET /v1/racecards/{date}     -> cartes de course du jour
  GET /v1/horses/{id}          -> fiche cheval
  GET /v1/courses              -> liste des hippodromes
CRITIQUE pour : Structured Race Data, Results, Racecards

NOTE: This scraper uses HTTP requests (not Playwright) since it's a REST API.
Free tier has limited requests/day — respects rate limits.
Raw JSON responses are saved to cache.

Requires:
    pip install requests
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

SCRIPT_NAME = "149_racing_api_free"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
RAW_JSON_DIR = os.path.join(OUTPUT_DIR, "raw_json")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(RAW_JSON_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import (
    smart_pause,
    append_jsonl,
    load_checkpoint,
    save_checkpoint,
    create_session,
    fetch_with_retry,
)

log = setup_logging("149_racing_api_free")

BASE_API_URL = "https://the-racing-api.com/v1"
# Free tier: ~200 requests/day, respect rate limits
MAX_REQUESTS_PER_DAY = 180
REQUEST_DELAY_BASE = 3.0  # seconds between requests
REQUEST_DELAY_JITTER = 1.5


# ------------------------------------------------------------------
# API helper
# ------------------------------------------------------------------

def api_get(session, endpoint, params=None):
    """Make a GET request to The Racing API. Returns parsed JSON or None."""
    url = f"{BASE_API_URL}{endpoint}"
    resp = fetch_with_retry(session, url, max_retries=3, timeout=30, params=params, logger=log)
    if resp is None:
        return None
    try:
        data = resp.json()
        return data
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("  JSON decode error on %s: %s", endpoint, e)
        return None


def save_raw_json(data, filename):
    """Save raw JSON response to disk."""
    filepath = os.path.join(RAW_JSON_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filepath


# ------------------------------------------------------------------
# Scraping functions
# ------------------------------------------------------------------

def scrape_results_day(session, date_str, request_count):
    """Fetch results for a given date from the API."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f), request_count

    data = api_get(session, f"/results", params={"date": date_str})
    request_count += 1

    if data is None:
        return None, request_count

    # Save raw response
    save_raw_json(data, f"results_{date_str}.json")

    records = []
    results_list = data if isinstance(data, list) else data.get("results", data.get("data", []))
    if isinstance(results_list, dict):
        results_list = [results_list]

    for item in results_list:
        if not isinstance(item, dict):
            continue
        record = {
            "date": date_str,
            "source": "racing_api",
            "type": "result",
            "scraped_at": datetime.now().isoformat(),
        }
        record.update(item)
        records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records, request_count


def scrape_racecards_day(session, date_str, request_count):
    """Fetch racecards for a given date from the API."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f), request_count

    data = api_get(session, f"/racecards", params={"date": date_str})
    request_count += 1

    if data is None:
        return None, request_count

    save_raw_json(data, f"racecards_{date_str}.json")

    records = []
    cards_list = data if isinstance(data, list) else data.get("racecards", data.get("data", []))
    if isinstance(cards_list, dict):
        cards_list = [cards_list]

    for item in cards_list:
        if not isinstance(item, dict):
            continue
        record = {
            "date": date_str,
            "source": "racing_api",
            "type": "racecard",
            "scraped_at": datetime.now().isoformat(),
        }
        record.update(item)
        records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records, request_count


def scrape_horses_from_results(session, results, request_count, max_horses=10):
    """Fetch individual horse data for horses found in results."""
    records = []
    horse_ids = set()

    # Extract horse IDs from results
    for result in (results or []):
        for key in ["horse_id", "horseId", "horse", "runner_id"]:
            val = result.get(key)
            if val and isinstance(val, (str, int)):
                horse_ids.add(str(val))
        # Check nested runners
        for runner in result.get("runners", result.get("entries", [])):
            if isinstance(runner, dict):
                for key in ["horse_id", "horseId", "id"]:
                    val = runner.get(key)
                    if val:
                        horse_ids.add(str(val))

    horse_ids = sorted(horse_ids)[:max_horses]

    for horse_id in horse_ids:
        if request_count >= MAX_REQUESTS_PER_DAY:
            log.warning("  Daily request limit reached (%d), stopping horse lookups",
                        MAX_REQUESTS_PER_DAY)
            break

        cache_file = os.path.join(CACHE_DIR, f"horse_{horse_id}.json")
        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                records.append(json.load(f))
            continue

        data = api_get(session, f"/horses/{horse_id}")
        request_count += 1

        if data is None:
            smart_pause(REQUEST_DELAY_BASE, REQUEST_DELAY_JITTER)
            continue

        save_raw_json(data, f"horse_{horse_id}.json")

        record = {
            "source": "racing_api",
            "type": "horse",
            "horse_id": horse_id,
            "scraped_at": datetime.now().isoformat(),
        }
        if isinstance(data, dict):
            record.update(data)
        else:
            record["data"] = data

        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        records.append(record)

        smart_pause(REQUEST_DELAY_BASE, REQUEST_DELAY_JITTER)

    return records, request_count


def scrape_courses(session, request_count):
    """Fetch list of courses/venues from the API."""
    cache_file = os.path.join(CACHE_DIR, "courses.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f), request_count

    data = api_get(session, "/courses")
    request_count += 1

    if data is None:
        return None, request_count

    save_raw_json(data, "courses.json")

    records = []
    courses_list = data if isinstance(data, list) else data.get("courses", data.get("data", []))
    if isinstance(courses_list, dict):
        courses_list = [courses_list]

    for item in courses_list:
        if not isinstance(item, dict):
            continue
        record = {
            "source": "racing_api",
            "type": "course",
            "scraped_at": datetime.now().isoformat(),
        }
        record.update(item)
        records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records, request_count


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 149 — The Racing API Free Tier Scraper (results, racecards, horses)"
    )
    parser.add_argument("--start", type=str, default="2024-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=yesterday")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--api-key", type=str, default=None,
                        help="API key (optional, for higher rate limits)")
    parser.add_argument("--max-requests", type=int, default=MAX_REQUESTS_PER_DAY,
                        help="Max API requests per session (default=%d)" % MAX_REQUESTS_PER_DAY)
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
                else datetime.now() - timedelta(days=1))

    log.info("=" * 60)
    log.info("SCRIPT 149 — The Racing API Free Tier Scraper (HTTP)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("  Max requests/session: %d", args.max_requests)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "racing_api_data.jsonl")

    # Create HTTP session
    session = create_session()
    session.headers.update({
        "Accept": "application/json",
        "Accept-Language": "en-GB,en;q=0.9",
    })
    if args.api_key:
        session.headers.update({"Authorization": f"Bearer {args.api_key}"})

    max_req = args.max_requests
    request_count = 0
    day_count = 0
    total_records = 0

    # First, scrape courses list (one-time)
    courses, request_count = scrape_courses(session, request_count)
    if courses:
        for rec in courses:
            append_jsonl(output_file, rec)
            total_records += 1
        log.info("  Fetched %d courses", len(courses))

    current = start_date
    while current <= end_date:
        if args.max_days and day_count >= args.max_days:
            break
        if request_count >= max_req:
            log.warning("  Daily request limit reached (%d), stopping", max_req)
            # Save checkpoint at current date so we can resume tomorrow
            save_checkpoint(CHECKPOINT_FILE, {
                "last_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"),
                "total_records": total_records,
                "request_count": request_count,
                "status": "rate_limited",
            })
            break

        date_str = current.strftime("%Y-%m-%d")

        # Fetch results
        results, request_count = scrape_results_day(session, date_str, request_count)
        if results:
            for rec in results:
                append_jsonl(output_file, rec)
                total_records += 1
        smart_pause(REQUEST_DELAY_BASE, REQUEST_DELAY_JITTER)

        # Fetch racecards
        if request_count < max_req:
            racecards, request_count = scrape_racecards_day(session, date_str, request_count)
            if racecards:
                for rec in racecards:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(REQUEST_DELAY_BASE, REQUEST_DELAY_JITTER)

        # Fetch horse details for horses found in results (limited)
        if request_count < max_req and results:
            horses, request_count = scrape_horses_from_results(
                session, results, request_count, max_horses=5
            )
            if horses:
                for rec in horses:
                    append_jsonl(output_file, rec)
                    total_records += 1

        day_count += 1

        if day_count % 5 == 0:
            log.info("  %s | days=%d records=%d requests=%d/%d",
                     date_str, day_count, total_records, request_count, max_req)
            save_checkpoint(CHECKPOINT_FILE, {
                "last_date": date_str,
                "total_records": total_records,
                "request_count": request_count,
            })

        current += timedelta(days=1)
        smart_pause(REQUEST_DELAY_BASE, REQUEST_DELAY_JITTER)

    save_checkpoint(CHECKPOINT_FILE, {
        "last_date": min(current - timedelta(days=1), end_date).strftime("%Y-%m-%d"),
        "total_records": total_records,
        "request_count": request_count,
        "status": "done" if current > end_date else "rate_limited",
    })

    log.info("=" * 60)
    log.info("DONE: %d days, %d records, %d API requests -> %s",
             day_count, total_records, request_count, output_file)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
