#!/usr/bin/env python3
"""
Script 155 — data.gouv.fr Horse Racing Datasets Scraper (HTTP)
Source : data.gouv.fr (open data portal)
Collecte : Open datasets related to courses hippiques, PMU, turf
Methode : HTTP requests (pas Playwright) — recherche via l'API data.gouv.fr,
          telecharge les CSVs, convertit en JSONL
URL patterns :
  /api/1/datasets/?q={query}  -> recherche de datasets
  /api/1/datasets/{id}/       -> detail d'un dataset
  resource URLs               -> telechargement CSV direct
CRITIQUE pour : Donnees officielles open data, historique PMU, stats hippiques FR

Requires:
    pip install requests
"""

import argparse
import csv
import io
import json
import os
import sys
import time
from datetime import datetime

import requests

SCRIPT_NAME = "155_data_gov_fr"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("155_data_gov_fr")

API_BASE = "https://www.data.gouv.fr/api/1"
SEARCH_QUERIES = ["courses hippiques", "PMU", "turf", "hippodrome",
                  "paris hippiques", "cheval course"]
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60
MAX_CSV_SIZE_MB = 200


# ------------------------------------------------------------------
# HTTP helpers
# ------------------------------------------------------------------

def get_with_retry(url, params=None, retries=MAX_RETRIES, stream=False):
    """GET request with retry logic. Returns response or None."""
    headers = {
        "User-Agent": "turf-data-pipeline/1.0 (research)",
        "Accept": "application/json",
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers,
                                timeout=REQUEST_TIMEOUT, stream=stream)
            if resp.status_code == 429:
                log.warning("  Rate limited on %s (attempt %d/%d)", url, attempt, retries)
                time.sleep(60 * attempt)
                continue
            if resp.status_code >= 500:
                log.warning("  Server error %d on %s (attempt %d/%d)",
                            resp.status_code, url, attempt, retries)
                time.sleep(10 * attempt)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            log.warning("  Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(10 * attempt)
        except requests.exceptions.RequestException as exc:
            log.warning("  Request error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


# ------------------------------------------------------------------
# API interaction
# ------------------------------------------------------------------

def search_datasets(query, page_num=1, page_size=50):
    """Search data.gouv.fr API for datasets matching a query."""
    cache_file = os.path.join(CACHE_DIR, f"search_{query.replace(' ', '_')}_p{page_num}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    params = {
        "q": query,
        "page": page_num,
        "page_size": page_size,
    }
    resp = get_with_retry(f"{API_BASE}/datasets/", params=params)
    if not resp:
        return None

    data = resp.json()
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return data


def get_dataset_detail(dataset_id):
    """Get full dataset details including resource URLs."""
    cache_file = os.path.join(CACHE_DIR, f"dataset_{dataset_id}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = get_with_retry(f"{API_BASE}/datasets/{dataset_id}/")
    if not resp:
        return None

    data = resp.json()
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return data


def download_csv_resource(resource_url, resource_id):
    """Download a CSV resource and return rows as list of dicts."""
    cache_file = os.path.join(CACHE_DIR, f"resource_{resource_id}.csv")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    else:
        # Check file size first via HEAD
        try:
            head_resp = requests.head(resource_url, timeout=30,
                                       allow_redirects=True,
                                       headers={"User-Agent": "turf-data-pipeline/1.0"})
            content_length = int(head_resp.headers.get("Content-Length", 0))
            if content_length > MAX_CSV_SIZE_MB * 1024 * 1024:
                log.warning("  Skipping large CSV (%d MB): %s",
                            content_length // (1024 * 1024), resource_url)
                return None
        except Exception:
            pass

        resp = get_with_retry(resource_url)
        if not resp:
            return None

        content = resp.text
        with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
            f.write(content)

    # Parse CSV
    rows = []
    try:
        # Try different delimiters
        for delimiter in [",", ";", "\t", "|"]:
            try:
                reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
                fieldnames = reader.fieldnames
                if fieldnames and len(fieldnames) >= 2:
                    rows = list(reader)
                    if rows:
                        break
            except csv.Error:
                continue
    except Exception as exc:
        log.warning("  CSV parsing error: %s", str(exc)[:200])
        return None

    return rows


# ------------------------------------------------------------------
# Relevance filtering
# ------------------------------------------------------------------

def is_horse_racing_relevant(dataset_info):
    """Check if a dataset is relevant to horse racing."""
    text = " ".join([
        dataset_info.get("title", ""),
        dataset_info.get("description", ""),
        " ".join(dataset_info.get("tags", [])),
    ]).lower()

    positive_keywords = ["cheval", "hippique", "hippodrome", "pmu", "turf",
                         "jockey", "galop", "trot", "course", "paris",
                         "tierce", "quarte", "quinte", "sulky", "haie",
                         "steeple", "plat", "obstacle"]
    negative_keywords = ["velo", "automobile", "moto", "foot", "tennis",
                         "natation", "athletisme", "ski"]

    pos_score = sum(1 for kw in positive_keywords if kw in text)
    neg_score = sum(1 for kw in negative_keywords if kw in text)

    return pos_score >= 2 and neg_score == 0


# ------------------------------------------------------------------
# Main processing
# ------------------------------------------------------------------

def process_datasets(output_file, max_datasets=0):
    """Search, download and convert all relevant datasets."""
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    processed_ids = set(checkpoint.get("processed_ids", []))
    total_records = checkpoint.get("total_records", 0)

    all_dataset_ids = set()

    # Search with multiple queries
    for query in SEARCH_QUERIES:
        log.info("Searching data.gouv.fr for: '%s'", query)
        page_num = 1

        while True:
            result = search_datasets(query, page_num=page_num)
            if not result:
                break

            datasets = result.get("data", [])
            if not datasets:
                break

            for ds in datasets:
                ds_id = ds.get("id", "")
                if ds_id and ds_id not in all_dataset_ids:
                    all_dataset_ids.add(ds_id)

            total_pages = result.get("total", 0) // 50 + 1
            if page_num >= total_pages or page_num >= 5:
                break
            page_num += 1
            smart_pause(1.0, 0.3)

    log.info("Found %d unique dataset IDs across all queries", len(all_dataset_ids))

    dataset_count = 0
    for ds_id in sorted(all_dataset_ids):
        if max_datasets and dataset_count >= max_datasets:
            break
        if ds_id in processed_ids:
            continue

        detail = get_dataset_detail(ds_id)
        if not detail:
            continue

        if not is_horse_racing_relevant(detail):
            log.debug("  Skipping non-relevant dataset: %s", detail.get("title", "")[:80])
            processed_ids.add(ds_id)
            continue

        title = detail.get("title", "unknown")
        log.info("Processing dataset: %s", title[:80])

        # Record dataset metadata
        meta_record = {
            "source": "data_gouv_fr",
            "type": "dataset_metadata",
            "dataset_id": ds_id,
            "title": title,
            "description": (detail.get("description") or "")[:2000],
            "tags": detail.get("tags", []),
            "organization": (detail.get("organization") or {}).get("name", ""),
            "created_at": detail.get("created_at", ""),
            "last_modified": detail.get("last_modified", ""),
            "frequency": detail.get("frequency", ""),
            "license": detail.get("license", ""),
            "scraped_at": datetime.now().isoformat(),
        }
        append_jsonl(output_file, meta_record)
        total_records += 1

        # Process resources (CSVs)
        resources = detail.get("resources", [])
        csv_resources = [r for r in resources
                         if r.get("format", "").lower() in ("csv", "text/csv")
                         or r.get("url", "").lower().endswith(".csv")]

        for resource in csv_resources[:10]:
            res_url = resource.get("url", "")
            res_id = resource.get("id", "")
            res_title = resource.get("title", "")

            if not res_url:
                continue

            log.info("  Downloading CSV: %s", res_title[:60])
            rows = download_csv_resource(res_url, res_id)

            if not rows:
                log.info("  No rows parsed from %s", res_title[:60])
                continue

            log.info("  Parsed %d rows from %s", len(rows), res_title[:60])

            for row in rows:
                record = {
                    "source": "data_gouv_fr",
                    "type": "csv_row",
                    "dataset_id": ds_id,
                    "dataset_title": title,
                    "resource_id": res_id,
                    "resource_title": res_title,
                    "data": dict(row),
                    "scraped_at": datetime.now().isoformat(),
                }
                append_jsonl(output_file, record)
                total_records += 1

            smart_pause(1.0, 0.3)

        processed_ids.add(ds_id)
        dataset_count += 1

        if dataset_count % 5 == 0:
            log.info("  Datasets processed: %d, total records: %d",
                     dataset_count, total_records)
            save_checkpoint(CHECKPOINT_FILE, {
                "processed_ids": sorted(processed_ids),
                "total_records": total_records,
            })

        smart_pause(1.5, 0.5)

    save_checkpoint(CHECKPOINT_FILE, {
        "processed_ids": sorted(processed_ids),
        "total_records": total_records,
        "status": "done",
    })

    return dataset_count, total_records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 155 — data.gouv.fr Horse Racing Datasets (CSV -> JSONL)"
    )
    parser.add_argument("--max-datasets", type=int, default=0,
                        help="Max datasets to process (0=unlimited)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 155 — data.gouv.fr Horse Racing Datasets Scraper")
    log.info("  Queries: %s", ", ".join(SEARCH_QUERIES))
    log.info("=" * 60)

    output_file = os.path.join(OUTPUT_DIR, "data_gouv_fr_data.jsonl")

    dataset_count, total_records = process_datasets(
        output_file, max_datasets=args.max_datasets
    )

    log.info("=" * 60)
    log.info("DONE: %d datasets, %d records -> %s",
             dataset_count, total_records, output_file)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
