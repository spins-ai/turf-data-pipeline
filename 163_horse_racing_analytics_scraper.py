#!/usr/bin/env python3
"""
163_horse_racing_analytics_scraper.py — Horse Racing Analytics scraper
======================================================================
Scrapes free analytics data from horse-racing analytics sites.

Uses Playwright for JS-rendered pages. Saves to output/163_hra/

Usage:
    python 163_horse_racing_analytics_scraper.py [--max-pages 100]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.logging_setup import setup_logging

_PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = _PROJECT_ROOT / "output" / "163_hra"
CACHE_DIR = OUTPUT_DIR / "cache"

TARGETS = [
    {
        "name": "racing_analytics",
        "url": "https://www.horseracinganalytics.com/",
        "type": "analytics",
    },
    {
        "name": "racing_research",
        "url": "https://www.racingresearch.co.uk/",
        "type": "research",
    },
]


def scrape_analytics(logger, max_pages: int = 100):
    """Scrape racing analytics data using Playwright."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed.")
        return []

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        page = context.new_page()

        for target in TARGETS:
            logger.info("Scraping %s: %s", target["name"], target["url"])
            try:
                page.goto(target["url"], timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(3000)

                # Extract data tables
                tables = page.query_selector_all("table")
                logger.info("  Found %d tables on %s", len(tables), target["name"])

                for table in tables[:max_pages]:
                    rows = table.query_selector_all("tr")
                    headers = []
                    for row_idx, row in enumerate(rows):
                        cells = row.query_selector_all("td, th")
                        texts = [c.inner_text().strip() for c in cells]
                        if row_idx == 0:
                            headers = texts
                        elif texts:
                            record = {"source": target["name"]}
                            for i, val in enumerate(texts):
                                key = headers[i] if i < len(headers) else f"col_{i}"
                                record[key] = val
                            record["scraped_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                            results.append(record)

            except Exception as e:
                logger.warning("Error on %s: %s", target["name"], e)

        browser.close()

    logger.info("Total analytics records: %d", len(results))
    return results


def main():
    logger = setup_logging("hra_scraper")
    parser = argparse.ArgumentParser(description="Horse Racing Analytics Scraper")
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()

    results = scrape_analytics(logger, max_pages=args.max_pages)

    if results:
        out_path = OUTPUT_DIR / "hra_data.jsonl"
        with open(out_path, "w", encoding="utf-8") as f:
            for rec in results:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Saved %d records to %s", len(results), out_path)


if __name__ == "__main__":
    main()
