#!/usr/bin/env python3
"""
161_emirates_racing_scraper.py — Emirates Racing Authority scraper
==================================================================
Scrapes race results from Emirates Racing Authority (emiratesracing.com).

Uses Playwright for JS-rendered pages. Saves to output/161_emirates_racing/

Usage:
    python 161_emirates_racing_scraper.py [--max-pages 200]
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "161_emirates_racing"
CACHE_DIR = OUTPUT_DIR / "cache"

BASE_URL = "https://www.emiratesracing.com"
RESULTS_URL = f"{BASE_URL}/racing/results"


def scrape_emirates(logger, max_pages: int = 200):
    """Scrape Emirates Racing results using Playwright."""
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
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        logger.info("Navigating to %s", RESULTS_URL)
        try:
            page.goto(RESULTS_URL, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)

            meetings = page.query_selector_all("a[href*='result'], .meeting-card, .race-result-link")
            logger.info("Found %d result links", len(meetings))

            hrefs = []
            for m in meetings[:max_pages]:
                href = m.get_attribute("href")
                if href:
                    hrefs.append(href if href.startswith("http") else BASE_URL + href)

            for href in hrefs:
                try:
                    page.goto(href, timeout=20000, wait_until="domcontentloaded")
                    page.wait_for_timeout(2000)

                    race_title = page.query_selector("h1, .race-name, .page-heading")
                    title_text = race_title.inner_text().strip() if race_title else href

                    rows = page.query_selector_all("tr[data-horse], .result-row, tbody tr")
                    for row in rows:
                        cells = row.query_selector_all("td")
                        if len(cells) >= 3:
                            results.append({
                                "source": "emirates_racing",
                                "race": title_text,
                                "url": href,
                                "position": cells[0].inner_text().strip(),
                                "horse_name": cells[1].inner_text().strip(),
                                "jockey": cells[2].inner_text().strip() if len(cells) > 2 else "",
                                "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            })
                except Exception as e:
                    logger.warning("Error scraping %s: %s", href, e)

        except Exception as e:
            logger.error("Error loading Emirates Racing: %s", e)

        browser.close()

    logger.info("Total Emirates records: %d", len(results))
    return results


def main():
    logger = setup_logging("emirates_racing_scraper")
    parser = argparse.ArgumentParser(description="Emirates Racing Scraper")
    parser.add_argument("--max-pages", type=int, default=200)
    args = parser.parse_args()

    results = scrape_emirates(logger, max_pages=args.max_pages)

    if results:
        out_path = OUTPUT_DIR / "emirates_results.jsonl"
        with open(out_path, "w", encoding="utf-8") as f:
            for rec in results:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Saved %d records to %s", len(results), out_path)


if __name__ == "__main__":
    main()
