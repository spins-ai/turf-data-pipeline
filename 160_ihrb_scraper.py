#!/usr/bin/env python3
"""
160_ihrb_scraper.py — IHRB (Irish Horseracing Regulatory Board) scraper
========================================================================
Scrapes race results and regulatory data from IHRB (ihrb.ie).

Uses Playwright for JS-rendered pages. Saves to output/160_ihrb/

Usage:
    python 160_ihrb_scraper.py [--max-pages 200]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.logging_setup import setup_logging
from utils.scraping import load_checkpoint, save_checkpoint

_PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = _PROJECT_ROOT / "output" / "160_ihrb"
CACHE_DIR = OUTPUT_DIR / "cache"
CHECKPOINT_FILE = OUTPUT_DIR / ".checkpoint.json"

BASE_URL = "https://www.ihrb.ie"
RESULTS_URL = f"{BASE_URL}/racing/results"


def scrape_ihrb(logger, max_pages: int = 200):
    """Scrape IHRB race results using Playwright."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install")
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

            # Extract race meeting links
            meetings = page.query_selector_all("a[href*='/racing/results/'], .meeting-link, .race-link")
            logger.info("Found %d meeting links", len(meetings))

            hrefs = []
            for m in meetings[:max_pages]:
                href = m.get_attribute("href")
                if href:
                    hrefs.append(href if href.startswith("http") else BASE_URL + href)

            for href in hrefs:
                try:
                    page.goto(href, timeout=20000, wait_until="domcontentloaded")
                    page.wait_for_timeout(2000)

                    # Extract race data
                    race_title = page.query_selector("h1, .race-title, .page-title")
                    title_text = race_title.inner_text().strip() if race_title else href

                    rows = page.query_selector_all("tr[data-runner], .result-row, .runner-row, tbody tr")
                    for row in rows:
                        cells = row.query_selector_all("td")
                        if len(cells) >= 3:
                            results.append({
                                "source": "ihrb",
                                "race": title_text,
                                "url": href,
                                "position": cells[0].inner_text().strip() if cells[0] else "",
                                "horse_name": cells[1].inner_text().strip() if cells[1] else "",
                                "details": cells[2].inner_text().strip() if cells[2] else "",
                                "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            })
                except Exception as e:
                    logger.warning("Error scraping %s: %s", href, e)

        except Exception as e:
            logger.error("Error loading IHRB results page: %s", e)

        browser.close()

    logger.info("Total IHRB records: %d", len(results))
    return results


def main():
    logger = setup_logging("ihrb_scraper")
    parser = argparse.ArgumentParser(description="IHRB Scraper")
    parser.add_argument("--max-pages", type=int, default=200)
    args = parser.parse_args()

    checkpoint = load_checkpoint(str(CHECKPOINT_FILE))
    results = scrape_ihrb(logger, max_pages=args.max_pages)

    if results:
        out_path = OUTPUT_DIR / "ihrb_results.jsonl"
        with open(out_path, "w", encoding="utf-8", newline="\n") as f:
            for rec in results:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Saved %d records to %s", len(results), out_path)
        save_checkpoint(str(CHECKPOINT_FILE), {
            "total_records": len(results),
            "status": "done",
            "last_run": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })


if __name__ == "__main__":
    main()
