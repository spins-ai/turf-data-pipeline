#!/usr/bin/env python3
"""
162_european_bloodstock_scraper.py — European Bloodstock News scraper
=====================================================================
Scrapes breeding/bloodstock news and stallion data from European sources.

Uses Playwright for JS-rendered pages. Saves to output/162_european_bloodstock/

Usage:
    python 162_european_bloodstock_scraper.py [--max-pages 100]
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "162_european_bloodstock"
CACHE_DIR = OUTPUT_DIR / "cache"

TARGETS = [
    {
        "name": "european_bloodstock",
        "url": "https://www.europeanbloodstock.com/stallions/",
        "type": "stallion_directory",
    },
    {
        "name": "bloodstock_news",
        "url": "https://www.bloodstock.com.au/",
        "type": "bloodstock_news",
    },
]


def scrape_bloodstock(logger, max_pages: int = 100):
    """Scrape European bloodstock data using Playwright."""
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

                # Extract stallion/horse links
                links = page.query_selector_all("a[href*='stallion'], a[href*='sire'], .horse-link, .stallion-card")
                logger.info("  Found %d links on %s", len(links), target["name"])

                for link in links[:max_pages]:
                    try:
                        name = link.inner_text().strip()
                        href = link.get_attribute("href") or ""
                        if name:
                            results.append({
                                "source": target["name"],
                                "stallion_name": name,
                                "url": href,
                                "type": target["type"],
                                "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            })
                    except Exception:
                        pass

            except Exception as e:
                logger.warning("Error on %s: %s", target["name"], e)

        browser.close()

    logger.info("Total bloodstock records: %d", len(results))
    return results


def main():
    logger = setup_logging("european_bloodstock_scraper")
    parser = argparse.ArgumentParser(description="European Bloodstock Scraper")
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()

    results = scrape_bloodstock(logger, max_pages=args.max_pages)

    if results:
        out_path = OUTPUT_DIR / "european_bloodstock.jsonl"
        with open(out_path, "w", encoding="utf-8") as f:
            for rec in results:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Saved %d records to %s", len(results), out_path)


if __name__ == "__main__":
    main()
