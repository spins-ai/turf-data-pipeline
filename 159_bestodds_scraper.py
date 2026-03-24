#!/usr/bin/env python3
"""
159_bestodds_scraper.py — BestOdds / Betbrain odds comparison scraper
=====================================================================
Scrapes best-available odds from free comparison sites for horse racing.

Uses Playwright for JS-rendered pages. Saves to output/159_bestodds/

Usage:
    python 159_bestodds_scraper.py [--max-pages 100]
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "159_bestodds"
CACHE_DIR = OUTPUT_DIR / "cache"
CHECKPOINT_FILE = OUTPUT_DIR / ".checkpoint.json"

# Target URLs (free odds comparison sites)
TARGETS = [
    {
        "name": "oddschecker_racing",
        "url": "https://www.oddschecker.com/horse-racing",
        "type": "odds_comparison",
    },
    {
        "name": "bestodds_today",
        "url": "https://www.bestodds.com.au/horse-racing/",
        "type": "odds_comparison",
    },
]


def scrape_bestodds(logger, max_pages: int = 100):
    """Scrape best odds from comparison sites using Playwright."""
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

        for target in TARGETS:
            logger.info("Scraping %s: %s", target["name"], target["url"])
            try:
                page.goto(target["url"], timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(3000)

                # Extract race cards and odds from the page
                cards = page.query_selector_all("[data-race], .race-card, .racing-card, .event-card")
                logger.info("  Found %d race cards on %s", len(cards), target["name"])

                for card in cards[:max_pages]:
                    try:
                        race_name = card.query_selector(".race-name, .event-name, h3, h4")
                        race_text = race_name.inner_text().strip() if race_name else "Unknown"

                        runners = card.query_selector_all(".runner, .selection, .runner-row, tr[data-runner]")
                        for runner in runners:
                            name_el = runner.query_selector(".runner-name, .selection-name, .horse-name, td:first-child")
                            odds_el = runner.query_selector(".best-odds, .price, .odds, td:last-child")

                            name = name_el.inner_text().strip() if name_el else None
                            odds_text = odds_el.inner_text().strip() if odds_el else None

                            if name:
                                results.append({
                                    "source": target["name"],
                                    "race": race_text,
                                    "runner_name": name,
                                    "best_odds_text": odds_text,
                                    "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                })
                    except Exception as e:
                        logger.debug("  Error parsing card: %s", e)

            except Exception as e:
                logger.warning("Error on %s: %s", target["name"], e)

        browser.close()

    logger.info("Total records scraped: %d", len(results))
    return results


def main():
    logger = setup_logging("bestodds_scraper")
    parser = argparse.ArgumentParser(description="BestOdds/Betbrain Scraper")
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()

    checkpoint = load_checkpoint(str(CHECKPOINT_FILE))
    results = scrape_bestodds(logger, max_pages=args.max_pages)

    if results:
        out_path = OUTPUT_DIR / "bestodds.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="\n") as f:
            for rec in results:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Saved %d records to %s", len(results), out_path)
        save_checkpoint(str(CHECKPOINT_FILE), {
            "total_records": len(results),
            "status": "done",
            "last_run": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
    else:
        logger.info("No results to save.")


if __name__ == "__main__":
    main()
