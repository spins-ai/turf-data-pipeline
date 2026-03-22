#!/usr/bin/env python3
"""
Script 62 — Scraping HorseRacingNation.com (US Racing)
Source : horseracingnation.com
Collecte : news, entries, results, race previews
CRITIQUE pour : US Racing News, Entries & Results, Race Analysis
Backend : Playwright (headless Chromium) — bypasses anti-bot
"""

import argparse
import json
import os
import sys
import random
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "62_horse_racing_nation"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("62_horse_racing_nation")

BASE_URL = "https://www.horseracingnation.com"


def launch_browser(pw):
    """Launch headless Chromium with en-US locale for US sites."""
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    )
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
        timezone_id="America/New_York",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        java_script_enabled=True,
        ignore_https_errors=True,
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        window.chrome = {runtime: {}};
    """)
    page = context.new_page()
    page.set_default_timeout(60_000)
    return browser, context, page


def navigate_with_retry(page, url, retries=3):
    """Navigate to a URL with retry logic. Returns page HTML or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=60_000)
            if resp and resp.status >= 400:
                log.warning("  HTTP %d on %s (attempt %d/%d)", resp.status, url, attempt, retries)
                if resp.status == 429:
                    time.sleep(60 * attempt)
                elif resp.status == 403:
                    time.sleep(30 * attempt)
                else:
                    time.sleep(5 * attempt)
                continue
            page.wait_for_load_state("domcontentloaded")
            time.sleep(1.5)
            return page.content()
        except PlaywrightTimeout:
            log.warning("  Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(10 * attempt)
        except Exception as exc:
            log.warning("  Navigation error: %s (attempt %d/%d)", str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


def scrape_entries(page, date_str):
    """Scrape HRN entries for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"entries_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/entries/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract track sections
    for section in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["track", "card", "entries"]):
            track_name = ""
            h_tag = section.find(["h2", "h3", "h4"])
            if h_tag:
                track_name = h_tag.get_text(strip=True)

            for table in section.find_all("table"):
                rows = table.find_all("tr")
                headers = []
                if rows:
                    headers = [th.get_text(strip=True).lower().replace(" ", "_")
                               for th in rows[0].find_all(["th", "td"])]
                if len(headers) < 2:
                    continue

                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if not cells or len(cells) < 2:
                        continue
                    record = {
                        "date": date_str,
                        "source": "horse_racing_nation",
                        "type": "entry",
                        "track": track_name,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    for j, cell in enumerate(cells):
                        key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                        record[key] = cell
                    records.append(record)

    # Extract entry links for individual races
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if "/entries/" in href and text and len(text) > 2:
            records.append({
                "date": date_str,
                "source": "horse_racing_nation",
                "type": "entry_link",
                "text": text,
                "url": href if href.startswith("http") else BASE_URL + href,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(page, date_str):
    """Scrape HRN results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/results/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract results tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "date": date_str,
                "source": "horse_racing_nation",
                "type": "result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_news(page, date_str):
    """Scrape HRN news articles for context and analysis."""
    cache_file = os.path.join(CACHE_DIR, f"news_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/news"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract article previews
    for article in soup.find_all(["article", "div"], class_=True):
        classes = " ".join(article.get("class", []))
        if any(kw in classes.lower() for kw in ["article", "post", "story", "news-item", "card"]):
            title_el = article.find(["h2", "h3", "h4", "a"])
            title = title_el.get_text(strip=True) if title_el else ""
            link_el = article.find("a", href=True)
            link = ""
            if link_el:
                href = link_el["href"]
                link = href if href.startswith("http") else BASE_URL + href

            summary_el = article.find(["p", "div"], class_=lambda c: c and any(
                kw in c.lower() for kw in ["summary", "excerpt", "preview", "desc"]
            ) if c else False)
            summary = summary_el.get_text(strip=True)[:500] if summary_el else ""

            if title:
                records.append({
                    "date": date_str,
                    "source": "horse_racing_nation",
                    "type": "news",
                    "title": title,
                    "url": link,
                    "summary": summary,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 62 — Horse Racing Nation Scraper (US news, entries, results) [Playwright]")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=today")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 62 — Horse Racing Nation Scraper (US Racing) [Playwright]")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "hrn_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = launch_browser(pw)
    try:
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Scrape entries
            entry_records = scrape_entries(page, date_str)
            if entry_records:
                for rec in entry_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.0, 1.0)

            # Scrape results
            result_records = scrape_results(page, date_str)
            if result_records:
                for rec in result_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.0, 1.0)

            # Scrape news (once per week to avoid redundancy)
            if current.weekday() == 0:
                news_records = scrape_news(page, date_str)
                if news_records:
                    for rec in news_records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                smart_pause(2.0, 1.0)

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | days={day_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

            if day_count % 80 == 0:
                # Rotate browser context to avoid detection
                context.close()
                browser.close()
                browser, context, page = launch_browser(pw)
                time.sleep(random.uniform(5, 15))

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
        log.info("=" * 60)
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
