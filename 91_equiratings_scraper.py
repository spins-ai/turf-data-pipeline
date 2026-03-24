#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 91 -- Scraping EquiRatings.com (Playwright version)
Source : equiratings.com - Professional ratings and analytics
Collecte : ratings professionnels, classements chevaux, statistiques performance
CRITIQUE pour : Rating Model, Horse Ranking, Performance Analytics

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import sys
import re
import time
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "91_equiratings"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, load_checkpoint, save_checkpoint, append_jsonl

log = setup_logging("91_equiratings")

MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000

BASE_URL = "https://www.equiratings.com"


# NOTE: Local version kept because it returns HTML string (page.content()) instead of bool
def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to url with retry logic. Returns HTML string or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_MS)
            if resp and resp.status >= 400:
                log.warning("  HTTP %d on %s (attempt %d/%d)",
                            resp.status, url, attempt, retries)
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
            log.warning("  Navigation error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


def scrape_rankings_page(page, page_url):
    """Scrape a rankings/leaderboard page from EquiRatings."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"rank_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    html = navigate_with_retry(page, page_url)
    if not html:
        return []

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"rank_{url_hash}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # -- Extract ranking tables --
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                record = {
                    "source": "equiratings",
                    "type": "ranking",
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                # Try to extract numeric rating
                for cell in cells:
                    m = re.search(r'(\d+\.?\d*)', cell)
                    if m:
                        try:
                            val = float(m.group(1))
                            if 0 < val < 200:
                                record["rating_value"] = val
                                break
                        except ValueError:
                            pass
                records.append(record)

    # -- Extract card/grid items (horse profiles) --
    for card in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(card.get("class", []))
        if any(kw in classes.lower() for kw in ["card", "horse", "rider", "ranking",
                                                  "athlete", "leaderboard", "entry"]):
            text = card.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                link = card.find("a", href=True)
                record = {
                    "source": "equiratings",
                    "type": "profile_card",
                    "url": page_url,
                    "text": text[:1500],
                    "scraped_at": datetime.now().isoformat(),
                }
                if link:
                    record["detail_url"] = link["href"]
                # Extract name from heading
                heading = card.find(["h2", "h3", "h4", "h5"])
                if heading:
                    record["name"] = heading.get_text(strip=True)
                records.append(record)

    # -- Extract data from script tags --
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "equiratings",
                    "type": "embedded_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        for m in re.finditer(r'window\[?[\'"]?(\w+)[\'"]?\]?\s*=\s*(\{.+?\});', script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "equiratings",
                    "type": "embedded_window",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "source": "equiratings",
                "type": "script_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # -- Extract data-attributes --
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["rating", "score", "rank", "horse", "rider", "value"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "equiratings",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # -- Extract analytics paragraphs --
    for div in soup.find_all(["div", "p", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["analysis", "insight", "stat",
                                                  "performance", "summary", "metric"]):
            if text and 20 < len(text) < 3000:
                records.append({
                    "source": "equiratings",
                    "type": "analytics_text",
                    "contenu": text[:2500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def discover_pages(page):
    """Discover ranking/analytics pages from main site."""
    html = navigate_with_retry(page, BASE_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    pages = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(kw in href.lower() for kw in ["ranking", "leaderboard", "rating",
                                               "horse", "rider", "event", "result",
                                               "analytics", "stats", "data"]):
            if href.startswith("/"):
                href = BASE_URL + href
            if href.startswith("http"):
                pages.add(href)

    # Also try known paths
    known_paths = [
        "/rankings", "/leaderboard", "/horses", "/riders",
        "/events", "/results", "/analytics", "/data",
        "/rankings/horses", "/rankings/riders",
    ]
    for path in known_paths:
        pages.add(BASE_URL + path)

    return list(pages)


def main():
    parser = argparse.ArgumentParser(
        description="Script 91 -- EquiRatings Scraper (ratings, analytics)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-pages", type=int, default=500,
                        help="Nombre max de pages a scraper")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 91 -- EquiRatings Scraper (Playwright)")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    done_urls = set(checkpoint.get("done_urls", []))
    if args.resume and done_urls:
        log.info(f"  Reprise checkpoint: {len(done_urls)} pages deja traitees")

    output_file = os.path.join(OUTPUT_DIR, "equiratings_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw)
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        # Discover pages
        pages = discover_pages(page)
        log.info(f"  Pages decouvertes: {len(pages)}")

        if first_nav:
            accept_cookies(page)
            first_nav = False

        smart_pause()

        total_records = 0
        page_count = 0
        all_detail_urls = set()

        for page_url in pages:
            if page_url in done_urls:
                continue
            if page_count >= args.max_pages:
                break

            records = scrape_rankings_page(page, page_url)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1
                    # Collect detail links
                    detail = rec.get("detail_url", "")
                    if detail:
                        if detail.startswith("/"):
                            detail = BASE_URL + detail
                        all_detail_urls.add(detail)

            done_urls.add(page_url)
            page_count += 1

            if page_count % 10 == 0:
                log.info(f"  pages={page_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                                 "total_records": total_records})

            smart_pause()

        # Scrape detail pages
        log.info(f"  Detail pages a scraper: {len(all_detail_urls)}")
        for detail_url in all_detail_urls:
            if detail_url in done_urls:
                continue
            if page_count >= args.max_pages:
                break

            records = scrape_rankings_page(page, detail_url)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            done_urls.add(detail_url)
            page_count += 1

            if page_count % 10 == 0:
                log.info(f"  pages={page_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                                 "total_records": total_records})

            smart_pause()

        save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"TERMINE: {page_count} pages, {total_records} records -> {output_file}")
        log.info("=" * 60)

    finally:
        # Graceful cleanup
        try:
            if page and not page.is_closed():
                page.close()
        except Exception:
            pass
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass
        log.info("Browser closed")


if __name__ == "__main__":
    main()
