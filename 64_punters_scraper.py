#!/usr/bin/env python3
"""
Script 64 — Scraping Punters.com.au (Australian Racing)
Source : punters.com.au
Collecte : race data, form guides, tips, results, track conditions
CRITIQUE pour : Australian Racing Data, Form Analysis, Track Bias
Backend : Playwright (headless Chromium) — bypasses Cloudflare
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "64_punters"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("64_punters")

BASE_URL = "https://www.punters.com.au"


def launch_browser(pw):
    """Launch headless Chromium with en-AU locale for AU sites."""
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
        locale="en-AU",
        timezone_id="Australia/Sydney",
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
        Object.defineProperty(navigator, 'languages', {get: () => ['en-AU', 'en']});
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


def extract_embedded_json(soup, date_str, source="punters_au"):
    """Extract all embedded JSON from script tags."""
    records = []
    for script in soup.find_all("script"):
        script_text = script.string or ""
        if script.get("type") == "application/ld+json":
            try:
                ld = json.loads(script_text)
                records.append({
                    "date": date_str,
                    "source": source,
                    "type": "json_ld",
                    "ld_type": ld.get("@type", "") if isinstance(ld, dict) else "array",
                    "data": ld if isinstance(ld, dict) else ld[:20],
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, TypeError):
                pass
            continue
        if len(script_text) < 50:
            continue
        for kw in ["race", "runner", "horse", "jockey", "trainer", "odds", "form",
                    "tip", "speed", "track", "result", "barrier", "weight"]:
            if kw in script_text.lower():
                json_matches = re.findall(r'\{[^{}]{30,}\}', script_text)
                for jm in json_matches[:15]:
                    try:
                        data = json.loads(jm)
                        records.append({
                            "date": date_str,
                            "source": source,
                            "type": "embedded_json",
                            "data": data,
                            "scraped_at": datetime.now().isoformat(),
                        })
                    except json.JSONDecodeError:
                        pass
                array_matches = re.findall(r'\[[^\[\]]{30,}\]', script_text)
                for am in array_matches[:10]:
                    try:
                        data = json.loads(am)
                        if isinstance(data, list) and len(data) > 0:
                            records.append({
                                "date": date_str,
                                "source": source,
                                "type": "embedded_json_array",
                                "data": data[:30],
                                "scraped_at": datetime.now().isoformat(),
                            })
                    except json.JSONDecodeError:
                        pass
                break
    return records


def extract_data_attributes(soup, date_str, source="punters_au"):
    """Extract all data-* attributes from DOM elements."""
    records = []
    seen = set()
    for el in soup.find_all(True):
        data_attrs = {k: v for k, v in el.attrs.items()
                      if isinstance(k, str) and k.startswith("data-") and v}
        if len(data_attrs) >= 2:
            key = frozenset(data_attrs.items())
            if key in seen:
                continue
            seen.add(key)
            record = {
                "date": date_str,
                "source": source,
                "type": "data_attribute",
                "tag": el.name,
                "scraped_at": datetime.now().isoformat(),
            }
            for attr_name, attr_val in data_attrs.items():
                clean_name = attr_name.replace("data-", "").replace("-", "_")
                record[clean_name] = attr_val
            text = el.get_text(strip=True)
            if text and len(text) < 300:
                record["text_content"] = text
            records.append(record)
    return records


def extract_comments(soup, date_str, source="punters_au"):
    """Extract comments, previews and analysis divs."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "preview", "analysis",
                                                   "verdict", "expert", "assessment",
                                                   "race-comment", "summary"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": source,
                    "type": "comment",
                    "content": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_track_conditions_detail(soup, date_str, source="punters_au"):
    """Extract detailed track condition data."""
    records = []
    for el in soup.find_all(["div", "span", "section", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["track-condition", "going", "rail",
                                                   "weather", "surface", "moisture",
                                                   "penetrometer", "track-info"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "track_condition_detail",
                    "content": text,
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                rating_match = re.search(r'(Good|Soft|Heavy|Firm|Synthetic)\s*(\d+)?', text, re.I)
                if rating_match:
                    record["track_rating"] = rating_match.group(0).strip()
                records.append(record)
    return records


def extract_speed_maps(soup, date_str, source="punters_au"):
    """Extract speed map data from Punters."""
    records = []
    for el in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["speed-map", "pace-map", "race-pace",
                                                   "settling", "position-map"]):
            if el.name == "table":
                rows = el.find_all("tr")
                headers = []
                if rows:
                    headers = [th.get_text(strip=True).lower().replace(" ", "_")
                               for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        record = {
                            "date": date_str,
                            "source": source,
                            "type": "speed_map",
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                            record[key] = cell
                        records.append(record)
            else:
                text = el.get_text(strip=True)
                if text and 5 < len(text) < 1000:
                    records.append({
                        "date": date_str,
                        "source": source,
                        "type": "speed_map_data",
                        "content": text[:500],
                        "classes_css": classes,
                        "scraped_at": datetime.now().isoformat(),
                    })
    return records


def scrape_form_guide(page, date_str):
    """Scrape Punters.com.au form guide for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"form_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/form-guide/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, "punters_au"))
    records.extend(extract_data_attributes(soup, date_str, "punters_au"))
    records.extend(extract_comments(soup, date_str, "punters_au"))
    records.extend(extract_track_conditions_detail(soup, date_str, "punters_au"))
    records.extend(extract_speed_maps(soup, date_str, "punters_au"))

    # Extract meeting/track sections
    for section in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "race-card", "form-guide", "track"]):
            track_name = ""
            h_tag = section.find(["h2", "h3", "h4"])
            if h_tag:
                track_name = h_tag.get_text(strip=True)

            for link in section.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True)
                if ("race" in href.lower() or "form" in href.lower()) and text and len(text) > 2:
                    records.append({
                        "date": date_str,
                        "source": "punters_au",
                        "type": "race_link",
                        "track": track_name,
                        "text": text,
                        "url": href if href.startswith("http") else BASE_URL + href,
                        "scraped_at": datetime.now().isoformat(),
                    })

    # Extract form guide tables
    for table in soup.find_all("table"):
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
                "source": "punters_au",
                "type": "form_entry",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(page, date_str):
    """Scrape Punters.com.au results for a given date."""
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

    records.extend(extract_embedded_json(soup, date_str, "punters_au"))
    records.extend(extract_data_attributes(soup, date_str, "punters_au"))
    records.extend(extract_comments(soup, date_str, "punters_au"))
    records.extend(extract_track_conditions_detail(soup, date_str, "punters_au"))

    # Extract result tables
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
                "source": "punters_au",
                "type": "result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract track condition info
    for div in soup.find_all(["div", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["track-condition", "rail", "weather", "going"]):
            text = div.get_text(strip=True)
            if text and len(text) > 1:
                records.append({
                    "date": date_str,
                    "source": "punters_au",
                    "type": "track_condition",
                    "value": text,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_tips(page, date_str):
    """Scrape Punters.com.au tips — expert picks and consensus."""
    cache_file = os.path.join(CACHE_DIR, f"tips_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/tips/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str, "punters_au"))
    records.extend(extract_data_attributes(soup, date_str, "punters_au"))
    records.extend(extract_comments(soup, date_str, "punters_au"))

    # Extract tip sections
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["tip", "pick", "selection", "best-bet"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 1000:
                horse = ""
                race = ""
                h_tag = div.find(["h3", "h4", "h5", "strong"])
                if h_tag:
                    horse = h_tag.get_text(strip=True)
                race_tag = div.find(["span", "small"], class_=lambda c: c and "race" in " ".join(c).lower() if c else False)
                if race_tag:
                    race = race_tag.get_text(strip=True)

                records.append({
                    "date": date_str,
                    "source": "punters_au",
                    "type": "tip",
                    "horse": horse,
                    "race": race,
                    "content": text[:500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract tips from tables
    for table in soup.find_all("table"):
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
                "source": "punters_au",
                "type": "tip_table",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 64 — Punters.com.au Scraper (AU race data, form, tips) [Playwright]")
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
    log.info("SCRIPT 64 — Punters.com.au Scraper (Australian Racing) [Playwright]")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "punters_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = launch_browser(pw)
    try:
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Scrape form guide
            form_records = scrape_form_guide(page, date_str)
            if form_records:
                for rec in form_records:
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

            # Scrape tips
            tip_records = scrape_tips(page, date_str)
            if tip_records:
                for rec in tip_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

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
