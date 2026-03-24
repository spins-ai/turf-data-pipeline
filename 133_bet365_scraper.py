#!/usr/bin/env python3
"""
Script 133 — Bet365 Horse Racing Scraper (Playwright)
Source : bet365.com
Collecte : horse racing odds, markets, ante-post prices
URL patterns :
  /#/AC/B3/C1/...  -> horse racing section (SPA, hash-based routes)
  /sports/horse-racing/  -> alternative horse racing entry point
CRITIQUE pour : Betting Odds, Market Depth, Ante-Post Prices

NOTE: Bet365 is heavily anti-bot. This scraper includes extra stealth
measures: randomised delays, human-like scrolling, mouse movements,
viewport jitter, and session rotation.

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import math
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "133_bet365"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.playwright import launch_browser, accept_cookies

log = setup_logging("133_bet365")

BASE_URL = "https://www.bet365.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 90_000

# Bet365-specific user agents (recent Chrome on Windows)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Enhanced stealth init-script for Bet365
_STEALTH_SCRIPT_BET365 = """
    // Hide webdriver flag
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    // Realistic languages
    Object.defineProperty(navigator, 'languages', {get: () => ['en-GB', 'en', 'fr']});
    // Fake plugins array
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const arr = [
                {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer'},
                {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
                {name: 'Native Client', filename: 'internal-nacl-plugin'},
            ];
            arr.length = 3;
            return arr;
        }
    });
    // Chrome runtime object
    window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}};
    // Hide automation indicators
    delete navigator.__proto__.webdriver;
    // Realistic screen properties
    Object.defineProperty(screen, 'colorDepth', {get: () => 24});
    Object.defineProperty(screen, 'pixelDepth', {get: () => 24});
    // WebGL vendor spoofing
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) return 'Intel Inc.';
        if (parameter === 37446) return 'Intel Iris OpenGL Engine';
        return getParameter.call(this, parameter);
    };
    // Notification permission
    const originalQuery = window.Notification && Notification.permission;
    if (originalQuery === 'denied') {
        Object.defineProperty(Notification, 'permission', {get: () => 'default'});
    }
"""


# ------------------------------------------------------------------
# Anti-detection: human-like behaviours
# ------------------------------------------------------------------

def human_like_scroll(page, direction="down", distance=None):
    """Scroll the page in a human-like way with variable speed."""
    if distance is None:
        distance = random.randint(200, 600)
    steps = random.randint(3, 8)
    step_distance = distance // steps
    for _ in range(steps):
        delta = step_distance + random.randint(-30, 30)
        if direction == "up":
            delta = -abs(delta)
        page.mouse.wheel(0, delta)
        time.sleep(random.uniform(0.05, 0.20))


def random_mouse_movement(page):
    """Move the mouse cursor to a random position to mimic human behaviour."""
    try:
        x = random.randint(100, 1600)
        y = random.randint(100, 800)
        page.mouse.move(x, y, steps=random.randint(5, 15))
        time.sleep(random.uniform(0.1, 0.4))
    except Exception:
        pass


def anti_bot_delay():
    """Random delay that mimics human reading / decision time."""
    base = random.uniform(2.0, 5.0)
    # Occasionally take a longer "thinking" pause
    if random.random() < 0.12:
        base += random.uniform(5.0, 15.0)
    time.sleep(base)


def stealth_pause():
    """Short random pause between actions."""
    time.sleep(random.uniform(0.3, 1.5))


# ------------------------------------------------------------------
# Browser launch with extra stealth
# ------------------------------------------------------------------

def launch_stealth_browser(pw):
    """Launch browser with enhanced stealth for Bet365."""
    ua = random.choice(USER_AGENTS)
    # Randomise viewport slightly
    vw = 1920 + random.randint(-100, 100)
    vh = 1080 + random.randint(-50, 50)

    browser, context, page = launch_browser(
        pw,
        locale="en-GB",
        timezone="Europe/London",
        user_agent=ua,
        viewport_width=vw,
        viewport_height=vh,
        default_timeout_ms=DEFAULT_TIMEOUT_MS,
    )
    # Inject enhanced stealth script
    context.add_init_script(_STEALTH_SCRIPT_BET365)
    log.info("Stealth browser launched (UA=%s..., viewport=%dx%d)", ua[:50], vw, vh)
    return browser, context, page


# ------------------------------------------------------------------
# Navigation with retry + anti-detection
# ------------------------------------------------------------------

def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to url with retry logic and anti-bot mitigations."""
    for attempt in range(1, retries + 1):
        try:
            random_mouse_movement(page)
            stealth_pause()

            resp = page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
            if resp and resp.status >= 400:
                log.warning("  HTTP %d on %s (attempt %d/%d)",
                            resp.status, url, attempt, retries)
                if resp.status == 429:
                    time.sleep(60 * attempt + random.uniform(10, 30))
                elif resp.status == 403:
                    time.sleep(30 * attempt + random.uniform(5, 15))
                else:
                    time.sleep(5 * attempt + random.uniform(2, 8))
                continue

            # Wait for SPA content to render
            time.sleep(random.uniform(3.0, 6.0))

            # Human-like scroll to trigger lazy loading
            human_like_scroll(page, "down", random.randint(200, 400))
            stealth_pause()
            human_like_scroll(page, "up", random.randint(100, 200))
            stealth_pause()

            return page.content()
        except PlaywrightTimeout:
            log.warning("  Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(10 * attempt + random.uniform(5, 15))
        except Exception as exc:
            log.warning("  Navigation error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt + random.uniform(2, 8))
    log.error("  Failed after %d retries: %s", retries, url)
    return None


# ------------------------------------------------------------------
# Extraction helpers
# ------------------------------------------------------------------

def extract_racing_events(soup):
    """Extract horse racing event/meeting links from the racing section."""
    events = []
    for el in soup.find_all(["div", "a", "span", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in [
            "event", "meeting", "fixture", "coupon", "race-card",
            "participant", "gl-market", "rcl-participant",
        ]):
            href = el.get("href", "") if el.name == "a" else ""
            if text and len(text) > 2:
                events.append({
                    "text": text[:500],
                    "href": href,
                    "classes": classes,
                })
    return events


def extract_odds_data(soup, date_str, market_type="win"):
    """Extract odds from the page — Bet365 renders odds in specific class patterns."""
    records = []
    # Look for odds containers (Bet365 uses custom class names)
    for el in soup.find_all(["div", "span", "button", "a"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if any(kw in classes.lower() for kw in [
            "odds", "price", "gl-participant", "sgl-participant",
            "betbutton", "bet-btn", "rcl-price", "odds-btn",
            "gl-market__content", "participant-odds",
        ]):
            if text and re.match(r'^[\d/.]+$|^EVS$|^SP$', text.strip()):
                records.append({
                    "date": date_str,
                    "source": "bet365",
                    "type": "odds",
                    "market_type": market_type,
                    "odds_text": text.strip(),
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                })
    return records


def extract_market_data(soup, date_str):
    """Extract full market blocks (runner name + odds pairs)."""
    records = []
    # Bet365 uses participant rows with runner name + price
    for row in soup.find_all(["div", "tr", "li"], class_=True):
        classes = " ".join(row.get("class", []))
        if any(kw in classes.lower() for kw in [
            "participant", "runner", "gl-participant", "rcl-participant",
            "selection", "market-row", "coupon-row",
        ]):
            runner_name = ""
            odds_text = ""
            # Find runner name
            for name_el in row.find_all(["span", "div", "a"], class_=True):
                name_classes = " ".join(name_el.get("class", []))
                if any(k in name_classes.lower() for k in [
                    "name", "participant-name", "runner-name", "label",
                    "gl-participant__name", "rcl-participantname",
                ]):
                    runner_name = name_el.get_text(strip=True)
                    break
            if not runner_name:
                # Fallback: first meaningful text child
                for child in row.children:
                    t = child.get_text(strip=True) if hasattr(child, "get_text") else str(child).strip()
                    if t and len(t) > 1 and not re.match(r'^[\d/.]+$', t):
                        runner_name = t[:200]
                        break
            # Find odds
            for odds_el in row.find_all(["span", "div", "button", "a"], class_=True):
                odds_classes = " ".join(odds_el.get("class", []))
                if any(k in odds_classes.lower() for k in [
                    "odds", "price", "betbutton", "bet-btn",
                    "gl-participant__odds", "rcl-price",
                ]):
                    odds_text = odds_el.get_text(strip=True)
                    break
            if runner_name or odds_text:
                record = {
                    "date": date_str,
                    "source": "bet365",
                    "type": "market_runner",
                    "runner_name": runner_name[:200],
                    "odds_text": odds_text,
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Data attributes
                for attr_name, attr_val in row.attrs.items():
                    if attr_name.startswith("data-"):
                        clean = attr_name.replace("data-", "").replace("-", "_")
                        record[clean] = attr_val
                records.append(record)
    return records


def extract_ante_post_markets(soup, date_str):
    """Extract ante-post / future market data."""
    records = []
    page_text = soup.get_text()
    # Look for ante-post indicators
    if not any(kw in page_text.lower() for kw in [
        "ante-post", "antepost", "ante post", "future", "outright",
    ]):
        return records

    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in [
            "antepost", "ante-post", "outright", "future", "coupon",
            "market-group", "gl-market",
        ]):
            text = section.get_text(strip=True)
            if text and 10 < len(text) < 5000:
                # Try to extract race name from heading
                race_name = ""
                heading = section.find(["h1", "h2", "h3", "h4", "strong"])
                if heading:
                    race_name = heading.get_text(strip=True)

                record = {
                    "date": date_str,
                    "source": "bet365",
                    "type": "ante_post",
                    "race_name": race_name[:300],
                    "contenu": text[:3000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)

                # Also extract individual runners from within
                records.extend(extract_market_data(section, date_str))
    return records


def extract_embedded_json(soup, date_str):
    """Extract JSON data from script tags."""
    records = []
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, (dict, list)):
                records.append({
                    "date": date_str,
                    "source": "bet365",
                    "type": "embedded_json",
                    "data_id": script.get("id", ""),
                    "data": data if isinstance(data, dict) else {"items": data},
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_racing_section(page, date_str):
    """Navigate to the Bet365 horse racing section and scrape available markets."""
    cache_file = os.path.join(CACHE_DIR, f"racing_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Bet365 horse racing URL
    url = f"{BASE_URL}/#/AC/B3/C1/D1002/E2/F2/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML
    html_file = os.path.join(HTML_CACHE_DIR, f"racing_{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_odds_data(soup, date_str, market_type="win"))
    records.extend(extract_market_data(soup, date_str))

    # Extract event/meeting list
    events = extract_racing_events(soup)
    for ev in events:
        records.append({
            "date": date_str,
            "source": "bet365",
            "type": "racing_event",
            "event_text": ev["text"][:500],
            "event_href": ev.get("href", ""),
            "classes_css": ev.get("classes", ""),
            "scraped_at": datetime.now().isoformat(),
        })

    result = {"records": records, "events": events}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_ante_post(page, date_str):
    """Scrape the ante-post / futures horse racing markets."""
    cache_file = os.path.join(CACHE_DIR, f"antepost_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Ante-post section (typical Bet365 SPA route fragment)
    url = f"{BASE_URL}/#/AC/B3/C1/D1002/E3/F2/"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    records.extend(extract_ante_post_markets(soup, date_str))
    records.extend(extract_embedded_json(soup, date_str))

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_race_card(page, event_href, date_str):
    """Scrape an individual race card page for detailed odds."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', event_href[-80:])
    cache_file = os.path.join(CACHE_DIR, f"race_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = event_href if event_href.startswith("http") else f"{BASE_URL}{event_href}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2", "h3"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    records.extend(extract_embedded_json(soup, date_str))
    records.extend(extract_market_data(soup, date_str))
    records.extend(extract_odds_data(soup, date_str))

    # Tag all records with race name
    for rec in records:
        rec["race_name"] = race_name[:300]

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Script 133 — Bet365 Horse Racing Odds Scraper (anti-bot stealth)"
    )
    parser.add_argument("--start", type=str, default=None,
                        help="Start date (YYYY-MM-DD), default=today")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=today")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    parser.add_argument("--max-days", type=int, default=0,
                        help="Max days to scrape (0=unlimited)")
    parser.add_argument("--ante-post", action="store_true", default=False,
                        help="Also scrape ante-post / futures markets")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    start_date = datetime.strptime(args.start or today, "%Y-%m-%d")
    end_date = datetime.strptime(args.end or today, "%Y-%m-%d")

    log.info("=" * 60)
    log.info("SCRIPT 133 — Bet365 Horse Racing Scraper (Playwright + Stealth)")
    log.info("  Period: %s -> %s", start_date.date(), end_date.date())
    log.info("  Ante-post: %s", args.ante_post)
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date <= end_date:
            start_date = resume_date
            log.info("  Resuming from checkpoint: %s", start_date.date())

    output_file = os.path.join(OUTPUT_DIR, "bet365_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_stealth_browser(pw)

        first_nav = True
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            if args.max_days and day_count >= args.max_days:
                break

            date_str = current.strftime("%Y-%m-%d")
            log.info("Scraping %s ...", date_str)

            # Main racing section
            result = scrape_racing_section(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scrape individual race cards (limit to avoid detection)
                events_with_href = [
                    e for e in result.get("events", []) if e.get("href")
                ]
                for event in events_with_href[:8]:
                    anti_bot_delay()
                    random_mouse_movement(page)
                    detail = scrape_race_card(page, event["href"], date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(3.0, 1.5)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Ante-post markets
            if args.ante_post:
                anti_bot_delay()
                ap_data = scrape_ante_post(page, date_str)
                if ap_data:
                    for rec in (ap_data if isinstance(ap_data, list) else []):
                        append_jsonl(output_file, rec)
                        total_records += 1

            day_count += 1

            if day_count % 5 == 0:
                log.info("  %s | days=%d records=%d", date_str, day_count, total_records)
                save_checkpoint(CHECKPOINT_FILE, {
                    "last_date": date_str,
                    "total_records": total_records,
                })

            current += timedelta(days=1)
            # Longer pause between days to avoid detection
            smart_pause(5.0, 3.0)

        save_checkpoint(CHECKPOINT_FILE, {
            "last_date": end_date.strftime("%Y-%m-%d"),
            "total_records": total_records,
            "status": "done",
        })

        log.info("=" * 60)
        log.info("DONE: %d days, %d records -> %s", day_count, total_records, output_file)
        log.info("=" * 60)

    finally:
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
