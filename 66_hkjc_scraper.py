#!/usr/bin/env python3
"""
Script 66 — Scraping racing.hkjc.com (Playwright version)
Source : racing.hkjc.com
Collecte : sectional times, GPS tracking, results, race cards, dividends
CRITIQUE pour : HK Sectionals, GPS Data, Race Analysis, Pace Model

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
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
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "66_hkjc"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("66_hkjc")

MAX_RETRIES = 3

BASE_URL = "https://racing.hkjc.com"
RESULTS_URL = f"{BASE_URL}/racing/information/English/Racing/LocalResults.aspx"
ENTRIES_URL = f"{BASE_URL}/racing/information/English/Racing/RaceCard.aspx"
SECTIONALS_URL = f"{BASE_URL}/racing/information/English/Racing/SectionalTime.aspx"
RUNNING_POS_URL = f"{BASE_URL}/racing/information/English/Racing/RunningPosition.aspx"
HORSE_URL = f"{BASE_URL}/racing/information/English/Horse/Horse.aspx"
RACE_REPLAY_URL = f"{BASE_URL}/racing/information/English/Racing/RaceReplay.aspx"
HKJC_API_BASE = "https://racing.hkjc.com/racing/information/english/racing"








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


def navigate_with_params(page, base_url, params, retries=MAX_RETRIES):
    """Navigate to url with query params. Returns HTML string or None."""
    from urllib.parse import urlencode
    url = f"{base_url}?{urlencode(params)}"
    return navigate_with_retry(page, url, retries)


# ------------------------------------------------------------------
# Scraping functions (BeautifulSoup-based, fed from page.content())
# ------------------------------------------------------------------

def scrape_race_card(page, date_str):
    """Scrape HKJC race card for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecard_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # HKJC uses DD/MM/YYYY format
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    html = navigate_with_params(page, ENTRIES_URL, {"RaceDate": hkjc_date})
    if not html:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract race links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if "RaceCard" in href and text and len(text) > 1:
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "race_link",
                "text": text,
                "url": href if href.startswith("http") else BASE_URL + href,
                "scraped_at": datetime.now().isoformat(),
            })

    # Extract race card tables
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
                "source": "hkjc",
                "type": "race_card_entry",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # --- Extract embedded JSON/JavaScript data from race card ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        # Look for race data objects
        for m in re.finditer(r'(?:var|let|const)\s+(\w*(?:race|card|entry|horse|runner)\w*)\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL | re.IGNORECASE):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "racecard_embedded_var",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        # JSON.parse patterns
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "racecard_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "racecard_script_json",
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Extract all data-attributes (HKJC uses data-* extensively) ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k.lower() for kw in
            ["horse", "race", "runner", "jockey", "trainer", "weight", "draw", "odds", "no"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "racecard_data_attrs",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(page, date_str):
    """Scrape HKJC race results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    html = navigate_with_params(page, RESULTS_URL, {"RaceDate": hkjc_date})
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
                "source": "hkjc",
                "type": "result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse finish time
                time_match = re.search(r'(\d+:\d+\.\d+)', cell)
                if time_match:
                    record["finish_time_parsed"] = time_match.group(1)

            records.append(record)

    # Extract dividend/payout info
    for div in soup.find_all(["div", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["dividend", "payout", "pool"]):
            text = div.get_text(strip=True)
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "dividend",
                    "content": text[:1000],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extract race comments / stewards reports ---
    for div in soup.find_all(["div", "p", "td", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["comment", "steward", "report", "race-remark",
                                                  "incident", "inquiry", "running-comment"]):
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "race_comment",
                    "content": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extract running positions from results page ---
    for table in soup.find_all("table"):
        table_text = table.get_text().lower()
        if any(kw in table_text for kw in ["running position", "running pos", "1st sec", "2nd sec"]):
            rows = table.find_all("tr")
            rp_headers = []
            if rows:
                rp_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                              for th in rows[0].find_all(["th", "td"])]
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if cells and len(cells) >= 2:
                    entry = {
                        "date": date_str,
                        "source": "hkjc",
                        "type": "running_position",
                        "scraped_at": datetime.now().isoformat(),
                    }
                    for j, cell in enumerate(cells):
                        key = rp_headers[j] if j < len(rp_headers) and rp_headers[j] else f"col_{j}"
                        entry[key] = cell
                    records.append(entry)

    # --- Embedded JSON from results page ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w*(?:result|dividend|position|pool|race)\w*)\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL | re.IGNORECASE):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "result_embedded_var",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "result_script_json",
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_sectionals(page, date_str):
    """Scrape HKJC sectional times and GPS data."""
    cache_file = os.path.join(CACHE_DIR, f"sectionals_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    html = navigate_with_params(page, SECTIONALS_URL, {"RaceDate": hkjc_date})
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract sectional time tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Detect if this is a sectional table
        header_text = " ".join(headers).lower()
        is_sectional = any(kw in header_text for kw in ["sectional", "section", "200m", "400m", "furlong"])

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "date": date_str,
                "source": "hkjc",
                "type": "sectional_time" if is_sectional else "timing_data",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse sectional time values
                sec_match = re.search(r'(\d+\.\d{1,2})', cell)
                if sec_match and j > 0:
                    record[f"sec_{j}_parsed"] = float(sec_match.group(1))

            records.append(record)

    # Extract GPS/tracking data elements
    for el in soup.find_all(["div", "span", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["gps", "tracking", "position", "sectional"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d', text):
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "gps_data",
                    "value": text,
                    "classes": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract race replay / running position data
    for script in soup.find_all("script"):
        script_text = script.string or ""
        if any(kw in script_text.lower() for kw in ["runposition", "gps", "sectiontime", "trackingdata"]):
            json_matches = re.findall(r'\{[^{}]{20,}\}', script_text)
            for jm in json_matches[:10]:
                try:
                    data = json.loads(jm)
                    records.append({
                        "date": date_str,
                        "source": "hkjc",
                        "type": "embedded_data",
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass

    # --- Enhanced GPS/tracking data extraction from scripts ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(
            r'(?:var|let|const)\s+(\w*(?:gps|track|section|position|running|speed|distance)\w*)\s*=\s*(\{[\s\S]+?\}|\[[\s\S]+?\]);',
            script_text, re.IGNORECASE
        ):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "gps_tracking_var",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        # Look for large arrays of coordinate/position data
        for m in re.finditer(r'\[\s*\[\s*[\d.]+\s*,\s*[\d.]+(?:\s*,\s*[\d.]+)*\s*\](?:\s*,\s*\[[\d.,\s]+\]){5,}\s*\]',
                             script_text):
            try:
                data = json.loads(m.group(0))
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "gps_coordinate_array",
                    "num_points": len(data),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    # --- Extract script type="application/json" ---
    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "sectional_script_json",
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Data attributes on sectional elements ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k.lower() for kw in
            ["section", "time", "speed", "position", "gps", "horse", "split"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "sectional_data_attrs",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_running_positions(page, date_str):
    """Scrape HKJC running positions for all races on a given date."""
    cache_file = os.path.join(CACHE_DIR, f"runpos_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    html = navigate_with_params(page, RUNNING_POS_URL, {"RaceDate": hkjc_date})
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract running position tables
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
                "source": "hkjc",
                "type": "running_position_detail",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract embedded JS data for positions
    for script in soup.find_all("script"):
        script_text = script.string or ""
        if any(kw in script_text.lower() for kw in ["runposition", "running", "raceposition"]):
            for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\{[\s\S]+?\}|\[[\s\S]+?\]);', script_text):
                try:
                    data = json.loads(m.group(2))
                    records.append({
                        "date": date_str,
                        "source": "hkjc",
                        "type": "running_position_js",
                        "var_name": m.group(1),
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_race_replay_metadata(page, date_str):
    """Scrape HKJC race replay metadata (video URLs, thumbnails)."""
    cache_file = os.path.join(CACHE_DIR, f"replay_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hkjc_date = dt.strftime("%d/%m/%Y")

    html = navigate_with_params(page, RACE_REPLAY_URL, {"RaceDate": hkjc_date})
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Extract video/media elements
    for el in soup.find_all(["video", "source", "iframe", "a", "div"]):
        src = el.get("src") or el.get("data-src") or el.get("data-video") or el.get("href", "")
        if src and any(kw in src.lower() for kw in ["replay", "video", "stream", "mp4", "m3u8",
                                                      "rtmp", "hls", "media"]):
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "race_replay_url",
                "media_url": src,
                "media_tag": el.name,
                "poster": el.get("poster", el.get("data-poster", "")),
                "title": el.get("title", el.get_text(strip=True)[:100]),
                "scraped_at": datetime.now().isoformat(),
            })

    # Extract replay JS data
    for script in soup.find_all("script"):
        script_text = script.string or ""
        if any(kw in script_text.lower() for kw in ["replay", "video", "media", "stream"]):
            for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\{[\s\S]+?\}|\[[\s\S]+?\]);', script_text):
                try:
                    data = json.loads(m.group(2))
                    records.append({
                        "date": date_str,
                        "source": "hkjc",
                        "type": "replay_embedded_data",
                        "var_name": m.group(1),
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass
        # URL patterns for video streams
        for m in re.finditer(r'["\']((https?://[^"\']+\.(?:m3u8|mp4|flv))[^"\']*)["\']', script_text):
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "replay_stream_url",
                "media_url": m.group(1),
                "scraped_at": datetime.now().isoformat(),
            })

    # Extract image thumbnails for replays
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        alt = img.get("alt", "").lower()
        if any(kw in src.lower() or kw in alt for kw in ["replay", "race", "finish", "photo"]):
            records.append({
                "date": date_str,
                "source": "hkjc",
                "type": "replay_thumbnail",
                "image_url": src,
                "alt": img.get("alt", ""),
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_horse_form(page, horse_url, date_str):
    """Scrape full form history for a horse (last 10+ races)."""
    if not horse_url:
        return []
    if not horse_url.startswith("http"):
        horse_url = BASE_URL + horse_url

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', horse_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"horse_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, horse_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Horse name
    horse_name = ""
    h1 = soup.find("h1")
    if h1:
        horse_name = h1.get_text(strip=True)

    # Extract form tables (past performances)
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
                "source": "hkjc",
                "type": "horse_form_entry",
                "horse_name": horse_name,
                "horse_url": horse_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Stats by distance/track/going
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["stats", "record", "summary", "career"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "horse_stats_summary",
                    "horse_name": horse_name,
                    "content": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # Embedded JSON for horse
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w*(?:horse|form|perf|season|career)\w*)\s*=\s*(\{[\s\S]+?\}|\[[\s\S]+?\]);',
                             script_text, re.IGNORECASE):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "hkjc",
                    "type": "horse_embedded_data",
                    "horse_name": horse_name,
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 66 — HKJC Scraper (Playwright, HK sectionals, GPS, results)")
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
    log.info("SCRIPT 66 — HKJC Scraper (Playwright)")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "hkjc_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw, locale="en-HK", timezone="Asia/Hong_Kong")
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        current = start_date
        day_count = 0
        total_records = 0

        # HKJC races typically on Wed & Sun -- but scrape all days in case
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Scrape race card
            card_records = scrape_race_card(page, date_str)

            if first_nav and card_records is not None:
                accept_cookies(page)
                first_nav = False

            if card_records:
                for rec in card_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.5, 1.0)

            # Scrape results
            result_records = scrape_results(page, date_str)
            if result_records:
                for rec in result_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.5, 1.0)

            # Scrape sectional times
            sect_records = scrape_sectionals(page, date_str)
            if sect_records:
                for rec in sect_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.0, 1.0)

            # Scrape running positions
            runpos_records = scrape_running_positions(page, date_str)
            if runpos_records:
                for rec in runpos_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.0, 1.0)

            # Scrape race replay metadata
            replay_records = scrape_race_replay_metadata(page, date_str)
            if replay_records:
                for rec in replay_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            # Scrape horse form for horses found in race cards (limit per day)
            if card_records:
                horse_urls = set()
                for rec in card_records:
                    for key, val in rec.items():
                        if isinstance(val, str) and "Horse.aspx" in val:
                            horse_urls.add(val)
                    # Also look for horse links in data attributes
                    if rec.get("type") == "racecard_data_attrs":
                        attrs = rec.get("attributes", {})
                        for v in attrs.values():
                            if isinstance(v, str) and "Horse" in v:
                                horse_urls.add(v)

                for hurl in list(horse_urls)[:10]:  # Limit to 10 horses per day
                    horse_data = scrape_horse_form(page, hurl, date_str)
                    if horse_data:
                        for rec in horse_data:
                            append_jsonl(output_file, rec)
                            total_records += 1
                    smart_pause(2.0, 1.0)

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | days={day_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
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
