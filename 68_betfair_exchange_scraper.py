#!/usr/bin/env python3
"""
Script 68 — Scraping Betfair Exchange API (Global Betting Exchange)
Source : betfair.com (Exchange API / public pages)
Collecte : odds, volume, market data, price movements, liquidity
CRITIQUE pour : Market Odds Model, Volume Analysis, Price Discovery
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "68_betfair_exchange"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint, create_session

log = setup_logging("68_betfair_exchange")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.betfair.com"
EXCHANGE_URL = f"{BASE_URL}/exchange/horse-racing"
# Betfair public API endpoint for market data
SITELOGIN_URL = f"{BASE_URL}/exchange/plus/"



# NOTE: Not migrated to utils.scraping.fetch_with_retry because this version
# supports headers_extra param and has Betfair-specific 429/403 handling
# (60s*attempt for 429, 90s wait for 403) unlike the generic util.
def fetch_with_retry(session, url, max_retries=3, timeout=30, params=None, headers_extra=None):
    """GET with automatic retry (3 attempts then skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            kwargs = {"timeout": timeout}
            if params:
                kwargs["params"] = params
            if headers_extra:
                kwargs["headers"] = headers_extra
            resp = session.get(url, **kwargs)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Too Many Requests, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden on {url}, waiting 90s...")
                time.sleep(90)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} on {url} (attempt {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Network error: {e} (attempt {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Failed after {max_retries} attempts: {url}")
    return None


def post_with_retry(session, url, json_data=None, max_retries=3, timeout=30):
    """POST with automatic retry (3 attempts then skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.post(url, json=json_data, timeout=timeout)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Too Many Requests, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden on {url}, waiting 90s...")
                time.sleep(90)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} on {url} (attempt {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Network error: {e} (attempt {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Failed after {max_retries} attempts: {url}")
    return None





def extract_embedded_json(soup, date_str, source="betfair_exchange"):
    """Extract all embedded JSON from script tags (market data, runner data)."""
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
        for kw in ["market", "runner", "selection", "odds", "price", "volume",
                    "matched", "traded", "back", "lay", "exchange", "depth"]:
            if kw in script_text.lower():
                json_matches = re.findall(r'\{[^{}]{30,}\}', script_text)
                for jm in json_matches[:20]:
                    try:
                        data = json.loads(jm)
                        if any(k in str(data).lower() for k in ["market", "runner", "odds",
                                                                   "price", "matched", "volume"]):
                            records.append({
                                "date": date_str,
                                "source": source,
                                "type": "embedded_market_json",
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
                                "type": "embedded_market_array",
                                "data": data[:50],
                                "scraped_at": datetime.now().isoformat(),
                            })
                    except json.JSONDecodeError:
                        pass
                break
    return records


def extract_data_attributes(soup, date_str, source="betfair_exchange"):
    """Extract all data-* attributes from DOM elements (market IDs, runner IDs, etc.)."""
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


def extract_market_depth(soup, date_str, source="betfair_exchange"):
    """Extract market depth data (multiple back/lay price levels)."""
    records = []
    for el in soup.find_all(["div", "td", "span", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["depth", "ladder", "price-level",
                                                   "back-price", "lay-price",
                                                   "best-price", "available"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d', text) and len(text) < 300:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "market_depth",
                    "content": text,
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse price/size pairs
                prices = re.findall(r'(\d+\.?\d*)', text)
                if prices:
                    record["prices_parsed"] = [float(p) for p in prices[:10]]
                records.append(record)
    return records


def extract_volume_timeline(soup, date_str, source="betfair_exchange"):
    """Extract volume timeline and traded price data."""
    records = []
    for el in soup.find_all(["div", "section", "span"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["volume", "traded", "turnover",
                                                   "matched-amount", "money-matched",
                                                   "total-matched", "timeline"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d', text) and len(text) < 500:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "volume_timeline",
                    "content": text[:300],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse monetary amounts
                amounts = re.findall(r'[\$\xa3\u20ac]?([\d,]+\.?\d*)', text.replace(",", ""))
                if amounts:
                    record["amounts_parsed"] = [float(a) for a in amounts[:10] if float(a) > 0]
                records.append(record)
    return records


def extract_traded_prices(soup, date_str, source="betfair_exchange"):
    """Extract traded price history data."""
    records = []
    for el in soup.find_all(["div", "table", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["traded-price", "price-history",
                                                   "wap", "last-price-traded",
                                                   "graph-data", "chart-data"]):
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
                            "type": "traded_price",
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                            record[key] = cell
                        records.append(record)
            else:
                text = el.get_text(strip=True)
                if text and 3 < len(text) < 500:
                    record = {
                        "date": date_str,
                        "source": source,
                        "type": "traded_price_data",
                        "content": text,
                        "classes_css": classes,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    prices = re.findall(r'(\d+\.?\d*)', text)
                    if prices:
                        record["prices"] = [float(p) for p in prices[:20]]
                    records.append(record)
    return records


def scrape_exchange_markets(session, date_str):
    """Scrape Betfair Exchange horse racing market listings."""
    cache_file = os.path.join(CACHE_DIR, f"markets_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{EXCHANGE_URL}?action=showTimeForm&date={date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        # Fallback to main exchange page
        resp = fetch_with_retry(session, EXCHANGE_URL)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction pattern ---
    records.extend(extract_embedded_json(soup, date_str, "betfair_exchange"))
    records.extend(extract_data_attributes(soup, date_str, "betfair_exchange"))
    records.extend(extract_market_depth(soup, date_str, "betfair_exchange"))
    records.extend(extract_volume_timeline(soup, date_str, "betfair_exchange"))
    records.extend(extract_traded_prices(soup, date_str, "betfair_exchange"))

    # Extract market links (race events)
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if ("horse-racing" in href or "market" in href.lower()) and text and len(text) > 2:
            records.append({
                "date": date_str,
                "source": "betfair_exchange",
                "type": "market_link",
                "text": text,
                "url": href if href.startswith("http") else BASE_URL + href,
                "scraped_at": datetime.now().isoformat(),
            })

    # Extract meeting/race sections
    for section in soup.find_all(["div", "section", "li"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["market", "event", "race", "coupon"]):
            event_name = ""
            h_tag = section.find(["h2", "h3", "h4", "span"])
            if h_tag:
                event_name = h_tag.get_text(strip=True)

            # Extract time
            time_el = section.find(["span", "time"], class_=lambda c: c and any(
                kw in " ".join(c).lower() for kw in ["time", "start", "schedule"]
            ) if c else False)
            race_time = time_el.get_text(strip=True) if time_el else ""

            if event_name:
                records.append({
                    "date": date_str,
                    "source": "betfair_exchange",
                    "type": "market_event",
                    "event": event_name,
                    "race_time": race_time,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_market_odds(session, date_str):
    """Scrape Betfair Exchange odds / prices for horse racing markets."""
    cache_file = os.path.join(CACHE_DIR, f"odds_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{EXCHANGE_URL}?action=showMarkets&date={date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on odds page ---
    records.extend(extract_embedded_json(soup, date_str, "betfair_exchange"))
    records.extend(extract_data_attributes(soup, date_str, "betfair_exchange"))
    records.extend(extract_market_depth(soup, date_str, "betfair_exchange"))
    records.extend(extract_traded_prices(soup, date_str, "betfair_exchange"))

    # Extract odds tables
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
                "source": "betfair_exchange",
                "type": "odds",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Parse odds value
                odds_match = re.search(r'(\d+\.?\d*)', cell)
                if odds_match and j > 0:
                    record[f"odds_{j}_parsed"] = float(odds_match.group(1))

            records.append(record)

    # Extract odds from div-based runner cards
    for div in soup.find_all(["div", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["runner", "selection", "outcome"]):
            runner_name = ""
            back_odds = ""
            lay_odds = ""
            matched_amount = ""

            name_el = div.find(["a", "span", "h4"], class_=lambda c: c and any(
                kw in " ".join(c).lower() for kw in ["name", "runner", "selection"]
            ) if c else False)
            if name_el:
                runner_name = name_el.get_text(strip=True)

            # Extract back/lay odds
            for btn in div.find_all(["button", "span", "td"], class_=True):
                bc = " ".join(btn.get("class", []))
                txt = btn.get_text(strip=True)
                if "back" in bc.lower() and re.search(r'\d+\.?\d*', txt):
                    back_odds = txt
                elif "lay" in bc.lower() and re.search(r'\d+\.?\d*', txt):
                    lay_odds = txt
                elif any(kw in bc.lower() for kw in ["matched", "volume", "traded"]):
                    matched_amount = txt

            if runner_name:
                record = {
                    "date": date_str,
                    "source": "betfair_exchange",
                    "type": "runner_odds",
                    "runner": runner_name,
                    "back_odds": back_odds,
                    "lay_odds": lay_odds,
                    "matched_amount": matched_amount,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Parse numeric odds
                for field, val in [("back_odds", back_odds), ("lay_odds", lay_odds)]:
                    m = re.search(r'(\d+\.?\d*)', val)
                    if m:
                        record[f"{field}_parsed"] = float(m.group(1))

                # Parse matched amount
                vol_match = re.search(r'[\$\xa3]?([\d,]+\.?\d*)', matched_amount.replace(",", ""))
                if vol_match:
                    record["volume_parsed"] = float(vol_match.group(1))

                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_market_volume(session, date_str):
    """Scrape Betfair Exchange volume and market depth data."""
    cache_file = os.path.join(CACHE_DIR, f"volume_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{EXCHANGE_URL}?action=showVolume&date={date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on volume page ---
    records.extend(extract_embedded_json(soup, date_str, "betfair_exchange"))
    records.extend(extract_data_attributes(soup, date_str, "betfair_exchange"))
    records.extend(extract_volume_timeline(soup, date_str, "betfair_exchange"))
    records.extend(extract_traded_prices(soup, date_str, "betfair_exchange"))

    # Extract volume/liquidity data from market sections
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["market", "volume", "matched", "liquidity"]):
            text = div.get_text(strip=True)
            if text and re.search(r'\d', text) and len(text) < 500:
                # Extract market name
                market_name = ""
                h_tag = div.find(["h3", "h4", "a"])
                if h_tag:
                    market_name = h_tag.get_text(strip=True)

                # Extract total matched
                vol_match = re.search(r'[\$\xa3]?([\d,]+\.?\d*)', text.replace(",", ""))
                volume = float(vol_match.group(1)) if vol_match else 0

                records.append({
                    "date": date_str,
                    "source": "betfair_exchange",
                    "type": "market_volume",
                    "market": market_name,
                    "content": text[:300],
                    "volume_parsed": volume,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract data from script tags (Betfair often embeds JSON market data)
    for script in soup.find_all("script"):
        script_text = script.string or ""
        if any(kw in script_text.lower() for kw in ["marketid", "runnerid", "exchangemarket", "totalmatched"]):
            # Try to extract JSON data
            json_matches = re.findall(r'\{[^{}]{30,}\}', script_text)
            for jm in json_matches[:20]:
                try:
                    data = json.loads(jm)
                    if any(k in str(data).lower() for k in ["market", "runner", "odds", "matched"]):
                        records.append({
                            "date": date_str,
                            "source": "betfair_exchange",
                            "type": "embedded_market_data",
                            "data": data,
                            "scraped_at": datetime.now().isoformat(),
                        })
                except json.JSONDecodeError:
                    pass

            # Try to extract larger JSON arrays
            array_matches = re.findall(r'\[[^\[\]]{30,}\]', script_text)
            for am in array_matches[:10]:
                try:
                    data = json.loads(am)
                    if isinstance(data, list) and len(data) > 0:
                        records.append({
                            "date": date_str,
                            "source": "betfair_exchange",
                            "type": "embedded_array_data",
                            "data": data[:50],  # Limit array size
                            "scraped_at": datetime.now().isoformat(),
                        })
                except json.JSONDecodeError:
                    pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 68 — Betfair Exchange Scraper (odds, volume, market data)")
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
    log.info("SCRIPT 68 — Betfair Exchange Scraper (Global Odds & Volume)")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "betfair_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape market listings
        market_records = scrape_exchange_markets(session, date_str)
        if market_records:
            for rec in market_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(3.0, 1.5)

        # Scrape odds
        odds_records = scrape_market_odds(session, date_str)
        if odds_records:
            for rec in odds_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(3.0, 1.5)

        # Scrape volume data
        volume_records = scrape_market_volume(session, date_str)
        if volume_records:
            for rec in volume_records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        if day_count % 60 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(10, 25))

        current += timedelta(days=1)
        smart_pause(1.5, 0.8)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
