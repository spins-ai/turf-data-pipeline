#!/usr/bin/env python3
"""
Script 60 — Scraping Oddschecker (Odds Comparison)
Source : oddschecker.com/horse-racing
Collecte : odds comparison across bookmakers, market movers, best odds
CRITIQUE pour : Odds Model, Value Detection, Market Efficiency, Bookmaker Comparison
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests
try:
    import cloudscraper
except ImportError:
    cloudscraper = None
from bs4 import BeautifulSoup

SCRIPT_NAME = "60_oddschecker"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.oddschecker.com"


def new_session():
    s = cloudscraper.create_scraper() if cloudscraper else requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def smart_pause(base=3.0, jitter=2.0):
    """Oddschecker has aggressive rate limiting — use longer pauses."""
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.10:
        pause += random.uniform(8, 20)
    time.sleep(max(1.5, pause))


def fetch_with_retry(session, url, max_retries=3, timeout=30):
    """GET with automatic retry (3 attempts then skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                wait = 90 * attempt
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
        except Exception as e:
            log.warning(f"  Network error: {e} (attempt {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Failed after {max_retries} attempts: {url}")
    return None


def append_jsonl(filepath, record):
    """Append a JSONL record (append mode)."""
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    """Load resume checkpoint."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    """Save checkpoint."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape_meetings(session, date_str):
    """Scrape Oddschecker horse racing meetings list for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"meetings_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/horse-racing/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(resp.text)

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract race links (meetings and individual races)
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/horse-racing/" in href and href != f"/horse-racing/{date_str}":
            text = link.get_text(strip=True)
            if text and len(text) > 2:
                full_url = href if href.startswith("http") else BASE_URL + href
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "race_link",
                    "text": text,
                    "url": full_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract meeting sections
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["meeting", "event", "fixture", "race-card"]):
            title_el = section.find(["h2", "h3", "h4", "a"])
            if title_el:
                record = {
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "meeting",
                    "meeting_name": title_el.get_text(strip=True),
                    "scraped_at": datetime.now().isoformat(),
                }
                href = title_el.get("href")
                if href:
                    record["url"] = href if href.startswith("http") else BASE_URL + href
                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_race_odds(session, race_url, date_str):
    """Scrape odds comparison for a specific race from Oddschecker."""
    if not race_url.startswith("http"):
        race_url = BASE_URL + race_url

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"odds_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, race_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race time / conditions
    race_time = ""
    for el in soup.find_all(["span", "div", "time"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["time", "clock", "race-time"]):
            race_time = el.get_text(strip=True)
            break

    # Extract bookmaker headers from the odds table
    bookmaker_names = []
    for header_row in soup.find_all("tr", class_=lambda c: c and
                                    any(k in " ".join(c).lower() for k in ["header", "bookmaker", "bookie"])):
        for th in header_row.find_all(["th", "td"]):
            bk_name = th.get("data-bk", "") or th.get("title", "") or th.get_text(strip=True)
            if bk_name:
                bookmaker_names.append(bk_name)

    # Extract odds comparison table
    for table in soup.find_all("table"):
        table_classes = " ".join(table.get("class", []))
        rows = table.find_all("tr")
        if not rows:
            continue

        # Get bookmaker names from header row if not found above
        if not bookmaker_names and rows:
            header_cells = rows[0].find_all(["th", "td"])
            for cell in header_cells:
                bk = cell.get("data-bk", "") or cell.get("title", "") or cell.get_text(strip=True)
                if bk:
                    bookmaker_names.append(bk)

        for row in rows:
            row_classes = " ".join(row.get("class", []))
            # Skip header rows
            if "header" in row_classes.lower():
                continue

            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            # First cell is typically the horse name
            horse_name_el = row.find(["td", "th", "a", "span"],
                                     class_=lambda c: c and any(k in " ".join(c).lower()
                                                                for k in ["runner", "horse", "name", "sel"]))
            horse_name = ""
            if horse_name_el:
                horse_name = horse_name_el.get_text(strip=True)
            elif cells:
                horse_name = cells[0].get_text(strip=True)

            if not horse_name or len(horse_name) < 2:
                continue

            record = {
                "date": date_str,
                "source": "oddschecker",
                "type": "odds_comparison",
                "race_name": race_name,
                "race_time": race_time,
                "horse_name": horse_name,
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }

            # Extract odds for each bookmaker
            odds_by_bookmaker = {}
            best_odds = None
            best_odds_decimal = 0.0

            for idx, cell in enumerate(cells):
                odds_text = cell.get_text(strip=True)
                bk_name = cell.get("data-bk", "")

                if not bk_name and idx < len(bookmaker_names):
                    bk_name = bookmaker_names[idx]

                # Parse fractional odds (e.g., 5/1, 11/4, evens)
                frac_match = re.match(r'^(\d+)/(\d+)$', odds_text)
                evens_match = re.match(r'^evens?$', odds_text, re.IGNORECASE)
                decimal_match = re.match(r'^(\d+\.\d+)$', odds_text)

                odds_decimal = None
                if frac_match:
                    num, den = int(frac_match.group(1)), int(frac_match.group(2))
                    if den > 0:
                        odds_decimal = round(num / den + 1.0, 2)
                elif evens_match:
                    odds_decimal = 2.0
                elif decimal_match:
                    odds_decimal = float(decimal_match.group(1))

                if odds_decimal and bk_name:
                    odds_by_bookmaker[bk_name] = {
                        "fractional": odds_text,
                        "decimal": odds_decimal,
                    }
                    if odds_decimal > best_odds_decimal:
                        best_odds_decimal = odds_decimal
                        best_odds = {
                            "bookmaker": bk_name,
                            "fractional": odds_text,
                            "decimal": odds_decimal,
                        }

                # Also check data attributes for odds
                data_odds = cell.get("data-odig", "") or cell.get("data-odds", "")
                if data_odds and bk_name:
                    try:
                        dec_val = float(data_odds)
                        odds_by_bookmaker[bk_name] = {
                            "fractional": odds_text,
                            "decimal": dec_val,
                        }
                        if dec_val > best_odds_decimal:
                            best_odds_decimal = dec_val
                            best_odds = {
                                "bookmaker": bk_name,
                                "fractional": odds_text,
                                "decimal": dec_val,
                            }
                    except ValueError:
                        pass

            if odds_by_bookmaker:
                record["odds_by_bookmaker"] = odds_by_bookmaker
                record["num_bookmakers"] = len(odds_by_bookmaker)
                if best_odds:
                    record["best_odds"] = best_odds
                records.append(record)

    # Extract market movers section
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["mover", "market-mover", "steamer", "drifter"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "market_mover",
                    "race_name": race_name,
                    "content": text[:400],
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract data attributes from odds cells
    for el in soup.find_all(attrs={"data-odig": True}):
        parent_row = el.find_parent("tr")
        horse = ""
        if parent_row:
            name_el = parent_row.find(["td", "a"], class_=lambda c: c and
                                      any(k in " ".join(c).lower() for k in ["name", "runner", "sel"]))
            if name_el:
                horse = name_el.get_text(strip=True)

        records.append({
            "date": date_str,
            "source": "oddschecker",
            "type": "odds_data_attr",
            "race_name": race_name,
            "horse_name": horse,
            "odds_decimal": el.get("data-odig"),
            "odds_display": el.get_text(strip=True),
            "bookmaker": el.get("data-bk", ""),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- Historical odds movements timeline ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["odds-history", "price-history", "movement",
                                                  "timeline", "odds-graph", "chart-data"]):
            if div.name == "table":
                rows = div.find_all("tr")
                hist_headers = []
                if rows:
                    hist_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                                    for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        entry = {
                            "date": date_str,
                            "source": "oddschecker",
                            "type": "odds_history_entry",
                            "race_name": race_name,
                            "url": race_url,
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = hist_headers[j] if j < len(hist_headers) and hist_headers[j] else f"col_{j}"
                            entry[key] = cell
                        records.append(entry)
            else:
                text = div.get_text(strip=True)
                if text and 10 < len(text) < 3000:
                    records.append({
                        "date": date_str,
                        "source": "oddschecker",
                        "type": "odds_history_text",
                        "race_name": race_name,
                        "content": text[:2500],
                        "url": race_url,
                        "scraped_at": datetime.now().isoformat(),
                    })

    # --- Market percentage / overround calculation ---
    all_best_decimals = []
    for rec in records:
        if rec.get("type") == "odds_comparison" and rec.get("best_odds"):
            best_dec = rec["best_odds"].get("decimal")
            if best_dec and best_dec > 1.0:
                all_best_decimals.append(best_dec)
    if all_best_decimals:
        implied_probs = [1.0 / d for d in all_best_decimals]
        market_pct = sum(implied_probs) * 100
        records.append({
            "date": date_str,
            "source": "oddschecker",
            "type": "market_percentage",
            "race_name": race_name,
            "market_percentage": round(market_pct, 2),
            "overround": round(market_pct - 100, 2),
            "num_runners_priced": len(all_best_decimals),
            "url": race_url,
            "scraped_at": datetime.now().isoformat(),
        })

    # --- Extract ALL data-* attributes from odds cells comprehensively ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["odds", "odig", "bk", "runner", "horse", "sel", "market", "price",
             "best", "movement", "history", "prev", "open"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            parent_row = el.find_parent("tr")
            horse = ""
            if parent_row:
                name_el = parent_row.find(["td", "a", "span"], class_=lambda c: c and
                                          any(k in " ".join(c).lower() for k in ["name", "runner", "sel"]))
                if name_el:
                    horse = name_el.get_text(strip=True)
            records.append({
                "date": date_str,
                "source": "oddschecker",
                "type": "comprehensive_data_attrs",
                "race_name": race_name,
                "horse_name": horse,
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Embedded JSON data from scripts ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        # window.__DATA, oddsData, marketData
        for m in re.finditer(r'window\[?[\'"]?(__\w+|__NEXT_DATA__|oddsData|marketData|raceData|graphData)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "embedded_window_data",
                    "race_name": race_name,
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
                    "source": "oddschecker",
                    "type": "embedded_json_parse",
                    "race_name": race_name,
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
                "source": "oddschecker",
                "type": "script_application_json",
                "race_name": race_name,
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Extract tips/predictions sections ---
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["tip", "prediction", "verdict", "nap",
                                                  "best-bet", "each-way"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "tip_prediction",
                    "race_name": race_name,
                    "content": text[:1500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extract betting offers / promotions metadata ---
    for div in soup.find_all(["div", "section", "a"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["offer", "promo", "bonus", "free-bet",
                                                  "enhanced-odds", "boost"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "betting_offer",
                    "race_name": race_name,
                    "content": text[:400],
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 60 — Oddschecker Scraper (odds comparison across bookmakers)")
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
    log.info("SCRIPT 60 — Oddschecker Scraper (Odds Comparison)")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "oddschecker_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape meetings list
        meeting_records = scrape_meetings(session, date_str)
        if meeting_records:
            # Scrape odds for each race found
            race_urls = [r.get("url") for r in meeting_records
                         if r.get("url") and "/horse-racing/" in r.get("url", "")]
            for rurl in list(set(race_urls))[:20]:
                odds = scrape_race_odds(session, rurl, date_str)
                if odds:
                    meeting_records.extend(odds)
                smart_pause(2.5, 1.5)

            for rec in meeting_records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
            save_checkpoint({"last_date": date_str, "total_records": total_records})

        if day_count % 60 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(10, 25))

        current += timedelta(days=1)
        smart_pause(1.5, 0.8)

    save_checkpoint({"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
