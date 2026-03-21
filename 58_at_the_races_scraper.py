#!/usr/bin/env python3
"""
Script 58 — Scraping At The Races (UK/IRE Racing)
Source : attheraces.com
Collecte : UK/IRE results, form guides, race cards, trainer/jockey stats
CRITIQUE pour : UK/IRE Form Data, Results Archive, Performance Tracking
"""

import argparse
import json
import logging
import os
import sys
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

SCRIPT_NAME = "58_at_the_races"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry

log = setup_logging("58_at_the_races")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.attheraces.com"


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


def extract_embedded_json(soup, date_str, source="at_the_races"):
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
                    "verdict", "sectional", "result", "going", "meeting"]:
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


def extract_data_attributes(soup, date_str, source="at_the_races"):
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


def extract_verdicts_comments(soup, date_str, source="at_the_races"):
    """Extract verdicts, race comments and detailed analyses."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["verdict", "comment", "analysis",
                                                   "spotlight", "assessment",
                                                   "race-comment", "expert",
                                                   "preview", "report"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "verdict",
                    "content": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                author_el = el.find(["span", "strong", "a"],
                                     class_=lambda c: c and any(kw in " ".join(c).lower()
                                                                for kw in ["author", "tipster", "expert"]))
                if author_el:
                    record["author"] = author_el.get_text(strip=True)
                records.append(record)
    return records


def extract_sectionals(soup, date_str, source="at_the_races"):
    """Extract sectional timing data."""
    records = []
    for el in soup.find_all(["div", "table", "section", "span"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["sectional", "split", "timing",
                                                   "furlong", "time-figure"]):
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
                            "type": "sectional_time",
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
                        "type": "sectional_data",
                        "content": text,
                        "classes_css": classes,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    # Parse time values
                    times = re.findall(r'(\d{1,2}[.:]\d{2}[.:]\d{2}|\d{1,2}[.:]\d{2})', text)
                    if times:
                        record["times_parsed"] = times[:10]
                    records.append(record)
    return records


def scrape_racecards(session, date_str):
    """Scrape At The Races race cards for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # ATR uses date format DD-Month-YYYY in URLs
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = dt.strftime("%d-%B-%Y").lstrip("0")
    url = f"{BASE_URL}/racecards/{url_date}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(resp.text)

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction pattern ---
    records.extend(extract_embedded_json(soup, date_str, "at_the_races"))
    records.extend(extract_data_attributes(soup, date_str, "at_the_races"))
    records.extend(extract_verdicts_comments(soup, date_str, "at_the_races"))
    records.extend(extract_sectionals(soup, date_str, "at_the_races"))

    # Extract meeting/course links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if any(kw in href.lower() for kw in ["racecard", "race-card", "/racecards/", "/meeting/"]):
            text = link.get_text(strip=True)
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "at_the_races",
                    "type": "race_link",
                    "text": text,
                    "url": href if href.startswith("http") else BASE_URL + href,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract runner tables
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
                "source": "at_the_races",
                "type": "racecard_runner",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract runner cards from div-based layout
    for section in soup.find_all(["div", "li", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["runner", "horse", "card-entry", "participant"]):
            horse_el = section.find(["h3", "h4", "a", "span"],
                                    class_=lambda c: c and any(k in " ".join(c).lower()
                                                               for k in ["horse", "name", "runner"]))
            if not horse_el:
                horse_el = section.find(["h3", "h4", "a"])

            horse_name = horse_el.get_text(strip=True) if horse_el else None
            if not horse_name or len(horse_name) < 2:
                continue

            record = {
                "date": date_str,
                "source": "at_the_races",
                "type": "runner_card",
                "horse_name": horse_name,
                "scraped_at": datetime.now().isoformat(),
            }

            # Jockey
            jockey_el = section.find(["span", "a", "div"],
                                     class_=lambda c: c and "jockey" in " ".join(c).lower())
            if jockey_el:
                record["jockey"] = jockey_el.get_text(strip=True)

            # Trainer
            trainer_el = section.find(["span", "a", "div"],
                                      class_=lambda c: c and "trainer" in " ".join(c).lower())
            if trainer_el:
                record["trainer"] = trainer_el.get_text(strip=True)

            # Form
            form_el = section.find(["span", "div"],
                                   class_=lambda c: c and "form" in " ".join(c).lower())
            if form_el:
                record["form"] = form_el.get_text(strip=True)

            # Weight
            weight_el = section.find(["span", "div"],
                                     class_=lambda c: c and "weight" in " ".join(c).lower())
            if weight_el:
                record["weight"] = weight_el.get_text(strip=True)

            # Age
            age_el = section.find(["span", "div"],
                                  class_=lambda c: c and "age" in " ".join(c).lower())
            if age_el:
                record["age"] = age_el.get_text(strip=True)

            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(session, date_str):
    """Scrape At The Races results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = dt.strftime("%d-%B-%Y").lstrip("0")
    url = f"{BASE_URL}/results/{url_date}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on results page ---
    records.extend(extract_embedded_json(soup, date_str, "at_the_races"))
    records.extend(extract_data_attributes(soup, date_str, "at_the_races"))
    records.extend(extract_verdicts_comments(soup, date_str, "at_the_races"))
    records.extend(extract_sectionals(soup, date_str, "at_the_races"))

    # Extract result tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        # Identify race name from preceding header
        race_name = ""
        prev = table.find_previous(["h2", "h3", "h4"])
        if prev:
            race_name = prev.get_text(strip=True)

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "date": date_str,
                "source": "at_the_races",
                "type": "result",
                "race_name": race_name,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Parse finishing position
            if cells:
                pos_match = re.match(r'^(\d+)(st|nd|rd|th)?$', cells[0].strip(), re.IGNORECASE)
                if pos_match:
                    record["position_parsed"] = int(pos_match.group(1))

            # Parse SP odds
            for cell in cells:
                odds_match = re.search(r'(\d+/\d+|\d+\.\d+|evens|evs)', cell, re.IGNORECASE)
                if odds_match:
                    record["sp_parsed"] = odds_match.group(1)
                    break

            records.append(record)

    # Extract result sections from div layout
    for section in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finish", "placed"]):
            text = section.get_text(strip=True)
            if text and 10 < len(text) < 1000:
                records.append({
                    "date": date_str,
                    "source": "at_the_races",
                    "type": "result_section",
                    "content": text[:800],
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_form_guide(session, race_url, date_str):
    """Scrape detailed form guide for a specific race."""
    if not race_url.startswith("http"):
        race_url = BASE_URL + race_url

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"form_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, race_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on form guide page ---
    records.extend(extract_embedded_json(soup, date_str, "at_the_races"))
    records.extend(extract_data_attributes(soup, date_str, "at_the_races"))
    records.extend(extract_verdicts_comments(soup, date_str, "at_the_races"))
    records.extend(extract_sectionals(soup, date_str, "at_the_races"))

    # Race name
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Race conditions (distance, class, prize, going)
    conditions = {}
    for el in soup.find_all(["span", "div", "p"], class_=True):
        classes = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if "distance" in classes.lower() and text:
            conditions["distance"] = text
        elif "class" in classes.lower() and text:
            conditions["race_class"] = text
        elif "prize" in classes.lower() and text:
            conditions["prize"] = text
        elif "going" in classes.lower() and text:
            conditions["going"] = text

    # Runner detail tables
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
                "source": "at_the_races",
                "type": "form_detail",
                "race_name": race_name,
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            record.update(conditions)
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract comments / verdict
    for div in soup.find_all(["div", "p", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "verdict", "analysis", "spotlight"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "at_the_races",
                    "type": "form_comment",
                    "race_name": race_name,
                    "content": text[:1500],
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 58 — At The Races Scraper (UK/IRE results, form)")
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
    log.info("SCRIPT 58 — At The Races Scraper (UK/IRE)")
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
    output_file = os.path.join(OUTPUT_DIR, "at_the_races_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape race cards
        racecard_records = scrape_racecards(session, date_str)
        if racecard_records:
            # Scrape form details for each race
            race_urls = [r.get("url") for r in racecard_records
                         if r.get("type") == "race_link" and r.get("url")]
            for rurl in list(set(race_urls))[:15]:
                detail = scrape_form_guide(session, rurl, date_str)
                if detail:
                    racecard_records.extend(detail)
                smart_pause(1.5, 0.8)

            for rec in racecard_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(2.0, 1.0)

        # Scrape results
        result_records = scrape_results(session, date_str)
        if result_records:
            for rec in result_records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
            save_checkpoint({"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(1.0, 0.5)

    save_checkpoint({"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
