#!/usr/bin/env python3
"""
Script 57 — Scraping Sporting Life (UK Racing)
Source : sportinglife.com/racing
Collecte : race cards, results, tips, form, non-runners
CRITIQUE pour : UK Race Data, Tips Aggregation, Form Analysis
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

SCRIPT_NAME = "57_sporting_life"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint, create_session
from utils.html_parsing import extract_embedded_json, extract_data_attributes

log = setup_logging("57_sporting_life")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.sportinglife.com"



def extract_comments_and_tips(soup, date_str, source="sporting_life"):
    """Extract race comments, verdicts and detailed tips."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "verdict", "analysis",
                                                   "spotlight", "tip-detail",
                                                   "race-comment", "expert",
                                                   "prediction", "assessment"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "race_comment",
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


def extract_form_history(soup, date_str, source="sporting_life"):
    """Extract detailed form history for runners."""
    records = []
    for el in soup.find_all(["div", "span", "td", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "history", "past-performance",
                                                   "form-figure", "recent-runs",
                                                   "performance-line"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "form_history",
                    "content": text,
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse form figures
                form_match = re.search(r'([0-9PFU/-]{3,})', text)
                if form_match:
                    record["form_figures"] = form_match.group(1)
                records.append(record)
    return records


def scrape_racecards(session, date_str):
    """Scrape Sporting Life race cards for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/racecards/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction pattern ---
    records.extend(extract_embedded_json(soup, date_str, "sporting_life"))
    records.extend(extract_data_attributes(soup, date_str, "sporting_life"))
    records.extend(extract_comments_and_tips(soup, date_str, "sporting_life"))
    records.extend(extract_form_history(soup, date_str, "sporting_life"))

    # Extract meeting links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/racing/" in href and any(kw in href.lower() for kw in ["racecard", "results", "meeting"]):
            text = link.get_text(strip=True)
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "sporting_life",
                    "type": "meeting_link",
                    "text": text,
                    "url": href if href.startswith("http") else BASE_URL + href,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract race card tables (runners, jockeys, trainers, odds)
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
                "source": "sporting_life",
                "type": "racecard_runner",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract race sections (div-based layouts)
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["race-card", "racecard", "runner", "horse-card"]):
            # Horse name
            horse_el = section.find(["h3", "h4", "a", "span"], class_=lambda c: c and
                                    any(k in " ".join(c).lower() for k in ["horse", "name", "runner"]))
            if not horse_el:
                horse_el = section.find(["h3", "h4", "a"])

            horse_name = horse_el.get_text(strip=True) if horse_el else None
            if not horse_name:
                continue

            record = {
                "date": date_str,
                "source": "sporting_life",
                "type": "runner_card",
                "horse_name": horse_name,
                "scraped_at": datetime.now().isoformat(),
            }

            # Jockey
            jockey_el = section.find(["span", "a"], class_=lambda c: c and "jockey" in " ".join(c).lower())
            if jockey_el:
                record["jockey"] = jockey_el.get_text(strip=True)

            # Trainer
            trainer_el = section.find(["span", "a"], class_=lambda c: c and "trainer" in " ".join(c).lower())
            if trainer_el:
                record["trainer"] = trainer_el.get_text(strip=True)

            # Form figures
            form_el = section.find(["span", "div"], class_=lambda c: c and "form" in " ".join(c).lower())
            if form_el:
                record["form"] = form_el.get_text(strip=True)

            # Odds
            odds_el = section.find(["span", "div"], class_=lambda c: c and
                                   any(k in " ".join(c).lower() for k in ["odds", "price", "sp"]))
            if odds_el:
                record["odds"] = odds_el.get_text(strip=True)

            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(session, date_str):
    """Scrape Sporting Life results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/results/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on results page ---
    records.extend(extract_embedded_json(soup, date_str, "sporting_life"))
    records.extend(extract_data_attributes(soup, date_str, "sporting_life"))
    records.extend(extract_comments_and_tips(soup, date_str, "sporting_life"))

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
                "source": "sporting_life",
                "type": "result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Parse position
            if cells:
                pos_match = re.match(r'^(\d+)(st|nd|rd|th)?$', cells[0].strip(), re.IGNORECASE)
                if pos_match:
                    record["position_parsed"] = int(pos_match.group(1))

            records.append(record)

    # Extract result sections (div-based)
    for section in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "finishing", "placed"]):
            text = section.get_text(strip=True)
            if text and 5 < len(text) < 1000:
                records.append({
                    "date": date_str,
                    "source": "sporting_life",
                    "type": "result_section",
                    "content": text[:800],
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_tips(session, date_str):
    """Scrape Sporting Life tips and predictions for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"tips_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/racing/tips/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on tips page ---
    records.extend(extract_embedded_json(soup, date_str, "sporting_life"))
    records.extend(extract_data_attributes(soup, date_str, "sporting_life"))
    records.extend(extract_comments_and_tips(soup, date_str, "sporting_life"))

    # Extract tips sections
    for div in soup.find_all(["div", "article", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["tip", "nap", "pick", "selection", "best-bet"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 1500:
                record = {
                    "date": date_str,
                    "source": "sporting_life",
                    "type": "tip",
                    "content": text[:1200],
                    "scraped_at": datetime.now().isoformat(),
                }

                # Extract horse name from tip
                horse_el = div.find(["a", "strong", "b", "span"],
                                    class_=lambda c: c and any(k in " ".join(c).lower()
                                                               for k in ["horse", "selection", "name"]))
                if horse_el:
                    record["horse_name"] = horse_el.get_text(strip=True)

                records.append(record)

    # Extract tips from structured data (JSON-LD)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            if isinstance(ld, dict) and ld.get("@type"):
                records.append({
                    "date": date_str,
                    "source": "sporting_life",
                    "type": "structured_data",
                    "ld_type": ld.get("@type"),
                    "name": ld.get("name", ""),
                    "description": (ld.get("description", "") or "")[:500],
                    "scraped_at": datetime.now().isoformat(),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 57 — Sporting Life Scraper (race cards, results, tips)")
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
    log.info("SCRIPT 57 — Sporting Life Scraper (UK Racing)")
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
    output_file = os.path.join(OUTPUT_DIR, "sporting_life_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape race cards
        racecard_records = scrape_racecards(session, date_str)
        if racecard_records:
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

        smart_pause(2.0, 1.0)

        # Scrape tips
        tip_records = scrape_tips(session, date_str)
        if tip_records:
            for rec in tip_records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(1.0, 0.5)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
