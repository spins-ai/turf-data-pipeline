#!/usr/bin/env python3
"""
Script 58 — Scraping At The Races (UK/IRE Racing)
Source : attheraces.com
Collecte : UK/IRE results, form guides, race cards, trainer/jockey stats
CRITIQUE pour : UK/IRE Form Data, Results Archive, Performance Tracking
Backend : Playwright (headless Chromium) — bypasses Cloudflare
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

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser

SCRIPT_NAME = "58_at_the_races"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("58_at_the_races")

BASE_URL = "https://www.attheraces.com"




# NOTE: Local version kept because it returns HTML string (page.content()) instead of bool
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


def scrape_racecards(page, date_str):
    """Scrape At The Races race cards for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # ATR uses date format DD-Month-YYYY in URLs
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = dt.strftime("%d-%B-%Y").lstrip("0")
    url = f"{BASE_URL}/racecards/{url_date}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []

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

            jockey_el = section.find(["span", "a", "div"],
                                     class_=lambda c: c and "jockey" in " ".join(c).lower())
            if jockey_el:
                record["jockey"] = jockey_el.get_text(strip=True)

            trainer_el = section.find(["span", "a", "div"],
                                      class_=lambda c: c and "trainer" in " ".join(c).lower())
            if trainer_el:
                record["trainer"] = trainer_el.get_text(strip=True)

            form_el = section.find(["span", "div"],
                                   class_=lambda c: c and "form" in " ".join(c).lower())
            if form_el:
                record["form"] = form_el.get_text(strip=True)

            weight_el = section.find(["span", "div"],
                                     class_=lambda c: c and "weight" in " ".join(c).lower())
            if weight_el:
                record["weight"] = weight_el.get_text(strip=True)

            age_el = section.find(["span", "div"],
                                  class_=lambda c: c and "age" in " ".join(c).lower())
            if age_el:
                record["age"] = age_el.get_text(strip=True)

            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_results(page, date_str):
    """Scrape At The Races results for a given date."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = dt.strftime("%d-%B-%Y").lstrip("0")
    url = f"{BASE_URL}/results/{url_date}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

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

            if cells:
                pos_match = re.match(r'^(\d+)(st|nd|rd|th)?$', cells[0].strip(), re.IGNORECASE)
                if pos_match:
                    record["position_parsed"] = int(pos_match.group(1))

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


def scrape_form_guide(page, race_url, date_str):
    """Scrape detailed form guide for a specific race."""
    if not race_url.startswith("http"):
        race_url = BASE_URL + race_url

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"form_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, race_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

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

    # Race conditions
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
    parser = argparse.ArgumentParser(description="Script 58 — At The Races Scraper (UK/IRE results, form) [Playwright]")
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
    log.info("SCRIPT 58 — At The Races Scraper (UK/IRE) [Playwright]")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "at_the_races_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = launch_browser(pw, locale="en-GB", timezone="Europe/London")
    try:
        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Scrape race cards
            racecard_records = scrape_racecards(page, date_str)
            if racecard_records:
                # Scrape form details for each race
                race_urls = [r.get("url") for r in racecard_records
                             if r.get("type") == "race_link" and r.get("url")]
                for rurl in list(set(race_urls))[:15]:
                    detail = scrape_form_guide(page, rurl, date_str)
                    if detail:
                        racecard_records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in racecard_records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            smart_pause(2.0, 1.0)

            # Scrape results
            result_records = scrape_results(page, date_str)
            if result_records:
                for rec in result_records:
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
                browser, context, page = launch_browser(pw, locale="en-GB", timezone="Europe/London")
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
