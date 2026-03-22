#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 99 -- Clerk of Course Reports Scraper
Source : Multiple UK/IRE racecourse sites + BHA going reports
Collecte : going changes, course inspections, abandonment notices, drainage reports
CRITIQUE pour : Going Model, Real-time Track Updates, Abandonment Prediction
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
from bs4 import BeautifulSoup

SCRIPT_NAME = "99_clerk_of_course"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, load_checkpoint, save_checkpoint, append_jsonl, create_session

log = setup_logging("99_clerk_of_course")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Sources for clerk of course / going reports
GOING_REPORT_SOURCES = {
    # BHA / official
    "bha_going": "https://www.britishhorseracing.com/racing/going-reports",
    "bha_inspections": "https://www.britishhorseracing.com/racing/inspections",
    # Racing Post going reports
    "rp_going": "https://www.racingpost.com/going",
    "rp_reports": "https://www.racingpost.com/news/going-reports",
    # Sporting Life
    "sl_going": "https://www.sportinglife.com/racing/going",
    # At The Races
    "atr_going": "https://www.attheraces.com/going",
    # Timeform
    "tf_going": "https://www.timeform.com/horse-racing/going",
    # Racing TV
    "rtv_going": "https://www.racingtv.com/going",
    # IHR Ireland
    "hri_going": "https://www.goracing.ie/going-reports",
    # France Galop
    "fg_going": "https://www.france-galop.com/fr/courses/terrains",
}

# UK/IRE racecourse websites for direct clerk reports
COURSE_SITES = {
    "ascot": "https://www.ascot.com",
    "cheltenham": "https://www.thejockeyclub.co.uk/cheltenham",
    "newmarket": "https://www.thejockeyclub.co.uk/newmarket",
    "epsom": "https://www.thejockeyclub.co.uk/epsom",
    "sandown": "https://www.thejockeyclub.co.uk/sandown",
    "kempton": "https://www.thejockeyclub.co.uk/kempton",
    "haydock": "https://www.thejockeyclub.co.uk/haydock",
    "newbury": "https://www.newburyracecourse.co.uk",
    "york": "https://www.yorkracecourse.co.uk",
    "doncaster": "https://www.thejockeyclub.co.uk/doncaster",
    "goodwood": "https://www.goodwood.com/horseracing",
    "aintree": "https://www.thejockeyclub.co.uk/aintree",
    "leopardstown": "https://www.leopardstown.com",
    "curragh": "https://www.curragh.ie",
    "fairyhouse": "https://www.fairyhouse.ie",
}



def parse_going_text(text):
    """Extract structured going info from free text."""
    result = {}

    # Official going
    for going_term in ["Heavy", "Soft", "Good to Soft", "Good to Firm", "Good",
                       "Firm", "Hard", "Standard", "Standard to Slow", "Slow",
                       "Yielding", "Yielding to Soft", "Bon", "Souple", "Lourd",
                       "Tres Souple", "Collant", "Leger", "Bon Souple"]:
        if going_term.lower() in text.lower():
            result["going_official"] = going_term
            break

    # GoingStick reading
    gs_match = re.search(r'(?:GoingStick|going\s*stick)[:\s]*(\d+\.?\d*)', text, re.IGNORECASE)
    if gs_match:
        result["goingstick"] = float(gs_match.group(1))

    # Watering
    water_match = re.search(r'(?:water(?:ed|ing))[:\s]*(\d+)\s*mm', text, re.IGNORECASE)
    if water_match:
        result["watering_mm"] = int(water_match.group(1))

    # Rail movement
    rail_match = re.search(r'(?:rail|dolling)[:\s]*(\d+)\s*(?:yards?|metres?|m)\s*(out|in)',
                           text, re.IGNORECASE)
    if rail_match:
        result["rail_yards"] = int(rail_match.group(1))
        result["rail_direction"] = rail_match.group(2).lower()

    # Inspection time
    insp_match = re.search(r'(?:inspection|inspect)[:\s]*(\d{1,2}[:.]\d{2})', text, re.IGNORECASE)
    if insp_match:
        result["inspection_time"] = insp_match.group(1)

    # Abandoned
    if re.search(r'abandon', text, re.IGNORECASE):
        result["abandoned"] = True

    return result


def scrape_going_report_page(session, source_name, page_url):
    """Scrape a going report page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"report_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    resp = fetch_with_retry(session, page_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # -- Going report tables --
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "source": "clerk_of_course",
                "sub_source": source_name,
                "type": "going_table",
                "url": page_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            # Parse going from combined text
            combined = " ".join(cells)
            parsed = parse_going_text(combined)
            record.update(parsed)
            records.append(record)

    # -- Going report text blocks --
    for div in soup.find_all(["div", "p", "article", "section", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["going", "ground", "report",
                                                  "clerk", "course", "inspection",
                                                  "condition", "update", "notice"]):
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "clerk_of_course",
                    "sub_source": source_name,
                    "type": "going_report",
                    "contenu": text[:2500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                parsed = parse_going_text(text)
                record.update(parsed)
                # Extract date from text
                date_match = re.search(r'(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s*(\d{4})',
                                       text, re.IGNORECASE)
                if date_match:
                    record["report_date"] = f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}"
                # Extract course name
                for course in list(COURSE_SITES.keys()) + [
                    "bath", "beverley", "brighton", "carlisle", "catterick",
                    "chester", "exeter", "fontwell", "lingfield", "musselburgh",
                    "newcastle", "nottingham", "pontefract", "redcar", "ripon",
                    "salisbury", "thirsk", "warwick", "wetherby", "windsor",
                    "wolverhampton", "worcester",
                ]:
                    if course.lower() in text.lower():
                        record["course"] = course
                        break
                records.append(record)

    # -- Time-stamped updates --
    for div in soup.find_all(["div", "span", "p", "time"]):
        text = div.get_text(strip=True)
        time_match = re.search(r'(\d{1,2}[:.]\d{2})\s*[-:]\s*(.+?)(?:\.|$)', text)
        if time_match and any(kw in text.lower() for kw in ["going", "ground",
                                                              "inspection", "watering",
                                                              "rail", "abandon"]):
            record = {
                "source": "clerk_of_course",
                "sub_source": source_name,
                "type": "timed_update",
                "time": time_match.group(1),
                "contenu": text[:500],
                "url": page_url,
                "scraped_at": datetime.now().isoformat(),
            }
            parsed = parse_going_text(text)
            record.update(parsed)
            records.append(record)

    # -- Embedded JSON --
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\{[\s\S]{50,}?\});', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "clerk_of_course",
                    "sub_source": source_name,
                    "type": "embedded_data",
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
                "source": "clerk_of_course",
                "sub_source": source_name,
                "type": "script_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # -- Links to deeper going reports --
    for a in soup.find_all("a", href=True):
        href = a["href"]
        link_text = a.get_text(strip=True)
        if any(kw in href.lower() for kw in ["going", "clerk", "inspection",
                                               "report", "ground"]):
            if link_text and len(link_text) > 3:
                records.append({
                    "source": "clerk_of_course",
                    "sub_source": source_name,
                    "type": "report_link",
                    "link_text": link_text,
                    "link_url": href if href.startswith("http") else page_url.split("/")[0] + "//" + page_url.split("/")[2] + href,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 99 -- Clerk of Course Reports Scraper")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-pages", type=int, default=500,
                        help="Nombre max de pages a scraper")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 99 -- Clerk of Course Reports Scraper")
    log.info(f"  Sources : {len(GOING_REPORT_SOURCES)}")
    log.info(f"  Course sites : {len(COURSE_SITES)}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    done_urls = set(checkpoint.get("done_urls", []))
    if args.resume and done_urls:
        log.info(f"  Reprise checkpoint: {len(done_urls)} pages deja traitees")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "clerk_of_course_data.jsonl")

    total_records = 0
    page_count = 0
    all_sub_links = []

    # Phase 1: Scrape main going report sources
    log.info("  Phase 1: Sources principales de going reports")
    for source_name, url in GOING_REPORT_SOURCES.items():
        if url in done_urls:
            continue
        if page_count >= args.max_pages:
            break

        records = scrape_going_report_page(session, source_name, url)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1
                # Collect sub-links
                if rec.get("type") == "report_link" and rec.get("link_url"):
                    all_sub_links.append((source_name, rec["link_url"]))

        done_urls.add(url)
        page_count += 1
        log.info(f"    {source_name}: {len(records) if records else 0} records")

        save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                         "total_records": total_records})
        smart_pause()

    # Phase 2: Scrape individual course sites
    log.info("  Phase 2: Sites d'hippodromes individuels")
    for course_name, base_url in COURSE_SITES.items():
        going_urls = [
            f"{base_url}/going",
            f"{base_url}/racing/going",
            f"{base_url}/the-course/going",
            f"{base_url}/going-report",
        ]
        for url in going_urls:
            if url in done_urls:
                continue
            if page_count >= args.max_pages:
                break

            records = scrape_going_report_page(session, course_name, url)
            if records:
                for rec in records:
                    rec["course"] = course_name
                    append_jsonl(output_file, rec)
                    total_records += 1
                    if rec.get("type") == "report_link" and rec.get("link_url"):
                        all_sub_links.append((course_name, rec["link_url"]))

            done_urls.add(url)
            page_count += 1
            smart_pause(1.5, 0.8)

        if page_count % 10 == 0:
            log.info(f"    pages={page_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                             "total_records": total_records})

        if page_count % 40 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

    # Phase 3: Follow sub-links
    log.info(f"  Phase 3: Sous-liens ({len(all_sub_links)} liens)")
    for source_name, link_url in all_sub_links:
        if link_url in done_urls:
            continue
        if page_count >= args.max_pages:
            break

        records = scrape_going_report_page(session, source_name, link_url)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        done_urls.add(link_url)
        page_count += 1

        if page_count % 10 == 0:
            log.info(f"    pages={page_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                             "total_records": total_records})

        if page_count % 50 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        smart_pause()

    save_checkpoint(CHECKPOINT_FILE, {"done_urls": list(done_urls),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {page_count} pages, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
