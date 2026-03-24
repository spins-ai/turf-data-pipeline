#!/usr/bin/env python3
"""
Script 54 — Scraping TurfInfo.fr (Playwright)
Migrated from cloudscraper to Playwright to bypass Cloudflare.

Source : turfinfo.fr/courses/{date}
Collecte : informations detaillees de courses, partants, cotes, resultats, musique
CRITIQUE pour : Race Detail Features, Partant History, Form Analysis

Usage:
    pip install playwright beautifulsoup4
    playwright install chromium
    python 54_turfinfo_scraper.py --start 2024-01-01 --end 2024-03-31
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

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, navigate_with_retry, accept_cookies

SCRIPT_NAME = "54_turfinfo"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.html_parsing import extract_embedded_json, extract_data_attributes

log = setup_logging("54_turfinfo")








def extract_comments_analyses(soup, date_str, source="turfinfo"):
    """Extract comment and analysis divs including race comments."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "analyse", "expert", "avis",
                                                   "resume", "verdict", "recap",
                                                   "race-comment", "course-comment",
                                                   "description", "editorial"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "commentaire_course",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                author_el = el.find(["span", "strong", "a"],
                                     class_=lambda c: c and any(kw in " ".join(c).lower()
                                                                for kw in ["author", "auteur", "expert"]))
                if author_el:
                    record["auteur"] = author_el.get_text(strip=True)
                records.append(record)
    return records


def extract_musique_detaillee(soup, date_str, source="turfinfo"):
    """Extract detailed musique (form) data from TurfInfo."""
    records = []
    for el in soup.find_all(["div", "span", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["musique", "form", "perf", "historique",
                                                   "past-results", "derniere-course"]):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 500:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "musique_detaillee",
                    "contenu": text,
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse musique codes
                musique_match = re.search(r'([0-9DATap]{4,})', text)
                if musique_match:
                    record["musique_code"] = musique_match.group(1)
                records.append(record)
    return records


def scrape_programme_day(page, date_str):
    """Scraper le programme TurfInfo d'un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"programme_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.turfinfo.fr/courses/{date_str}"
    if not navigate_with_retry(page, url):
        return None

    html = page.content()

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []
    course_links = []

    # --- Full extraction pattern ---
    records.extend(extract_embedded_json(soup, date_str, "turfinfo"))
    records.extend(extract_data_attributes(soup, date_str, "turfinfo"))
    records.extend(extract_comments_analyses(soup, date_str, "turfinfo"))
    records.extend(extract_musique_detaillee(soup, date_str, "turfinfo"))

    # --- Extraire les reunions ---
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["reunion", "meeting", "hippodrome",
                                                   "course", "race"]):
            record = {
                "date": date_str,
                "source": "turfinfo",
                "type": "reunion",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong"])
            if title:
                record["hippodrome"] = title.get_text(strip=True)

            # Infos complementaires (discipline, distance, etc.)
            for span in div.find_all(["span", "small", "em"]):
                text = span.get_text(strip=True)
                if re.search(r'\d+\s*m', text):
                    record["distance"] = text
                elif re.search(r'trot|galop|plat|haies|steeple|obstacle', text, re.I):
                    record["discipline"] = text

            # Liens vers les courses
            for a in div.find_all("a", href=True):
                href = a["href"]
                if re.search(r'/course/|/partants/|/pronostic/', href):
                    full_url = href if href.startswith("http") else f"https://www.turfinfo.fr{href}"
                    course_links.append(full_url)

            text_content = div.get_text(strip=True)
            if text_content and len(text_content) < 500:
                record["resume"] = text_content[:300]
            records.append(record)

    # --- Tables de donnees ---
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 3:
                record = {
                    "date": date_str,
                    "source": "turfinfo",
                    "type": "info_course",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                records.append(record)

    result = {"records": records, "course_links": list(set(course_links))}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_course_detail(page, course_url, date_str):
    """Scraper les informations detaillees d'une course (partants, musique, cotes)."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    if not navigate_with_retry(page, course_url):
        return None

    soup = BeautifulSoup(page.content(), "html.parser")
    records = []

    # --- Full extraction on course detail page ---
    records.extend(extract_embedded_json(soup, date_str, "turfinfo"))
    records.extend(extract_data_attributes(soup, date_str, "turfinfo"))
    records.extend(extract_comments_analyses(soup, date_str, "turfinfo"))
    records.extend(extract_musique_detaillee(soup, date_str, "turfinfo"))

    # Titre de la course
    nom_prix = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_prix = text
            break

    # Conditions de course
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d[\d\s]*)\s*m(?:etre)?', page_text)
    if dist_match:
        conditions["distance_m"] = dist_match.group(1).replace(" ", "")

    dotation_match = re.search(r'(\d[\d\s,.]*)\s*\u20ac', page_text)
    if dotation_match:
        conditions["dotation"] = dotation_match.group(0)

    disc_match = re.search(r'(trot attele|trot monte|plat|haies|steeple|cross)',
                           page_text, re.I)
    if disc_match:
        conditions["discipline"] = disc_match.group(1)

    terrain_match = re.search(r'terrain\s*:?\s*([\w\s]+)', page_text, re.I)
    if terrain_match:
        conditions["terrain"] = terrain_match.group(1).strip()

    # Extraire les partants depuis les tables
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
                "source": "turfinfo",
                "type": "partant_detail",
                "nom_prix": nom_prix,
                "conditions": conditions,
                "url_course": course_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extraire la musique (sequence de performances passees)
            for cell in cells:
                musique_match = re.search(r'([0-9DATap]{5,})', cell)
                if musique_match:
                    record["musique"] = musique_match.group(1)
                    break

            # Extraire le poids
            for cell in cells:
                poids_match = re.search(r'(\d{2}[.,]?\d?)\s*kg', cell)
                if poids_match:
                    record["poids_kg"] = poids_match.group(1).replace(",", ".")
                    break

            records.append(record)

    # Resultats si disponibles
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["resultat", "result", "arrivee"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 1000:
                records.append({
                    "date": date_str,
                    "source": "turfinfo",
                    "type": "resultat",
                    "nom_prix": nom_prix,
                    "contenu": text,
                    "conditions": conditions,
                    "url_course": course_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 54 — TurfInfo Scraper (Playwright)")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 54 — TurfInfo Scraper (Playwright)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "turfinfo_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = launch_browser(pw)

    try:
        # Accept cookies on first navigation
        first_page = True

        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            result = scrape_programme_day(page, date_str)

            if first_page:
                accept_cookies(page)
                first_page = False

            if result:
                records = result.get("records", [])

                # Scraper les details de chaque course
                for curl in result.get("course_links", [])[:12]:
                    detail = scrape_course_detail(page, curl, date_str)
                    if detail:
                        records.extend(detail)
                    smart_pause(1.5, 0.8)

                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            day_count += 1

            if day_count % 30 == 0:
                log.info(f"  {date_str} | jours={day_count} records={total_records}")
                save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

            if day_count % 80 == 0:
                log.info("  Rotating browser context...")
                context.close()
                browser.close()
                smart_pause(5.0, 3.0)
                browser, context, page = launch_browser(pw)

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
        log.info("=" * 60)

    finally:
        try:
            page.close()
        except Exception:
            pass
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
        log.info("Browser closed")


if __name__ == "__main__":
    main()
