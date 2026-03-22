#!/usr/bin/env python3
"""
Script 52 — Scraping Turfomania.fr (Playwright)
Migrated from cloudscraper to Playwright to bypass Cloudflare.

Source : turfomania.fr — pronostics, partants, stats
Flux :
  1) /partants-programmes/ -> Schema.org JSON-LD -> URLs detail-reunion.php?idreunion=XXX
  2) detail-reunion.php -> tables avec partants + liens /pronostics/partants-...?idcourse=XXX
  3) Page course individuelle -> table detaillee des partants + pronostics

Usage:
    pip install playwright beautifulsoup4
    playwright install chromium
    python 52_turfomania_scraper.py --max-courses 200
"""

import argparse
import json
import logging
import os
import sys
import random
import re
import time
from datetime import datetime

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, navigate_with_retry, accept_cookies

SCRIPT_NAME = "52_turfomania"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("52_turfomania")

BASE_URL = "https://www.turfomania.fr"








def get_reunion_urls(page):
    """Etape 1: Extraire les URLs de reunions depuis /partants-programmes/ via Schema.org JSON-LD."""
    if not navigate_with_retry(page, f"{BASE_URL}/partants-programmes/"):
        return []

    accept_cookies(page)
    soup = BeautifulSoup(page.content(), "html.parser")
    reunions = []
    seen_ids = set()

    # Methode 1: Schema.org JSON-LD dans les <script type="application/ld+json">
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict) and data.get("url"):
                url = data["url"]
                m = re.search(r'idreunion=(\d+)', url)
                if m and m.group(1) not in seen_ids:
                    seen_ids.add(m.group(1))
                    reunions.append({
                        "id_reunion": m.group(1),
                        "url": url,
                        "name": data.get("name", ""),
                        "startDate": data.get("startDate", ""),
                    })
        except (json.JSONDecodeError, AttributeError):
            pass

    # Methode 2: URLs dans le HTML (schema.org inline ou liens)
    pattern = re.compile(r'detail-reunion\.php\?idreunion=(\d+)')
    for script in soup.find_all("script"):
        txt = script.string or ""
        for m in pattern.finditer(txt):
            if m.group(1) not in seen_ids:
                seen_ids.add(m.group(1))
                reunions.append({
                    "id_reunion": m.group(1),
                    "url": f"{BASE_URL}/partants-programmes/detail-reunion.php?idreunion={m.group(1)}&choixtype=1",
                    "name": "",
                    "startDate": "",
                })

    # Methode 3: liens <a> directs
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = pattern.search(href)
        if m and m.group(1) not in seen_ids:
            seen_ids.add(m.group(1))
            if href.startswith("http"):
                url = href
            elif href.startswith("/"):
                url = BASE_URL + href
            else:
                url = BASE_URL + "/" + href
            reunions.append({
                "id_reunion": m.group(1),
                "url": url,
                "name": a.get_text(strip=True)[:200],
                "startDate": "",
            })

    return reunions


def scrape_reunion(page, reunion_info, date_iso, output_file):
    """Etape 2: Scraper une page de reunion — extraire les tables de partants + liens courses."""
    id_reunion = reunion_info["id_reunion"]
    cache_file = os.path.join(CACHE_DIR, f"reunion_{id_reunion}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_records", 0), cached.get("course_links", [])

    if not navigate_with_retry(page, reunion_info["url"]):
        return 0, []

    soup = BeautifulSoup(page.content(), "html.parser")
    records = []
    course_links = []

    # Extraire les liens de courses individuelles
    seen_courses = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r'partants[^"]*\?idcourse=(\d+)', href)
        if m and m.group(1) not in seen_courses:
            seen_courses.add(m.group(1))
            url = href if href.startswith("http") else BASE_URL + href
            course_links.append({
                "id_course": m.group(1),
                "url": url,
                "titre": a.get_text(strip=True)[:200],
            })

    # Extraire les tables de la page reunion (resume des courses)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [th.get_text(strip=True).lower().replace(" ", "_").replace("\u00b0", "")
                   for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Detecter si c'est une table de partants (cheval, driver, etc.)
        is_partants = any(kw in " ".join(headers) for kw in
                         ["cheval", "driver", "jockey", "entraineur", "record", "gain"])

        for row in rows[1:]:
            cells = [td.get_text(strip=True)[:500] for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                entry = {
                    "date": date_iso,
                    "source": "turfomania",
                    "type": "partant" if is_partants else "reunion_row",
                    "id_reunion": id_reunion,
                    "reunion_name": reunion_info.get("name", ""),
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # Sauvegarder chaque record
    for rec in records:
        append_jsonl(output_file, rec)

    # Cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump({
            "id_reunion": id_reunion,
            "nb_records": len(records),
            "nb_courses": len(course_links),
            "course_links": course_links,
            "date": date_iso,
        }, f, ensure_ascii=False)

    return len(records), course_links


def scrape_course(page, course_info, date_iso, output_file):
    """Etape 3: Scraper une course individuelle — partants detailles + pronostics."""
    id_course = course_info["id_course"]
    cache_file = os.path.join(CACHE_DIR, f"course_{id_course}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_records", 0)

    if not navigate_with_retry(page, course_info["url"]):
        return 0

    soup = BeautifulSoup(page.content(), "html.parser")
    records = []

    # Info course
    info = {
        "date": date_iso,
        "source": "turfomania",
        "type": "course",
        "id_course": id_course,
        "url": course_info["url"],
        "titre_lien": course_info.get("titre", ""),
        "scraped_at": datetime.now().isoformat(),
    }
    h1 = soup.find("h1")
    if h1:
        info["titre"] = h1.get_text(strip=True)
    records.append(info)

    # Tables de partants
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [th.get_text(strip=True).lower().replace(" ", "_").replace("\u00b0", "")
                   for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True)[:500] for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                entry = {
                    "date": date_iso,
                    "source": "turfomania",
                    "type": "partant",
                    "id_course": id_course,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # Pronostics (divs avec classes specifiques)
    for div in soup.find_all(["div", "span", "p", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["prono", "tip", "selection", "favori",
                                                  "base", "complement", "analyse", "avis"]):
            if text and 5 < len(text) < 1000:
                records.append({
                    "date": date_iso,
                    "source": "turfomania",
                    "type": "pronostic",
                    "id_course": id_course,
                    "contenu": text[:800],
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Sauvegarder
    for rec in records:
        append_jsonl(output_file, rec)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump({"id_course": id_course, "nb_records": len(records), "date": date_iso},
                  f, ensure_ascii=False)

    return len(records)


def main():
    parser = argparse.ArgumentParser(description="Script 52 — Turfomania Scraper (Playwright)")
    parser.add_argument("--max-courses", type=int, default=200,
                        help="Max courses individuelles a scraper")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 52 — Turfomania Scraper (Playwright)")
    log.info("=" * 60)

    output_file = os.path.join(OUTPUT_DIR, "turfomania_data.jsonl")
    date_iso = datetime.now().strftime("%Y-%m-%d")

    pw = sync_playwright().start()
    browser, context, page = launch_browser(pw)

    try:
        # Etape 1: Recuperer les reunions
        log.info("  Etape 1: Recuperation des reunions...")
        reunions = get_reunion_urls(page)
        log.info(f"  {len(reunions)} reunions trouvees")

        total_records = 0
        all_course_links = []

        # Etape 2: Scraper chaque reunion
        log.info("  Etape 2: Scraping des reunions...")
        for i, reunion in enumerate(reunions):
            nb, courses = scrape_reunion(page, reunion, date_iso, output_file)
            total_records += nb
            all_course_links.extend(courses)
            log.info(f"    Reunion {i+1}/{len(reunions)}: {reunion.get('name', reunion['id_reunion'])} "
                     f"-> {nb} records, {len(courses)} courses")
            smart_pause(2.0, 1.0)

            if (i + 1) % 10 == 0:
                log.info("  Rotating browser context...")
                context.close()
                browser.close()
                smart_pause(3.0, 2.0)
                browser, context, page = launch_browser(pw)

        log.info(f"  Total reunions: {total_records} records, {len(all_course_links)} courses individuelles")

        # Etape 3: Scraper les courses individuelles
        log.info("  Etape 3: Scraping des courses individuelles...")
        course_count = 0
        for i, course in enumerate(all_course_links[:args.max_courses]):
            nb = scrape_course(page, course, date_iso, output_file)
            total_records += nb
            course_count += 1
            if (i + 1) % 10 == 0:
                log.info(f"    {i+1}/{min(len(all_course_links), args.max_courses)} courses, "
                         f"{total_records} records total")
            smart_pause(1.5, 0.8)

            if (i + 1) % 30 == 0:
                log.info("  Rotating browser context...")
                context.close()
                browser.close()
                smart_pause(5.0, 3.0)
                browser, context, page = launch_browser(pw)

        save_checkpoint(CHECKPOINT_FILE, {
            "last_date": date_iso,
            "total_records": total_records,
            "nb_reunions": len(reunions),
            "nb_courses": course_count,
            "status": "done",
        })

        log.info("=" * 60)
        log.info(f"TERMINE: {len(reunions)} reunions, {course_count} courses, {total_records} records")
        log.info(f"  -> {output_file}")
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
