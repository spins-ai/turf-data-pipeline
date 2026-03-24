#!/usr/bin/env python3
"""
Script 104 — Scraping TurfPronos.fr (Playwright)

Source : turfpronos.fr — pronostics hippiques du jour
Flux :
  1) Page d'accueil / pronostics du jour -> liens vers les reunions et courses
  2) Pages de pronostics -> selections, bases, complements, analyses

Usage:
    pip install playwright beautifulsoup4
    playwright install chromium
    python 104_turfpronos_scraper.py --max-courses 200
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

SCRIPT_NAME = "104_turfpronos"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.playwright import launch_browser, navigate_with_retry, accept_cookies
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("104_turfpronos")

BASE_URL = "https://www.turfpronos.fr"


# ======================================================================
# Etape 1 : Recuperer les liens de reunions et courses du jour
# ======================================================================

def get_reunion_and_course_links(page):
    """Extraire les liens vers les reunions et courses depuis la page d'accueil."""
    urls_to_try = [
        f"{BASE_URL}/pronostics",
        f"{BASE_URL}/pronostics-du-jour",
        f"{BASE_URL}/programme",
        BASE_URL,
    ]

    reunions = []
    courses = []
    seen_urls = set()

    for start_url in urls_to_try:
        if not navigate_with_retry(page, start_url):
            continue
        accept_cookies(page)
        soup = BeautifulSoup(page.content(), "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)[:300]

            # Normaliser l'URL
            if href.startswith("/"):
                full_url = BASE_URL + href
            elif href.startswith("http"):
                full_url = href
            else:
                full_url = BASE_URL + "/" + href

            if full_url in seen_urls or BASE_URL not in full_url:
                continue

            href_lower = href.lower()

            # Liens de reunions
            if any(kw in href_lower for kw in ["reunion", "hippodrome", "r1", "r2", "r3", "r4", "r5"]):
                seen_urls.add(full_url)
                reunions.append({
                    "url": full_url,
                    "titre": text,
                    "type": "reunion",
                })

            # Liens de courses / pronostics
            elif any(kw in href_lower for kw in [
                "pronostic", "course", "quinte", "tierce", "quarte",
                "partant", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8",
            ]):
                seen_urls.add(full_url)
                courses.append({
                    "url": full_url,
                    "titre": text,
                    "type": "course",
                })

        if reunions or courses:
            break
        smart_pause(2.0, 1.0)

    return reunions, courses


# ======================================================================
# Etape 2 : Scraper une page de reunion
# ======================================================================

def scrape_reunion_page(page, reunion_info, date_iso, output_file):
    """Scraper une page de reunion — extraire les liens de courses et infos."""
    url = reunion_info["url"]
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', url)[-80:]
    cache_file = os.path.join(CACHE_DIR, f"reunion_{url_hash}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_records", 0), cached.get("course_links", [])

    if not navigate_with_retry(page, url):
        return 0, []

    soup = BeautifulSoup(page.content(), "html.parser")
    records = []
    course_links = []
    seen_courses = set()

    # Info reunion
    reunion_record = {
        "date": date_iso,
        "source": "turfpronos",
        "type": "reunion",
        "url": url,
        "titre_lien": reunion_info.get("titre", ""),
        "scraped_at": datetime.now().isoformat(),
    }
    h1 = soup.find("h1")
    if h1:
        reunion_record["titre"] = h1.get_text(strip=True)[:500]
    records.append(reunion_record)

    # Extraire les liens vers les courses individuelles
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)[:300]
        href_lower = href.lower()

        if any(kw in href_lower for kw in ["course", "pronostic", "c1", "c2", "c3", "c4",
                                            "c5", "c6", "c7", "c8", "partant"]):
            if href.startswith("/"):
                full_url = BASE_URL + href
            elif href.startswith("http"):
                full_url = href
            else:
                full_url = BASE_URL + "/" + href

            if full_url not in seen_courses and BASE_URL in full_url:
                seen_courses.add(full_url)
                course_links.append({
                    "url": full_url,
                    "titre": text,
                    "type": "course",
                })

    # Tables de la page reunion
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [
            th.get_text(strip=True).lower().replace(" ", "_").replace("\u00b0", "")
            for th in rows[0].find_all(["th", "td"])
        ]
        if len(headers) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True)[:500] for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                entry = {
                    "date": date_iso,
                    "source": "turfpronos",
                    "type": "reunion_row",
                    "url": url,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # Sauvegarder
    for rec in records:
        append_jsonl(output_file, rec)

    with open(cache_file, "w", encoding="utf-8", newline="\n") as f:
        json.dump({
            "url": url,
            "nb_records": len(records),
            "nb_courses": len(course_links),
            "course_links": course_links,
            "date": date_iso,
        }, f, ensure_ascii=False)

    return len(records), course_links


# ======================================================================
# Etape 3 : Scraper une page de course / pronostics
# ======================================================================

def scrape_course_page(page, course_info, date_iso, output_file):
    """Scraper une page de course — partants detailles + pronostics."""
    url = course_info["url"]
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', url)[-80:]
    cache_file = os.path.join(CACHE_DIR, f"course_{url_hash}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_records", 0)

    if not navigate_with_retry(page, url):
        return 0

    soup = BeautifulSoup(page.content(), "html.parser")
    records = []

    # Info course
    course_record = {
        "date": date_iso,
        "source": "turfpronos",
        "type": "course",
        "url": url,
        "titre_lien": course_info.get("titre", ""),
        "scraped_at": datetime.now().isoformat(),
    }
    h1 = soup.find("h1")
    if h1:
        course_record["titre"] = h1.get_text(strip=True)[:500]
    records.append(course_record)

    # Tables de partants
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [
            th.get_text(strip=True).lower().replace(" ", "_").replace("\u00b0", "")
            for th in rows[0].find_all(["th", "td"])
        ]
        if len(headers) < 2:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True)[:500] for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                entry = {
                    "date": date_iso,
                    "source": "turfpronos",
                    "type": "partant",
                    "url": url,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # Pronostics (divs, spans, etc. avec classes pertinentes)
    prono_keywords = [
        "prono", "tip", "selection", "favori", "base",
        "complement", "analyse", "avis", "quinte", "tierce",
        "quarte", "prediction", "conseil", "pick",
    ]
    for elem in soup.find_all(["div", "span", "p", "li", "article", "section"], class_=True):
        classes = " ".join(elem.get("class", []))
        text = elem.get_text(strip=True)
        if any(kw in classes.lower() for kw in prono_keywords):
            if text and 5 < len(text) < 2000:
                records.append({
                    "date": date_iso,
                    "source": "turfpronos",
                    "type": "pronostic",
                    "url": url,
                    "contenu": text[:1500],
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Pronostics dans des listes ordonnees (classement des chevaux)
    for ol in soup.find_all("ol"):
        items = ol.find_all("li")
        if items and len(items) >= 3:
            selections = []
            for li in items:
                txt = li.get_text(strip=True)
                if txt and len(txt) < 200:
                    selections.append(txt)
            if selections:
                records.append({
                    "date": date_iso,
                    "source": "turfpronos",
                    "type": "pronostic_liste",
                    "url": url,
                    "selections": selections,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Sauvegarder
    for rec in records:
        append_jsonl(output_file, rec)

    with open(cache_file, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"url": url, "nb_records": len(records), "date": date_iso},
                  f, ensure_ascii=False)

    return len(records)


# ======================================================================
# Main
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="Script 104 — TurfPronos Scraper (Playwright)")
    parser.add_argument("--max-courses", type=int, default=200,
                        help="Max courses individuelles a scraper")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 104 — TurfPronos Scraper (Playwright)")
    log.info("=" * 60)

    output_file = os.path.join(OUTPUT_DIR, "turfpronos_data.jsonl")
    date_iso = datetime.now().strftime("%Y-%m-%d")

    # Charger checkpoint pour resume
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    start_index = 0
    if checkpoint.get("last_date") == date_iso and checkpoint.get("status") != "done":
        start_index = checkpoint.get("last_index", 0)
        log.info(f"  Reprise depuis l'index {start_index} (checkpoint du {date_iso})")

    pw = sync_playwright().start()
    browser, context, page = launch_browser(pw)

    try:
        # Etape 1: Recuperer les liens
        log.info("  Etape 1: Recuperation des reunions et courses...")
        reunions, direct_courses = get_reunion_and_course_links(page)
        log.info(f"  {len(reunions)} reunions, {len(direct_courses)} courses directes trouvees")

        total_records = 0
        all_course_links = list(direct_courses)

        # Etape 2: Scraper les reunions
        log.info("  Etape 2: Scraping des reunions...")
        for i, reunion in enumerate(reunions):
            nb, courses = scrape_reunion_page(page, reunion, date_iso, output_file)
            total_records += nb
            all_course_links.extend(courses)
            log.info(f"    Reunion {i+1}/{len(reunions)}: {reunion.get('titre', '?')[:50]} "
                     f"-> {nb} records, {len(courses)} courses")
            smart_pause(2.0, 1.0)

            if (i + 1) % 10 == 0:
                log.info("  Rotating browser context...")
                context.close()
                browser.close()
                smart_pause(3.0, 2.0)
                browser, context, page = launch_browser(pw)

        log.info(f"  Total apres reunions: {total_records} records, "
                 f"{len(all_course_links)} courses a scraper")

        # Dedupliquer les cours par URL
        seen_urls = set()
        unique_courses = []
        for c in all_course_links:
            if c["url"] not in seen_urls:
                seen_urls.add(c["url"])
                unique_courses.append(c)
        all_course_links = unique_courses

        # Etape 3: Scraper les courses individuelles
        log.info("  Etape 3: Scraping des courses individuelles...")
        courses_to_scrape = all_course_links[start_index:args.max_courses]
        course_count = 0
        for i, course in enumerate(courses_to_scrape):
            nb = scrape_course_page(page, course, date_iso, output_file)
            total_records += nb
            course_count += 1
            actual_index = start_index + i

            if (i + 1) % 10 == 0:
                log.info(f"    {i+1}/{len(courses_to_scrape)} courses, "
                         f"{total_records} records total")
                save_checkpoint(CHECKPOINT_FILE, {
                    "last_date": date_iso,
                    "last_index": actual_index + 1,
                    "total_records": total_records,
                    "status": "in_progress",
                })

            smart_pause(1.5, 0.8)

            if (i + 1) % 30 == 0:
                log.info("  Rotating browser context...")
                context.close()
                browser.close()
                smart_pause(5.0, 3.0)
                browser, context, page = launch_browser(pw)

        # Checkpoint final
        save_checkpoint(CHECKPOINT_FILE, {
            "last_date": date_iso,
            "total_records": total_records,
            "nb_reunions": len(reunions),
            "nb_courses": course_count,
            "status": "done",
        })

        log.info("=" * 60)
        log.info(f"TERMINE: {len(reunions)} reunions, {course_count} courses, "
                 f"{total_records} records")
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
