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

import argparse
import json
import logging
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

SCRIPT_NAME = "54_turfinfo"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint

log = setup_logging("54_turfinfo")


def launch_browser(pw):
    """Launch headless Chromium with fr-FR locale and Chrome UA."""
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    )
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        locale="fr-FR",
        timezone_id="Europe/Paris",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        java_script_enabled=True,
        ignore_https_errors=True,
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        window.chrome = {runtime: {}};
    """)
    page = context.new_page()
    page.set_default_timeout(60000)
    log.info("Browser launched (headless Chromium)")
    return browser, context, page


def navigate_with_retry(page, url, retries=3):
    """Navigate to url with retry. Returns True on success."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=60000)
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
            return True
        except Exception as exc:
            log.warning("  Navigation error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(10 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return False


def accept_cookies(page):
    """Try to click a cookie-consent button."""
    selectors = [
        "button:has-text('Accepter')",
        "button:has-text('Tout accepter')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "[id*='accept']",
        "[class*='accept']",
        "#onetrust-accept-btn-handler",
        "#didomi-notice-agree-button",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=1500):
                btn.click(timeout=3000)
                log.info("  Cookies accepted via: %s", sel)
                time.sleep(1)
                return True
        except Exception:
            continue
    return False


def extract_embedded_json(soup, date_str, source="turfinfo"):
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
        for kw in ["course", "cheval", "partant", "musique", "cote", "resultat",
                    "pronostic", "reunion", "hippodrome"]:
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


def extract_data_attributes(soup, date_str, source="turfinfo"):
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
