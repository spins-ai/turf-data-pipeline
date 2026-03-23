#!/usr/bin/env python3
"""
Script 55 — Scraping Equidia.fr (Playwright version)
Source : equidia.fr/courses/{date}
Collecte : stats terrain, données vidéo/replay, résumés, indices de forme
CRITIQUE pour : Terrain Features, Video Analysis Metadata, Track Conditions

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from utils.playwright import launch_browser, accept_cookies

SCRIPT_NAME = "55_equidia_data"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.html_parsing import extract_embedded_json, extract_data_attributes

log = setup_logging("55_equidia_data")

MAX_RETRIES = 3








# NOTE: Local version kept because it returns HTML string (page.content()) instead of bool
def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to url with retry logic. Returns HTML string or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_MS)
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
            return page.content()
        except PlaywrightTimeout:
            log.warning("  Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(10 * attempt)
        except Exception as exc:
            log.warning("  Navigation error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


# ------------------------------------------------------------------
# Extraction helpers (BeautifulSoup-based)
# extract_embedded_json and extract_data_attributes are now in utils.html_parsing
# ------------------------------------------------------------------

def extract_comments_analyses(soup, date_str, source="equidia"):
    """Extract comment and analysis divs."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "analyse", "resume",
                                                   "description", "recap", "editorial",
                                                   "expert", "avis"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "commentaire",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                records.append(record)
    return records


def extract_terrain_piste_detail(soup, date_str, source="equidia"):
    """Extract detailed terrain/track data from Equidia."""
    records = []
    for div in soup.find_all(["div", "section", "span", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["terrain", "piste", "track",
                                                   "ground", "rail", "corde",
                                                   "parcours", "penetrometre"]):
            text = div.get_text(strip=True)
            if text and 3 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "terrain_detail",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Parse penetrometre value
                pene_match = re.search(r'p[eé]n[eé]trom[eè]tre\s*:?\s*(\d+[.,]?\d*)', text, re.I)
                if pene_match:
                    record["penetrometre"] = pene_match.group(1).replace(",", ".")
                records.append(record)
    return records


def extract_video_metadata(soup, date_str, source="equidia"):
    """Extract detailed video/replay metadata from Equidia."""
    records = []
    # Video players and iframes
    for el in soup.find_all(["video", "iframe", "source", "div"], class_=True):
        classes = " ".join(el.get("class", [])) if el.get("class") else ""
        is_video = el.name in ("video", "iframe", "source")
        is_video_div = any(kw in classes.lower() for kw in ["video", "replay", "player",
                                                              "media", "stream"])
        if not (is_video or is_video_div):
            continue
        record = {
            "date": date_str,
            "source": source,
            "type": "video_detail",
            "tag": el.name,
            "scraped_at": datetime.now().isoformat(),
        }
        for attr in ["src", "data-src", "data-video-id", "data-video-url",
                      "data-race-id", "data-duration", "data-title",
                      "data-thumbnail", "data-poster", "poster",
                      "data-event-id", "data-replay-url"]:
            val = el.get(attr)
            if val:
                clean = attr.replace("data-", "").replace("-", "_")
                record[clean] = val
        title = el.get_text(strip=True)
        if title and len(title) < 300:
            record["titre"] = title
        if len(record) > 4:  # Has meaningful data beyond base fields
            records.append(record)
    return records


# ------------------------------------------------------------------
# Main scraping functions
# ------------------------------------------------------------------

def scrape_equidia_day(page, date_str):
    """Scraper les données Equidia pour un jour donné."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.equidia.fr/courses/{date_str}"
    html = navigate_with_retry(page, url)
    if not html:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    soup = BeautifulSoup(html, "html.parser")
    records = []
    course_links = []

    # --- Full extraction pattern ---
    records.extend(extract_embedded_json(soup, date_str, "equidia"))
    records.extend(extract_data_attributes(soup, date_str, "equidia"))
    records.extend(extract_comments_analyses(soup, date_str, "equidia"))
    records.extend(extract_terrain_piste_detail(soup, date_str, "equidia"))
    records.extend(extract_video_metadata(soup, date_str, "equidia"))

    # --- Extraire les réunions et hippodromes ---
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["reunion", "meeting", "hippodrome",
                                                   "course", "race", "programme"]):
            record = {
                "date": date_str,
                "source": "equidia",
                "type": "reunion",
                "scraped_at": datetime.now().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong"])
            if title:
                record["hippodrome"] = title.get_text(strip=True)

            # Terrain info
            for span in div.find_all(["span", "small", "em", "p"]):
                text = span.get_text(strip=True)
                if re.search(r'terrain|piste|sol', text, re.I):
                    record["terrain_info"] = text
                elif re.search(r'(bon|souple|très souple|collant|lourd|léger|sec)',
                               text, re.I):
                    record["etat_terrain"] = text
                elif re.search(r'corde\s*(à\s*)?(droite|gauche)', text, re.I):
                    record["corde"] = text

            # Liens vers les courses
            for a in div.find_all("a", href=True):
                href = a["href"]
                if re.search(r'/course/|/replay/|/partants/|/programme/', href):
                    full_url = href if href.startswith("http") else f"https://www.equidia.fr{href}"
                    course_links.append(full_url)

            records.append(record)

    # --- Extraire les données terrain depuis les meta-infos ---
    for div in soup.find_all(["div", "span", "p"]):
        text = div.get_text(strip=True)
        terrain_match = re.search(
            r'(terrain|piste|sol)\s*:?\s*(bon|souple|très souple|collant|lourd|léger|sec|'
            r'pénétrant|très léger)',
            text, re.I
        )
        if terrain_match:
            records.append({
                "date": date_str,
                "source": "equidia",
                "type": "terrain",
                "etat_terrain": terrain_match.group(2).strip(),
                "contexte": text[:200],
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Extraire les vidéos / replays (métadonnées uniquement) ---
    for el in soup.find_all(["a", "div", "iframe"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["video", "replay", "player", "media"]):
            video_record = {
                "date": date_str,
                "source": "equidia",
                "type": "video_metadata",
                "scraped_at": datetime.now().isoformat(),
            }
            # URL de la vidéo ou du replay
            href = el.get("href") or el.get("src") or el.get("data-src")
            if href:
                video_record["video_url"] = href
            title_text = el.get_text(strip=True)
            if title_text:
                video_record["titre_video"] = title_text[:200]
            # Data attributes
            for attr in ["data-video-id", "data-race-id", "data-duration"]:
                val = el.get(attr)
                if val:
                    video_record[attr.replace("data-", "")] = val
            records.append(video_record)

    # --- Tables de données ---
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
                    "source": "equidia",
                    "type": "stats_course",
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
    """Scraper le détail d'une course Equidia (terrain, stats, vidéo metadata)."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    html = navigate_with_retry(page, course_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # --- Full extraction on course detail page ---
    records.extend(extract_embedded_json(soup, date_str, "equidia"))
    records.extend(extract_data_attributes(soup, date_str, "equidia"))
    records.extend(extract_comments_analyses(soup, date_str, "equidia"))
    records.extend(extract_terrain_piste_detail(soup, date_str, "equidia"))
    records.extend(extract_video_metadata(soup, date_str, "equidia"))

    # Titre
    nom_prix = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_prix = text
            break

    # Conditions détaillées
    conditions = {}
    page_text = soup.get_text()

    dist_match = re.search(r'(\d[\d\s]*)\s*m(?:ètre)?', page_text)
    if dist_match:
        conditions["distance_m"] = dist_match.group(1).replace(" ", "")

    terrain_match = re.search(
        r'(terrain|piste|sol)\s*:?\s*(bon|souple|très souple|collant|lourd|léger|sec|'
        r'pénétrant|très léger)',
        page_text, re.I
    )
    if terrain_match:
        conditions["etat_terrain"] = terrain_match.group(2).strip()

    disc_match = re.search(r'(trot attelé|trot monté|plat|haies|steeple|cross)',
                           page_text, re.I)
    if disc_match:
        conditions["discipline"] = disc_match.group(1)

    corde_match = re.search(r'corde\s*(à\s*)?(droite|gauche)', page_text, re.I)
    if corde_match:
        conditions["corde"] = corde_match.group(2)

    # Partants
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
                "source": "equidia",
                "type": "partant_detail",
                "nom_prix": nom_prix,
                "conditions": conditions,
                "url_course": course_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Résumé vidéo / commentaire de course
    for div in soup.find_all(["div", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "resume", "analyse",
                                                   "description", "recap"]):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "equidia",
                    "type": "resume_course",
                    "nom_prix": nom_prix,
                    "contenu": text,
                    "conditions": conditions,
                    "url_course": course_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Replay metadata (URLs vidéo, durées)
    for el in soup.find_all(["video", "iframe", "source"]):
        src = el.get("src") or el.get("data-src")
        if src:
            records.append({
                "date": date_str,
                "source": "equidia",
                "type": "replay_url",
                "nom_prix": nom_prix,
                "video_src": src,
                "conditions": conditions,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 55 — Equidia Scraper (vidéo metadata, stats terrain, données course)"
    )
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), défaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 55 — Equidia Data Scraper (Playwright)")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "equidia_data.jsonl")

    pw = sync_playwright().start()
    browser, context, page = None, None, None
    try:
        browser, context, page = launch_browser(pw)
        log.info("Browser launched (headless Chromium)")

        # Accept cookies on first navigation
        first_nav = True

        current = start_date
        day_count = 0
        total_records = 0

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            result = scrape_equidia_day(page, date_str)

            if first_nav and result is not None:
                accept_cookies(page)
                first_nav = False

            if result:
                records = result.get("records", [])

                # Scraper le détail de chaque course
                for curl in result.get("course_links", [])[:10]:
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

            current += timedelta(days=1)
            smart_pause(1.0, 0.5)

        save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                         "total_records": total_records, "status": "done"})

        log.info("=" * 60)
        log.info(f"TERMINÉ: {day_count} jours, {total_records} records → {output_file}")
        log.info("=" * 60)

    finally:
        # Graceful cleanup
        try:
            if page and not page.is_closed():
                page.close()
        except Exception:
            pass
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
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
