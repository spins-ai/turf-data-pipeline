#!/usr/bin/env python3
"""
Script 55 — Scraping Equidia.fr
Source : equidia.fr/courses/{date}
Collecte : stats terrain, données vidéo/replay, résumés, indices de forme
CRITIQUE pour : Terrain Features, Video Analysis Metadata, Track Conditions
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

SCRIPT_NAME = "55_equidia_data"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause

log = setup_logging("55_equidia_data")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def new_session():
    s = cloudscraper.create_scraper() if cloudscraper else requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s



def fetch_with_retry(session, url, max_retries=3, timeout=30):
    """GET avec retry automatique (3 essais puis skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Too Many Requests, pause {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden sur {url}, pause 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except Exception as e:
            log.warning(f"  Erreur réseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Échec après {max_retries} essais: {url}")
    return None


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def extract_embedded_json(soup, date_str, source="equidia"):
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
        for kw in ["course", "cheval", "partant", "terrain", "piste", "video",
                    "replay", "reunion", "hippodrome", "programme"]:
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


def extract_data_attributes(soup, date_str, source="equidia"):
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


def scrape_equidia_day(session, date_str):
    """Scraper les données Equidia pour un jour donné."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"https://www.equidia.fr/courses/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(resp.text)

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []
    course_links = []

    # --- NEW: Full extraction pattern ---
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


def scrape_course_detail(session, course_url, date_str):
    """Scraper le détail d'une course Equidia (terrain, stats, vidéo metadata)."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, course_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on course detail page ---
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
    log.info("SCRIPT 55 — Equidia Data Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "equidia_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        result = scrape_equidia_day(session, date_str)

        if result:
            records = result.get("records", [])

            # Scraper le détail de chaque course
            for curl in result.get("course_links", [])[:10]:
                detail = scrape_course_detail(session, curl, date_str)
                if detail:
                    records.extend(detail)
                smart_pause(1.5, 0.8)

            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | jours={day_count} records={total_records}")
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
    log.info(f"TERMINÉ: {day_count} jours, {total_records} records → {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
