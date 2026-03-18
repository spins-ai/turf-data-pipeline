#!/usr/bin/env python3
"""
Script 53 — Scraping Paris-Turf.com
Source : paris-turf.com/programme-des-courses/{date}
Collecte : prédictions experts, fiches courses, pronostics détaillés, cotes PMU
CRITIQUE pour : Expert Consensus, Race Cards, Odds Comparison
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "53_paris_turf"
OUTPUT_DIR = os.path.join("output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def smart_pause(base=3.0, jitter=1.5):
    """Pause adaptative avec jitter — Paris-Turf est plus sensible au rate limiting."""
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.1:
        pause += random.uniform(8, 20)
    time.sleep(max(1.5, pause))


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
                log.warning(f"  403 Forbidden sur {url}, pause 90s...")
                time.sleep(90)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Erreur réseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Échec après {max_retries} essais: {url}")
    return None


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def extract_embedded_json(soup, date_str, source="paris_turf"):
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
                    "scraped_at": datetime.utcnow().isoformat(),
                })
            except (json.JSONDecodeError, TypeError):
                pass
            continue
        if len(script_text) < 50:
            continue
        for kw in ["pronostic", "cheval", "course", "partant", "cote", "odds",
                    "runner", "race", "expert", "prediction", "terrain", "distance"]:
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
                            "scraped_at": datetime.utcnow().isoformat(),
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
                                "scraped_at": datetime.utcnow().isoformat(),
                            })
                    except json.JSONDecodeError:
                        pass
                break
    return records


def extract_data_attributes(soup, date_str, source="paris_turf"):
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
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for attr_name, attr_val in data_attrs.items():
                clean_name = attr_name.replace("data-", "").replace("-", "_")
                record[clean_name] = attr_val
            text = el.get_text(strip=True)
            if text and len(text) < 300:
                record["text_content"] = text
            records.append(record)
    return records


def extract_comments_analyses(soup, date_str, source="paris_turf"):
    """Extract comment and analysis divs."""
    records = []
    for el in soup.find_all(["div", "p", "section", "article", "blockquote"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "analyse", "expert", "avis",
                                                   "conseil", "editorial", "resume",
                                                   "verdict", "opinion", "recap",
                                                   "pronostic-text", "prediction"]):
            text = el.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "commentaire",
                    "contenu": text[:2000],
                    "classes_css": classes,
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                author_el = el.find(["span", "strong", "a"],
                                     class_=lambda c: c and any(kw in " ".join(c).lower()
                                                                for kw in ["author", "auteur", "expert", "name"]))
                if author_el:
                    record["auteur"] = author_el.get_text(strip=True)
                records.append(record)
    return records


def extract_terrain_distance_stats(soup, date_str, source="paris_turf"):
    """Extract terrain and distance statistics from Paris-Turf."""
    records = []
    for div in soup.find_all(["div", "section", "span", "td"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["terrain", "going", "ground",
                                                   "distance", "stat-terrain",
                                                   "stat-distance", "piste"]):
            text = div.get_text(strip=True)
            if text and 5 < len(text) < 1000:
                record = {
                    "date": date_str,
                    "source": source,
                    "type": "stats_terrain_distance",
                    "contenu": text[:500],
                    "classes_css": classes,
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                # Parse percentages
                pcts = re.findall(r'(\d{1,3})\s*%', text)
                if pcts:
                    record["pourcentages"] = pcts[:10]
                # Parse distances
                dists = re.findall(r'(\d[\d\s]*)\s*m', text)
                if dists:
                    record["distances_m"] = [d.replace(" ", "") for d in dists[:5]]
                records.append(record)
    return records


def scrape_programme_day(session, date_str):
    """Scraper le programme Paris-Turf d'un jour donné."""
    cache_file = os.path.join(CACHE_DIR, f"programme_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # Paris-Turf utilise le format dd-mm-yyyy dans certaines URLs
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    date_fr = date_obj.strftime("%d-%m-%Y")

    url = f"https://www.paris-turf.com/programme-des-courses/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        # Essayer format alternatif
        url = f"https://www.paris-turf.com/programme-des-courses/{date_fr}"
        resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []
    course_links = []

    # --- NEW: Full extraction pattern ---
    records.extend(extract_embedded_json(soup, date_str, "paris_turf"))
    records.extend(extract_data_attributes(soup, date_str, "paris_turf"))
    records.extend(extract_comments_analyses(soup, date_str, "paris_turf"))
    records.extend(extract_terrain_distance_stats(soup, date_str, "paris_turf"))

    # --- Extraire les réunions ---
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["reunion", "meeting", "hippodrome",
                                                   "race-card", "programme"]):
            record = {
                "date": date_str,
                "source": "paris_turf",
                "type": "reunion",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            title = div.find(["h2", "h3", "h4", "strong"])
            if title:
                record["hippodrome"] = title.get_text(strip=True)

            # Collecter les liens de courses
            for a in div.find_all("a", href=True):
                href = a["href"]
                if re.search(r'/course/|/pronostic/|/partants/', href):
                    full_url = href if href.startswith("http") else f"https://www.paris-turf.com{href}"
                    course_links.append(full_url)
                    record["url_course"] = full_url

            text = div.get_text(strip=True)
            if text and len(text) < 500:
                record["resume"] = text[:300]
            records.append(record)

    # --- Extraire les pronostics du jour ---
    for div in soup.find_all(["div", "p", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pronostic", "prediction", "tip",
                                                   "expert", "conseil", "selection"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 1500:
                records.append({
                    "date": date_str,
                    "source": "paris_turf",
                    "type": "pronostic_expert",
                    "contenu": text,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

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
                    "source": "paris_turf",
                    "type": "partant",
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                records.append(record)

    result = {"records": records, "course_links": list(set(course_links))}
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def scrape_course_predictions(session, course_url, date_str):
    """Scraper les prédictions détaillées d'une course."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"pred_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, course_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- NEW: Full extraction on course prediction page ---
    records.extend(extract_embedded_json(soup, date_str, "paris_turf"))
    records.extend(extract_data_attributes(soup, date_str, "paris_turf"))
    records.extend(extract_comments_analyses(soup, date_str, "paris_turf"))
    records.extend(extract_terrain_distance_stats(soup, date_str, "paris_turf"))

    # Nom du prix et conditions
    nom_prix = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_prix = text
            break

    # Conditions de course (distance, terrain, etc.)
    conditions = {}
    for div in soup.find_all(["div", "span", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if "distance" in classes.lower() or "distance" in text.lower():
            dist_match = re.search(r'(\d[\d\s]*)\s*m', text)
            if dist_match:
                conditions["distance_m"] = dist_match.group(1).replace(" ", "")
        if "terrain" in classes.lower() or "terrain" in text.lower():
            conditions["terrain"] = text
        if "dotation" in classes.lower() or "allocation" in text.lower():
            conditions["dotation"] = text

    # Commentaires experts détaillés
    for div in soup.find_all(["div", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["comment", "expert", "analyse",
                                                   "avis", "editorial"]):
            text = div.get_text(strip=True)
            if text and 30 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "paris_turf",
                    "type": "commentaire_expert",
                    "nom_prix": nom_prix,
                    "contenu": text,
                    "conditions": conditions,
                    "url_course": course_url,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

    # Table des partants avec cotes et pronostics
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
                "source": "paris_turf",
                "type": "partant_prediction",
                "nom_prix": nom_prix,
                "conditions": conditions,
                "url_course": course_url,
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

                # Extraire la cote si présente
                cote_match = re.search(r'(\d+[.,]\d+)', cell)
                if cote_match and "cote" not in record:
                    record["cote_paris_turf"] = cote_match.group(1).replace(",", ".")

            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 53 — Paris-Turf Scraper (experts, race cards)")
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
    log.info("SCRIPT 53 — Paris-Turf Scraper")
    log.info(f"  Période : {start_date.date()} → {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint()
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        if resume_date > start_date:
            start_date = resume_date
            log.info(f"  Reprise au checkpoint : {start_date.date()}")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "paris_turf_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        result = scrape_programme_day(session, date_str)

        if result:
            records = result.get("records", [])

            # Scraper les prédictions par course
            for curl in result.get("course_links", [])[:8]:
                detail = scrape_course_predictions(session, curl, date_str)
                if detail:
                    records.extend(detail)
                smart_pause(2.0, 1.0)

            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | jours={day_count} records={total_records}")
            save_checkpoint({"last_date": date_str, "total_records": total_records})

        if day_count % 60 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(8, 20))

        current += timedelta(days=1)
        smart_pause(1.5, 0.8)

    save_checkpoint({"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {day_count} jours, {total_records} records → {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
