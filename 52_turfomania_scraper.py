#!/usr/bin/env python3
"""
Script 52 — Scraping Turfomania.fr (corrigé v3)
Source : turfomania.fr — pronostics, partants, stats
Flux :
  1) /partants-programmes/ → Schema.org JSON-LD → URLs detail-reunion.php?idreunion=XXX
  2) detail-reunion.php → tables avec partants + liens /pronostics/partants-...?idcourse=XXX
  3) Page course individuelle → table détaillée des partants + pronostics
Utilise cloudscraper pour contourner Cloudflare.
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime

try:
    import cloudscraper
except ImportError:
    cloudscraper = None
from bs4 import BeautifulSoup

SCRIPT_NAME = "52_turfomania"
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

BASE_URL = "https://www.turfomania.fr"


def new_session():
    if cloudscraper:
        return cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    return s


def smart_pause(base=2.5, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.0, pause))


def fetch_with_retry(session, url, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(60 * attempt)
                continue
            if resp.status_code == 403:
                time.sleep(30)
                continue
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt})")
                time.sleep(5 * attempt)
                continue
            return resp
        except Exception as e:
            log.warning(f"  Erreur: {e} (essai {attempt})")
            time.sleep(5 * attempt)
    return None


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_reunion_urls(session):
    """Etape 1: Extraire les URLs de reunions depuis /partants-programmes/ via Schema.org JSON-LD."""
    resp = fetch_with_retry(session, f"{BASE_URL}/partants-programmes/")
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
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


def scrape_reunion(session, reunion_info, date_iso, output_file):
    """Etape 2: Scraper une page de reunion — extraire les tables de partants + liens courses."""
    id_reunion = reunion_info["id_reunion"]
    cache_file = os.path.join(CACHE_DIR, f"reunion_{id_reunion}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_records", 0), cached.get("course_links", [])

    resp = fetch_with_retry(session, reunion_info["url"])
    if not resp:
        return 0, []

    soup = BeautifulSoup(resp.text, "html.parser")
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


def scrape_course(session, course_info, date_iso, output_file):
    """Etape 3: Scraper une course individuelle — partants detailles + pronostics."""
    id_course = course_info["id_course"]
    cache_file = os.path.join(CACHE_DIR, f"course_{id_course}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
            return cached.get("nb_records", 0)

    resp = fetch_with_retry(session, course_info["url"])
    if not resp:
        return 0

    soup = BeautifulSoup(resp.text, "html.parser")
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
    parser = argparse.ArgumentParser(description="Script 52 — Turfomania Scraper v3")
    parser.add_argument("--max-courses", type=int, default=200,
                        help="Max courses individuelles a scraper")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 52 — Turfomania Scraper v3 (cloudscraper)")
    log.info("=" * 60)

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "turfomania_data.jsonl")
    date_iso = datetime.now().strftime("%Y-%m-%d")

    # Etape 1: Recuperer les reunions
    log.info("  Etape 1: Recuperation des reunions...")
    reunions = get_reunion_urls(session)
    log.info(f"  {len(reunions)} reunions trouvees")

    total_records = 0
    all_course_links = []

    # Etape 2: Scraper chaque reunion
    log.info("  Etape 2: Scraping des reunions...")
    for i, reunion in enumerate(reunions):
        nb, courses = scrape_reunion(session, reunion, date_iso, output_file)
        total_records += nb
        all_course_links.extend(courses)
        log.info(f"    Reunion {i+1}/{len(reunions)}: {reunion.get('name', reunion['id_reunion'])} "
                 f"-> {nb} records, {len(courses)} courses")
        smart_pause(2.0, 1.0)

        if (i + 1) % 10 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(3, 6))

    log.info(f"  Total reunions: {total_records} records, {len(all_course_links)} courses individuelles")

    # Etape 3: Scraper les courses individuelles
    log.info("  Etape 3: Scraping des courses individuelles...")
    course_count = 0
    for i, course in enumerate(all_course_links[:args.max_courses]):
        nb = scrape_course(session, course, date_iso, output_file)
        total_records += nb
        course_count += 1
        if (i + 1) % 10 == 0:
            log.info(f"    {i+1}/{min(len(all_course_links), args.max_courses)} courses, "
                     f"{total_records} records total")
        smart_pause(1.5, 0.8)

        if (i + 1) % 30 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 10))

    save_checkpoint({
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


if __name__ == "__main__":
    main()
