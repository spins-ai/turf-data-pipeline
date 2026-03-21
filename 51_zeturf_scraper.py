#!/usr/bin/env python3
"""
Script 51 — Scraping ZeTurf.fr
Source : zeturf.fr/fr/course/{date}
Collecte : cotes, pronostics, données de course (partants, conditions, rapports)
CRITIQUE pour : Odds Model, Value Detection, Market Features
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
try:
    import cloudscraper
except ImportError:
    cloudscraper = None
from bs4 import BeautifulSoup

SCRIPT_NAME = "51_zeturf"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

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


def smart_pause(base=2.5, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.0, pause))


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
    """Ajouter un enregistrement JSONL (append mode)."""
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    """Charger le checkpoint de reprise."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    """Sauvegarder le checkpoint."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape_zeturf_day(session, date_str):
    """Scraper les courses ZeTurf pour un jour donné."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # URL corrigée : /fr/resultats-et-rapports-du-jour avec date en paramètre
    # Ou directement la page programme du jour
    url = f"https://www.zeturf.fr/fr/programme?date={date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        # Fallback: page résultats
        url = f"https://www.zeturf.fr/fr/resultats-et-rapports-du-jour?date={date_str}"
        resp = fetch_with_retry(session, url)
    if not resp:
        return None

    # Save raw HTML to cache
    html_file = os.path.join(HTML_CACHE_DIR, f"{date_str}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(resp.text)

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Extraire les réunions et courses ---
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["race", "course", "reunion", "programme"]):
            record = {
                "date": date_str,
                "source": "zeturf",
                "scraped_at": datetime.now().isoformat(),
            }

            # Titre / nom du prix
            title_el = section.find(["h2", "h3", "h4", "a"])
            if title_el:
                record["titre"] = title_el.get_text(strip=True)

            # Lien course
            link = section.find("a", href=True)
            if link:
                record["url_course"] = link["href"]

            records.append(record)

    # --- Extraire les tables de cotes ---
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 3:
                partant = {
                    "date": date_str,
                    "source": "zeturf",
                    "type": "cote_partant",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    partant[key] = cell

                # Chercher la cote dans les cellules
                for cell in cells:
                    cote_match = re.search(r'(\d+[.,]\d+)', cell)
                    if cote_match:
                        partant["cote_zeturf"] = cote_match.group(1).replace(",", ".")
                        break

                records.append(partant)

    # --- Extraire les pronostics ---
    for div in soup.find_all(["div", "span", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["prono", "tip", "favori", "prediction"]):
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "pronostic",
                    "contenu": text,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extraire les cotes depuis les data-attributes ---
    for el in soup.find_all(attrs={"data-odds": True}):
        records.append({
            "date": date_str,
            "source": "zeturf",
            "type": "cote_data",
            "odds": el.get("data-odds"),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- Extraire les commentaires de course ---
    for div in soup.find_all(["div", "p", "span", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["comment", "analyse", "avis", "editorial",
                                                  "recap", "resume", "chronique", "rapport"]):
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "commentaire_course",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extraire les JSON embarques dans les scripts ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        # JSON.parse(...) patterns
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        # window.__DATA or window.XXX = {...} patterns
        for m in re.finditer(r'window\[?[\'"]?(__\w+|raceData|courseData|betData|oddsData)[\'"]?\]?\s*=\s*(\{.+?\});',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "embedded_window_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        # var xxx = [...] or var xxx = {...} large data
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "embedded_var_array",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    # --- Extraire script type="application/json" ---
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "zeturf",
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Extraire TOUS les data-attributes pertinents ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["cote", "odd", "cheval", "horse", "runner", "race", "pari", "bet", "mise"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "zeturf",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Extraire les cotes detaillees par type de pari ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pari", "bet-type", "simple", "couple", "tierce",
                                                  "quarte", "quinte", "multi", "2sur4"]):
            bet_type = classes
            for sub in div.find_all(["tr", "li", "div"]):
                text = sub.get_text(strip=True)
                if text and re.search(r'\d', text) and 3 < len(text) < 500:
                    records.append({
                        "date": date_str,
                        "source": "zeturf",
                        "type": "cote_par_pari",
                        "bet_type_class": bet_type,
                        "contenu": text,
                        "scraped_at": datetime.now().isoformat(),
                    })

    # Sauvegarder cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_course_detail(session, course_url, date_str):
    """Scraper le détail d'une course individuelle pour les cotes précises."""
    if not course_url.startswith("http"):
        course_url = f"https://www.zeturf.fr{course_url}"

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, course_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    partants = []

    # Extraire le nom du prix
    nom_prix = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            nom_prix = text
            break

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
            partant = {
                "date": date_str,
                "source": "zeturf",
                "type": "partant_detail",
                "nom_prix": nom_prix,
                "url_course": course_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                partant[key] = cell
            partants.append(partant)

    # --- Commentaires de course dans la page detail ---
    for div in soup.find_all(["div", "p", "article", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["comment", "analyse", "avis", "recap",
                                                  "rapport", "resume", "editorial", "verdict"]):
            if text and 20 < len(text) < 3000:
                partants.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "commentaire_detail",
                    "nom_prix": nom_prix,
                    "contenu": text[:2500],
                    "url_course": course_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Historique forme du cheval (dernieres courses) ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "historique", "dernieres", "palmares",
                                                  "previous", "last-runs", "perf"]):
            # Extract from sub-table if present
            sub_table = div.find("table")
            if sub_table:
                rows = sub_table.find_all("tr")
                sub_headers = []
                if rows:
                    sub_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                                   for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        entry = {
                            "date": date_str,
                            "source": "zeturf",
                            "type": "historique_forme",
                            "nom_prix": nom_prix,
                            "url_course": course_url,
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = sub_headers[j] if j < len(sub_headers) and sub_headers[j] else f"col_{j}"
                            entry[key] = cell
                        partants.append(entry)
            else:
                # Extract as text block
                text = div.get_text(strip=True)
                if text and 10 < len(text) < 2000:
                    partants.append({
                        "date": date_str,
                        "source": "zeturf",
                        "type": "historique_forme_text",
                        "nom_prix": nom_prix,
                        "contenu": text[:1500],
                        "url_course": course_url,
                        "scraped_at": datetime.now().isoformat(),
                    })

    # --- JSON embarque dans la page detail ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                partants.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "detail_embedded_json",
                    "nom_prix": nom_prix,
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(r'window\[?[\'"]?(__\w+|raceData|courseData|partants|runners)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                partants.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "detail_window_data",
                    "var_name": m.group(1),
                    "nom_prix": nom_prix,
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            partants.append({
                "date": date_str,
                "source": "zeturf",
                "type": "detail_script_json",
                "data_id": script.get("id", ""),
                "nom_prix": nom_prix,
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Video/photo metadata ---
    for el in soup.find_all(["video", "source", "iframe", "img"]):
        src = el.get("src") or el.get("data-src") or el.get("data-video-url") or ""
        if src and any(kw in src.lower() for kw in ["replay", "video", "course", "stream", "mp4", "m3u8"]):
            partants.append({
                "date": date_str,
                "source": "zeturf",
                "type": "video_metadata",
                "nom_prix": nom_prix,
                "media_url": src,
                "media_tag": el.name,
                "poster": el.get("poster", ""),
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(partants, f, ensure_ascii=False, indent=2)

    return partants


def main():
    parser = argparse.ArgumentParser(description="Script 51 — ZeTurf Scraper (cotes, pronostics, courses)")
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
    log.info("SCRIPT 51 — ZeTurf Scraper")
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
    output_file = os.path.join(OUTPUT_DIR, "zeturf_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        records = scrape_zeturf_day(session, date_str)

        if records:
            # Scraper les détails des courses trouvées
            course_urls = [r.get("url_course") for r in records
                           if r.get("url_course") and r.get("type") != "partant_detail"]
            for curl in set(filter(None, course_urls)):
                detail = scrape_course_detail(session, curl, date_str)
                if detail:
                    records.extend(detail)
                smart_pause(1.5, 0.8)

            # Écrire en JSONL
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
