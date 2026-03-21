#!/usr/bin/env python3
"""
Script 80 — Scraping france-galop.com (donnees officielles courses FR)
Source : france-galop.com
Collecte : resultats officiels, classements, programmes, statistiques,
           fiches chevaux, fiches proprietaires, fiches eleveurs,
           calendrier, allocations, terrains officiels
CRITIQUE pour : Source officielle FR, Ground Truth, Validation Pipeline
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

SCRIPT_NAME = "80_france_galop"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
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

BASE_URL = "https://www.france-galop.com"

# Hippodromes majeurs France Galop
HIPPODROMES_FR = [
    "longchamp", "chantilly", "deauville", "saint-cloud", "auteuil",
    "maisons-laffitte", "enghien", "vincennes", "fontainebleau",
    "compiegne", "lyon-parilly", "marseille-borely", "bordeaux-le-bouscat",
    "toulouse", "strasbourg", "vichy", "clairefontaine", "craon",
    "le-lion-dangers", "nantes", "pau", "cagnes-sur-mer", "mont-de-marsan",
    "dieppe", "cabourg", "royan-la-palmyre", "la-teste-de-buch",
    "le-mans", "angers", "cholet", "nancy", "moulins",
]

# Types de courses
RACE_TYPES = ["plat", "obstacles", "haies", "steeple-chase", "cross-country"]


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
        except requests.RequestException as e:
            log.warning(f"  Erreur reseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Echec apres {max_retries} essais: {url}")
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


def scrape_programme_jour(session, date_str):
    """Scraper le programme des courses pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"prog_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    date_fmt = date_str.replace("-", "")
    urls_to_try = [
        f"{BASE_URL}/fr/courses/programme/{date_str}",
        f"{BASE_URL}/fr/programme/{date_str}",
        f"{BASE_URL}/courses/programme?date={date_str}",
        f"{BASE_URL}/fr/courses/resultats/{date_str}",
    ]

    soup = None
    used_url = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            used_url = url
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []

    # Extraire les reunions
    for section in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["reunion", "meeting", "programme", "fixture"]):
            record = {
                "source": "france_galop",
                "date": date_str,
                "type": "reunion",
                "scraped_at": datetime.now().isoformat(),
            }

            title = section.find(["h2", "h3", "h4"])
            if title:
                record["reunion_titre"] = title.get_text(strip=True)

            # Hippodrome
            for hippo in HIPPODROMES_FR:
                if hippo.lower().replace("-", " ") in section.get_text().lower():
                    record["hippodrome"] = hippo
                    break

            # Terrain
            terrain_el = section.find(string=re.compile(r'(terrain|sol|going)', re.I))
            if terrain_el:
                parent = terrain_el.find_parent()
                if parent:
                    record["terrain"] = parent.get_text(strip=True)

            records.append(record)

    # Extraire les courses depuis les tables
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
                "source": "france_galop",
                "date": date_str,
                "type": "course_programme",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Lien course
            link = row.find("a", href=True)
            if link:
                record["url_course"] = link["href"] if link["href"].startswith("http") else f"{BASE_URL}{link['href']}"

            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_resultats_jour(session, date_str):
    """Scraper les resultats officiels pour un jour donne."""
    cache_file = os.path.join(CACHE_DIR, f"res_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/fr/courses/resultats/{date_str}",
        f"{BASE_URL}/fr/resultats/{date_str}",
        f"{BASE_URL}/courses/resultats?date={date_str}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []

    # Extraire les resultats depuis les tables
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
                "source": "france_galop",
                "date": date_str,
                "type": "resultat_officiel",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extraire position d'arrivee
            for cell in cells[:3]:
                pos_match = re.match(r'^(\d+)(er|e|eme|ème)?$', cell.strip())
                if pos_match:
                    record["position"] = int(pos_match.group(1))
                    break

            # Extraire allocation
            for cell in cells:
                alloc_match = re.search(r'([\d\s.,]+)\s*(EUR|€)', cell)
                if alloc_match:
                    val = alloc_match.group(1).replace(" ", "").replace(".", "").replace(",", ".")
                    try:
                        record["allocation_eur"] = float(val)
                    except ValueError:
                        pass
                    break

            # Extraire le temps
            for cell in cells:
                time_match = re.search(r"(\d+)['\u2019](\d{2})[\"″](\d+)?", cell)
                if time_match:
                    minutes = int(time_match.group(1))
                    seconds = int(time_match.group(2))
                    hundredths = int(time_match.group(3)) if time_match.group(3) else 0
                    record["temps_brut"] = cell
                    record["temps_secondes"] = minutes * 60 + seconds + hundredths / 100.0
                    break

            records.append(record)

    # Extraire les resultats depuis les cartes
    for card in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(card.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "resultat", "arrivee", "classement"]):
            record = {
                "source": "france_galop",
                "date": date_str,
                "type": "resultat_card",
                "scraped_at": datetime.now().isoformat(),
            }

            # Nom du prix
            title = card.find(["h3", "h4", "strong"])
            if title:
                record["nom_prix"] = title.get_text(strip=True)

            # Hippodrome
            for hippo in HIPPODROMES_FR:
                if hippo.lower().replace("-", " ") in card.get_text().lower():
                    record["hippodrome"] = hippo
                    break

            # Distance
            dist_el = card.find(string=re.compile(r'\d+\s*m\b', re.I))
            if dist_el:
                dist_match = re.search(r'(\d+)\s*m', dist_el)
                if dist_match:
                    record["distance_m"] = int(dist_match.group(1))

            # Terrain
            for kw in ["bon", "souple", "leger", "collant", "lourd", "tres lourd",
                        "bon souple", "bon leger", "tres souple"]:
                if kw in card.get_text().lower():
                    record["terrain"] = kw
                    break

            # Lien detail
            link = card.find("a", href=True)
            if link:
                record["url_detail"] = link["href"] if link["href"].startswith("http") else f"{BASE_URL}{link['href']}"

            if record.get("nom_prix") or record.get("hippodrome"):
                records.append(record)

    # --- Extraire les commentaires officiels ---
    for div in soup.find_all(["div", "p", "article", "section", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["commentaire", "comment", "analyse", "rapport",
                                                  "resume", "observation", "avis-officiel",
                                                  "compte-rendu", "chronique"]):
            if text and 20 < len(text) < 5000:
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "commentaire_officiel",
                    "contenu": text[:4000],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extraire les JSON embarques ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(r'window\[?[\'"]?(__\w+|raceData|resultData|courseData)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "embedded_window_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "source": "france_galop",
                "date": date_str,
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Extraire les liens vers PDF resultats ---
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith(".pdf") or "pdf" in href.lower():
            records.append({
                "source": "france_galop",
                "date": date_str,
                "type": "pdf_resultat_link",
                "pdf_url": href if href.startswith("http") else f"{BASE_URL}{href}",
                "text": link.get_text(strip=True),
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Extraire les data-attributes pertinents ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["course", "race", "cheval", "horse", "hippo", "resultat", "classement"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "france_galop",
                "date": date_str,
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Video/photo metadata ---
    for el in soup.find_all(["video", "source", "iframe", "img", "a"]):
        src = el.get("src") or el.get("data-src") or el.get("href", "")
        if src and any(kw in src.lower() for kw in ["replay", "video", "photo-arrivee",
                                                      "stream", "mp4", "m3u8", "finish-photo"]):
            records.append({
                "source": "france_galop",
                "date": date_str,
                "type": "media_metadata",
                "media_url": src if src.startswith("http") else f"{BASE_URL}{src}",
                "media_tag": el.name,
                "text": el.get_text(strip=True)[:100],
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_course_detail(session, course_url, date_str):
    """Scraper le detail d'une course individuelle."""
    if not course_url:
        return []

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', course_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    if not course_url.startswith("http"):
        course_url = f"{BASE_URL}{course_url}"

    resp = fetch_with_retry(session, course_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Infos de la course
    course_info = {
        "source": "france_galop",
        "date": date_str,
        "type": "course_detail",
        "url": course_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Titre
    h1 = soup.find("h1")
    if h1:
        course_info["nom_prix"] = h1.get_text(strip=True)

    # Extraire les paires cle/valeur
    for dt in soup.find_all(["dt", "th", "label", "strong"]):
        dd = dt.find_next_sibling(["dd", "td", "span", "div"])
        if dd:
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val and len(key) < 50:
                course_info[key] = val

    # Conditions
    for el in soup.find_all(string=re.compile(r'(conditions|dotation|allocation|age|poids)', re.I)):
        parent = el.find_parent()
        if parent:
            text = parent.get_text(strip=True)
            if 5 < len(text) < 500:
                course_info["conditions_brut"] = text
                break

    records.append(course_info)

    # Partants et arrivee
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
                "source": "france_galop",
                "date": date_str,
                "type": "partant_detail",
                "nom_prix": course_info.get("nom_prix", ""),
                "url_course": course_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                partant[key] = cell

            # Position
            for cell in cells[:3]:
                pos_match = re.match(r'^(\d+)', cell.strip())
                if pos_match and int(pos_match.group(1)) <= 30:
                    partant["position"] = int(pos_match.group(1))
                    break

            # Poids
            for cell in cells:
                poids_match = re.search(r'(\d+[.,]?\d*)\s*kg', cell, re.I)
                if poids_match:
                    partant["poids_kg"] = float(poids_match.group(1).replace(",", "."))
                    break

            # Cote
            for cell in cells:
                cote_match = re.search(r'(\d+[.,]\d+)/1', cell)
                if cote_match:
                    partant["cote"] = float(cote_match.group(1).replace(",", "."))
                    break

            records.append(partant)

    # --- Commentaires officiels du detail de course ---
    for div in soup.find_all(["div", "p", "article", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["commentaire", "comment", "analyse", "rapport",
                                                  "observation", "verdict", "compte-rendu",
                                                  "race-comment", "steward"]):
            if text and 20 < len(text) < 5000:
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "commentaire_course_detail",
                    "nom_prix": course_info.get("nom_prix", ""),
                    "contenu": text[:4000],
                    "url": course_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Stats par hippodrome/terrain/distance dans la page detail ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["statistique", "stats", "bilan", "record",
                                                  "historique", "palmares", "track-record"]):
            if div.name == "table":
                rows = div.find_all("tr")
                stat_headers = []
                if rows:
                    stat_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                                    for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        entry = {
                            "source": "france_galop",
                            "date": date_str,
                            "type": "stats_detail_table",
                            "nom_prix": course_info.get("nom_prix", ""),
                            "url": course_url,
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = stat_headers[j] if j < len(stat_headers) and stat_headers[j] else f"col_{j}"
                            entry[key] = cell
                        records.append(entry)
            else:
                text = div.get_text(strip=True)
                if text and 10 < len(text) < 3000:
                    records.append({
                        "source": "france_galop",
                        "date": date_str,
                        "type": "stats_detail_text",
                        "nom_prix": course_info.get("nom_prix", ""),
                        "contenu": text[:2500],
                        "url": course_url,
                        "scraped_at": datetime.now().isoformat(),
                    })

    # --- Historique complet cheval (depuis la page course) ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["form", "historique", "dernieres-courses",
                                                  "palmares", "previous", "carriere", "perf"]):
            if div.name == "table":
                rows = div.find_all("tr")
                form_headers = []
                if rows:
                    form_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                                    for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        entry = {
                            "source": "france_galop",
                            "date": date_str,
                            "type": "historique_forme",
                            "nom_prix": course_info.get("nom_prix", ""),
                            "url": course_url,
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = form_headers[j] if j < len(form_headers) and form_headers[j] else f"col_{j}"
                            entry[key] = cell
                        records.append(entry)
            else:
                text = div.get_text(strip=True)
                if text and 10 < len(text) < 3000:
                    records.append({
                        "source": "france_galop",
                        "date": date_str,
                        "type": "historique_forme_text",
                        "nom_prix": course_info.get("nom_prix", ""),
                        "contenu": text[:2500],
                        "url": course_url,
                        "scraped_at": datetime.now().isoformat(),
                    })

    # --- JSON embarque dans la page detail ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "detail_embedded_json",
                    "nom_prix": course_info.get("nom_prix", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(r'window\[?[\'"]?(__\w+|courseDetail|partantsData|arriveeData)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "detail_window_data",
                    "var_name": m.group(1),
                    "nom_prix": course_info.get("nom_prix", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "source": "france_galop",
                "date": date_str,
                "type": "detail_script_json",
                "nom_prix": course_info.get("nom_prix", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- PDF links dans la page detail ---
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith(".pdf") or "pdf" in href.lower():
            records.append({
                "source": "france_galop",
                "date": date_str,
                "type": "detail_pdf_link",
                "nom_prix": course_info.get("nom_prix", ""),
                "pdf_url": href if href.startswith("http") else f"{BASE_URL}{href}",
                "text": link.get_text(strip=True),
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Video/photo arrivee ---
    for el in soup.find_all(["video", "source", "iframe", "img", "a"]):
        src = el.get("src") or el.get("data-src") or el.get("href", "")
        if src and any(kw in src.lower() for kw in ["replay", "video", "photo", "arrivee",
                                                      "stream", "mp4", "m3u8", "finish"]):
            records.append({
                "source": "france_galop",
                "date": date_str,
                "type": "detail_media",
                "nom_prix": course_info.get("nom_prix", ""),
                "media_url": src if src.startswith("http") else f"{BASE_URL}{src}",
                "media_tag": el.name,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_classements(session, year, category="entraineurs"):
    """Scraper les classements annuels (entraineurs, jockeys, proprietaires, eleveurs)."""
    cache_file = os.path.join(CACHE_DIR, f"classement_{category}_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/fr/classements/{category}/{year}",
        f"{BASE_URL}/fr/statistiques/{category}?annee={year}",
        f"{BASE_URL}/classements/{category}/{year}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue

            record = {
                "source": "france_galop",
                "year": year,
                "category": category,
                "type": f"classement_{category}",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Rang
            for cell in cells[:2]:
                rang_match = re.match(r'^(\d+)$', cell.strip())
                if rang_match:
                    record["rang"] = int(rang_match.group(1))
                    break

            # Gains
            for cell in cells:
                gains_match = re.search(r'([\d\s.,]+)\s*(EUR|€)?', cell)
                if gains_match:
                    val = gains_match.group(1).replace(" ", "").replace(".", "").replace(",", ".")
                    try:
                        val_f = float(val)
                        if val_f > 1000:  # Au moins 1000 EUR
                            record["gains_eur"] = val_f
                            break
                    except ValueError:
                        pass

            records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_fiche_cheval(session, horse_url):
    """Scraper la fiche d'un cheval sur France Galop."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', horse_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"cheval_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    if not horse_url.startswith("http"):
        horse_url = f"{BASE_URL}{horse_url}"

    resp = fetch_with_retry(session, horse_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    fiche = {
        "source": "france_galop",
        "type": "fiche_cheval",
        "url": horse_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Nom
    h1 = soup.find("h1")
    if h1:
        fiche["nom"] = h1.get_text(strip=True)

    # Toutes les paires cle/valeur
    for dt in soup.find_all(["dt", "th", "label", "strong"]):
        dd = dt.find_next_sibling(["dd", "td", "span", "div"])
        if dd:
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val and len(key) < 50:
                fiche[key] = val

    # Pedigree
    for kw_field in [("pere", "pere"), ("mere", "mere"), ("pere_de_mere", "pere_mere"),
                      ("sire", "pere"), ("dam", "mere"), ("damsire", "pere_mere")]:
        el = soup.find(string=re.compile(kw_field[0], re.I))
        if el:
            parent = el.find_parent()
            if parent:
                link = parent.find("a")
                if link:
                    fiche[kw_field[1]] = link.get_text(strip=True)
                else:
                    fiche[kw_field[1]] = parent.get_text(strip=True)

    # Performances (carriere)
    for el in soup.find_all(string=re.compile(r'(\d+)\s*course', re.I)):
        match = re.search(r'(\d+)\s*course', el, re.I)
        if match:
            fiche["nb_courses"] = int(match.group(1))
            break

    for el in soup.find_all(string=re.compile(r'(\d+)\s*victoire', re.I)):
        match = re.search(r'(\d+)\s*victoire', el, re.I)
        if match:
            fiche["nb_victoires"] = int(match.group(1))
            break

    # Gains
    for el in soup.find_all(string=re.compile(r'gains?[:\s]*([\d\s.,]+)', re.I)):
        match = re.search(r'gains?[:\s]*([\d\s.,]+)', el, re.I)
        if match:
            val = match.group(1).replace(" ", "").replace(".", "").replace(",", ".")
            try:
                fiche["gains_carriere"] = float(val)
            except ValueError:
                pass
            break

    # --- Historique complet des courses (last 10+) ---
    form_history = []
    for table in soup.find_all("table"):
        table_text = table.get_text().lower()
        if any(kw in table_text for kw in ["date", "course", "hippodrome", "place", "distance"]):
            rows = table.find_all("tr")
            headers = []
            if rows:
                headers = [th.get_text(strip=True).lower().replace(" ", "_")
                           for th in rows[0].find_all(["th", "td"])]
            if len(headers) < 3:
                continue
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if cells and len(cells) >= 3:
                    entry = {}
                    for j, cell in enumerate(cells):
                        key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                        entry[key] = cell
                    form_history.append(entry)
    if form_history:
        fiche["historique_courses"] = form_history
        fiche["nb_courses_historique"] = len(form_history)

    # --- Stats par terrain/distance/hippodrome ---
    stats_sections = {}
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["stats", "statistique", "bilan", "record",
                                                  "par-terrain", "par-distance", "par-hippodrome"]):
            section_name = classes
            if div.name == "table":
                rows = div.find_all("tr")
                stat_headers = []
                if rows:
                    stat_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                                    for th in rows[0].find_all(["th", "td"])]
                entries = []
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        entry = {}
                        for j, cell in enumerate(cells):
                            key = stat_headers[j] if j < len(stat_headers) and stat_headers[j] else f"col_{j}"
                            entry[key] = cell
                        entries.append(entry)
                if entries:
                    stats_sections[section_name] = entries
            else:
                text = div.get_text(strip=True)
                if text and 10 < len(text) < 3000:
                    stats_sections[section_name] = text[:2500]
    if stats_sections:
        fiche["stats_par_categorie"] = stats_sections

    # --- JSON embarque dans la fiche cheval ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'window\[?[\'"]?(__\w+|horseData|ficheData|performanceData)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                fiche[f"embedded_{m.group(1)}"] = data
            except json.JSONDecodeError:
                pass
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                if "embedded_json_data" not in fiche:
                    fiche["embedded_json_data"] = []
                fiche["embedded_json_data"].append(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            fiche["structured_data"] = data
        except json.JSONDecodeError:
            pass

    # --- Data attributes sur la fiche ---
    data_attrs_all = {}
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["cheval", "horse", "pere", "mere", "gain", "perf", "race"])
            for k in attrs)):
        for k, v in el.attrs.items():
            if k.startswith("data-"):
                data_attrs_all[k] = v
    if data_attrs_all:
        fiche["data_attributes"] = data_attrs_all

    # --- Photo du cheval ---
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        alt = img.get("alt", "").lower()
        if any(kw in src.lower() or kw in alt for kw in ["cheval", "horse", "photo", "profil"]):
            fiche["photo_url"] = src if src.startswith("http") else f"{BASE_URL}{src}"
            break

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(fiche, f, ensure_ascii=False, indent=2)

    return fiche


def scrape_stats_hippodrome(session, hippodrome_name):
    """Scraper les statistiques par hippodrome."""
    cache_file = os.path.join(CACHE_DIR, f"hippo_{hippodrome_name}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    urls_to_try = [
        f"{BASE_URL}/fr/hippodromes/{hippodrome_name}",
        f"{BASE_URL}/fr/courses/hippodrome/{hippodrome_name}",
        f"{BASE_URL}/hippodromes/{hippodrome_name}",
    ]

    soup = None
    for url in urls_to_try:
        resp = fetch_with_retry(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        smart_pause(1.0, 0.5)

    if not soup:
        return []

    records = []

    # Info hippodrome
    info = {
        "source": "france_galop",
        "type": "hippodrome_info",
        "hippodrome": hippodrome_name,
        "scraped_at": datetime.now().isoformat(),
    }

    h1 = soup.find("h1")
    if h1:
        info["titre"] = h1.get_text(strip=True)

    for dt in soup.find_all(["dt", "th", "label", "strong"]):
        dd = dt.find_next_sibling(["dd", "td", "span", "div"])
        if dd:
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val and len(key) < 50:
                info[key] = val

    records.append(info)

    # Stats tables (distances, records, etc.)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                record = {
                    "source": "france_galop",
                    "type": "hippodrome_stats",
                    "hippodrome": hippodrome_name,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 80 — France Galop Scraper (donnees officielles FR)")
    parser.add_argument("--start", type=str, default="2018-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--mode", choices=["resultats", "classements", "detail", "all"], default="all",
                        help="Mode: resultats, classements, detail (courses), all")
    parser.add_argument("--detail", action="store_true", default=False,
                        help="Scraper aussi le detail de chaque course")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 80 — France Galop Scraper (officiel FR)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info(f"  Mode : {args.mode}")
    log.info(f"  Detail courses : {args.detail}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "france_galop_data.jsonl")

    total_records = checkpoint.get("total_records", 0)

    # --- Phase 1: Resultats jour par jour ---
    if args.mode in ("resultats", "all"):
        log.info("--- Phase 1: Resultats quotidiens ---")
        last_date = checkpoint.get("last_date")
        current = start_date
        if args.resume and last_date:
            resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            if resume_date > current:
                current = resume_date
                log.info(f"  Reprise au checkpoint : {current.date()}")

        day_count = 0
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")

            # Programme
            prog = scrape_programme_jour(session, date_str)
            if prog:
                for rec in prog:
                    append_jsonl(output_file, rec)
                    total_records += 1
            smart_pause(1.5, 0.8)

            # Resultats
            results = scrape_resultats_jour(session, date_str)
            if results:
                for rec in results:
                    append_jsonl(output_file, rec)
                    total_records += 1

                # Detail optionnel
                if args.detail:
                    course_urls = set()
                    for rec in results:
                        url = rec.get("url_detail") or rec.get("url_course")
                        if url:
                            course_urls.add(url)

                    for curl in course_urls:
                        detail = scrape_course_detail(session, curl, date_str)
                        if detail:
                            for drec in detail:
                                append_jsonl(output_file, drec)
                                total_records += 1
                        smart_pause(1.5, 0.8)

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

    # --- Phase 2: Classements annuels ---
    if args.mode in ("classements", "all"):
        log.info("--- Phase 2: Classements annuels ---")
        year_start = start_date.year
        year_end = end_date.year

        categories = ["entraineurs", "jockeys", "proprietaires", "eleveurs"]

        for year in range(year_start, year_end + 1):
            for cat in categories:
                log.info(f"  Classement {cat} {year}")
                records = scrape_classements(session, year, cat)
                if records:
                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                    log.info(f"    -> {len(records)} entrees")
                smart_pause(2.0, 1.0)

    # --- Phase 3: Stats par hippodrome ---
    if args.mode in ("all",):
        log.info("--- Phase 3: Statistiques par hippodrome ---")
        for hippo in HIPPODROMES_FR[:15]:  # Top 15 hippodromes
            log.info(f"  Hippodrome: {hippo}")
            hippo_records = scrape_stats_hippodrome(session, hippo)
            if hippo_records:
                for rec in hippo_records:
                    append_jsonl(output_file, rec)
                    total_records += 1
                log.info(f"    -> {len(hippo_records)} entrees")
            smart_pause(2.0, 1.0)

    save_checkpoint({
        "last_date": end_date.strftime("%Y-%m-%d"),
        "total_records": total_records,
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINE: {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
