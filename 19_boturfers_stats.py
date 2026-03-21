#!/usr/bin/env python3
"""
Script 19 — Scraping stats hippodromes Boturfers
Source : https://www.boturfers.fr/hippodrome
Collecte les statistiques par hippodrome (nb courses, rapports moyens, disciplines)
"""

import requests
import json
import time
import random
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "19_boturfers_stats")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "DNT": "1",
    })
    return s

def smart_pause(base=2.0, jitter=1.0):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.1:
        pause += random.uniform(3, 8)
    time.sleep(max(0.8, pause))

def fetch_with_retry(session, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt * 30
                print(f"  Rate limited, attente {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                session.close()
                session = new_session()
                time.sleep(random.uniform(30, 60))
                continue
            return resp
        except Exception as e:
            time.sleep(2 ** attempt * 5)
    return None

def scrape_hippodrome_list(session):
    """Récupérer la liste de tous les hippodromes"""
    cache_file = os.path.join(CACHE_DIR, "hippodrome_list.json")
    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    print(f"[{datetime.now():%H:%M:%S}] Fetching liste hippodromes Boturfers...")
    url = "https://www.boturfers.fr/hippodrome"
    resp = fetch_with_retry(session, url)
    if not resp or resp.status_code != 200:
        print(f"  Erreur: impossible d'accéder à {url}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    hippodromes = []

    # Chercher les liens vers les pages d'hippodrome
    for link in soup.find_all("a", href=True):
        href = link["href"]
        name = link.get_text(strip=True)
        if "/hippodrome/" in href and name and len(name) > 2:
            full_url = href if href.startswith("http") else f"https://www.boturfers.fr{href}"
            hippodromes.append({"url": full_url, "name": name})

    # Tables de stats
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if cells:
                link = cells[0].find("a")
                if link:
                    name = link.get_text(strip=True)
                    href = link.get("href", "")
                    full_url = href if href.startswith("http") else f"https://www.boturfers.fr{href}"

                    entry = {"url": full_url, "name": name}
                    for j, cell in enumerate(cells):
                        text = cell.get_text(strip=True)
                        if j < len(headers) and headers[j]:
                            entry[headers[j]] = text
                    hippodromes.append(entry)

    # Dédupliquer
    seen = set()
    unique = []
    for h in hippodromes:
        if h["name"] not in seen:
            seen.add(h["name"])
            unique.append(h)

    with open(cache_file, "w") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    print(f"  Trouvé {len(unique)} hippodromes")
    return unique

def scrape_hippodrome_detail(session, hippo):
    """Scraper les détails d'un hippodrome"""
    import hashlib
    url_hash = hashlib.md5(hippo["url"].encode()).hexdigest()[:12]
    cache_file = os.path.join(CACHE_DIR, f"hippo_{url_hash}.json")

    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, hippo["url"])
    if not resp or resp.status_code != 200:
        return hippo

    soup = BeautifulSoup(resp.text, "html.parser")
    detail = dict(hippo)

    # Extraire les stats de la page détail
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower().replace(" ", "_")
                val = cells[1].get_text(strip=True)
                if key and val:
                    detail[key] = val

    # Extraire les textes descriptifs
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if any(kw in text.lower() for kw in ["piste", "corde", "distance", "terrain", "catégorie"]):
            detail["description"] = detail.get("description", "") + " " + text

    # Chercher des stats dans les div
    for div in soup.find_all("div", class_=True):
        classes = " ".join(div.get("class", []))
        if "stat" in classes.lower() or "info" in classes.lower():
            text = div.get_text(strip=True)
            if text and len(text) < 200:
                detail[f"info_{classes}"] = text

    with open(cache_file, "w") as f:
        json.dump(detail, f, ensure_ascii=False, indent=2)

    return detail

def main():
    print("=" * 60)
    print("SCRIPT 19 — Stats hippodromes Boturfers")
    print("=" * 60)

    session = new_session()

    # [1/3] Liste des hippodromes
    print("\n[1/3] Récupération liste hippodromes...")
    hippodromes = scrape_hippodrome_list(session)

    if not hippodromes:
        print("ERREUR: Aucun hippodrome trouvé")
        return

    smart_pause(2, 1)

    # [2/3] Détails de chaque hippodrome
    print(f"\n[2/3] Scraping détails de {len(hippodromes)} hippodromes...")
    all_stats = []
    for i, hippo in enumerate(hippodromes):
        detail = scrape_hippodrome_detail(session, hippo)
        all_stats.append(detail)

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(hippodromes)}] {hippo['name']}")

        smart_pause(1.5, 0.8)

        if (i + 1) % 40 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 10))

    # [3/3] Sauvegarde
    print(f"\n[3/3] Sauvegarde de {len(all_stats)} hippodromes...")

    output_file = os.path.join(OUTPUT_DIR, "boturfers_hippodromes.json")
    with open(output_file, "w") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2)

    # CSV
    if all_stats:
        import csv
        all_keys = set()
        for s in all_stats:
            all_keys.update(s.keys())
        all_keys = sorted(all_keys)

        csv_file = os.path.join(OUTPUT_DIR, "boturfers_hippodromes.csv")
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(all_stats)

    print(f"\nTERMINÉ: {len(all_stats)} hippodromes sauvegardés")

if __name__ == "__main__":
    main()
