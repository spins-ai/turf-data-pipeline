#!/usr/bin/env python3
"""
Script 18 — Scraping des records de piste LeTrot
Source : https://www.letrot.com/stats/champrecords/hippodrome
Collecte les records par hippodrome, distance, spécialité
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
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "18_letrot_records")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s

def smart_pause(base=1.5, jitter=0.8):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.1:
        pause += random.uniform(2, 5)
    time.sleep(max(0.5, pause))

def fetch_hippodromes(session):
    """Récupérer la liste des hippodromes depuis la page principale"""
    url = "https://www.letrot.com/stats/champrecords/hippodrome"
    cache_file = os.path.join(CACHE_DIR, "hippodromes_list.json")

    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    print(f"[{datetime.now():%H:%M:%S}] Fetching liste hippodromes...")
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    hippodromes = []
    # Chercher les liens/options d'hippodromes
    for option in soup.find_all("option"):
        val = option.get("value", "")
        name = option.get_text(strip=True)
        if val and name and val != "":
            hippodromes.append({"id": val, "name": name})

    # Si pas d'options, chercher les liens
    if not hippodromes:
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/stats/champrecords/hippodrome/" in href or "hippodrome" in href.lower():
                name = link.get_text(strip=True)
                if name:
                    hippodromes.append({"id": href, "name": name})

    # Fallback: chercher dans les tables
    if not hippodromes:
        for td in soup.find_all("td"):
            text = td.get_text(strip=True)
            if text and len(text) > 2:
                link = td.find("a")
                if link and link.get("href"):
                    hippodromes.append({"id": link["href"], "name": text})

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(hippodromes, f, ensure_ascii=False, indent=2)

    print(f"  Trouvé {len(hippodromes)} hippodromes")
    return hippodromes

def fetch_records_page(session, url, hippodrome_name):
    """Récupérer les records pour un hippodrome donné"""
    import hashlib
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    cache_file = os.path.join(CACHE_DIR, f"records_{url_hash}.json")

    if os.path.exists(cache_file):
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
    except Exception as e:
        print(f"  Erreur {hippodrome_name}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Parser les tables de records
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        for th in rows[0].find_all(["th", "td"]) if rows else []:
            headers.append(th.get_text(strip=True).lower())

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 3:
                record = {
                    "hippodrome": hippodrome_name,
                    "raw_cells": [c.get_text(strip=True) for c in cells],
                }
                # Essayer de mapper les colonnes
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    if i < len(headers):
                        record[headers[i]] = text
                    else:
                        record[f"col_{i}"] = text
                records.append(record)

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records

def main():
    print("=" * 60)
    print("SCRIPT 18 — Records de piste LeTrot")
    print("=" * 60)

    session = new_session()

    # Étape 1: Récupérer la page principale pour comprendre la structure
    print("\n[1/3] Analyse de la structure du site...")

    # Tester différentes URLs possibles
    urls_to_try = [
        "https://www.letrot.com/stats/champrecords/hippodrome",
        "https://www.letrot.com/fr/stats/champrecords/hippodrome",
        "https://www.letrot.com/stats/records",
    ]

    page_content = None
    working_url = None
    for url in urls_to_try:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                page_content = resp.text
                working_url = url
                print(f"  URL fonctionnelle: {url}")
                break
        except Exception as e:
            print(f"  {url} -> erreur: {e}")
            continue

    if not page_content:
        print("ERREUR: Impossible d'accéder à LeTrot")
        return

    # Sauvegarder la page brute pour analyse
    with open(os.path.join(OUTPUT_DIR, "page_brute.html"), "w") as f:
        f.write(page_content)

    soup = BeautifulSoup(page_content, "html.parser")

    # Chercher les données
    all_records = []

    # Méthode 1: Tables directes
    tables = soup.find_all("table")
    print(f"  Tables trouvées: {len(tables)}")

    for i, table in enumerate(tables):
        rows = table.find_all("tr")
        headers = []
        first_row = rows[0] if rows else None
        if first_row:
            headers = [th.get_text(strip=True) for th in first_row.find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and any(c for c in cells):
                record = {"table_index": i}
                for j, cell in enumerate(cells):
                    if j < len(headers) and headers[j]:
                        record[headers[j]] = cell
                    else:
                        record[f"col_{j}"] = cell
                all_records.append(record)

    # Méthode 2: Chercher les liens vers les hippodromes
    hippo_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        name = link.get_text(strip=True)
        if "hippodrome" in href.lower() and name and len(name) > 2:
            full_url = href if href.startswith("http") else f"https://www.letrot.com{href}"
            hippo_links.append({"url": full_url, "name": name})

    print(f"  Liens hippodromes: {len(hippo_links)}")

    # Méthode 3: Select/options
    for select in soup.find_all("select"):
        for option in select.find_all("option"):
            val = option.get("value", "")
            name = option.get_text(strip=True)
            if val and name and val not in ("", "0", "-1"):
                hippo_links.append({"url": val, "name": name})

    # Dédupliquer
    seen = set()
    unique_links = []
    for h in hippo_links:
        if h["name"] not in seen:
            seen.add(h["name"])
            unique_links.append(h)

    print(f"  Hippodromes uniques: {len(unique_links)}")

    # [2/3] Scraper chaque hippodrome
    if unique_links:
        print(f"\n[2/3] Scraping de {len(unique_links)} hippodromes...")
        for i, hippo in enumerate(unique_links):
            url = hippo["url"]
            if not url.startswith("http"):
                url = f"https://www.letrot.com{url}"

            records = fetch_records_page(session, url, hippo["name"])
            all_records.extend(records)

            if (i + 1) % 10 == 0:
                print(f"  [{i+1}/{len(unique_links)}] {hippo['name']} -> {len(records)} records")

            smart_pause(1.0, 0.5)

            # Changer de session tous les 50 hippodromes
            if (i + 1) % 50 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(3, 8))

    # [3/3] Sauvegarde
    print(f"\n[3/3] Sauvegarde de {len(all_records)} records...")

    output_file = os.path.join(OUTPUT_DIR, "letrot_records.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    # CSV
    if all_records:
        import csv
        all_keys = set()
        for r in all_records:
            all_keys.update(r.keys())
        all_keys = sorted(all_keys)

        csv_file = os.path.join(OUTPUT_DIR, "letrot_records.csv")
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(all_records)

    print(f"\nTERMINÉ: {len(all_records)} records sauvegardés")
    print(f"  JSON: {output_file}")

if __name__ == "__main__":
    main()
