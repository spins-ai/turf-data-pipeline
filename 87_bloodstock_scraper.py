#!/usr/bin/env python3
"""
Script 87 — Scraping Bloodstock News (BloodHorse + TDN)
Source : bloodhorse.com + thoroughbreddailynews.com
Collecte : bloodstock news, stallion stats, breeding data, sire rankings, auction results
CRITIQUE pour : Breeding Features, Stallion Model, Bloodstock Market Intelligence
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "87_bloodstock"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, load_checkpoint, save_checkpoint, append_jsonl, create_session

log = setup_logging("87_bloodstock")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

SOURCES = {
    "bloodhorse": {
        "base_url": "https://www.bloodhorse.com",
        "stallion_url": "https://www.bloodhorse.com/horse-racing/thoroughbred-breeding/sire-lists",
        "news_url": "https://www.bloodhorse.com/horse-racing/articles",
    },
    "tdn": {
        "base_url": "https://www.thoroughbreddailynews.com",
        "news_url": "https://www.thoroughbreddailynews.com/category/breeding/",
    },
}



def scrape_bloodhorse_sires(session, year):
    """Scraper les sire lists de BloodHorse pour une année donnée."""
    cache_file = os.path.join(CACHE_DIR, f"sires_{year}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{SOURCES['bloodhorse']['stallion_url']}/{year}/leading-sires-by-progeny-earnings"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Sire ranking tables ---
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
            if cells and len(cells) >= 3:
                entry = {
                    "year": str(year),
                    "source": "bloodhorse",
                    "type": "sire_ranking",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                # Extract earnings
                for cell in cells:
                    earnings_match = re.search(r'\$[\d,]+', cell)
                    if earnings_match:
                        entry["earnings_raw"] = earnings_match.group(0)
                        break
                records.append(entry)

    # --- Stallion details ---
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["stallion", "sire", "stud", "breeding",
                                                  "progeny", "offspring"]):
            text = div.get_text(strip=True)
            if text and 20 < len(text) < 3000:
                records.append({
                    "year": str(year),
                    "source": "bloodhorse",
                    "type": "stallion_detail",
                    "contenu": text[:2500],
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- JSON embedded ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "year": str(year),
                    "source": "bloodhorse",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(r'window\[?[\'"]?(__\w+|sireData|stallionData|breedingData)[\'"]?\]?\s*=\s*(\{.+?\});',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "year": str(year),
                    "source": "bloodhorse",
                    "type": "embedded_window_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "year": str(year),
                    "source": "bloodhorse",
                    "type": "embedded_var_array",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "year": str(year),
                "source": "bloodhorse",
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Data-attributes ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["sire", "stallion", "horse", "breeding", "earnings", "rank", "progeny"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "year": str(year),
                "source": "bloodhorse",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_tdn_breeding(session, page_num):
    """Scraper les articles breeding de TDN."""
    cache_file = os.path.join(CACHE_DIR, f"tdn_breeding_p{page_num}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{SOURCES['tdn']['news_url']}page/{page_num}/"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # --- Article listing ---
    for article in soup.find_all(["article", "div", "section"], class_=True):
        classes = " ".join(article.get("class", []))
        if any(kw in classes.lower() for kw in ["post", "article", "entry", "item",
                                                  "story", "news"]):
            record = {
                "page": page_num,
                "source": "tdn",
                "type": "breeding_article",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = article.find(["h2", "h3", "h4", "a"])
            if title_el:
                record["titre"] = title_el.get_text(strip=True)
                link = title_el.find("a", href=True) if title_el.name != "a" else title_el
                if link and link.get("href"):
                    record["url_article"] = link["href"]

            # Date
            date_el = article.find(["time", "span"], class_=lambda c: c and "date" in str(c).lower())
            if date_el:
                record["date_article"] = date_el.get_text(strip=True)
                if date_el.get("datetime"):
                    record["date_iso"] = date_el["datetime"]

            # Excerpt
            excerpt = article.find(["p", "div"], class_=lambda c: c and any(
                kw in str(c).lower() for kw in ["excerpt", "summary", "intro", "desc"]))
            if excerpt:
                record["excerpt"] = excerpt.get_text(strip=True)[:500]

            records.append(record)

    # --- JSON embedded ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "page": page_num,
                    "source": "tdn",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "page": page_num,
                "source": "tdn",
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 87 — Bloodstock Scraper (BloodHorse + TDN)")
    parser.add_argument("--start-year", type=int, default=2018,
                        help="Année de début pour les sire lists")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Année de fin, défaut=année courante")
    parser.add_argument("--tdn-pages", type=int, default=50,
                        help="Nombre de pages TDN breeding à scraper")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_year = args.start_year
    end_year = args.end_year or datetime.now().year

    log.info("=" * 60)
    log.info("SCRIPT 87 — Bloodstock Scraper (BloodHorse + TDN)")
    log.info(f"  Sire Lists : {start_year} → {end_year}")
    log.info(f"  TDN pages : {args.tdn_pages}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "bloodstock_data.jsonl")
    total_records = 0

    # --- BloodHorse Sire Lists ---
    last_year = checkpoint.get("last_year", start_year - 1)
    if args.resume:
        start_year = max(start_year, last_year + 1)

    for year in range(start_year, end_year + 1):
        log.info(f"  BloodHorse sire list {year}...")
        records = scrape_bloodhorse_sires(session, year)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1
        save_checkpoint(CHECKPOINT_FILE, {"last_year": year, "total_records": total_records})
        smart_pause(2.0, 1.0)

    # --- TDN Breeding Articles ---
    last_page = checkpoint.get("last_tdn_page", 0)
    start_page = last_page + 1 if args.resume else 1

    for page in range(start_page, args.tdn_pages + 1):
        log.info(f"  TDN breeding page {page}/{args.tdn_pages}...")
        records = scrape_tdn_breeding(session, page)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1
        save_checkpoint(CHECKPOINT_FILE, {"last_year": end_year, "last_tdn_page": page,
                         "total_records": total_records})
        smart_pause(2.0, 1.0)

        if page % 20 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

    save_checkpoint(CHECKPOINT_FILE, {"last_year": end_year, "last_tdn_page": args.tdn_pages,
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINÉ: {total_records} records → {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
