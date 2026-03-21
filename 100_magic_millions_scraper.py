#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 100 -- Scraping Magic Millions
Source : magicmillions.com.au - Magic Millions AU horse sales
Collecte : lots vendus, prix, pedigrees, acheteurs, vendeurs, catalogues
CRITIQUE pour : Sales Model, Pedigree Valuation, Investment Analysis
"""

import argparse
import json
import logging
import os
import random
import sys
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "100_magic_millions"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry

log = setup_logging("100_magic_millions")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.magicmillions.com.au"

# Known sale categories
SALE_TYPES = [
    "gold-coast-yearling-sale",
    "gold-coast-march-yearling-sale",
    "national-yearling-sale",
    "national-broodmare-sale",
    "national-weanling-sale",
    "gold-coast-broodmare-sale",
    "adelaide-yearling-sale",
    "perth-yearling-sale",
    "tasmanian-yearling-sale",
    "national-racehorse-sale",
    "2yo-classic-sale",
]

# Year range for historical sales
SALE_YEARS = list(range(2015, 2027))


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s

def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8", errors="replace", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8", errors="replace") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def scrape_sale_results_page(session, page_url, sale_name, year):
    """Scrape sale results page (catalogue/results listing)."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"sale_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    resp = fetch_with_retry(session, page_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # -- Results tables --
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
                "source": "magic_millions",
                "type": "sale_result",
                "sale_name": sale_name,
                "year": year,
                "url": page_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract price
            combined = " ".join(cells)
            price_match = re.search(r'\$\s*([\d,]+)', combined)
            if price_match:
                record["price_aud"] = int(price_match.group(1).replace(",", ""))

            # Extract lot number
            lot_match = re.search(r'(?:Lot|#)\s*(\d+)', combined, re.IGNORECASE)
            if lot_match:
                record["lot_number"] = int(lot_match.group(1))

            records.append(record)

    # -- Lot cards / catalogue entries --
    for card in soup.find_all(["div", "article", "li", "section"], class_=True):
        classes = " ".join(card.get("class", []))
        if any(kw in classes.lower() for kw in ["lot", "catalogue", "horse",
                                                  "entry", "result", "listing"]):
            text = card.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                record = {
                    "source": "magic_millions",
                    "type": "lot_card",
                    "sale_name": sale_name,
                    "year": year,
                    "text": text[:2500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                }

                # Extract horse name
                heading = card.find(["h2", "h3", "h4", "h5"])
                if heading:
                    record["horse_name"] = heading.get_text(strip=True)

                # Extract price
                price_match = re.search(r'\$\s*([\d,]+)', text)
                if price_match:
                    record["price_aud"] = int(price_match.group(1).replace(",", ""))

                # Extract lot number
                lot_match = re.search(r'(?:Lot|#)\s*(\d+)', text, re.IGNORECASE)
                if lot_match:
                    record["lot_number"] = int(lot_match.group(1))

                # Extract sire/dam
                sire_match = re.search(r'(?:by|sire)[:\s]+([A-Z][A-Za-z\s\'-]+)', text)
                if sire_match:
                    record["sire"] = sire_match.group(1).strip()
                dam_match = re.search(r'(?:out of|dam|from)[:\s]+([A-Z][A-Za-z\s\'-]+)', text)
                if dam_match:
                    record["dam"] = dam_match.group(1).strip()

                # Extract buyer/vendor
                buyer_match = re.search(r'(?:buyer|purchaser|sold to)[:\s]+(.+?)(?:\n|\||$)',
                                        text, re.IGNORECASE)
                if buyer_match:
                    record["buyer"] = buyer_match.group(1).strip()[:200]
                vendor_match = re.search(r'(?:vendor|consignor|offered by)[:\s]+(.+?)(?:\n|\||$)',
                                         text, re.IGNORECASE)
                if vendor_match:
                    record["vendor"] = vendor_match.group(1).strip()[:200]

                # Detail link
                link = card.find("a", href=True)
                if link:
                    href = link["href"]
                    if href.startswith("/"):
                        href = BASE_URL + href
                    record["detail_url"] = href

                records.append(record)

    # -- Pedigree sections --
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["pedigree", "breeding", "bloodline",
                                                  "family", "lineage"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                records.append({
                    "source": "magic_millions",
                    "type": "pedigree",
                    "sale_name": sale_name,
                    "year": year,
                    "contenu": text[:1500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # -- Sale statistics / summaries --
    for div in soup.find_all(["div", "section", "p"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["summary", "statistics", "stats",
                                                  "aggregate", "total", "average",
                                                  "median", "clearance"]):
            if text and 10 < len(text) < 2000:
                record = {
                    "source": "magic_millions",
                    "type": "sale_summary",
                    "sale_name": sale_name,
                    "year": year,
                    "contenu": text[:1500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                # Extract summary stats
                avg_match = re.search(r'(?:average|avg)[:\s]*\$?\s*([\d,]+)', text, re.IGNORECASE)
                if avg_match:
                    record["average_aud"] = int(avg_match.group(1).replace(",", ""))
                median_match = re.search(r'(?:median)[:\s]*\$?\s*([\d,]+)', text, re.IGNORECASE)
                if median_match:
                    record["median_aud"] = int(median_match.group(1).replace(",", ""))
                gross_match = re.search(r'(?:gross|total|turnover|aggregate)[:\s]*\$?\s*([\d,]+)',
                                        text, re.IGNORECASE)
                if gross_match:
                    record["gross_aud"] = int(gross_match.group(1).replace(",", ""))
                lots_match = re.search(r'(\d+)\s*(?:lots?\s*(?:sold|offered))', text, re.IGNORECASE)
                if lots_match:
                    record["lots_count"] = int(lots_match.group(1))
                clearance_match = re.search(r'(?:clearance)[:\s]*(\d+\.?\d*)\s*%', text, re.IGNORECASE)
                if clearance_match:
                    record["clearance_pct"] = float(clearance_match.group(1))
                records.append(record)

    # -- Embedded JSON --
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "magic_millions",
                    "type": "embedded_data",
                    "sale_name": sale_name,
                    "year": year,
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\{[\s\S]{50,}?\});', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "magic_millions",
                    "type": "embedded_object",
                    "sale_name": sale_name,
                    "year": year,
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
                "source": "magic_millions",
                "type": "script_json",
                "sale_name": sale_name,
                "year": year,
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # -- Data attributes --
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["lot", "horse", "price", "sale", "sire", "dam", "buyer"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "magic_millions",
                "type": "data_attributes",
                "sale_name": sale_name,
                "year": year,
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def scrape_lot_detail(session, lot_url, sale_name, year):
    """Scrape individual lot detail page."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', lot_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"lot_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    resp = fetch_with_retry(session, lot_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract all text content with structure
    record = {
        "source": "magic_millions",
        "type": "lot_detail",
        "sale_name": sale_name,
        "year": year,
        "url": lot_url,
        "scraped_at": datetime.now().isoformat(),
    }

    # Horse name
    h1 = soup.find("h1")
    if h1:
        record["horse_name"] = h1.get_text(strip=True)

    # All tables (pedigree, results, etc.)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        table_data = []
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells:
                row_dict = {}
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    row_dict[key] = cell
                table_data.append(row_dict)
        if table_data:
            records.append({
                "source": "magic_millions",
                "type": "lot_detail_table",
                "sale_name": sale_name,
                "year": year,
                "url": lot_url,
                "horse_name": record.get("horse_name", ""),
                "table_data": table_data,
                "scraped_at": datetime.now().isoformat(),
            })

    # Key info fields
    for div in soup.find_all(["div", "span", "p", "dl", "dt", "dd"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["detail", "info", "property",
                                                  "field", "specification",
                                                  "attribute"]):
            if text and 3 < len(text) < 500:
                # Try to parse key-value
                kv_match = re.match(r'^(.+?)[:\s]+(.+)$', text)
                if kv_match:
                    key = kv_match.group(1).strip().lower().replace(" ", "_")
                    val = kv_match.group(2).strip()
                    record[key] = val

    if len(record) > 5:  # Only add if we got meaningful data
        records.insert(0, record)

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(
        description="Script 100 -- Magic Millions AU Horse Sales Scraper")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-pages", type=int, default=2000,
                        help="Nombre max de pages a scraper")
    parser.add_argument("--scrape-details", action="store_true", default=False,
                        help="Scraper aussi les pages de detail de chaque lot")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 100 -- Magic Millions AU Sales Scraper")
    log.info(f"  Sale types : {len(SALE_TYPES)}")
    log.info(f"  Years : {SALE_YEARS[0]}-{SALE_YEARS[-1]}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    done_urls = set(checkpoint.get("done_urls", []))
    if args.resume and done_urls:
        log.info(f"  Reprise checkpoint: {len(done_urls)} pages deja traitees")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "magic_millions_data.jsonl")

    total_records = 0
    page_count = 0
    all_lot_urls = []

    # Phase 1: Scrape sale results pages
    log.info("  Phase 1: Pages de resultats de ventes")
    for year in SALE_YEARS:
        for sale_type in SALE_TYPES:
            if page_count >= args.max_pages:
                break

            # Try multiple URL patterns
            urls_to_try = [
                f"{BASE_URL}/sales/{year}/{sale_type}/results",
                f"{BASE_URL}/sales/{sale_type}/{year}/results",
                f"{BASE_URL}/sales-results/{year}/{sale_type}",
                f"{BASE_URL}/{year}-{sale_type}/results",
                f"{BASE_URL}/sales/{year}/{sale_type}",
            ]

            for url in urls_to_try:
                if url in done_urls:
                    continue

                records = scrape_sale_results_page(session, url, sale_type, year)
                if records:
                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1
                        # Collect lot detail URLs
                        detail_url = rec.get("detail_url")
                        if detail_url and args.scrape_details:
                            all_lot_urls.append((sale_type, year, detail_url))

                done_urls.add(url)
                page_count += 1

                if records:
                    log.info(f"    {year} {sale_type}: {len(records)} records")
                    break  # Found data, stop trying URL patterns

                smart_pause(1.0, 0.5)

            if page_count % 20 == 0:
                save_checkpoint({"done_urls": list(done_urls),
                                 "total_records": total_records})

            if page_count % 60 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

    # Phase 2: Scrape lot detail pages (if enabled)
    if args.scrape_details and all_lot_urls:
        log.info(f"  Phase 2: Detail des lots ({len(all_lot_urls)} lots)")
        for sale_type, year, lot_url in all_lot_urls:
            if lot_url in done_urls:
                continue
            if page_count >= args.max_pages:
                break

            records = scrape_lot_detail(session, lot_url, sale_type, year)
            if records:
                for rec in records:
                    append_jsonl(output_file, rec)
                    total_records += 1

            done_urls.add(lot_url)
            page_count += 1

            if page_count % 20 == 0:
                log.info(f"    lots: pages={page_count} records={total_records}")
                save_checkpoint({"done_urls": list(done_urls),
                                 "total_records": total_records})

            if page_count % 60 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

            smart_pause(1.5, 0.8)

    # Phase 3: Discover additional pages from main site
    log.info("  Phase 3: Decouverte de pages additionnelles")
    main_resp = fetch_with_retry(session, BASE_URL)
    if main_resp:
        main_soup = BeautifulSoup(main_resp.text, "html.parser")
        for a in main_soup.find_all("a", href=True):
            href = a["href"]
            if any(kw in href.lower() for kw in ["sale", "result", "catalogue",
                                                   "lot", "statistics"]):
                if href.startswith("/"):
                    href = BASE_URL + href
                if href.startswith("http") and href not in done_urls and page_count < args.max_pages:
                    records = scrape_sale_results_page(session, href, "discovered", 0)
                    if records:
                        for rec in records:
                            append_jsonl(output_file, rec)
                            total_records += 1
                    done_urls.add(href)
                    page_count += 1
                    smart_pause()

    save_checkpoint({"done_urls": list(done_urls),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {page_count} pages, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
