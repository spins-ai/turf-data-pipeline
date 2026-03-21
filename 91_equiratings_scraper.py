#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 91 -- Scraping EquiRatings.com
Source : equiratings.com - Professional ratings and analytics
Collecte : ratings professionnels, classements chevaux, statistiques performance
CRITIQUE pour : Rating Model, Horse Ranking, Performance Analytics
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
from bs4 import BeautifulSoup

SCRIPT_NAME = "91_equiratings"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("91_equiratings")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.equiratings.com"


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9,fr-FR;q=0.8",
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


def scrape_rankings_page(session, page_url):
    """Scrape a rankings/leaderboard page from EquiRatings."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', page_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"rank_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8", errors="replace") as f:
            return json.load(f)

    resp = fetch_with_retry(session, page_url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # -- Extract ranking tables --
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                record = {
                    "source": "equiratings",
                    "type": "ranking",
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    record[key] = cell
                # Try to extract numeric rating
                for cell in cells:
                    m = re.search(r'(\d+\.?\d*)', cell)
                    if m:
                        try:
                            val = float(m.group(1))
                            if 0 < val < 200:
                                record["rating_value"] = val
                                break
                        except ValueError:
                            pass
                records.append(record)

    # -- Extract card/grid items (horse profiles) --
    for card in soup.find_all(["div", "article", "li"], class_=True):
        classes = " ".join(card.get("class", []))
        if any(kw in classes.lower() for kw in ["card", "horse", "rider", "ranking",
                                                  "athlete", "leaderboard", "entry"]):
            text = card.get_text(strip=True)
            if text and 5 < len(text) < 2000:
                link = card.find("a", href=True)
                record = {
                    "source": "equiratings",
                    "type": "profile_card",
                    "url": page_url,
                    "text": text[:1500],
                    "scraped_at": datetime.now().isoformat(),
                }
                if link:
                    record["detail_url"] = link["href"]
                # Extract name from heading
                heading = card.find(["h2", "h3", "h4", "h5"])
                if heading:
                    record["name"] = heading.get_text(strip=True)
                records.append(record)

    # -- Extract data from script tags --
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'(?:var|let|const)\s+(\w+)\s*=\s*(\[[\s\S]{50,}?\]);', script_text):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "equiratings",
                    "type": "embedded_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass
        for m in re.finditer(r'window\[?[\'"]?(\w+)[\'"]?\]?\s*=\s*(\{.+?\});', script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "source": "equiratings",
                    "type": "embedded_window",
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
                "source": "equiratings",
                "type": "script_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # -- Extract data-attributes --
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["rating", "score", "rank", "horse", "rider", "value"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "source": "equiratings",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # -- Extract analytics paragraphs --
    for div in soup.find_all(["div", "p", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["analysis", "insight", "stat",
                                                  "performance", "summary", "metric"]):
            if text and 20 < len(text) < 3000:
                records.append({
                    "source": "equiratings",
                    "type": "analytics_text",
                    "contenu": text[:2500],
                    "url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8", errors="replace") as f:
        json.dump(records, f, ensure_ascii=True, indent=2)

    return records


def discover_pages(session):
    """Discover ranking/analytics pages from main site."""
    resp = fetch_with_retry(session, BASE_URL)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    pages = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(kw in href.lower() for kw in ["ranking", "leaderboard", "rating",
                                               "horse", "rider", "event", "result",
                                               "analytics", "stats", "data"]):
            if href.startswith("/"):
                href = BASE_URL + href
            if href.startswith("http"):
                pages.add(href)

    # Also try known paths
    known_paths = [
        "/rankings", "/leaderboard", "/horses", "/riders",
        "/events", "/results", "/analytics", "/data",
        "/rankings/horses", "/rankings/riders",
    ]
    for path in known_paths:
        pages.add(BASE_URL + path)

    return list(pages)


def main():
    parser = argparse.ArgumentParser(
        description="Script 91 -- EquiRatings Scraper (ratings, analytics)")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    parser.add_argument("--max-pages", type=int, default=500,
                        help="Nombre max de pages a scraper")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 91 -- EquiRatings Scraper")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    done_urls = set(checkpoint.get("done_urls", []))
    if args.resume and done_urls:
        log.info(f"  Reprise checkpoint: {len(done_urls)} pages deja traitees")

    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "equiratings_data.jsonl")

    # Discover pages
    pages = discover_pages(session)
    log.info(f"  Pages decouvertes: {len(pages)}")
    smart_pause()

    total_records = 0
    page_count = 0
    all_detail_urls = set()

    for page_url in pages:
        if page_url in done_urls:
            continue
        if page_count >= args.max_pages:
            break

        records = scrape_rankings_page(session, page_url)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1
                # Collect detail links
                detail = rec.get("detail_url", "")
                if detail:
                    if detail.startswith("/"):
                        detail = BASE_URL + detail
                    all_detail_urls.add(detail)

        done_urls.add(page_url)
        page_count += 1

        if page_count % 10 == 0:
            log.info(f"  pages={page_count} records={total_records}")
            save_checkpoint({"done_urls": list(done_urls),
                             "total_records": total_records})

        if page_count % 50 == 0:
            session.close()
            session = new_session()
            time.sleep(random.uniform(5, 15))

        smart_pause()

    # Scrape detail pages
    log.info(f"  Detail pages a scraper: {len(all_detail_urls)}")
    for detail_url in all_detail_urls:
        if detail_url in done_urls:
            continue
        if page_count >= args.max_pages:
            break

        records = scrape_rankings_page(session, detail_url)
        if records:
            for rec in records:
                append_jsonl(output_file, rec)
                total_records += 1

        done_urls.add(detail_url)
        page_count += 1

        if page_count % 10 == 0:
            log.info(f"  pages={page_count} records={total_records}")
            save_checkpoint({"done_urls": list(done_urls),
                             "total_records": total_records})

        smart_pause()

    save_checkpoint({"done_urls": list(done_urls),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"TERMINE: {page_count} pages, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
