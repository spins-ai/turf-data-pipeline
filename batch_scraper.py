#!/usr/bin/env python3
"""
Batch scraper — Scrape multiple horse racing sites in one pass.
For each site: fetch homepage, extract all race/course/results links,
scrape each link, extract tables + JSON + data-attributes.
Uses cloudscraper for sites with anti-bot, requests for others.
Outputs JSONL per site in output/{site_name}/
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
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "DNT": "1",
}

# Sites configuration: name, start URLs, use_cloudscraper, link_patterns
SITES = {
    "55_equidia": {
        "urls": ["https://www.equidia.fr/"],
        "cloudscraper": False,
        "link_keywords": ["course", "resultat", "programme", "quinte", "prono"],
    },
    "59_racing_tv": {
        "urls": ["https://www.racingtv.com/results", "https://www.racingtv.com/racecards"],
        "cloudscraper": False,
        "link_keywords": ["race", "result", "card", "course", "meeting"],
    },
    "62_horse_racing_nation": {
        "urls": ["https://www.horseracingnation.com/race", "https://www.horseracingnation.com/horse"],
        "cloudscraper": False,
        "link_keywords": ["race", "horse", "result", "entry", "past-performance"],
    },
    "63_daily_racing_form": {
        "urls": ["https://www.drf.com/"],
        "cloudscraper": False,
        "link_keywords": ["race", "result", "entries", "handicapping"],
    },
    "66_hkjc": {
        "urls": ["https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx"],
        "cloudscraper": False,
        "link_keywords": ["result", "race", "card", "horse", "jockey"],
    },
    "67_jra": {
        "urls": ["https://www.jra.go.jp/JRADB/accessS.html"],
        "cloudscraper": False,
        "link_keywords": ["race", "result", "horse", "jockey"],
    },
    "72_tattersalls": {
        "urls": ["https://www.tattersalls.com/sales", "https://www.tattersalls.com/results"],
        "cloudscraper": False,
        "link_keywords": ["sale", "lot", "result", "catalogue", "horse"],
    },
    "73_goffs": {
        "urls": ["https://www.goffs.com/sales-results"],
        "cloudscraper": True,
        "link_keywords": ["sale", "lot", "result", "catalogue"],
    },
    "74_arqana": {
        "urls": ["https://www.arqana.com/lots/ventes_purs-sang/", "https://www.arqana.com/resultat.html"],
        "cloudscraper": False,
        "link_keywords": ["lot", "vente", "sale", "result", "cheval", "horse"],
    },
    "75_keeneland": {
        "urls": ["https://www.keeneland.com/racing/entries", "https://www.keeneland.com/sales"],
        "cloudscraper": False,
        "link_keywords": ["race", "entry", "result", "sale", "horse"],
    },
    "80_france_galop": {
        "urls": ["https://www.france-galop.com/fr/courses/aujourdhui", "https://www.france-galop.com/fr/courses/hier"],
        "cloudscraper": False,
        "link_keywords": ["course", "cheval", "jockey", "entraineur", "resultat"],
    },
    "82_turf_fr": {
        "urls": ["https://www.turf-fr.com/"],
        "cloudscraper": False,
        "link_keywords": ["course", "resultat", "prono", "partant", "programme", "cheval"],
    },
    "86_smartform": {
        "urls": ["https://www.smartform.co.uk/"],
        "cloudscraper": False,
        "link_keywords": ["race", "result", "card", "horse", "form"],
    },
    "87_bloodstock": {
        "urls": ["https://www.bloodstockauction.com/"],
        "cloudscraper": False,
        "link_keywords": ["sale", "lot", "auction", "horse", "catalogue"],
    },
    "88_weatherbys": {
        "urls": ["https://www.weatherbys.co.uk/"],
        "cloudscraper": False,
        "link_keywords": ["race", "result", "horse", "stallion", "stud"],
    },
    "90_korea_racing": {
        "urls": ["https://www.kra.co.kr/"],
        "cloudscraper": False,
        "link_keywords": ["race", "result", "horse"],
    },
}

# Sites with anti-bot that need cloudscraper for detail pages too
ANTI_BOT_SITES = {
    "58_at_the_races": {
        "urls": ["https://www.attheraces.com/"],
        "link_keywords": ["race", "result", "card", "racecard", "horse"],
    },
    "60_oddschecker": {
        "urls": ["https://www.oddschecker.com/horse-racing"],
        "link_keywords": ["horse-racing", "odds", "race", "result"],
    },
    "64_punters": {
        "urls": ["https://www.punters.com.au/horse-racing/"],
        "link_keywords": ["race", "result", "form", "horse", "tips"],
    },
    "65_racenet": {
        "urls": ["https://www.racenet.com.au/results/"],
        "link_keywords": ["race", "result", "form", "horse"],
    },
    "85_racing_and_sports": {
        "urls": ["https://www.racingandsports.com/"],
        "link_keywords": ["race", "result", "horse", "form"],
    },
}


def make_session(use_cloudscraper=False):
    if use_cloudscraper and cloudscraper:
        s = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    else:
        s = requests.Session()
        s.headers.update(HEADERS)
    return s


def fetch(session, url, max_retries=2, timeout=20):
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 403):
                time.sleep(30)
                continue
            if resp.status_code in (404, 410):
                return None
        except Exception:
            time.sleep(5 * attempt)
    return None


def extract_links(soup, base_url, keywords):
    """Extract links matching keywords from a page."""
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].split("#")[0].strip()
        if not href or href == "/":
            continue
        href_lower = href.lower()
        if any(kw in href_lower for kw in keywords):
            if not href.startswith("http"):
                if href.startswith("/"):
                    # Absolute path
                    from urllib.parse import urlparse
                    parsed = urlparse(base_url)
                    href = f"{parsed.scheme}://{parsed.netloc}{href}"
                else:
                    href = base_url.rstrip("/") + "/" + href
            links.add(href)
    return links


def extract_page_data(soup, url, source_name, date_iso):
    """Extract structured data from a page (tables, JSON, data-attrs)."""
    records = []

    # Tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")[:50]
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True)[:500] for td in row.find_all(["td", "th"])]
            if cells and len(cells) >= 2:
                entry = {
                    "date": date_iso,
                    "source": source_name,
                    "type": "table_row",
                    "url": url,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # __NEXT_DATA__
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if script and script.string:
        try:
            data = json.loads(script.string)
            page_props = data.get("props", {}).get("pageProps", {})
            if page_props:
                records.append({
                    "date": date_iso,
                    "source": source_name,
                    "type": "next_data",
                    "url": url,
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                })
        except json.JSONDecodeError:
            pass

    # application/json scripts
    for sc in soup.find_all("script", {"type": "application/json"}):
        if sc.get("id") == "__NEXT_DATA__":
            continue
        try:
            data = json.loads(sc.string or "")
            if data:
                records.append({
                    "date": date_iso,
                    "source": source_name,
                    "type": "script_json",
                    "url": url,
                    "data_id": sc.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
        except json.JSONDecodeError:
            pass

    # JSON.parse in scripts
    for sc in soup.find_all("script"):
        st = sc.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', st, re.DOTALL):
            try:
                raw = m.group(1).encode("raw_unicode_escape").decode("unicode_escape")
                data = json.loads(raw)
                records.append({
                    "date": date_iso,
                    "source": source_name,
                    "type": "embedded_json",
                    "url": url,
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    # Data attributes
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["horse", "race", "runner", "odds", "jockey", "trainer", "result", "cheval", "course"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_iso,
                "source": source_name,
                "type": "data_attrs",
                "url": url,
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    return records


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def scrape_site(name, config, use_cloudscraper=False, max_pages=50):
    """Scrape one site: fetch start URLs, follow links, extract data."""
    output_dir = os.path.join("output", name)
    cache_dir = os.path.join(output_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{name}_data.jsonl")
    date_iso = datetime.now().strftime("%Y-%m-%d")

    session = make_session(use_cloudscraper)
    total_records = 0
    visited = set()

    urls_to_visit = list(config["urls"])
    keywords = config["link_keywords"]

    page_count = 0
    while urls_to_visit and page_count < max_pages:
        url = urls_to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)

        resp = fetch(session, url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract data from this page
        records = extract_page_data(soup, url, name, date_iso)
        for rec in records:
            append_jsonl(output_file, rec)
            total_records += 1

        # Find more links (only from start pages)
        if page_count < len(config["urls"]) + 3:
            from urllib.parse import urlparse
            base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            new_links = extract_links(soup, base, keywords)
            for link in new_links:
                if link not in visited and urlparse(link).netloc == urlparse(url).netloc:
                    urls_to_visit.append(link)

        page_count += 1
        time.sleep(random.uniform(1.0, 2.5))

        if page_count % 10 == 0:
            log.info(f"  {name}: {page_count} pages, {total_records} records")

    log.info(f"  {name}: DONE - {page_count} pages, {total_records} records")
    return total_records


def main():
    parser = argparse.ArgumentParser(description="Batch scraper for multiple racing sites")
    parser.add_argument("--sites", nargs="*", default=None,
                        help="Specific sites to scrape (default: all)")
    parser.add_argument("--max-pages", type=int, default=30,
                        help="Max pages per site")
    parser.add_argument("--include-antibot", action="store_true",
                        help="Also scrape anti-bot sites with cloudscraper")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("BATCH SCRAPER — Multiple racing sites")
    log.info("=" * 60)

    grand_total = 0

    # Regular sites
    for name, config in sorted(SITES.items()):
        if args.sites and name not in args.sites:
            continue
        log.info(f"Scraping {name}...")
        try:
            n = scrape_site(name, config, config.get("cloudscraper", False), args.max_pages)
            grand_total += n
        except Exception as e:
            log.error(f"  {name}: ERROR {e}")

    # Anti-bot sites
    if args.include_antibot:
        if cloudscraper:
            cs = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "desktop": True}
            )
        else:
            log.warning("cloudscraper non installé, fallback requests.Session()")
            cs = requests.Session()
            cs.headers.update(HEADERS)
        for name, config in sorted(ANTI_BOT_SITES.items()):
            if args.sites and name not in args.sites:
                continue
            log.info(f"Scraping {name} (cloudscraper)...")
            try:
                n = scrape_site(name, config, True, args.max_pages)
                grand_total += n
            except Exception as e:
                log.error(f"  {name}: ERROR {e}")

    log.info("=" * 60)
    log.info(f"BATCH TERMINÉ: {grand_total} records total")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
