#!/usr/bin/env python3
"""
Script 81 — Scraping Pronosoft.com (Playwright version)
Source : pronosoft.com (pronostics gratuits, base PMU, statistiques)
Collecte : pronostics, resultats, rapports PMU, statistiques chevaux/jockeys
CRITIQUE pour : Pronostics Model, Consensus Tips (% par cheval), Base PMU Features

Migrated from requests to Playwright to bypass JS-blocking.
Focus on extracting consensus votes (% per horse) — Pronosoft's unique value.

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import os
import sys
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "81_pronosoft"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, load_checkpoint, save_checkpoint, append_jsonl
from utils.playwright import launch_browser, accept_cookies

log = setup_logging("81_pronosoft")

MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 30_000


def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to url with retry logic. Returns HTML string or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_MS)
            if resp and resp.status >= 400:
                log.warning("  HTTP %d on %s (attempt %d/%d)",
                            resp.status, url, attempt, retries)
                if resp.status == 429:
                    time.sleep(60 * attempt)
                elif resp.status == 403:
                    time.sleep(30 * attempt)
                else:
                    time.sleep(5 * attempt)
                continue
            page.wait_for_load_state("domcontentloaded")
            time.sleep(1.5)
            return page.content()
        except PlaywrightTimeout:
            log.warning("  Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(10 * attempt)
        except Exception as exc:
            log.warning("  Navigation error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    log.error("  Failed after %d retries: %s", retries, url)
    return None


def _extract_consensus_votes(soup, date_str):
    """Extract consensus vote percentages per horse — Pronosoft's unique value.

    Pronosoft shows community voting results as percentages next to each horse.
    These appear in tables/divs with vote/consensus/pourcentage classes or
    as bar charts with percentage labels.
    """
    records = []

    # Pattern 1: Tables with percentage columns
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        headers = [th.get_text(strip=True).lower().replace(" ", "_")
                   for th in rows[0].find_all(["th", "td"])]

        # Check if this table has vote/percentage columns
        has_pct = any(
            kw in h for h in headers
            for kw in ["vote", "pourcent", "%", "consensus", "prono", "avis"]
        )

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue

            # Look for percentage values in cells
            pct_values = []
            for cell in cells:
                pct_match = re.search(r'(\d{1,3}(?:[.,]\d+)?)\s*%', cell)
                if pct_match:
                    pct_values.append(pct_match.group(0))

            entry = {
                "date": date_str,
                "source": "pronosoft",
                "type": "consensus_vote" if (has_pct or pct_values) else "pronostic_table",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                entry[key] = cell
            if pct_values:
                entry["vote_pct_raw"] = pct_values

            # Extract odds
            for cell in cells:
                cote_match = re.search(r'(\d+[.,]\d+)', cell)
                if cote_match:
                    entry["cote"] = cote_match.group(1).replace(",", ".")
                    break

            records.append(entry)

    # Pattern 2: Divs/spans with vote percentages (bar chart style)
    for el in soup.find_all(
        ["div", "span", "li", "p"],
        class_=re.compile(r"vote|consensus|pourcentage|percent|barre|bar|prono-pct", re.I)
    ):
        text = el.get_text(strip=True)
        pct_match = re.search(r'(\d{1,3}(?:[.,]\d+)?)\s*%', text)
        if pct_match and len(text) < 500:
            # Try to find horse name nearby
            horse_name = ""
            name_el = el.find(["a", "span", "strong", "b"])
            if name_el:
                horse_name = name_el.get_text(strip=True)

            records.append({
                "date": date_str,
                "source": "pronosoft",
                "type": "consensus_vote",
                "cheval": horse_name,
                "vote_pct": pct_match.group(1).replace(",", "."),
                "contenu": text[:300],
                "css_class": " ".join(el.get("class", [])),
                "scraped_at": datetime.now().isoformat(),
            })

    # Pattern 3: Elements with data-vote or data-pct attributes
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in ["vote", "pct", "percent", "score"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        records.append({
            "date": date_str,
            "source": "pronosoft",
            "type": "consensus_vote_data",
            "tag": el.name,
            "text": el.get_text(strip=True)[:200],
            "attributes": data_attrs,
            "scraped_at": datetime.now().isoformat(),
        })

    return records


def scrape_pronosoft_day(page, date_str):
    """Scrape pronostics and data from Pronosoft for a given day."""
    cache_file = os.path.join(CACHE_DIR, f"day_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_url = dt.strftime("%d-%m-%Y")
    url = f"https://www.pronosoft.com/fr/turf/pronostics/{date_url}/"

    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # --- Consensus votes (primary extraction target) ---
    consensus = _extract_consensus_votes(soup, date_str)
    records.extend(consensus)

    # --- Reunions and courses ---
    for section in soup.find_all(["div", "section", "article", "table"], class_=True):
        classes = " ".join(section.get("class", []))
        if any(kw in classes.lower() for kw in ["race", "course", "reunion", "programme",
                                                  "prono", "tierce", "quarte", "quinte"]):
            record = {
                "date": date_str,
                "source": "pronosoft",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = section.find(["h2", "h3", "h4", "a", "strong"])
            if title_el:
                record["titre"] = title_el.get_text(strip=True)
            link = section.find("a", href=True)
            if link:
                record["url_course"] = link["href"]
            records.append(record)

    # --- Textual pronostics ---
    for div in soup.find_all(["div", "span", "p", "li"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["prono", "tip", "favori", "prediction",
                                                  "selection", "base", "complement"]):
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "pronostic",
                    "contenu": text,
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Results / rapports ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["resultat", "result", "rapport", "arrivee",
                                                  "classement", "paiement"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "resultat_rapport",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Data attributes (odds, etc.) ---
    for el in soup.find_all(attrs={"data-odds": True}):
        records.append({
            "date": date_str,
            "source": "pronosoft",
            "type": "cote_data",
            "odds": el.get("data-odds"),
            "text": el.get_text(strip=True),
            "scraped_at": datetime.now().isoformat(),
        })

    # --- All relevant data-attributes ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["cote", "odd", "cheval", "horse", "runner", "race", "prono", "tip"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "pronosoft",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Embedded JSON in scripts ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        for m in re.finditer(
            r'window\[?[\'"]?(__\w+|raceData|courseData|pronoData|statsData)[\'"]?\]?\s*=\s*(\{.+?\});',
            script_text, re.DOTALL
        ):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
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
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "embedded_var_array",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    # --- script type="application/json" ---
    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "pronosoft",
                "type": "script_application_json",
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Comments / analyses ---
    for div in soup.find_all(["div", "p", "span", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["comment", "analyse", "avis", "editorial",
                                                  "recap", "resume", "chronique"]):
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "commentaire",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # Save cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_pronosoft_stats(page, date_str):
    """Scrape the stats/base PMU page from Pronosoft."""
    cache_file = os.path.join(CACHE_DIR, f"stats_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_url = dt.strftime("%d-%m-%Y")
    url = f"https://www.pronosoft.com/fr/turf/base-pmu/{date_url}/"

    html = navigate_with_retry(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # --- PMU stats tables ---
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
                entry = {
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "base_pmu_stats",
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, cell in enumerate(cells):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    entry[key] = cell
                records.append(entry)

    # --- Jockey/trainer stats ---
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["jockey", "driver", "entraineur", "trainer",
                                                  "stats", "classement", "ranking"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "stats_acteurs",
                    "contenu": text[:2500],
                    "css_class": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Embedded JSON ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "pronosoft",
                    "type": "stats_embedded_json",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "pronosoft",
                "type": "stats_script_json",
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
    parser = argparse.ArgumentParser(
        description="Script 81 -- Pronosoft Scraper (Playwright, pronostics, consensus, base PMU)"
    )
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Date de debut (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="Date de fin (YYYY-MM-DD), defaut=aujourd'hui")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 81 -- Pronosoft Scraper (Playwright)")
    log.info(f"  Periode : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Reprise au checkpoint : {start_date.date()}")

    output_file = os.path.join(OUTPUT_DIR, "pronosoft_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    with sync_playwright() as pw:
        browser, context, page = launch_browser(
            pw,
            locale="fr-FR",
            timezone="Europe/Paris",
        )
        log.info("Playwright browser launched (fr-FR, Europe/Paris)")

        try:
            # Accept cookies on first navigation
            first_nav = True

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")

                # Pronostics page
                records = scrape_pronosoft_day(page, date_str)

                if first_nav:
                    accept_cookies(page)
                    first_nav = False

                if records:
                    for rec in records:
                        append_jsonl(output_file, rec)
                        total_records += 1

                smart_pause(1.5, 0.8)

                # Stats / Base PMU page
                stats = scrape_pronosoft_stats(page, date_str)
                if stats:
                    for rec in stats:
                        append_jsonl(output_file, rec)
                        total_records += 1

                day_count += 1

                if day_count % 30 == 0:
                    log.info(f"  {date_str} | jours={day_count} records={total_records}")
                    save_checkpoint(CHECKPOINT_FILE, {
                        "last_date": date_str,
                        "total_records": total_records,
                    })

                current += timedelta(days=1)
                smart_pause(1.0, 0.5)

        finally:
            browser.close()
            log.info("Browser closed")

    save_checkpoint(CHECKPOINT_FILE, {
        "last_date": end_date.strftime("%Y-%m-%d"),
        "total_records": total_records,
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINE: {day_count} jours, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
