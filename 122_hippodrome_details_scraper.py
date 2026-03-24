#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script 122 -- Hippodrome Details Scraper (Playwright)
Source : france-galop.com/hippodromes/{slug}
Collecte : Track surface types (PSF, herbe, sable), track lengths and
           configurations, available distances, cordes (left/right),
           infrastructure info for all French hippodromes.
CRITIQUE pour : Venue Feature Enrichment, Track Bias Analysis,
                hippodromes_db.py verification / enrichment

Reads all hippodromes with pays='france' from hippodromes_db.py, builds a
france-galop.com URL slug for each, scrapes the page with Playwright, and
writes enriched records to output/122_hippodrome_details/hippodrome_details.jsonl.

Locale : fr-FR
Checkpoint/resume support via .checkpoint.json.

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium

Usage:
    python 122_hippodrome_details_scraper.py
    python 122_hippodrome_details_scraper.py --resume
    python 122_hippodrome_details_scraper.py --hippodrome longchamp
    python 122_hippodrome_details_scraper.py --list
    python 122_hippodrome_details_scraper.py --max-pages 50
"""

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup

SCRIPT_NAME = "122_hippodrome_details"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
HTML_CACHE_DIR = os.path.join(OUTPUT_DIR, "html_cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HTML_CACHE_DIR, exist_ok=True)

sys.path.insert(0, SCRIPT_DIR)
from hippodromes_db import HIPPODROMES_DB
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, append_jsonl, load_checkpoint, save_checkpoint
from utils.playwright import launch_browser, accept_cookies

log = setup_logging("122_hippodrome_details")

FRANCE_GALOP_BASE = "https://www.france-galop.com"
MAX_RETRIES = 3
DEFAULT_TIMEOUT_MS = 60_000


# ======================================================================
# Helpers
# ======================================================================

def slugify(name: str) -> str:
    """Convert a hippodrome name to a france-galop URL slug.

    Examples:
        'longchamp'             -> 'longchamp'
        'aix les bains'         -> 'aix-les-bains'
        'saint cloud'           -> 'saint-cloud'
        'la roche posay'        -> 'la-roche-posay'
        'cagnes sur mer'        -> 'cagnes-sur-mer'
    """
    # Normalise unicode (strip accents)
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_text = nfkd.encode("ascii", "ignore").decode("ascii")
    # Lowercase, replace spaces/underscores with hyphens
    slug = re.sub(r"[\s_]+", "-", ascii_text.lower().strip())
    # Remove anything that isn't alphanumeric or hyphen
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    # Collapse multiple hyphens
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug


def get_french_hippodromes() -> list[dict]:
    """Extract all French hippodromes from HIPPODROMES_DB."""
    results = []
    for name, data in HIPPODROMES_DB.items():
        pays = (data.get("pays") or "").lower().strip()
        if pays == "france":
            entry = {"db_key": name, **data}
            results.append(entry)
    return sorted(results, key=lambda h: h["db_key"])


# ======================================================================
# Navigation
# ======================================================================

def navigate_with_retry(page, url, retries=MAX_RETRIES):
    """Navigate to URL with retry logic. Returns HTML string or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
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
            time.sleep(2)
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except PlaywrightTimeout:
                pass
            return page.content()
        except PlaywrightTimeout:
            log.warning("  Timeout on %s (attempt %d/%d)", url, attempt, retries)
            time.sleep(10 * attempt)
        except Exception as exc:
            log.warning("  Navigation error: %s (attempt %d/%d)",
                        str(exc)[:200], attempt, retries)
            time.sleep(5 * attempt)
    return None


def save_html_cache(hippo_key, html):
    """Save raw HTML for audit trail."""
    if not html:
        return
    safe_name = re.sub(r"[^a-z0-9_-]", "_", hippo_key) + ".html"
    path = os.path.join(HTML_CACHE_DIR, safe_name)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
    except OSError as exc:
        log.warning("  Could not save HTML cache: %s", exc)


# ======================================================================
# Extraction logic
# ======================================================================

def extract_text_blocks(html):
    """Extract meaningful text blocks from HTML."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "noscript", "iframe"]):
        tag.decompose()
    blocks = []
    for el in soup.find_all(["p", "li", "td", "th", "h1", "h2", "h3", "h4",
                              "span", "div", "dt", "dd"]):
        text = el.get_text(separator=" ", strip=True)
        if 10 < len(text) < 3000:
            blocks.append(text)
    return blocks


def extract_surface_types(blocks):
    """Detect track surface types from text (PSF, herbe, sable, gazon, etc.)."""
    surfaces = set()
    surface_map = {
        "psf": "PSF",
        "polytrack": "PSF",
        "piste en sable fibre": "PSF",
        "piste en sable fibré": "PSF",
        "herbe": "herbe",
        "gazon": "herbe",
        "turf": "herbe",
        "sable": "sable",
        "cendrée": "cendree",
        "cendree": "cendree",
        "mâchefer": "cendree",
        "machefer": "cendree",
        "all weather": "PSF",
        "all-weather": "PSF",
        "synthétique": "PSF",
        "synthetique": "PSF",
        "dirt": "sable",
        "fibresand": "PSF",
    }
    full_text = " ".join(blocks).lower()
    for keyword, label in surface_map.items():
        if keyword in full_text:
            surfaces.add(label)
    return sorted(surfaces)


def extract_corde(blocks):
    """Detect corde (left/right hand) from text."""
    full_text = " ".join(blocks).lower()
    corde_patterns = [
        (r"corde\s*(?:a\s+|à\s+)?gauche", "gauche"),
        (r"corde\s*(?:a\s+|à\s+)?droite", "droite"),
        (r"main\s+gauche", "gauche"),
        (r"main\s+droite", "droite"),
        (r"left[- ]?hand", "gauche"),
        (r"right[- ]?hand", "droite"),
        (r"sens\s+inverse\b.*\baiguilles", "gauche"),
        (r"sens\b.*\baiguilles", "droite"),
    ]
    for pattern, value in corde_patterns:
        if re.search(pattern, full_text):
            return value
    return None


def extract_distances(blocks):
    """Extract available race distances from text."""
    distances = set()
    full_text = " ".join(blocks)
    # Pattern: 1000m, 1 200m, 2.400m, 1600 mètres, etc.
    for m in re.finditer(
        r"(\d[\d\s.,]*\d)\s*(?:m(?:ètres?|etres?)?)\b", full_text, re.IGNORECASE
    ):
        raw = m.group(1)
        # Clean: remove spaces, replace comma with nothing
        cleaned = re.sub(r"[\s.]", "", raw).replace(",", "")
        try:
            d = int(cleaned)
            if 500 <= d <= 10000:
                distances.add(d)
        except ValueError:
            pass
    return sorted(distances)


def extract_track_lengths(blocks):
    """Extract track lengths / circumference data."""
    results = {}
    full_text = " ".join(blocks)

    circum = re.search(
        r"(?:circumf[eé]rence|p[eé]rim[eè]tre|tour de piste|développé|developpe)"
        r"[:\s]*(?:environ\s+)?(\d[\d\s.,]*)\s*(?:m|km)",
        full_text, re.IGNORECASE
    )
    if circum:
        results["circumference_raw"] = circum.group(0).strip()

    straight = re.search(
        r"(?:ligne\s+droite|dernier\w*\s+droit\w*|straight)"
        r"[:\s]*(?:environ\s+)?(\d[\d\s.,]*)\s*(?:m|km)",
        full_text, re.IGNORECASE
    )
    if straight:
        results["straight_raw"] = straight.group(0).strip()

    # Piste length mentions: "piste de 2000m", "parcours de 1800 mètres"
    for m in re.finditer(
        r"(?:piste|parcours|tracé|trace)\s+(?:de\s+)?(\d[\d\s.,]*)\s*(?:m|km)",
        full_text, re.IGNORECASE
    ):
        results.setdefault("track_lengths_raw", []).append(m.group(0).strip())

    return results


def extract_configurations(blocks):
    """Extract track configurations (number of parcours, turns, etc.)."""
    configs = []
    full_text = " ".join(blocks)

    # "X parcours", "X pistes"
    for m in re.finditer(
        r"(\d+)\s*(?:parcours|pistes?|circuits?|tracés?|traces?)",
        full_text, re.IGNORECASE
    ):
        configs.append(f"{m.group(1)} parcours")

    # Virages / turns
    for m in re.finditer(r"(\d+)\s*(?:virages?|turns?|bends?)", full_text, re.IGNORECASE):
        configs.append(f"{m.group(1)} virages")

    return list(dict.fromkeys(configs))  # deduplicate preserving order


def extract_infrastructure(blocks):
    """Extract infrastructure mentions (tribunes, eclairage, etc.)."""
    infra = []
    keywords = [
        "tribune", "éclairage", "eclairage", "nocturne", "parking",
        "restaurant", "vestiaire", "pesage", "rond de présentation",
        "rond de presentation", "départ", "depart", "paddock",
        "photo-finish", "photo finish", "écran", "ecran",
        "arrosage", "drainage", "pénétromètre", "penetrometre",
    ]
    full_text = " ".join(blocks).lower()
    for kw in keywords:
        if kw in full_text:
            infra.append(kw)
    return sorted(set(infra))


def extract_disciplines(blocks):
    """Extract racing disciplines mentioned."""
    disciplines = set()
    discipline_map = {
        "plat": "plat",
        "flat": "plat",
        "obstacle": "obstacle",
        "haie": "haie",
        "hurdle": "haie",
        "steeple": "steeple",
        "steeplechase": "steeple",
        "cross": "cross_country",
        "cross-country": "cross_country",
        "trot attelé": "trot_attele",
        "trot attele": "trot_attele",
        "trot monté": "trot_monte",
        "trot monte": "trot_monte",
        "trot": "trot_attele",
    }
    full_text = " ".join(blocks).lower()
    for keyword, label in discipline_map.items():
        if keyword in full_text:
            disciplines.add(label)
    return sorted(disciplines)


# ======================================================================
# Scrape a single hippodrome
# ======================================================================

def scrape_hippodrome(page, hippo_key, hippo_data):
    """Scrape france-galop.com page for one French hippodrome.

    Returns a dict with enriched data, or None on complete failure.
    """
    slug = slugify(hippo_key)
    url = f"{FRANCE_GALOP_BASE}/fr/hippodrome/{slug}"

    log.info("  Scraping %s -> %s", hippo_key, url)

    html = navigate_with_retry(page, url)
    if not html:
        # Try alternative slug patterns
        alt_slugs = []
        # Remove common suffixes like "le passage", "la garenne"
        base = hippo_key.split(" ")[0] if " " in hippo_key else None
        if base and base != hippo_key:
            alt_slugs.append(slugify(base))
        # Try with "hippodrome-de-" prefix removed or added
        if not slug.startswith("hippodrome"):
            alt_slugs.append(f"hippodrome-de-{slug}")

        for alt in alt_slugs:
            alt_url = f"{FRANCE_GALOP_BASE}/fr/hippodrome/{alt}"
            log.info("    Trying alternative: %s", alt_url)
            html = navigate_with_retry(page, alt_url)
            if html:
                url = alt_url
                slug = alt
                break
            smart_pause(1.5, 0.5)

    if not html:
        log.warning("    Failed to load any page for %s", hippo_key)
        return {
            "scraper": SCRIPT_NAME,
            "scrape_date": datetime.now().isoformat(),
            "hippodrome_key": hippo_key,
            "slug": slug,
            "url_attempted": url,
            "status": "failed",
            "lat": hippo_data.get("lat"),
            "lon": hippo_data.get("lon"),
            "region": hippo_data.get("region"),
            "pays": "france",
        }

    # Accept cookies on first page
    try:
        accept_cookies(page)
    except Exception:
        pass

    save_html_cache(hippo_key, html)

    # Extract text blocks
    blocks = extract_text_blocks(html)
    log.info("    Extracted %d text blocks", len(blocks))

    # Also try JS-rendered content via Playwright evaluate
    try:
        js_blocks = page.evaluate("""() => {
            const results = [];
            const els = document.querySelectorAll(
                'main, article, .content, .field, .hippodrome, ' +
                '.piste, .parcours, .track, [class*="hippo"], [class*="piste"], ' +
                '[class*="track"], [class*="course"]'
            );
            for (const el of els) {
                const text = (el.textContent || '').trim();
                if (text.length > 15 && text.length < 5000) {
                    results.push(text);
                }
            }
            return results;
        }""")
        if js_blocks:
            blocks.extend(js_blocks)
    except Exception:
        pass

    # Extract structured data
    surface_types = extract_surface_types(blocks)
    corde = extract_corde(blocks)
    distances = extract_distances(blocks)
    track_lengths = extract_track_lengths(blocks)
    configurations = extract_configurations(blocks)
    infrastructure = extract_infrastructure(blocks)
    disciplines = extract_disciplines(blocks)

    # Build enriched record
    record = {
        "scraper": SCRIPT_NAME,
        "scrape_date": datetime.now().isoformat(),
        "hippodrome_key": hippo_key,
        "slug": slug,
        "url": url,
        "status": "ok",
        "pays": "france",
        "region": hippo_data.get("region", ""),
        "lat": hippo_data.get("lat"),
        "lon": hippo_data.get("lon"),
        "altitude": hippo_data.get("altitude"),
        # Existing DB data (for cross-reference)
        "db_type_piste": hippo_data.get("type_piste"),
        "db_corde": hippo_data.get("corde"),
        "db_disciplines": hippo_data.get("disciplines", []),
        "db_distance_min": hippo_data.get("distance_min"),
        "db_distance_max": hippo_data.get("distance_max"),
        # Scraped enrichment data
        "surface_types": surface_types,
        "corde": corde,
        "distances_available": distances,
        "track_lengths": track_lengths,
        "configurations": configurations,
        "disciplines_scraped": disciplines,
        "infrastructure": infrastructure,
        "text_blocks_count": len(blocks),
        "source_urls": [url],
    }

    return record


# ======================================================================
# Main
# ======================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Script 122 -- Hippodrome Details Scraper (france-galop.com, Playwright)"
    )
    parser.add_argument("--hippodrome", "-r", type=str, default=None,
                        help="Scrape only this hippodrome key (e.g. 'longchamp', 'auteuil')")
    parser.add_argument("--list", action="store_true",
                        help="List all French hippodrome keys and exit")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint")
    parser.add_argument("--max-pages", type=int, default=0,
                        help="Max hippodromes to scrape (0 = all)")
    parser.add_argument("--headless", action="store_true", default=True,
                        help="Run browser in headless mode (default: True)")
    parser.add_argument("--no-headless", dest="headless", action="store_false",
                        help="Run browser in headed mode for debugging")
    args = parser.parse_args()

    # Get all French hippodromes
    all_french = get_french_hippodromes()
    log.info("Found %d French hippodromes in hippodromes_db.py", len(all_french))

    if args.list:
        print(f"French hippodromes ({len(all_french)}):")
        for h in all_french:
            region = h.get("region", "")
            print(f"  {h['db_key']:40s}  {region}")
        return

    # Filter to single hippodrome if requested
    if args.hippodrome:
        all_french = [h for h in all_french if h["db_key"] == args.hippodrome]
        if not all_french:
            log.error("Unknown hippodrome key: %s", args.hippodrome)
            return

    # Load checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE) if args.resume else {}
    completed = set(checkpoint.get("completed", []))
    total_records = checkpoint.get("total_records", 0)

    if args.resume and completed:
        log.info("Resuming: %d hippodromes already completed", len(completed))

    output_file = os.path.join(OUTPUT_DIR, "hippodrome_details.jsonl")
    max_pages = args.max_pages if args.max_pages > 0 else len(all_french)

    log.info("=" * 60)
    log.info("SCRIPT 122 -- Hippodrome Details Scraper (france-galop.com)")
    log.info("  Hippodromes : %d French (targeting up to %d)", len(all_french), max_pages)
    log.info("  Output      : %s", output_file)
    log.info("  Locale      : fr-FR")
    log.info("=" * 60)

    with sync_playwright() as pw:
        browser, context, page = launch_browser(
            pw,
            locale="fr-FR",
            timezone="Europe/Paris",
            headless=args.headless,
        )

        try:
            pages_done = 0
            for hippo in all_french:
                if pages_done >= max_pages:
                    log.info("Reached max-pages limit (%d)", max_pages)
                    break

                key = hippo["db_key"]
                if key in completed:
                    log.info("  Skipping %s (checkpoint)", key)
                    continue

                try:
                    record = scrape_hippodrome(page, key, hippo)
                    if record:
                        append_jsonl(output_file, record)
                        total_records += 1

                        # Save individual cache
                        safe_key = re.sub(r"[^a-z0-9_-]", "_", key)
                        cache_path = os.path.join(CACHE_DIR, f"{safe_key}.json")
                        with open(cache_path, "w", encoding="utf-8") as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)

                        completed.add(key)
                        save_checkpoint(CHECKPOINT_FILE, {
                            "completed": sorted(completed),
                            "total_records": total_records,
                            "last_updated": datetime.now().isoformat(),
                        })

                        status = record.get("status", "?")
                        n_surfaces = len(record.get("surface_types", []))
                        n_dist = len(record.get("distances_available", []))
                        log.info("    -> %s  surfaces=%d  distances=%d  status=%s",
                                 key, n_surfaces, n_dist, status)

                except Exception as exc:
                    log.error("  Error scraping %s: %s", key, str(exc)[:300])

                pages_done += 1

                # Rotate browser every 50 pages to avoid memory leaks
                if pages_done % 50 == 0 and pages_done > 0:
                    log.info("  Rotating browser context (%d pages done)...", pages_done)
                    browser.close()
                    smart_pause(5, 3)
                    browser, context, page = launch_browser(
                        pw,
                        locale="fr-FR",
                        timezone="Europe/Paris",
                        headless=args.headless,
                    )

                smart_pause(3, 2)

        finally:
            browser.close()

    log.info("=" * 60)
    log.info("DONE: %d / %d hippodromes scraped, %d total records -> %s",
             len(completed), len(all_french), total_records, output_file)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
