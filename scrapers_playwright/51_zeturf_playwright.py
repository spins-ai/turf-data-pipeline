#!/usr/bin/env python3
"""
Script 51 (Playwright) -- Scraping ZeTurf.fr via headless Chromium.
Bypasses Cloudflare/anti-bot by rendering JS in a real browser engine.

Source : zeturf.fr/fr/course/{date}
Collecte : cotes, pronostics, partants, conditions, rapports
CRITIQUE pour : Odds Model, Value Detection, Market Features

Usage:
    pip install playwright
    playwright install chromium
    python 51_zeturf_playwright.py --start 2024-01-01 --end 2024-03-31
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta

# Ensure parent dir is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper_base_playwright import PlaywrightScraperBase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


class ZeTurfPlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "51_zeturf_pw"
    BASE_URL = "https://www.zeturf.fr"

    # ------------------------------------------------------------------
    # Day-level scrape
    # ------------------------------------------------------------------

    def scrape_day(self, date_str):
        """Scrape all ZeTurf data for one day. Returns list of records."""
        cache_key = f"day_{date_str}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/fr/course/{date_str}"
        if not self.navigate(url):
            self.screenshot_on_error(f"zeturf_day_{date_str}")
            return []

        # First visit: accept cookies
        self.accept_cookies()

        # Let dynamic content render
        time.sleep(2)

        records = []

        # --- Reunions / courses from rendered DOM ---
        sections = self.page.query_selector_all(
            "div[class*='race'], div[class*='course'], "
            "section[class*='reunion'], article[class*='programme'], "
            "div[class*='reunion'], div[class*='programme']"
        )
        for section in sections:
            rec = {
                "date": date_str,
                "source": "zeturf",
                "type": "reunion_course",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = section.query_selector("h2, h3, h4, a")
            if title_el:
                rec["titre"] = (title_el.inner_text() or "").strip()
            link_el = section.query_selector("a[href]")
            if link_el:
                rec["url_course"] = link_el.get_attribute("href") or ""
            records.append(rec)

        # --- Tables (cotes / partants) ---
        records.extend(
            self.extract_tables(date_str, "zeturf", "cote_partant")
        )

        # --- Pronostics ---
        pronostic_els = self.page.query_selector_all(
            "[class*='prono'], [class*='tip'], [class*='favori'], [class*='prediction']"
        )
        for el in pronostic_els:
            text = (el.inner_text() or "").strip()
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "pronostic",
                    "contenu": text,
                    "scraped_at": datetime.now().isoformat(),
                })

        # --- Embedded JSON from scripts ---
        records.extend(self.extract_embedded_json(date_str, "zeturf"))

        # --- data-odds and similar data-attributes ---
        records.extend(self.extract_data_attributes(
            date_str, "zeturf",
            keywords=["odds", "cote", "cheval", "horse", "runner",
                       "race", "pari", "bet", "mise"],
        ))

        # --- Cotes from data-odds ---
        odds_els = self.page.query_selector_all("[data-odds]")
        for el in odds_els:
            records.append({
                "date": date_str,
                "source": "zeturf",
                "type": "cote_data",
                "odds": el.get_attribute("data-odds"),
                "text": (el.inner_text() or "").strip()[:200],
                "scraped_at": datetime.now().isoformat(),
            })

        # --- Commentaires de course ---
        comment_els = self.page.query_selector_all(
            "[class*='comment'], [class*='analyse'], [class*='avis'], "
            "[class*='editorial'], [class*='recap'], [class*='resume'], "
            "[class*='rapport']"
        )
        for el in comment_els:
            text = (el.inner_text() or "").strip()
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "commentaire_course",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

        # --- Cotes par type de pari ---
        bet_els = self.page.query_selector_all(
            "[class*='pari'], [class*='bet-type'], [class*='simple'], "
            "[class*='couple'], [class*='tierce'], [class*='quarte'], "
            "[class*='quinte'], [class*='multi'], [class*='2sur4']"
        )
        for el in bet_els:
            items = el.query_selector_all("tr, li, div")
            for item in items:
                text = (item.inner_text() or "").strip()
                if text and re.search(r'\d', text) and 3 < len(text) < 500:
                    records.append({
                        "date": date_str,
                        "source": "zeturf",
                        "type": "cote_par_pari",
                        "contenu": text,
                        "scraped_at": datetime.now().isoformat(),
                    })

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Course detail
    # ------------------------------------------------------------------

    def scrape_course_detail(self, course_url, date_str):
        """Scrape a single race detail page."""
        if not course_url.startswith("http"):
            course_url = f"{self.BASE_URL}{course_url}"

        cache_key = f"detail_{re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(course_url):
            self.screenshot_on_error(f"zeturf_detail_{date_str}")
            return []

        time.sleep(2)
        records = []

        # Race title
        nom_prix = ""
        for sel in ["h1", "h2"]:
            el = self.page.query_selector(sel)
            if el:
                nom_prix = (el.inner_text() or "").strip()
                if nom_prix:
                    break

        # Partants table
        table_records = self.extract_tables(date_str, "zeturf", "partant_detail")
        for rec in table_records:
            rec["nom_prix"] = nom_prix
            rec["url_course"] = course_url
        records.extend(table_records)

        # Commentaires
        comment_els = self.page.query_selector_all(
            "[class*='comment'], [class*='analyse'], [class*='avis'], "
            "[class*='recap'], [class*='rapport'], [class*='resume'], "
            "[class*='verdict']"
        )
        for el in comment_els:
            text = (el.inner_text() or "").strip()
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "commentaire_detail",
                    "nom_prix": nom_prix,
                    "contenu": text[:2500],
                    "url_course": course_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        # Historique forme
        form_els = self.page.query_selector_all(
            "[class*='form'], [class*='historique'], [class*='dernieres'], "
            "[class*='palmares'], [class*='previous'], [class*='last-run'], "
            "[class*='perf']"
        )
        for el in form_els:
            text = (el.inner_text() or "").strip()
            if text and 10 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "historique_forme",
                    "nom_prix": nom_prix,
                    "contenu": text[:1500],
                    "url_course": course_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        # Embedded JSON
        records.extend(self.extract_embedded_json(date_str, "zeturf"))

        # Video metadata
        video_els = self.page.query_selector_all(
            "video, iframe[src*='video'], iframe[src*='replay'], "
            "source[src*='mp4'], source[src*='m3u8']"
        )
        for el in video_els:
            src = (el.get_attribute("src") or el.get_attribute("data-src")
                   or el.get_attribute("data-video-url") or "")
            if src:
                records.append({
                    "date": date_str,
                    "source": "zeturf",
                    "type": "video_metadata",
                    "nom_prix": nom_prix,
                    "media_url": src,
                    "scraped_at": datetime.now().isoformat(),
                })

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self):
        start_date = datetime.strptime(self.args.start, "%Y-%m-%d")
        end_date = (datetime.strptime(self.args.end, "%Y-%m-%d")
                    if self.args.end else datetime.now())

        log.info("=" * 60)
        log.info("SCRIPT 51 (Playwright) -- ZeTurf Scraper")
        log.info("  Periode : %s -> %s", start_date.date(), end_date.date())
        log.info("=" * 60)

        checkpoint = self.load_checkpoint()
        if self.args.resume and checkpoint.get("last_date"):
            resume_dt = datetime.strptime(checkpoint["last_date"], "%Y-%m-%d") + timedelta(days=1)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("  Reprise au checkpoint : %s", start_date.date())

        output_file = os.path.join(self.output_dir, "zeturf_data.jsonl")
        self.launch_browser()

        try:
            current = start_date
            day_count = 0
            total_records = 0

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")
                records = self.scrape_day(date_str)

                if records:
                    # Collect course URLs for detail scraping
                    course_urls = list({
                        r.get("url_course") for r in records
                        if r.get("url_course")
                    })
                    for curl in course_urls:
                        detail = self.scrape_course_detail(curl, date_str)
                        if detail:
                            records.extend(detail)
                        self.smart_pause(5.0, 2.0)

                    for rec in records:
                        self.append_jsonl(output_file, rec)
                        total_records += 1

                day_count += 1

                if day_count % 30 == 0:
                    log.info("  %s | jours=%d records=%d",
                             date_str, day_count, total_records)
                    self.save_checkpoint({
                        "last_date": date_str,
                        "total_records": total_records,
                    })

                # Rotate browser context periodically
                if day_count % 80 == 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(10.0, 5.0)
                    self.launch_browser()

                current += timedelta(days=1)
                self.smart_pause(5.0, 2.5)

            self.save_checkpoint({
                "last_date": end_date.strftime("%Y-%m-%d"),
                "total_records": total_records,
                "status": "done",
            })
            log.info("=" * 60)
            log.info("TERMINE: %d jours, %d records -> %s",
                     day_count, total_records, output_file)
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description="Script 51 (Playwright) -- ZeTurf Scraper"
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = ZeTurfPlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
