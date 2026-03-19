#!/usr/bin/env python3
"""
Script 55 (Playwright) -- Scraping Equidia.fr via headless Chromium.
Bypasses Cloudflare/anti-bot by rendering JS in a real browser engine.

Source : equidia.fr/courses/{date}
Collecte : stats terrain, video/replay metadata, resumes, indices de forme
CRITIQUE pour : Terrain Features, Video Analysis Metadata, Track Conditions

Usage:
    pip install playwright
    playwright install chromium
    python 55_equidia_playwright.py --start 2024-01-01 --end 2024-03-31
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper_base_playwright import PlaywrightScraperBase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


class EquidiaPlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "55_equidia_pw"
    BASE_URL = "https://www.equidia.fr"

    # ------------------------------------------------------------------
    # Terrain extraction helpers
    # ------------------------------------------------------------------

    TERRAIN_REGEX = re.compile(
        r'(terrain|piste|sol)\s*:?\s*'
        r'(bon|souple|tr[eè]s souple|collant|lourd|l[eé]ger|sec|'
        r'p[eé]n[eé]trant|tr[eè]s l[eé]ger)',
        re.IGNORECASE,
    )
    PENETRO_REGEX = re.compile(
        r'p[eé]n[eé]trom[eè]tre\s*:?\s*(\d+[.,]?\d*)', re.IGNORECASE
    )

    def _extract_terrain_from_text(self, text, date_str):
        """Parse terrain state and penetrometre from free text."""
        records = []
        m = self.TERRAIN_REGEX.search(text)
        if m:
            rec = {
                "date": date_str,
                "source": "equidia",
                "type": "terrain",
                "etat_terrain": m.group(2).strip(),
                "contexte": text[:200],
                "scraped_at": datetime.utcnow().isoformat(),
            }
            pm = self.PENETRO_REGEX.search(text)
            if pm:
                rec["penetrometre"] = pm.group(1).replace(",", ".")
            records.append(rec)
        return records

    # ------------------------------------------------------------------
    # Day-level scrape
    # ------------------------------------------------------------------

    def scrape_day(self, date_str):
        cache_key = f"day_{date_str}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/courses/{date_str}"
        if not self.navigate(url):
            self.screenshot_on_error(f"equidia_day_{date_str}")
            return {"records": [], "course_links": []}

        self.accept_cookies()
        time.sleep(2)

        records = []
        course_links = []

        # --- Reunions / hippodromes ---
        section_els = self.page.query_selector_all(
            "[class*='reunion'], [class*='meeting'], [class*='hippodrome'], "
            "[class*='course'], [class*='race'], [class*='programme']"
        )
        for section in section_els:
            rec = {
                "date": date_str,
                "source": "equidia",
                "type": "reunion",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            title_el = section.query_selector("h2, h3, h4, strong")
            if title_el:
                rec["hippodrome"] = (title_el.inner_text() or "").strip()

            # Terrain info from inline spans
            spans = section.query_selector_all("span, small, em, p")
            for span in spans:
                text = (span.inner_text() or "").strip()
                if re.search(r'terrain|piste|sol', text, re.I):
                    rec["terrain_info"] = text
                elif re.search(r'(bon|souple|tr.s souple|collant|lourd|l.ger|sec)',
                               text, re.I):
                    rec["etat_terrain"] = text
                elif re.search(r'corde\s*(.*?)?(droite|gauche)', text, re.I):
                    rec["corde"] = text

            # Collect course links
            links = section.query_selector_all("a[href]")
            for a in links:
                href = a.get_attribute("href") or ""
                if re.search(r'/course/|/replay/|/partants/|/programme/', href):
                    full = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                    course_links.append(full)

            records.append(rec)

        # --- Terrain from page text ---
        page_text = self.page.inner_text("body") or ""
        records.extend(self._extract_terrain_from_text(page_text, date_str))

        # --- Terrain-specific DOM sections ---
        terrain_els = self.page.query_selector_all(
            "[class*='terrain'], [class*='piste'], [class*='track'], "
            "[class*='ground'], [class*='penetrometre'], [class*='parcours']"
        )
        for el in terrain_els:
            text = (el.inner_text() or "").strip()
            if text and 3 < len(text) < 1000:
                rec = {
                    "date": date_str,
                    "source": "equidia",
                    "type": "terrain_detail",
                    "contenu": text[:500],
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                pm = self.PENETRO_REGEX.search(text)
                if pm:
                    rec["penetrometre"] = pm.group(1).replace(",", ".")
                records.append(rec)

        # --- Video / replay metadata ---
        video_els = self.page.query_selector_all(
            "video, iframe, source, "
            "[class*='video'], [class*='replay'], [class*='player'], [class*='media']"
        )
        for el in video_els:
            rec = {
                "date": date_str,
                "source": "equidia",
                "type": "video_metadata",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for attr in ["src", "data-src", "data-video-id", "data-video-url",
                          "data-race-id", "data-duration", "data-title",
                          "data-thumbnail", "data-poster", "poster",
                          "data-replay-url", "href"]:
                val = el.get_attribute(attr)
                if val:
                    clean = attr.replace("data-", "").replace("-", "_")
                    rec[clean] = val
            title = (el.inner_text() or "").strip()
            if title and len(title) < 300:
                rec["titre"] = title
            if len(rec) > 4:
                records.append(rec)

        # --- Commentaires / analyses ---
        comment_els = self.page.query_selector_all(
            "[class*='comment'], [class*='analyse'], [class*='resume'], "
            "[class*='description'], [class*='recap'], [class*='editorial'], "
            "[class*='expert'], [class*='avis']"
        )
        for el in comment_els:
            text = (el.inner_text() or "").strip()
            if text and 20 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "equidia",
                    "type": "commentaire",
                    "contenu": text[:2000],
                    "scraped_at": datetime.utcnow().isoformat(),
                })

        # --- Tables ---
        records.extend(self.extract_tables(date_str, "equidia", "stats_course"))

        # --- Embedded JSON ---
        records.extend(self.extract_embedded_json(date_str, "equidia"))

        # --- data-attributes ---
        records.extend(self.extract_data_attributes(
            date_str, "equidia",
            keywords=["terrain", "piste", "cheval", "course", "replay",
                       "video", "hippodrome", "partant"],
        ))

        result = {"records": records, "course_links": list(set(course_links))}
        self.save_cache(cache_key, result)
        return result

    # ------------------------------------------------------------------
    # Course detail
    # ------------------------------------------------------------------

    def scrape_course_detail(self, course_url, date_str):
        cache_key = f"detail_{re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(course_url):
            self.screenshot_on_error(f"equidia_detail_{date_str}")
            return []

        time.sleep(2)
        records = []

        # Title
        nom_prix = ""
        for sel in ["h1", "h2"]:
            el = self.page.query_selector(sel)
            if el:
                nom_prix = (el.inner_text() or "").strip()
                if nom_prix:
                    break

        # Conditions from page text
        page_text = self.page.inner_text("body") or ""
        conditions = {}

        dist_m = re.search(r'(\d[\d\s]*)\s*m(?:etre)?', page_text)
        if dist_m:
            conditions["distance_m"] = dist_m.group(1).replace(" ", "")

        tm = self.TERRAIN_REGEX.search(page_text)
        if tm:
            conditions["etat_terrain"] = tm.group(2).strip()

        disc_m = re.search(r'(trot attel[eé]|trot mont[eé]|plat|haies|steeple|cross)',
                           page_text, re.I)
        if disc_m:
            conditions["discipline"] = disc_m.group(1)

        corde_m = re.search(r'corde\s*(.*?)?(droite|gauche)', page_text, re.I)
        if corde_m:
            conditions["corde"] = corde_m.group(2)

        # Partants
        table_records = self.extract_tables(date_str, "equidia", "partant_detail")
        for rec in table_records:
            rec["nom_prix"] = nom_prix
            rec["conditions"] = conditions
            rec["url_course"] = course_url
        records.extend(table_records)

        # Terrain detail sections
        terrain_els = self.page.query_selector_all(
            "[class*='terrain'], [class*='piste'], [class*='track'], "
            "[class*='ground'], [class*='penetrometre']"
        )
        for el in terrain_els:
            text = (el.inner_text() or "").strip()
            if text and 3 < len(text) < 1000:
                rec = {
                    "date": date_str,
                    "source": "equidia",
                    "type": "terrain_detail",
                    "nom_prix": nom_prix,
                    "contenu": text[:500],
                    "conditions": conditions,
                    "url_course": course_url,
                    "scraped_at": datetime.utcnow().isoformat(),
                }
                pm = self.PENETRO_REGEX.search(text)
                if pm:
                    rec["penetrometre"] = pm.group(1).replace(",", ".")
                records.append(rec)

        # Resume / commentaire
        comment_els = self.page.query_selector_all(
            "[class*='comment'], [class*='resume'], [class*='analyse'], "
            "[class*='description'], [class*='recap']"
        )
        for el in comment_els:
            text = (el.inner_text() or "").strip()
            if text and 20 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "equidia",
                    "type": "resume_course",
                    "nom_prix": nom_prix,
                    "contenu": text,
                    "conditions": conditions,
                    "url_course": course_url,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

        # Replay URLs
        for el in self.page.query_selector_all("video, iframe, source"):
            src = el.get_attribute("src") or el.get_attribute("data-src") or ""
            if src:
                records.append({
                    "date": date_str,
                    "source": "equidia",
                    "type": "replay_url",
                    "nom_prix": nom_prix,
                    "video_src": src,
                    "conditions": conditions,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

        # Embedded JSON
        records.extend(self.extract_embedded_json(date_str, "equidia"))

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
        log.info("SCRIPT 55 (Playwright) -- Equidia Data Scraper")
        log.info("  Periode : %s -> %s", start_date.date(), end_date.date())
        log.info("=" * 60)

        checkpoint = self.load_checkpoint()
        if self.args.resume and checkpoint.get("last_date"):
            resume_dt = datetime.strptime(checkpoint["last_date"], "%Y-%m-%d") + timedelta(days=1)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("  Reprise au checkpoint : %s", start_date.date())

        output_file = os.path.join(self.output_dir, "equidia_data.jsonl")
        self.launch_browser()

        try:
            current = start_date
            day_count = 0
            total_records = 0

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")
                result = self.scrape_day(date_str)

                if result:
                    records = result.get("records", [])
                    for curl in result.get("course_links", [])[:10]:
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
        description="Script 55 (Playwright) -- Equidia Scraper (terrain, video, stats)"
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = EquidiaPlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
