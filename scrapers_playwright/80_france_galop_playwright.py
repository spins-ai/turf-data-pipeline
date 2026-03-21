#!/usr/bin/env python3
"""
Script 80 (Playwright) -- Scraping france-galop.com via headless Chromium.
Bypasses Cloudflare/anti-bot by rendering JS in a real browser engine.

Source : france-galop.com
Collecte : resultats officiels, classements, programmes, statistiques,
           fiches chevaux, terrains officiels, allocations
CRITIQUE pour : Source officielle FR, Ground Truth, Validation Pipeline

Usage:
    pip install playwright
    playwright install chromium
    python 80_france_galop_playwright.py --start 2024-01-01 --end 2024-03-31
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from scraper_base_playwright import PlaywrightScraperBase
from utils.logging_setup import setup_logging
log = setup_logging("80_france_galop_playwright")

# Hippodromes majeurs France Galop
HIPPODROMES_FR = [
    "longchamp", "chantilly", "deauville", "saint-cloud", "auteuil",
    "maisons-laffitte", "enghien", "vincennes", "fontainebleau",
    "compiegne", "lyon-parilly", "marseille-borely", "bordeaux-le-bouscat",
    "toulouse", "strasbourg", "vichy", "clairefontaine", "craon",
    "le-lion-dangers", "nantes", "pau", "cagnes-sur-mer", "mont-de-marsan",
    "dieppe", "cabourg", "royan-la-palmyre", "la-teste-de-buch",
    "le-mans", "angers", "cholet", "nancy", "moulins",
]

RACE_TYPES = ["plat", "obstacles", "haies", "steeple-chase", "cross-country"]


class FranceGalopPlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "80_france_galop_pw"
    BASE_URL = "https://www.france-galop.com"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_hippodrome(text):
        """Match a known hippodrome name in text."""
        text_lower = text.lower()
        for h in HIPPODROMES_FR:
            if h.replace("-", " ") in text_lower:
                return h
        return ""

    @staticmethod
    def _parse_position(text):
        """Extract finishing position from text like '1er', '3e'."""
        m = re.match(r'^(\d+)(?:er|e|[eè]me)?$', text.strip())
        return int(m.group(1)) if m else None

    @staticmethod
    def _parse_allocation(text):
        """Extract EUR allocation amount from text."""
        m = re.search(r'([\d\s.,]+)\s*(?:EUR|euros?|\u20ac)', text, re.I)
        if m:
            val = m.group(1).replace(" ", "").replace(".", "").replace(",", ".")
            try:
                return float(val)
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_time(text):
        """Extract race time in seconds from text like 1'32\"40."""
        m = re.search(r"(\d+)['\u2019](\d{2})[\"'\u2033\u201D](\d+)?", text)
        if m:
            minutes = int(m.group(1))
            seconds = int(m.group(2))
            hundredths = int(m.group(3)) if m.group(3) else 0
            return minutes * 60 + seconds + hundredths / 100.0
        return None

    # ------------------------------------------------------------------
    # Programme du jour
    # ------------------------------------------------------------------

    def scrape_programme_jour(self, date_str):
        cache_key = f"prog_{date_str}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        urls_to_try = [
            f"{self.BASE_URL}/fr/courses/programme/{date_str}",
            f"{self.BASE_URL}/fr/programme/{date_str}",
            f"{self.BASE_URL}/courses/programme?date={date_str}",
            f"{self.BASE_URL}/fr/courses/resultats/{date_str}",
        ]

        loaded = False
        for url in urls_to_try:
            if self.navigate(url):
                loaded = True
                break
            self.smart_pause(3.0, 1.5)

        if not loaded:
            self.screenshot_on_error(f"fg_prog_{date_str}")
            return []

        # First visit: accept cookies
        self.accept_cookies()
        time.sleep(2)

        records = []

        # --- Reunions ---
        section_els = self.page.query_selector_all(
            "[class*='reunion'], [class*='meeting'], "
            "[class*='programme'], [class*='fixture']"
        )
        for section in section_els:
            rec = {
                "source": "france_galop",
                "date": date_str,
                "type": "reunion",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = section.query_selector("h2, h3, h4")
            if title_el:
                rec["reunion_titre"] = (title_el.inner_text() or "").strip()

            section_text = (section.inner_text() or "")
            hippo = self._find_hippodrome(section_text)
            if hippo:
                rec["hippodrome"] = hippo

            # Terrain
            terrain_el = section.query_selector(
                "[class*='terrain'], [class*='going'], [class*='sol']"
            )
            if terrain_el:
                rec["terrain"] = (terrain_el.inner_text() or "").strip()
            else:
                tm = re.search(
                    r'terrain\s*:?\s*(bon|souple|tr.s souple|collant|lourd|'
                    r'l.ger|sec|p.n.trant|tr.s l.ger|bon souple|bon l.ger)',
                    section_text, re.I
                )
                if tm:
                    rec["terrain"] = tm.group(1).strip()

            records.append(rec)

        # --- Courses from tables ---
        table_records = self.extract_tables(date_str, "france_galop", "course_programme")
        # Enrich with course links
        rows = self.page.query_selector_all("table tr")
        for row in rows:
            link_el = row.query_selector("a[href]")
            if link_el:
                href = link_el.get_attribute("href") or ""
                if href:
                    full = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                    # Match to the table record by row text
                    row_text = (row.inner_text() or "").strip()[:100]
                    for rec in table_records:
                        if any(v and v in row_text for v in rec.values()
                               if isinstance(v, str) and len(v) > 3):
                            rec["url_course"] = full
                            break
        records.extend(table_records)

        # --- Embedded JSON ---
        records.extend(self.extract_embedded_json(date_str, "france_galop"))

        # --- data-attributes ---
        records.extend(self.extract_data_attributes(
            date_str, "france_galop",
            keywords=["course", "race", "cheval", "horse", "reunion",
                       "hippodrome", "terrain", "allocation"],
        ))

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Resultats du jour
    # ------------------------------------------------------------------

    def scrape_resultats_jour(self, date_str):
        cache_key = f"res_{date_str}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        urls_to_try = [
            f"{self.BASE_URL}/fr/courses/resultats/{date_str}",
            f"{self.BASE_URL}/fr/resultats/{date_str}",
            f"{self.BASE_URL}/courses/resultats?date={date_str}",
        ]

        loaded = False
        for url in urls_to_try:
            if self.navigate(url):
                loaded = True
                break
            self.smart_pause(3.0, 1.5)

        if not loaded:
            self.screenshot_on_error(f"fg_res_{date_str}")
            return []

        time.sleep(2)
        records = []

        # --- Results from tables ---
        table_records = self.extract_tables(date_str, "france_galop", "resultat_officiel")
        for rec in table_records:
            # Parse position
            for key in list(rec.keys()):
                val = rec.get(key, "")
                if isinstance(val, str):
                    pos = self._parse_position(val)
                    if pos is not None and "position" not in rec:
                        rec["position"] = pos
                    alloc = self._parse_allocation(val)
                    if alloc is not None and "allocation_eur" not in rec:
                        rec["allocation_eur"] = alloc
                    t = self._parse_time(val)
                    if t is not None and "temps_secondes" not in rec:
                        rec["temps_brut"] = val
                        rec["temps_secondes"] = t
        records.extend(table_records)

        # --- Result cards ---
        card_els = self.page.query_selector_all(
            "[class*='result'], [class*='resultat'], "
            "[class*='arrivee'], [class*='classement']"
        )
        for card in card_els:
            rec = {
                "source": "france_galop",
                "date": date_str,
                "type": "resultat_card",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = card.query_selector("h3, h4, strong")
            if title_el:
                rec["nom_prix"] = (title_el.inner_text() or "").strip()

            card_text = (card.inner_text() or "")
            hippo = self._find_hippodrome(card_text)
            if hippo:
                rec["hippodrome"] = hippo

            # Distance
            dist_m = re.search(r'(\d+)\s*m\b', card_text)
            if dist_m:
                rec["distance_m"] = int(dist_m.group(1))

            # Terrain
            for kw in ["bon", "souple", "leger", "collant", "lourd", "tres lourd",
                        "bon souple", "bon leger", "tres souple"]:
                if kw in card_text.lower():
                    rec["terrain"] = kw
                    break

            # Detail link
            link_el = card.query_selector("a[href]")
            if link_el:
                href = link_el.get_attribute("href") or ""
                if href:
                    rec["url_detail"] = href if href.startswith("http") else f"{self.BASE_URL}{href}"

            if rec.get("nom_prix") or rec.get("hippodrome"):
                records.append(rec)

        # --- Commentaires officiels ---
        comment_els = self.page.query_selector_all(
            "[class*='commentaire'], [class*='comment'], [class*='analyse'], "
            "[class*='rapport'], [class*='resume'], [class*='observation'], "
            "[class*='compte-rendu'], [class*='avis-officiel']"
        )
        for el in comment_els:
            text = (el.inner_text() or "").strip()
            if text and 20 < len(text) < 5000:
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "commentaire_officiel",
                    "contenu": text[:4000],
                    "scraped_at": datetime.now().isoformat(),
                })

        # --- Embedded JSON ---
        records.extend(self.extract_embedded_json(date_str, "france_galop"))

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Course detail page
    # ------------------------------------------------------------------

    def scrape_course_detail(self, course_url, date_str):
        if not course_url.startswith("http"):
            course_url = f"{self.BASE_URL}{course_url}"

        cache_key = f"detail_{re.sub(r'[^a-zA-Z0-9]', '_', course_url[-60:])}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(course_url):
            self.screenshot_on_error(f"fg_detail_{date_str}")
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

        page_text = self.page.inner_text("body") or ""

        # Conditions
        conditions = {}
        dist_m = re.search(r'(\d[\d\s]*)\s*m(?:etre)?', page_text)
        if dist_m:
            conditions["distance_m"] = dist_m.group(1).replace(" ", "")

        tm = re.search(
            r'terrain\s*:?\s*(bon|souple|tr.s souple|collant|lourd|'
            r'l.ger|sec|p.n.trant|tr.s l.ger|bon souple|bon l.ger)',
            page_text, re.I
        )
        if tm:
            conditions["etat_terrain"] = tm.group(1).strip()

        disc_m = re.search(r'(plat|haies|steeple|cross|trot)', page_text, re.I)
        if disc_m:
            conditions["discipline"] = disc_m.group(1)

        corde_m = re.search(r'corde\s*(.*?)?(droite|gauche)', page_text, re.I)
        if corde_m:
            conditions["corde"] = corde_m.group(2)

        alloc_m = re.search(r'allocation\s*:?\s*([\d\s.,]+)\s*(?:EUR|\u20ac)', page_text, re.I)
        if alloc_m:
            val = alloc_m.group(1).replace(" ", "").replace(".", "").replace(",", ".")
            try:
                conditions["allocation_eur"] = float(val)
            except ValueError:
                pass

        # Partants table
        table_records = self.extract_tables(date_str, "france_galop", "partant_detail")
        for rec in table_records:
            rec["nom_prix"] = nom_prix
            rec["conditions"] = conditions
            rec["url_course"] = course_url
            # Parse fields
            for key in list(rec.keys()):
                val = rec.get(key, "")
                if isinstance(val, str):
                    pos = self._parse_position(val)
                    if pos is not None and "position" not in rec:
                        rec["position"] = pos
                    t = self._parse_time(val)
                    if t is not None and "temps_secondes" not in rec:
                        rec["temps_brut"] = val
                        rec["temps_secondes"] = t
        records.extend(table_records)

        # Statistiques chevaux (fiches liees)
        horse_links = self.page.query_selector_all(
            "a[href*='/cheval/'], a[href*='/horse/'], a[href*='/fiche/']"
        )
        for link in horse_links[:20]:
            href = link.get_attribute("href") or ""
            name = (link.inner_text() or "").strip()
            if href and name:
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "horse_link",
                    "nom_prix": nom_prix,
                    "nom_cheval": name,
                    "url_fiche": href if href.startswith("http") else f"{self.BASE_URL}{href}",
                    "scraped_at": datetime.now().isoformat(),
                })

        # Commentaires
        comment_els = self.page.query_selector_all(
            "[class*='commentaire'], [class*='comment'], [class*='analyse'], "
            "[class*='rapport'], [class*='resume']"
        )
        for el in comment_els:
            text = (el.inner_text() or "").strip()
            if text and 20 < len(text) < 5000:
                records.append({
                    "source": "france_galop",
                    "date": date_str,
                    "type": "commentaire_detail",
                    "nom_prix": nom_prix,
                    "contenu": text[:4000],
                    "conditions": conditions,
                    "url_course": course_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        # Embedded JSON
        records.extend(self.extract_embedded_json(date_str, "france_galop"))

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
        log.info("SCRIPT 80 (Playwright) -- France Galop Scraper")
        log.info("  Periode : %s -> %s", start_date.date(), end_date.date())
        log.info("=" * 60)

        checkpoint = self.load_checkpoint()
        if self.args.resume and checkpoint.get("last_date"):
            resume_dt = datetime.strptime(checkpoint["last_date"], "%Y-%m-%d") + timedelta(days=1)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("  Reprise au checkpoint : %s", start_date.date())

        output_file = os.path.join(self.output_dir, "france_galop_data.jsonl")
        self.launch_browser()

        try:
            current = start_date
            day_count = 0
            total_records = 0

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")

                # Programme
                prog_records = self.scrape_programme_jour(date_str)

                # Resultats
                res_records = self.scrape_resultats_jour(date_str)

                all_records = prog_records + res_records

                # Course detail pages
                course_urls = list({
                    r.get("url_course") or r.get("url_detail", "")
                    for r in all_records
                    if r.get("url_course") or r.get("url_detail")
                })
                for curl in course_urls[:15]:
                    detail = self.scrape_course_detail(curl, date_str)
                    if detail:
                        all_records.extend(detail)
                    self.smart_pause(5.0, 2.5)

                for rec in all_records:
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
        description="Script 80 (Playwright) -- France Galop Official Data Scraper"
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = FranceGalopPlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
