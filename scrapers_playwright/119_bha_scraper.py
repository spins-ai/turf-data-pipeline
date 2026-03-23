#!/usr/bin/env python3
"""
Script 119 (Playwright) -- British Horseracing Authority scraper.
Source : britishhorseracing.com / bha.co.uk
Collecte : licensed trainers, licensed jockeys, race results, penalties/sanctions
CRITIQUE pour : UK official regulatory data, validation croisee Racing Post

Usage:
    pip install playwright
    playwright install chromium
    python 119_bha_scraper.py --start 2022-01-01 --end 2026-03-23
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

log = setup_logging("119_bha_scraper")

# BHA race types
BHA_RACE_TYPES = ["flat", "hurdle", "chase", "nhf", "bumper"]

# Major UK courses
UK_COURSES = [
    "ascot", "cheltenham", "aintree", "epsom", "goodwood", "newmarket",
    "york", "doncaster", "sandown", "kempton", "newbury", "haydock",
    "chester", "windsor", "lingfield", "wolverhampton", "catterick",
    "thirsk", "ripon", "nottingham", "leicester", "warwick",
    "bangor-on-dee", "market-rasen", "wincanton", "exeter", "fontwell",
    "plumpton", "sedgefield", "wetherby", "uttoxeter", "carlisle",
    "musselburgh", "ayr", "hamilton", "perth", "kelso",
]


class BHAPlaywright(PlaywrightScraperBase):
    """Scraper for the British Horseracing Authority official website."""

    SCRIPT_NAME = "119_bha_scraper"
    BASE_URL = "https://www.britishhorseracing.com"
    ALT_URL = "https://www.bha.co.uk"

    # ------------------------------------------------------------------
    # Override browser launch for en-GB locale
    # ------------------------------------------------------------------

    def launch_browser(self):
        """Launch headless Chromium with en-GB locale."""
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()

        launch_args = {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        }
        if getattr(self.args, "proxy", None):
            launch_args["proxy"] = {"server": self.args.proxy}

        self._browser = self._playwright.chromium.launch(**launch_args)

        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-GB",
            timezone_id="Europe/London",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            java_script_enabled=True,
            ignore_https_errors=True,
        )
        self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-GB', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            window.chrome = {runtime: {}};
        """)
        self.page = self._context.new_page()
        self.page.set_default_timeout(self.DEFAULT_TIMEOUT_MS)
        log.info("Browser launched (headless Chromium, en-GB)")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_course(text):
        """Match a known UK course name in text."""
        text_lower = text.lower()
        for c in UK_COURSES:
            if c.replace("-", " ") in text_lower:
                return c
        return ""

    @staticmethod
    def _parse_position(text):
        """Extract finishing position from text like '1st', '3rd'."""
        m = re.match(r"^(\d+)(?:st|nd|rd|th)?$", text.strip(), re.I)
        return int(m.group(1)) if m else None

    @staticmethod
    def _parse_prize(text):
        """Extract GBP prize money from text."""
        m = re.search(r"[^\d]?([\d,]+(?:\.\d{2})?)\s*(?:GBP|\u00a3|pounds?)?", text)
        if m:
            val = m.group(1).replace(",", "")
            try:
                return float(val)
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_time(text):
        """Extract race time in seconds from text like '1m 32.40s'."""
        # Format: 1m 32.40s or 1:32.40
        m = re.search(r"(\d+)\s*m\s*(\d+(?:\.\d+)?)\s*s?", text, re.I)
        if m:
            return int(m.group(1)) * 60 + float(m.group(2))
        m = re.search(r"(\d+):(\d+(?:\.\d+)?)", text)
        if m:
            return int(m.group(1)) * 60 + float(m.group(2))
        return None

    # ------------------------------------------------------------------
    # Licensed trainers directory
    # ------------------------------------------------------------------

    def scrape_trainers(self):
        """Scrape the BHA licensed trainers directory."""
        cache_key = "trainers_directory"
        cached = self.load_cache(cache_key)
        if cached is not None:
            log.info("  Trainers: loaded %d from cache", len(cached))
            return cached

        urls_to_try = [
            f"{self.BASE_URL}/racing/participants/trainers",
            f"{self.BASE_URL}/directory/trainers",
            f"{self.BASE_URL}/regulation/licensed-trainers",
            f"{self.ALT_URL}/racing/participants/trainers",
        ]

        loaded = False
        for url in urls_to_try:
            if self.navigate(url):
                loaded = True
                break
            self.smart_pause(3.0, 1.5)

        if not loaded:
            self.screenshot_on_error("bha_trainers")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # Table-based listings
        table_records = self.extract_tables(
            datetime.now().strftime("%Y-%m-%d"), "bha", "licensed_trainer"
        )
        records.extend(table_records)

        # Card-based listings
        card_els = self.page.query_selector_all(
            "[class*='trainer'], [class*='participant'], "
            "[class*='person'], [class*='directory'] li, "
            "[class*='listing'] li"
        )
        for card in card_els:
            text = (card.inner_text() or "").strip()
            if not text or len(text) < 3:
                continue
            rec = {
                "source": "bha",
                "type": "licensed_trainer",
                "scraped_at": datetime.now().isoformat(),
            }
            # Name
            name_el = card.query_selector("h3, h4, strong, a, .name")
            if name_el:
                rec["name"] = (name_el.inner_text() or "").strip()
            elif text:
                rec["name"] = text.split("\n")[0].strip()

            # Location
            loc_el = card.query_selector(
                "[class*='location'], [class*='base'], [class*='address']"
            )
            if loc_el:
                rec["location"] = (loc_el.inner_text() or "").strip()

            # Licence type
            lic_el = card.query_selector(
                "[class*='licence'], [class*='license'], [class*='type']"
            )
            if lic_el:
                rec["licence_type"] = (lic_el.inner_text() or "").strip()

            # Profile link
            link_el = card.query_selector("a[href]")
            if link_el:
                href = link_el.get_attribute("href") or ""
                if href:
                    rec["url_profile"] = (
                        href if href.startswith("http")
                        else f"{self.BASE_URL}{href}"
                    )

            if rec.get("name"):
                records.append(rec)

        # Pagination
        records.extend(self._follow_pagination("trainers", "licensed_trainer"))

        # Embedded JSON
        records.extend(
            self.extract_embedded_json(
                datetime.now().strftime("%Y-%m-%d"), "bha"
            )
        )

        log.info("  Trainers: scraped %d records", len(records))
        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Licensed jockeys directory
    # ------------------------------------------------------------------

    def scrape_jockeys(self):
        """Scrape the BHA licensed jockeys directory."""
        cache_key = "jockeys_directory"
        cached = self.load_cache(cache_key)
        if cached is not None:
            log.info("  Jockeys: loaded %d from cache", len(cached))
            return cached

        urls_to_try = [
            f"{self.BASE_URL}/racing/participants/jockeys",
            f"{self.BASE_URL}/directory/jockeys",
            f"{self.BASE_URL}/regulation/licensed-jockeys",
            f"{self.ALT_URL}/racing/participants/jockeys",
        ]

        loaded = False
        for url in urls_to_try:
            if self.navigate(url):
                loaded = True
                break
            self.smart_pause(3.0, 1.5)

        if not loaded:
            self.screenshot_on_error("bha_jockeys")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # Table-based listings
        table_records = self.extract_tables(
            datetime.now().strftime("%Y-%m-%d"), "bha", "licensed_jockey"
        )
        records.extend(table_records)

        # Card-based listings
        card_els = self.page.query_selector_all(
            "[class*='jockey'], [class*='participant'], "
            "[class*='person'], [class*='directory'] li, "
            "[class*='listing'] li"
        )
        for card in card_els:
            text = (card.inner_text() or "").strip()
            if not text or len(text) < 3:
                continue
            rec = {
                "source": "bha",
                "type": "licensed_jockey",
                "scraped_at": datetime.now().isoformat(),
            }
            name_el = card.query_selector("h3, h4, strong, a, .name")
            if name_el:
                rec["name"] = (name_el.inner_text() or "").strip()
            elif text:
                rec["name"] = text.split("\n")[0].strip()

            # Category (flat, jump, amateur, apprentice, conditional)
            cat_el = card.query_selector(
                "[class*='category'], [class*='type'], [class*='licence']"
            )
            if cat_el:
                rec["category"] = (cat_el.inner_text() or "").strip()
            else:
                for cat_kw in [
                    "flat", "jump", "apprentice", "conditional", "amateur",
                ]:
                    if cat_kw in text.lower():
                        rec["category"] = cat_kw
                        break

            # Wins / stats
            stats_el = card.query_selector(
                "[class*='stat'], [class*='record'], [class*='win']"
            )
            if stats_el:
                rec["stats_text"] = (stats_el.inner_text() or "").strip()

            # Profile link
            link_el = card.query_selector("a[href]")
            if link_el:
                href = link_el.get_attribute("href") or ""
                if href:
                    rec["url_profile"] = (
                        href if href.startswith("http")
                        else f"{self.BASE_URL}{href}"
                    )

            if rec.get("name"):
                records.append(rec)

        # Pagination
        records.extend(self._follow_pagination("jockeys", "licensed_jockey"))

        # Embedded JSON
        records.extend(
            self.extract_embedded_json(
                datetime.now().strftime("%Y-%m-%d"), "bha"
            )
        )

        log.info("  Jockeys: scraped %d records", len(records))
        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Race results by date
    # ------------------------------------------------------------------

    def scrape_results_day(self, date_str):
        """Scrape BHA race results for a given date."""
        cache_key = f"results_{date_str}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        urls_to_try = [
            f"{self.BASE_URL}/racing/results?date={date_str}",
            f"{self.BASE_URL}/racing/results/{date_str}",
            f"{self.BASE_URL}/results/{date_str}",
            f"{self.ALT_URL}/racing/results?date={date_str}",
        ]

        loaded = False
        for url in urls_to_try:
            if self.navigate(url):
                loaded = True
                break
            self.smart_pause(3.0, 1.5)

        if not loaded:
            self.screenshot_on_error(f"bha_results_{date_str}")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # Results from tables
        table_records = self.extract_tables(date_str, "bha", "race_result")
        for rec in table_records:
            for key in list(rec.keys()):
                val = rec.get(key, "")
                if isinstance(val, str):
                    pos = self._parse_position(val)
                    if pos is not None and "position" not in rec:
                        rec["position"] = pos
                    prize = self._parse_prize(val)
                    if prize is not None and "prize_gbp" not in rec:
                        rec["prize_gbp"] = prize
                    t = self._parse_time(val)
                    if t is not None and "time_seconds" not in rec:
                        rec["time_raw"] = val
                        rec["time_seconds"] = t
        records.extend(table_records)

        # Race cards / result blocks
        card_els = self.page.query_selector_all(
            "[class*='result'], [class*='race-card'], "
            "[class*='meeting'], [class*='fixture']"
        )
        for card in card_els:
            rec = {
                "source": "bha",
                "date": date_str,
                "type": "race_result_card",
                "scraped_at": datetime.now().isoformat(),
            }
            title_el = card.query_selector("h2, h3, h4, strong")
            if title_el:
                rec["race_name"] = (title_el.inner_text() or "").strip()

            card_text = (card.inner_text() or "")
            course = self._find_course(card_text)
            if course:
                rec["course"] = course

            # Distance
            dist_m = re.search(
                r"(\d+)\s*(?:f|furlongs?)\b", card_text, re.I
            )
            if dist_m:
                rec["distance_furlongs"] = int(dist_m.group(1))
            dist_miles = re.search(
                r"(\d+)\s*m\s*(\d+)?\s*f?", card_text, re.I
            )
            if dist_miles:
                rec["distance_text"] = dist_miles.group(0).strip()

            # Going
            going_m = re.search(
                r"going\s*:?\s*(good|firm|soft|heavy|good to firm|"
                r"good to soft|good to yielding|yielding|standard|"
                r"standard to slow|slow|fast)",
                card_text, re.I,
            )
            if going_m:
                rec["going"] = going_m.group(1).strip()

            # Class
            class_m = re.search(r"class\s*(\d)", card_text, re.I)
            if class_m:
                rec["race_class"] = int(class_m.group(1))

            # Race type
            for rt in BHA_RACE_TYPES:
                if rt in card_text.lower():
                    rec["race_type"] = rt
                    break

            # Detail link
            link_el = card.query_selector("a[href]")
            if link_el:
                href = link_el.get_attribute("href") or ""
                if href:
                    rec["url_detail"] = (
                        href if href.startswith("http")
                        else f"{self.BASE_URL}{href}"
                    )

            if rec.get("race_name") or rec.get("course"):
                records.append(rec)

        # Embedded JSON
        records.extend(self.extract_embedded_json(date_str, "bha"))

        # data-attributes
        records.extend(
            self.extract_data_attributes(
                date_str, "bha",
                keywords=[
                    "race", "horse", "runner", "result", "jockey",
                    "trainer", "course", "going",
                ],
            )
        )

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Race detail page
    # ------------------------------------------------------------------

    def scrape_race_detail(self, race_url, date_str):
        """Scrape a single race result detail page."""
        if not race_url.startswith("http"):
            race_url = f"{self.BASE_URL}{race_url}"

        cache_key = f"detail_{re.sub(r'[^a-zA-Z0-9]', '_', race_url[-60:])}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(race_url):
            self.screenshot_on_error(f"bha_detail_{date_str}")
            return []

        time.sleep(2)
        records = []

        # Title
        race_name = ""
        for sel in ["h1", "h2"]:
            el = self.page.query_selector(sel)
            if el:
                race_name = (el.inner_text() or "").strip()
                if race_name:
                    break

        page_text = self.page.inner_text("body") or ""

        # Conditions
        conditions = {}
        going_m = re.search(
            r"going\s*:?\s*(good|firm|soft|heavy|good to firm|"
            r"good to soft|good to yielding|yielding|standard|"
            r"standard to slow|slow|fast)",
            page_text, re.I,
        )
        if going_m:
            conditions["going"] = going_m.group(1).strip()

        dist_m = re.search(r"(\d+)\s*(?:f|furlongs?)\b", page_text, re.I)
        if dist_m:
            conditions["distance_furlongs"] = int(dist_m.group(1))

        class_m = re.search(r"class\s*(\d)", page_text, re.I)
        if class_m:
            conditions["race_class"] = int(class_m.group(1))

        prize_m = re.search(
            r"prize\s*(?:money|fund)?\s*:?\s*\u00a3?\s*([\d,]+)", page_text, re.I
        )
        if prize_m:
            val = prize_m.group(1).replace(",", "")
            try:
                conditions["total_prize_gbp"] = float(val)
            except ValueError:
                pass

        # Runners table
        table_records = self.extract_tables(date_str, "bha", "runner_detail")
        for rec in table_records:
            rec["race_name"] = race_name
            rec["conditions"] = conditions
            rec["url_race"] = race_url
            for key in list(rec.keys()):
                val = rec.get(key, "")
                if isinstance(val, str):
                    pos = self._parse_position(val)
                    if pos is not None and "position" not in rec:
                        rec["position"] = pos
                    t = self._parse_time(val)
                    if t is not None and "time_seconds" not in rec:
                        rec["time_raw"] = val
                        rec["time_seconds"] = t
        records.extend(table_records)

        # Embedded JSON
        records.extend(self.extract_embedded_json(date_str, "bha"))

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Penalties / disciplinary
    # ------------------------------------------------------------------

    def scrape_penalties(self):
        """Scrape BHA disciplinary panel decisions and penalties."""
        cache_key = "penalties"
        cached = self.load_cache(cache_key)
        if cached is not None:
            log.info("  Penalties: loaded %d from cache", len(cached))
            return cached

        urls_to_try = [
            f"{self.BASE_URL}/regulation/disciplinary",
            f"{self.BASE_URL}/regulation/penalties",
            f"{self.BASE_URL}/regulation/results-of-disciplinary-enquiries",
            f"{self.ALT_URL}/regulation/disciplinary",
        ]

        loaded = False
        for url in urls_to_try:
            if self.navigate(url):
                loaded = True
                break
            self.smart_pause(3.0, 1.5)

        if not loaded:
            self.screenshot_on_error("bha_penalties")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # Table-based penalties
        table_records = self.extract_tables(
            datetime.now().strftime("%Y-%m-%d"), "bha", "penalty"
        )
        records.extend(table_records)

        # Card-based penalties
        card_els = self.page.query_selector_all(
            "[class*='penalty'], [class*='disciplinary'], "
            "[class*='enquiry'], [class*='sanction'], "
            "[class*='decision'], article"
        )
        for card in card_els:
            text = (card.inner_text() or "").strip()
            if not text or len(text) < 10:
                continue
            rec = {
                "source": "bha",
                "type": "penalty",
                "scraped_at": datetime.now().isoformat(),
            }

            # Date
            date_el = card.query_selector(
                "[class*='date'], time, [datetime]"
            )
            if date_el:
                rec["penalty_date"] = (date_el.inner_text() or "").strip()
                dt_attr = date_el.get_attribute("datetime")
                if dt_attr:
                    rec["penalty_date_iso"] = dt_attr

            # Person
            person_el = card.query_selector(
                "[class*='person'], [class*='name'], h3, h4, strong"
            )
            if person_el:
                rec["person"] = (person_el.inner_text() or "").strip()

            # Offence
            offence_el = card.query_selector(
                "[class*='offence'], [class*='charge'], [class*='rule']"
            )
            if offence_el:
                rec["offence"] = (offence_el.inner_text() or "").strip()

            # Penalty detail
            pen_el = card.query_selector(
                "[class*='penalty'], [class*='sanction'], [class*='fine']"
            )
            if pen_el:
                rec["penalty_detail"] = (pen_el.inner_text() or "").strip()

            # Suspension days
            susp_m = re.search(r"(\d+)\s*(?:day|jour)", text, re.I)
            if susp_m:
                rec["suspension_days"] = int(susp_m.group(1))

            # Fine amount
            fine_m = re.search(r"\u00a3\s*([\d,]+)", text)
            if fine_m:
                val = fine_m.group(1).replace(",", "")
                try:
                    rec["fine_gbp"] = float(val)
                except ValueError:
                    pass

            # Course
            course = self._find_course(text)
            if course:
                rec["course"] = course

            if rec.get("person") or rec.get("penalty_detail"):
                records.append(rec)

        # Pagination for penalties
        records.extend(self._follow_pagination("penalties", "penalty"))

        log.info("  Penalties: scraped %d records", len(records))
        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Pagination helper
    # ------------------------------------------------------------------

    def _follow_pagination(self, label, record_type, max_pages=20):
        """Follow 'Next' pagination links and extract table rows."""
        records = []
        for page_num in range(2, max_pages + 1):
            next_btn = None
            for sel in [
                "a:has-text('Next')", "a:has-text('next')",
                "[class*='next'] a", "[class*='pagination'] a:last-child",
                "button:has-text('Next')", "[aria-label='Next page']",
            ]:
                try:
                    el = self.page.locator(sel).first
                    if el.is_visible(timeout=2000):
                        next_btn = el
                        break
                except Exception:
                    continue

            if not next_btn:
                break

            try:
                next_btn.click(timeout=5000)
                self.page.wait_for_load_state("networkidle", timeout=15000)
                time.sleep(2)
            except Exception as exc:
                log.warning("  Pagination click failed on page %d: %s",
                            page_num, str(exc)[:100])
                break

            page_records = self.extract_tables(
                datetime.now().strftime("%Y-%m-%d"), "bha", record_type
            )
            if not page_records:
                break
            records.extend(page_records)
            log.info("  %s page %d: +%d records", label, page_num,
                     len(page_records))
            self.smart_pause(3.0, 1.5)

        return records

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self):
        start_date = datetime.strptime(self.args.start, "%Y-%m-%d")
        end_date = (
            datetime.strptime(self.args.end, "%Y-%m-%d")
            if self.args.end
            else datetime.now()
        )

        log.info("=" * 60)
        log.info("SCRIPT 119 (Playwright) -- BHA Scraper")
        log.info("  Period : %s -> %s", start_date.date(), end_date.date())
        log.info("=" * 60)

        checkpoint = self.load_checkpoint()
        if self.args.resume and checkpoint.get("last_date"):
            resume_dt = (
                datetime.strptime(checkpoint["last_date"], "%Y-%m-%d")
                + timedelta(days=1)
            )
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("  Resuming from checkpoint: %s", start_date.date())

        output_file = os.path.join(self.output_dir, "bha_data.jsonl")
        self.launch_browser()

        try:
            total_records = 0

            # --- Phase 1: Directory data (trainers, jockeys, penalties) ---
            if not checkpoint.get("directories_done"):
                log.info("--- Phase 1: Directories ---")

                for rec in self.scrape_trainers():
                    self.append_jsonl(output_file, rec)
                    total_records += 1
                self.smart_pause(8.0, 3.0)

                for rec in self.scrape_jockeys():
                    self.append_jsonl(output_file, rec)
                    total_records += 1
                self.smart_pause(8.0, 3.0)

                for rec in self.scrape_penalties():
                    self.append_jsonl(output_file, rec)
                    total_records += 1
                self.smart_pause(8.0, 3.0)

                self.save_checkpoint({
                    "directories_done": True,
                    "total_records": total_records,
                })
                log.info("  Phase 1 done: %d records", total_records)

            # --- Phase 2: Daily results ---
            log.info("--- Phase 2: Daily results ---")
            current = start_date
            day_count = 0

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")

                res_records = self.scrape_results_day(date_str)

                # Follow detail links
                detail_urls = list({
                    r.get("url_detail", "")
                    for r in res_records
                    if r.get("url_detail")
                })
                for durl in detail_urls[:10]:
                    detail = self.scrape_race_detail(durl, date_str)
                    if detail:
                        res_records.extend(detail)
                    self.smart_pause(5.0, 2.5)

                for rec in res_records:
                    self.append_jsonl(output_file, rec)
                    total_records += 1

                day_count += 1

                if day_count % 30 == 0:
                    log.info(
                        "  %s | days=%d records=%d",
                        date_str, day_count, total_records,
                    )
                    self.save_checkpoint({
                        "directories_done": True,
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
                "directories_done": True,
                "last_date": end_date.strftime("%Y-%m-%d"),
                "total_records": total_records,
                "status": "done",
            })
            log.info("=" * 60)
            log.info(
                "DONE: %d days, %d records -> %s",
                day_count, total_records, output_file,
            )
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description="Script 119 (Playwright) -- BHA Official Data Scraper"
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = BHAPlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
