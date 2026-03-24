#!/usr/bin/env python3
"""
Script 114 (Playwright) -- Australian Track Conditions Scraper
Source : racing.com (Racing Victoria / Australia)
Collecte : track condition reports, going descriptions, rail positions,
           weather impact, penetrometer readings, course drainage info
CRITIQUE pour : Going Model (AU), Track Bias AU, International Going Comparison

Locale : en-AU (Australian track conditions)

Usage:
    pip install playwright
    playwright install chromium
    python 114_racing_au_track_conditions_scraper.py --start 2024-01-01 --end 2024-12-31
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

log = setup_logging("114_racing_au_track_conditions")


class RacingAuTrackConditionsPlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "114_racing_au_track_pw"
    BASE_URL = "https://www.racing.com"
    DEFAULT_PAUSE_BASE = 6.0
    DEFAULT_PAUSE_JITTER = 3.0

    # Australian track condition ratings (Racing Australia scale)
    AU_TRACK_RATINGS = {
        "firm 1": 1, "firm 2": 2,
        "good 3": 3, "good 4": 4,
        "soft 5": 5, "soft 6": 6,
        "heavy 7": 7, "heavy 8": 8, "heavy 9": 9, "heavy 10": 10,
    }

    # Australian racecourses (major)
    AU_COURSES = [
        "flemington", "moonee-valley", "caulfield", "sandown", "cranbourne",
        "mornington", "pakenham", "geelong", "ballarat", "bendigo",
        "warrnambool", "sale", "wangaratta", "kilmore", "kyneton",
        "stony-creek", "swan-hill", "echuca", "seymour", "hamilton",
        "traralgon", "bairnsdale", "ararat", "stawell", "mildura",
        # NSW
        "randwick", "rosehill", "canterbury", "warwick-farm", "newcastle",
        "kembla-grange", "wyong", "gosford", "scone", "tamworth",
        "muswellbrook", "dubbo", "bathurst", "wagga",
        # QLD
        "eagle-farm", "doomben", "gold-coast", "sunshine-coast",
        "ipswich", "toowoomba", "rockhampton", "cairns", "townsville",
        # SA
        "morphettville", "murray-bridge", "gawler", "port-augusta",
        "balaklava", "strathalbyn", "mount-gambier",
        # WA
        "ascot-wa", "belmont", "pinjarra", "bunbury", "northam",
        "geraldton", "kalgoorlie",
        # TAS
        "elwick", "mowbray", "devonport",
    ]

    # Source pages for track conditions
    TRACK_CONDITION_PAGES = {
        "racing_com_tracks": "https://www.racing.com/form/track-conditions",
        "racing_com_results": "https://www.racing.com/results",
        "racing_com_form": "https://www.racing.com/form",
        "rv_going": "https://www.racingvictoria.com.au/the-sport/track-conditions",
        "racenet_going": "https://www.racenet.com.au/track-conditions",
        "punters_going": "https://www.punters.com.au/track-conditions/",
        "tab_going": "https://www.tab.com.au/racing/track-conditions",
        "racing_nsw": "https://www.racingnsw.com.au/track-conditions/",
        "racing_qld": "https://www.racingqueensland.com.au/track-conditions",
    }

    # ------------------------------------------------------------------
    # Override browser launch to use en-AU locale
    # ------------------------------------------------------------------

    def launch_browser(self):
        """Launch headless Chromium with en-AU locale for AU track data."""
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
            locale="en-AU",
            timezone_id="Australia/Melbourne",
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
            Object.defineProperty(navigator, 'languages', {get: () => ['en-AU', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            window.chrome = {runtime: {}};
        """)
        self.page = self._context.new_page()
        self.page.set_default_timeout(self.DEFAULT_TIMEOUT_MS)
        log.info("Browser launched (headless Chromium, locale=en-AU)")

    # ------------------------------------------------------------------
    # Track condition parsers
    # ------------------------------------------------------------------

    @staticmethod
    def parse_au_track_rating(text):
        """Parse Australian track condition rating (e.g. 'Good 4', 'Soft 5')."""
        if not text:
            return {}

        result = {}
        lower = text.lower().strip()

        # Numeric track rating (e.g. "Good 4", "Heavy 8")
        rating_match = re.search(
            r'(firm|good|soft|heavy|synthetic)\s*(\d{1,2})',
            lower)
        if rating_match:
            condition = rating_match.group(1).capitalize()
            number = int(rating_match.group(2))
            result["track_condition"] = condition
            result["track_rating"] = number
            result["track_condition_full"] = f"{condition} {number}"

        # Standalone condition words
        if not result.get("track_condition"):
            for cond in ["heavy", "soft", "good", "firm", "synthetic", "dead"]:
                if cond in lower:
                    result["track_condition"] = cond.capitalize()
                    break

        # Penetrometer reading
        pen = re.search(r'(?:penetrometer|pen)[:\s]*(\d+\.?\d*)',
                        text, re.IGNORECASE)
        if pen:
            result["penetrometer"] = float(pen.group(1))

        # Rail position
        rail = re.search(
            r'(?:rail|running\s*rail)[:\s]*(\d+)\s*(?:metres?|m)\s*(out|in|from\s*true)',
            text, re.IGNORECASE)
        if rail:
            result["rail_metres"] = int(rail.group(1))
            result["rail_position"] = rail.group(2).strip().lower()

        # True position
        if re.search(r'true\s*(?:position|rail|running)', lower):
            result["rail_position"] = "true"
            result["rail_metres"] = 0

        # Weather
        weather = re.search(
            r'(?:weather|forecast)[:\s]*(fine|overcast|cloudy|rain(?:ing)?|'
            r'showers?|drizzle|hot|warm|cool|cold|humid|windy|storm)',
            text, re.IGNORECASE)
        if weather:
            result["weather"] = weather.group(1).lower()

        # Irrigation / watering
        water = re.search(r'(?:water(?:ed|ing)|irrigat(?:ed|ion))[:\s]*(\d+)\s*mm',
                          text, re.IGNORECASE)
        if water:
            result["watering_mm"] = int(water.group(1))

        return result

    @staticmethod
    def extract_course_from_text(text):
        """Try to identify an Australian racecourse from free text."""
        lower = text.lower()
        for course in RacingAuTrackConditionsPlaywright.AU_COURSES:
            name = course.replace("-", " ")
            if name in lower:
                return course
        return None

    # ------------------------------------------------------------------
    # Scrape racing.com track conditions page
    # ------------------------------------------------------------------

    def scrape_track_conditions_page(self, source_name, url):
        """Scrape a track conditions page."""
        cache_key = f"au_track_{source_name}_{re.sub(r'[^a-zA-Z0-9]', '_', url[-60:])}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(url, wait_until="domcontentloaded"):
            self.screenshot_on_error(f"au_track_{source_name}")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # --- Extract tables (common format for track condition summaries) ---
        table_records = self.extract_tables(
            datetime.now().strftime("%Y-%m-%d"),
            "racing_au_track",
            record_type="track_condition_table"
        )
        for rec in table_records:
            rec["sub_source"] = source_name
            rec["url"] = url
            combined = " ".join(str(v) for v in rec.values() if isinstance(v, str))
            parsed = self.parse_au_track_rating(combined)
            rec.update(parsed)
            course = self.extract_course_from_text(combined)
            if course:
                rec["course"] = course
            records.append(rec)

        # --- Track condition blocks via JS ---
        condition_blocks = self.page.evaluate("""() => {
            const results = [];
            const keywords = ['track', 'condition', 'going', 'rail', 'ground',
                              'penetrometer', 'weather', 'rating', 'surface',
                              'water', 'irrigat', 'turf', 'synthetic',
                              'good', 'soft', 'heavy', 'firm'];
            const els = document.querySelectorAll(
                'div, p, article, section, li, span, td'
            );
            for (const el of els) {
                const cls = (el.className || '').toLowerCase();
                const text = (el.textContent || '').trim();
                if (text.length < 8 || text.length > 3000) continue;
                const hasKeywordClass = keywords.some(kw => cls.includes(kw));
                const hasKeywordText = keywords.some(kw =>
                    text.toLowerCase().includes(kw));
                if (hasKeywordClass || hasKeywordText) {
                    results.push({
                        text: text.substring(0, 2500),
                        classes: cls.substring(0, 200),
                        tag: el.tagName.toLowerCase()
                    });
                }
            }
            return results;
        }""")

        seen_texts = set()
        for block in (condition_blocks or []):
            text = block.get("text", "")
            text_key = text[:100]
            if text_key in seen_texts:
                continue
            seen_texts.add(text_key)

            rec = {
                "source": "racing_au_track",
                "sub_source": source_name,
                "type": "track_condition_report",
                "contenu": text,
                "url": url,
                "scraped_at": datetime.now().isoformat(),
            }
            parsed = self.parse_au_track_rating(text)
            rec.update(parsed)
            course = self.extract_course_from_text(text)
            if course:
                rec["course"] = course
            records.append(rec)

        # --- Rail position updates ---
        rail_updates = self.page.evaluate(r"""() => {
            const results = [];
            const els = document.querySelectorAll('div, span, p, li, td');
            for (const el of els) {
                const text = (el.textContent || '').trim();
                if (text.length < 5 || text.length > 500) continue;
                if (/rail|running\s*rail|true\s*position/i.test(text) &&
                    /\\d+\\s*m(etres?)?/i.test(text)) {
                    results.push({text: text.substring(0, 400)});
                }
            }
            return results;
        }""")

        for upd in (rail_updates or []):
            text = upd.get("text", "")
            rec = {
                "source": "racing_au_track",
                "sub_source": source_name,
                "type": "rail_position",
                "contenu": text,
                "url": url,
                "scraped_at": datetime.now().isoformat(),
            }
            parsed = self.parse_au_track_rating(text)
            rec.update(parsed)
            course = self.extract_course_from_text(text)
            if course:
                rec["course"] = course
            records.append(rec)

        # --- Embedded JSON data ---
        records.extend(self.extract_embedded_json(
            datetime.now().strftime("%Y-%m-%d"), "racing_au_track"))

        # --- Data attributes (racing.com often uses data-* for track info) ---
        records.extend(self.extract_data_attributes(
            datetime.now().strftime("%Y-%m-%d"),
            "racing_au_track",
            keywords=["track", "condition", "going", "rail", "rating",
                       "course", "venue", "surface"]
        ))

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Scrape racing.com racecards for embedded track conditions
    # ------------------------------------------------------------------

    def scrape_raceday_conditions(self, date_str):
        """Scrape racing.com racecards for a specific date to get track info."""
        cache_key = f"au_raceday_{date_str}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/races/{date_str}"
        if not self.navigate(url, wait_until="domcontentloaded"):
            # Try alternate URL format
            alt_url = f"{self.BASE_URL}/form/races/{date_str}"
            if not self.navigate(alt_url, wait_until="domcontentloaded"):
                return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # Extract meeting-level track conditions via JS
        meeting_data = self.page.evaluate("""() => {
            const results = [];
            // Look for meeting sections with track info
            const meetings = document.querySelectorAll(
                '[class*="meeting"], [class*="venue"], [class*="card"], ' +
                '[class*="fixture"], [class*="event"], article, section'
            );
            for (const meeting of meetings) {
                const title = meeting.querySelector('h2, h3, h4, a');
                if (!title) continue;
                const name = title.textContent.trim();
                if (!name || name.length < 2) continue;

                // Look for track condition elements within this meeting
                const condEls = meeting.querySelectorAll(
                    '[class*="track"], [class*="condition"], [class*="going"], ' +
                    '[class*="rail"], [class*="weather"], [class*="rating"]'
                );
                const condTexts = [];
                for (const el of condEls) {
                    const t = el.textContent.trim();
                    if (t.length > 2 && t.length < 500) condTexts.push(t);
                }

                // Also check data attributes on the meeting itself
                const dataAttrs = {};
                for (const attr of meeting.attributes) {
                    if (attr.name.startsWith('data-'))
                        dataAttrs[attr.name] = attr.value;
                }

                // Full text for parsing
                const fullText = meeting.textContent.trim().substring(0, 2000);

                results.push({
                    venue: name,
                    conditions: condTexts,
                    dataAttrs: Object.keys(dataAttrs).length > 0 ? dataAttrs : null,
                    fullText: fullText
                });
            }
            return results;
        }""")

        for mtg in (meeting_data or []):
            venue = mtg.get("venue", "")
            full_text = mtg.get("fullText", "")

            rec = {
                "date": date_str,
                "source": "racing_au_track",
                "sub_source": "racing_com_raceday",
                "type": "meeting_track_condition",
                "venue": venue,
                "url": url,
                "scraped_at": datetime.now().isoformat(),
            }

            # Parse track conditions from condition elements
            for cond_text in mtg.get("conditions", []):
                parsed = self.parse_au_track_rating(cond_text)
                rec.update(parsed)

            # Also parse from full text
            full_parsed = self.parse_au_track_rating(full_text)
            for k, v in full_parsed.items():
                if k not in rec:
                    rec[k] = v

            # Identify course
            course = self.extract_course_from_text(venue)
            if course:
                rec["course"] = course

            if mtg.get("conditions"):
                rec["condition_texts"] = mtg["conditions"]

            if mtg.get("dataAttrs"):
                rec["data_attributes"] = mtg["dataAttrs"]

            records.append(rec)

        # Also extract tables from this page
        table_records = self.extract_tables(date_str, "racing_au_track",
                                            record_type="raceday_table")
        for rec in table_records:
            combined = " ".join(str(v) for v in rec.values() if isinstance(v, str))
            parsed = self.parse_au_track_rating(combined)
            rec.update(parsed)
            rec["sub_source"] = "racing_com_raceday"
            rec["url"] = url
            course = self.extract_course_from_text(combined)
            if course:
                rec["course"] = course
            records.append(rec)

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self):
        start_date = datetime.strptime(self.args.start, "%Y-%m-%d")
        end_date = (datetime.strptime(self.args.end, "%Y-%m-%d")
                    if self.args.end else datetime.now())

        log.info("=" * 60)
        log.info("SCRIPT 114 (Playwright) -- AU Track Conditions Scraper")
        log.info("  Source : racing.com + AU racing sites")
        log.info("  Period : %s -> %s", start_date.date(), end_date.date())
        log.info("  AU Courses : %d", len(self.AU_COURSES))
        log.info("  Locale : en-AU")
        log.info("=" * 60)

        checkpoint = self.load_checkpoint()
        done_urls = set(checkpoint.get("done_urls", []))
        last_date = checkpoint.get("last_date")

        if self.args.resume and last_date:
            resume_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("  Resuming from checkpoint: %s", start_date.date())

        output_file = os.path.join(self.output_dir,
                                   "racing_au_track_conditions.jsonl")
        self.launch_browser()

        try:
            total_records = checkpoint.get("total_records", 0)
            page_count = 0

            # Phase 1: Static track condition pages
            log.info("  Phase 1: Track condition overview pages")
            for source_name, url in self.TRACK_CONDITION_PAGES.items():
                if url in done_urls:
                    continue

                records = self.scrape_track_conditions_page(source_name, url)
                for rec in records:
                    self.append_jsonl(output_file, rec)
                    total_records += 1

                done_urls.add(url)
                page_count += 1
                log.info("    %s: %d records", source_name,
                         len(records) if records else 0)

                self.save_checkpoint({
                    "done_urls": list(done_urls),
                    "total_records": total_records,
                    "last_date": start_date.strftime("%Y-%m-%d"),
                })
                self.smart_pause()

            # Phase 2: Daily racecard conditions
            log.info("  Phase 2: Daily raceday conditions")
            current = start_date
            day_count = 0

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")

                records = self.scrape_raceday_conditions(date_str)
                if records:
                    for rec in records:
                        self.append_jsonl(output_file, rec)
                        total_records += 1

                day_count += 1

                if day_count % 30 == 0:
                    log.info("  %s | days=%d records=%d",
                             date_str, day_count, total_records)
                    self.save_checkpoint({
                        "done_urls": list(done_urls),
                        "total_records": total_records,
                        "last_date": date_str,
                    })

                # Rotate browser every 60 days
                if day_count % 60 == 0 and day_count > 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(10.0, 5.0)
                    self.launch_browser()

                current += timedelta(days=1)
                self.smart_pause(5.0, 2.0)

            self.save_checkpoint({
                "done_urls": list(done_urls),
                "total_records": total_records,
                "last_date": end_date.strftime("%Y-%m-%d"),
                "status": "done",
            })

            log.info("=" * 60)
            log.info("DONE: %d pages + %d days, %d records -> %s",
                     page_count, day_count, total_records, output_file)
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description="Script 114 (Playwright) -- AU Track Conditions Scraper"
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = RacingAuTrackConditionsPlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
