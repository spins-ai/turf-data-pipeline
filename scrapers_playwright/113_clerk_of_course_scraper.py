#!/usr/bin/env python3
"""
Script 113 (Playwright) -- Clerk of Course Reports Scraper
Source : BHA going reports, Racing Post, Sporting Life, At The Races,
         Timeform, Racing TV, HRI (Ireland), individual UK/IRE course sites
Collecte : going changes, course inspections, abandonment notices,
           GoingStick readings, watering info, rail movements, drainage reports
CRITIQUE pour : Going Model, Real-time Track Updates, Abandonment Prediction

Replaces script 99 (requests/BeautifulSoup) with Playwright for JS-rendered pages.

Locale : en-GB (UK going data)

Usage:
    pip install playwright
    playwright install chromium
    python 113_clerk_of_course_scraper.py --start 2024-01-01 --end 2024-12-31
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from scraper_base_playwright import PlaywrightScraperBase
from utils.logging_setup import setup_logging

log = setup_logging("113_clerk_of_course")


class ClerkOfCoursePlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "113_clerk_of_course_pw"
    DEFAULT_PAUSE_BASE = 6.0
    DEFAULT_PAUSE_JITTER = 3.0

    # Sources for clerk of course / going reports
    GOING_REPORT_SOURCES = {
        # BHA / official
        "bha_going": "https://www.britishhorseracing.com/racing/going-reports",
        "bha_inspections": "https://www.britishhorseracing.com/racing/inspections",
        # Racing Post going reports
        "rp_going": "https://www.racingpost.com/going",
        "rp_reports": "https://www.racingpost.com/news/going-reports",
        # Sporting Life
        "sl_going": "https://www.sportinglife.com/racing/going",
        # At The Races
        "atr_going": "https://www.attheraces.com/going",
        # Timeform
        "tf_going": "https://www.timeform.com/horse-racing/going",
        # Racing TV
        "rtv_going": "https://www.racingtv.com/going",
        # HRI Ireland
        "hri_going": "https://www.goracing.ie/going-reports",
        # France Galop
        "fg_going": "https://www.france-galop.com/fr/courses/terrains",
    }

    # UK/IRE racecourse websites for direct clerk reports
    COURSE_SITES = {
        "ascot": "https://www.ascot.com",
        "cheltenham": "https://www.thejockeyclub.co.uk/cheltenham",
        "newmarket": "https://www.thejockeyclub.co.uk/newmarket",
        "epsom": "https://www.thejockeyclub.co.uk/epsom",
        "sandown": "https://www.thejockeyclub.co.uk/sandown",
        "kempton": "https://www.thejockeyclub.co.uk/kempton",
        "haydock": "https://www.thejockeyclub.co.uk/haydock",
        "newbury": "https://www.newburyracecourse.co.uk",
        "york": "https://www.yorkracecourse.co.uk",
        "doncaster": "https://www.thejockeyclub.co.uk/doncaster",
        "goodwood": "https://www.goodwood.com/horseracing",
        "aintree": "https://www.thejockeyclub.co.uk/aintree",
        "leopardstown": "https://www.leopardstown.com",
        "curragh": "https://www.curragh.ie",
        "fairyhouse": "https://www.fairyhouse.ie",
    }

    # Extended UK course list for text matching
    ALL_COURSES = [
        "ascot", "cheltenham", "aintree", "epsom", "goodwood", "newmarket",
        "york", "doncaster", "sandown", "kempton", "newbury", "haydock",
        "leicester", "lingfield", "wolverhampton", "chester", "windsor",
        "salisbury", "bath", "beverley", "brighton", "carlisle", "catterick",
        "chepstow", "exeter", "ffos-las", "fontwell", "hamilton", "huntingdon",
        "kelso", "market-rasen", "musselburgh", "newton-abbot", "nottingham",
        "perth", "plumpton", "pontefract", "redcar", "ripon", "sedgefield",
        "southwell", "stratford", "taunton", "thirsk", "uttoxeter", "warwick",
        "wetherby", "wincanton", "worcester",
        # Irish
        "leopardstown", "curragh", "fairyhouse", "punchestown", "naas",
        "galway", "killarney", "cork", "limerick", "tipperary", "dundalk",
        "navan", "gowran-park", "wexford", "listowel", "ballinrobe",
    ]

    # Official going terms (UK/IRE/FR)
    GOING_TERMS = [
        "Heavy", "Soft", "Good to Soft", "Good to Firm", "Good",
        "Firm", "Hard", "Standard", "Standard to Slow", "Slow",
        "Yielding", "Yielding to Soft", "Soft to Heavy",
        "Bon", "Souple", "Lourd", "Tres Souple", "Collant", "Leger",
        "Bon Souple",
    ]

    # ------------------------------------------------------------------
    # Override browser launch to use en-GB locale
    # ------------------------------------------------------------------

    def launch_browser(self):
        """Launch headless Chromium with en-GB locale for UK going data."""
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
        log.info("Browser launched (headless Chromium, locale=en-GB)")

    # ------------------------------------------------------------------
    # Going text parser
    # ------------------------------------------------------------------

    @staticmethod
    def parse_going_text(text):
        """Extract structured going info from free text."""
        result = {}

        # Official going description
        lower = text.lower()
        for term in sorted(ClerkOfCoursePlaywright.GOING_TERMS,
                           key=lambda t: -len(t)):
            if term.lower() in lower:
                result["going_official"] = term
                break

        # GoingStick reading
        gs = re.search(r'(?:GoingStick|going\s*stick)[:\s]*(\d+\.?\d*)',
                       text, re.IGNORECASE)
        if gs:
            result["goingstick"] = float(gs.group(1))

        # Watering amount
        water = re.search(r'(?:water(?:ed|ing))[:\s]*(\d+)\s*mm',
                          text, re.IGNORECASE)
        if water:
            result["watering_mm"] = int(water.group(1))

        # Rail movement
        rail = re.search(
            r'(?:rail|dolling)[:\s]*(\d+)\s*(?:yards?|metres?|m)\s*(out|in)',
            text, re.IGNORECASE)
        if rail:
            result["rail_yards"] = int(rail.group(1))
            result["rail_direction"] = rail.group(2).lower()

        # Inspection time
        insp = re.search(r'(?:inspection|inspect)[:\s]*(\d{1,2}[:.]\d{2})',
                         text, re.IGNORECASE)
        if insp:
            result["inspection_time"] = insp.group(1)

        # Abandoned flag
        if re.search(r'abandon', text, re.IGNORECASE):
            result["abandoned"] = True

        # Penetrometre (FR)
        pen = re.search(r'penetrometre[:\s]*(\d+\.?\d*)', text, re.IGNORECASE)
        if pen:
            result["penetrometre"] = float(pen.group(1))

        return result

    @staticmethod
    def extract_course_from_text(text):
        """Try to identify a racecourse name from free text."""
        lower = text.lower()
        for course in ClerkOfCoursePlaywright.ALL_COURSES:
            if course.lower().replace("-", " ") in lower or course.lower() in lower:
                return course
        return None

    @staticmethod
    def extract_date_from_text(text):
        """Extract a date from free text (e.g. '14 March 2024')."""
        m = re.search(
            r'(\d{1,2})\s*(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|'
            r'May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|'
            r'Nov(?:ember)?|Dec(?:ember)?)\w*\s*(\d{4})',
            text, re.IGNORECASE)
        if m:
            return f"{m.group(1)} {m.group(2)} {m.group(3)}"
        return None

    # ------------------------------------------------------------------
    # Scrape a single going report page
    # ------------------------------------------------------------------

    def scrape_going_page(self, source_name, url):
        """Scrape one going report page with Playwright."""
        cache_key = f"report_{source_name}_{re.sub(r'[^a-zA-Z0-9]', '_', url[-60:])}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(url, wait_until="domcontentloaded"):
            self.screenshot_on_error(f"clerk_{source_name}")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # --- Table rows ---
        table_records = self.extract_tables(
            datetime.now().strftime("%Y-%m-%d"),
            "clerk_of_course",
            record_type="going_table"
        )
        for rec in table_records:
            rec["sub_source"] = source_name
            rec["url"] = url
            combined = " ".join(str(v) for v in rec.values() if isinstance(v, str))
            parsed = self.parse_going_text(combined)
            rec.update(parsed)
            course = self.extract_course_from_text(combined)
            if course:
                rec["course"] = course
            records.append(rec)

        # --- Going report text blocks (JS-rendered divs) ---
        going_blocks = self.page.evaluate("""() => {
            const results = [];
            const keywords = ['going', 'ground', 'report', 'clerk', 'course',
                              'inspection', 'condition', 'update', 'notice',
                              'terrain', 'watering', 'rail'];
            const els = document.querySelectorAll(
                'div, p, article, section, li, span'
            );
            for (const el of els) {
                const cls = (el.className || '').toLowerCase();
                const text = (el.textContent || '').trim();
                if (text.length < 10 || text.length > 3000) continue;
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
        for block in (going_blocks or []):
            text = block.get("text", "")
            # Deduplicate by first 100 chars
            text_key = text[:100]
            if text_key in seen_texts:
                continue
            seen_texts.add(text_key)

            rec = {
                "source": "clerk_of_course",
                "sub_source": source_name,
                "type": "going_report",
                "contenu": text,
                "url": url,
                "scraped_at": datetime.now().isoformat(),
            }
            parsed = self.parse_going_text(text)
            rec.update(parsed)

            report_date = self.extract_date_from_text(text)
            if report_date:
                rec["report_date"] = report_date

            course = self.extract_course_from_text(text)
            if course:
                rec["course"] = course

            records.append(rec)

        # --- Time-stamped updates (e.g. "14:30 - Going changed to Soft") ---
        timed_updates = self.page.evaluate("""() => {
            const results = [];
            const els = document.querySelectorAll('div, span, p, time, li');
            const keywords = ['going', 'ground', 'inspection', 'watering',
                              'rail', 'abandon', 'changed', 'now reads'];
            for (const el of els) {
                const text = (el.textContent || '').trim();
                const timeMatch = text.match(
                    /(\\d{1,2}[:.:]\\d{2})\\s*[-:]\\s*(.+?)(?:\\.|$)/
                );
                if (timeMatch && keywords.some(kw =>
                    text.toLowerCase().includes(kw))) {
                    results.push({
                        time: timeMatch[1],
                        text: text.substring(0, 500)
                    });
                }
            }
            return results;
        }""")

        for upd in (timed_updates or []):
            text = upd.get("text", "")
            rec = {
                "source": "clerk_of_course",
                "sub_source": source_name,
                "type": "timed_update",
                "time": upd.get("time", ""),
                "contenu": text,
                "url": url,
                "scraped_at": datetime.now().isoformat(),
            }
            parsed = self.parse_going_text(text)
            rec.update(parsed)
            course = self.extract_course_from_text(text)
            if course:
                rec["course"] = course
            records.append(rec)

        # --- Embedded JSON data ---
        records.extend(self.extract_embedded_json(
            datetime.now().strftime("%Y-%m-%d"), "clerk_of_course"))

        # --- Going-related links for deeper scraping ---
        links_data = self.page.evaluate("""() => {
            const results = [];
            const keywords = ['going', 'clerk', 'inspection', 'report',
                              'ground', 'terrain'];
            const links = document.querySelectorAll('a[href]');
            for (const a of links) {
                const href = a.getAttribute('href') || '';
                const text = (a.textContent || '').trim();
                if (text.length < 3) continue;
                if (keywords.some(kw => href.toLowerCase().includes(kw))) {
                    results.push({href, text: text.substring(0, 200)});
                }
            }
            return results;
        }""")

        page_origin = url.split("/")[0] + "//" + url.split("/")[2]
        for link in (links_data or []):
            href = link.get("href", "")
            full_url = href if href.startswith("http") else page_origin + href
            records.append({
                "source": "clerk_of_course",
                "sub_source": source_name,
                "type": "report_link",
                "link_text": link.get("text", ""),
                "link_url": full_url,
                "scraped_at": datetime.now().isoformat(),
            })

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self):
        start_date = datetime.strptime(self.args.start, "%Y-%m-%d")
        end_date = (datetime.strptime(self.args.end, "%Y-%m-%d")
                    if self.args.end else datetime.now())

        max_pages = getattr(self.args, "max_pages", 500)

        log.info("=" * 60)
        log.info("SCRIPT 113 (Playwright) -- Clerk of Course Reports Scraper")
        log.info("  Sources : %d going report pages", len(self.GOING_REPORT_SOURCES))
        log.info("  Course sites : %d", len(self.COURSE_SITES))
        log.info("  Period : %s -> %s", start_date.date(), end_date.date())
        log.info("  Locale : en-GB")
        log.info("=" * 60)

        checkpoint = self.load_checkpoint()
        done_urls = set(checkpoint.get("done_urls", []))
        if self.args.resume and done_urls:
            log.info("  Resuming checkpoint: %d pages already done", len(done_urls))

        output_file = os.path.join(self.output_dir, "clerk_of_course_data.jsonl")
        self.launch_browser()

        try:
            total_records = checkpoint.get("total_records", 0)
            page_count = 0
            all_sub_links = []

            # Phase 1: Main going report sources
            log.info("  Phase 1: Main going report sources")
            for source_name, url in self.GOING_REPORT_SOURCES.items():
                if url in done_urls or page_count >= max_pages:
                    continue

                records = self.scrape_going_page(source_name, url)
                for rec in records:
                    self.append_jsonl(output_file, rec)
                    total_records += 1
                    if rec.get("type") == "report_link" and rec.get("link_url"):
                        all_sub_links.append((source_name, rec["link_url"]))

                done_urls.add(url)
                page_count += 1
                log.info("    %s: %d records", source_name,
                         len(records) if records else 0)

                self.save_checkpoint({
                    "done_urls": list(done_urls),
                    "total_records": total_records,
                })
                self.smart_pause()

            # Phase 2: Individual course sites
            log.info("  Phase 2: Individual course sites")
            for course_name, base_url in self.COURSE_SITES.items():
                going_urls = [
                    f"{base_url}/going",
                    f"{base_url}/racing/going",
                    f"{base_url}/the-course/going",
                    f"{base_url}/going-report",
                ]
                for url in going_urls:
                    if url in done_urls or page_count >= max_pages:
                        continue

                    records = self.scrape_going_page(course_name, url)
                    for rec in records:
                        rec["course"] = rec.get("course", course_name)
                        self.append_jsonl(output_file, rec)
                        total_records += 1
                        if rec.get("type") == "report_link" and rec.get("link_url"):
                            all_sub_links.append((course_name, rec["link_url"]))

                    done_urls.add(url)
                    page_count += 1
                    self.smart_pause(4.0, 2.0)

                if page_count % 10 == 0:
                    log.info("    pages=%d records=%d", page_count, total_records)
                    self.save_checkpoint({
                        "done_urls": list(done_urls),
                        "total_records": total_records,
                    })

                # Rotate browser every 40 pages
                if page_count % 40 == 0 and page_count > 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(10.0, 5.0)
                    self.launch_browser()

            # Phase 3: Follow sub-links
            log.info("  Phase 3: Sub-links (%d links)", len(all_sub_links))
            for source_name, link_url in all_sub_links:
                if link_url in done_urls or page_count >= max_pages:
                    continue

                records = self.scrape_going_page(source_name, link_url)
                for rec in records:
                    self.append_jsonl(output_file, rec)
                    total_records += 1

                done_urls.add(link_url)
                page_count += 1

                if page_count % 10 == 0:
                    log.info("    pages=%d records=%d", page_count, total_records)
                    self.save_checkpoint({
                        "done_urls": list(done_urls),
                        "total_records": total_records,
                    })

                if page_count % 50 == 0 and page_count > 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(10.0, 5.0)
                    self.launch_browser()

                self.smart_pause()

            self.save_checkpoint({
                "done_urls": list(done_urls),
                "total_records": total_records,
                "status": "done",
            })

            log.info("=" * 60)
            log.info("DONE: %d pages, %d records -> %s",
                     page_count, total_records, output_file)
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description="Script 113 (Playwright) -- Clerk of Course Reports Scraper"
    )
    PlaywrightScraperBase.add_common_args(parser)
    parser.add_argument("--max-pages", type=int, default=500,
                        help="Max pages to scrape")
    args = parser.parse_args()

    scraper = ClerkOfCoursePlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
