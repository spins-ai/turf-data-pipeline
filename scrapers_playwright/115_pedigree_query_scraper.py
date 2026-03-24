#!/usr/bin/env python3
"""
Script 115 (Playwright) -- Scraping PedigreeQuery.com via headless Chromium.
Bypasses Cloudflare/anti-bot by rendering JS in a real browser engine.
Replaces the old script 36 which was blocked by Cloudflare using requests.

Source : pedigreequery.com/{horse_name}
Collecte : pedigree complet 4-5 generations, lignees, pays, annee naissance, couleur
CRITIQUE pour : Pedigree Features, Bloodline Analysis, Inbreeding Detection (etape 7F)

Usage:
    pip install playwright
    playwright install chromium
    python 115_pedigree_query_scraper.py --horse-list horses.txt
    python 115_pedigree_query_scraper.py --horse "Frankel"
    python 115_pedigree_query_scraper.py --horse-list horses.txt --resume --proxy http://host:port
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

log = setup_logging("115_pedigree_query_pw")


class PedigreeQueryPlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "115_pedigree_query_pw"
    BASE_URL = "https://www.pedigreequery.com"
    DEFAULT_PAUSE_BASE = 6.0
    DEFAULT_PAUSE_JITTER = 4.0

    def __init__(self, args):
        super().__init__(args)
        self.html_dir = os.path.join(self.output_dir, "html_raw")
        os.makedirs(self.html_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Override browser launch to use en-US locale
    # ------------------------------------------------------------------

    def launch_browser(self):
        """Launch headless Chromium with en-US locale for pedigreequery.com."""
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
            locale="en-US",
            timezone_id="America/New_York",
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
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            window.chrome = {runtime: {}};
        """)
        self.page = self._context.new_page()
        self.page.set_default_timeout(self.DEFAULT_TIMEOUT_MS)
        log.info("Browser launched (headless Chromium, en-US locale)")

    # ------------------------------------------------------------------
    # Horse list loading
    # ------------------------------------------------------------------

    @staticmethod
    def load_horse_list(filepath):
        """Load horse names from a text file (one name per line)."""
        if not os.path.exists(filepath):
            log.error("Horse list file not found: %s", filepath)
            return []
        with open(filepath, "r", encoding="utf-8") as f:
            horses = [line.strip() for line in f if line.strip()]
        log.info("Loaded %d horses from %s", len(horses), filepath)
        return horses

    @staticmethod
    def _clean_horse_name(name):
        """Normalize horse name for URL usage."""
        cleaned = re.sub(r"[^\w\s'-]", "", name.strip())
        return cleaned

    @staticmethod
    def _url_encode_name(name):
        """Encode horse name for pedigreequery URL."""
        return name.strip().replace(" ", "+")

    # ------------------------------------------------------------------
    # Pedigree parsing from rendered DOM
    # ------------------------------------------------------------------

    def _parse_pedigree_table(self, horse_name):
        """Parse the pedigree table from the rendered page.

        PedigreeQuery uses nested tables for the pedigree tree.
        The table cells contain ancestor names as links.
        Returns a dict with structured pedigree data.
        """
        record = {
            "name": horse_name,
            "source": "pedigree_query",
            "type": "pedigree",
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract ancestors from table cells containing links
        ancestors = []
        cells = self.page.query_selector_all("table td a")
        for cell in cells:
            text = (cell.inner_text() or "").strip()
            href = cell.get_attribute("href") or ""
            if text and len(text) > 1 and "/" in href:
                # Skip navigation links, keep horse name links
                if any(skip in href.lower() for skip in
                       ["search", "login", "register", "contact",
                        "about", "help", "faq", "privacy", "terms",
                        "javascript", "mailto"]):
                    continue
                ancestors.append(text)

        if len(ancestors) >= 2:
            # Generation 1 (parents)
            record["sire"] = ancestors[0] if len(ancestors) > 0 else None
            record["dam"] = ancestors[1] if len(ancestors) > 1 else None

        if len(ancestors) >= 6:
            # Generation 2 (grandparents)
            record["sire_sire"] = ancestors[2] if len(ancestors) > 2 else None
            record["sire_dam"] = ancestors[3] if len(ancestors) > 3 else None
            record["dam_sire"] = ancestors[4] if len(ancestors) > 4 else None
            record["dam_dam"] = ancestors[5] if len(ancestors) > 5 else None

        if len(ancestors) >= 14:
            # Generation 3 (great-grandparents)
            record["sire_sire_sire"] = ancestors[6] if len(ancestors) > 6 else None
            record["sire_sire_dam"] = ancestors[7] if len(ancestors) > 7 else None
            record["sire_dam_sire"] = ancestors[8] if len(ancestors) > 8 else None
            record["sire_dam_dam"] = ancestors[9] if len(ancestors) > 9 else None
            record["dam_sire_sire"] = ancestors[10] if len(ancestors) > 10 else None
            record["dam_sire_dam"] = ancestors[11] if len(ancestors) > 11 else None
            record["dam_dam_sire"] = ancestors[12] if len(ancestors) > 12 else None
            record["dam_dam_dam"] = ancestors[13] if len(ancestors) > 13 else None

        if len(ancestors) >= 30:
            # Generation 4 (great-great-grandparents) -- 16 ancestors
            gen4_keys = [
                "sire_sire_sire_sire", "sire_sire_sire_dam",
                "sire_sire_dam_sire", "sire_sire_dam_dam",
                "sire_dam_sire_sire", "sire_dam_sire_dam",
                "sire_dam_dam_sire", "sire_dam_dam_dam",
                "dam_sire_sire_sire", "dam_sire_sire_dam",
                "dam_sire_dam_sire", "dam_sire_dam_dam",
                "dam_dam_sire_sire", "dam_dam_sire_dam",
                "dam_dam_dam_sire", "dam_dam_dam_dam",
            ]
            for k, idx in enumerate(range(14, 30)):
                if idx < len(ancestors) and k < len(gen4_keys):
                    record[gen4_keys[k]] = ancestors[idx]

        record["ancestors_count"] = len(ancestors)
        record["generations"] = (
            4 if len(ancestors) >= 30 else
            3 if len(ancestors) >= 14 else
            2 if len(ancestors) >= 6 else
            1 if len(ancestors) >= 2 else 0
        )

        return record

    def _extract_horse_info(self, horse_name):
        """Extract additional horse info (country, year, color, sex) from page text."""
        info = {}
        try:
            page_text = self.page.inner_text("body") or ""
        except Exception:
            return info

        # Country code
        country_match = re.search(
            r'\b(FR|GB|IRE|USA|AUS|GER|JPN|HK|SAF|UAE|NZ|CAN|BRZ|CHI|ARG|ITY|SPA|SWE|DEN|NOR)\b',
            page_text[:1000],
        )
        if country_match:
            info["country"] = country_match.group(1)

        # Birth year
        year_match = re.search(r'\b(19[5-9]\d|20[0-2]\d)\b', page_text[:800])
        if year_match:
            info["birth_year"] = int(year_match.group(1))

        # Color
        color_match = re.search(
            r'\b(bay|brown|chestnut|grey|gray|black|roan|dark bay|b\.|ch\.|gr\.|bl\.)\b',
            page_text[:800], re.IGNORECASE,
        )
        if color_match:
            info["color"] = color_match.group(1).lower()

        # Sex
        sex_match = re.search(
            r'\b(colt|filly|horse|mare|stallion|gelding|ridgling|rig)\b',
            page_text[:800], re.IGNORECASE,
        )
        if sex_match:
            info["sex"] = sex_match.group(1).lower()

        # Race record if present
        race_match = re.search(
            r'(\d+)\s*(?:starts?|runs?)\s*[-,]\s*(\d+)\s*(?:wins?)',
            page_text[:1500], re.IGNORECASE,
        )
        if race_match:
            info["starts"] = int(race_match.group(1))
            info["wins"] = int(race_match.group(2))

        return info

    # ------------------------------------------------------------------
    # Single horse scrape
    # ------------------------------------------------------------------

    def scrape_horse(self, horse_name):
        """Scrape pedigree data for a single horse."""
        clean_name = self._clean_horse_name(horse_name)
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', clean_name)[:60]

        # Check cache
        cached = self.load_cache(f"horse_{safe_name}")
        if cached is not None:
            return cached

        # Check for raw HTML cache
        html_file = os.path.join(self.html_dir, f"{safe_name}.html")

        url_name = self._url_encode_name(clean_name)
        url = f"{self.BASE_URL}/{url_name}"

        if not self.navigate(url, wait_until="domcontentloaded"):
            self.screenshot_on_error(f"pedigree_{safe_name}")
            return None

        self.accept_cookies()
        time.sleep(2)

        # Check for Cloudflare challenge or blocking
        page_text = self.page.inner_text("body") or ""
        if any(block_sign in page_text.lower() for block_sign in
               ["attention required", "please wait", "checking your browser",
                "ray id", "security check", "access denied"]):
            log.warning("  Cloudflare/block detected for %s, waiting...", clean_name)
            time.sleep(15)
            # Retry once after waiting
            if not self.navigate(url, wait_until="domcontentloaded"):
                self.screenshot_on_error(f"pedigree_block_{safe_name}")
                return None
            time.sleep(3)

        # Save raw HTML
        try:
            html = self.page.content()
            if html and len(html) > 500:
                with open(html_file, "w", encoding="utf-8") as f:
                    f.write(html)
        except Exception as exc:
            log.debug("  Could not save HTML: %s", exc)

        # Parse pedigree
        record = self._parse_pedigree_table(clean_name)

        # Add extra horse info
        extra = self._extract_horse_info(clean_name)
        record.update(extra)

        # Extract embedded JSON data if any
        embedded = self.extract_embedded_json(
            datetime.now().strftime("%Y-%m-%d"), "pedigree_query",
        )
        if embedded:
            record["embedded_data"] = embedded

        # Validate: at least sire or dam must be found
        if not record.get("sire") and not record.get("dam"):
            log.debug("  No pedigree found for %s", clean_name)
            # Still cache the empty result to avoid re-fetching
            record["status"] = "not_found"

        self.save_cache(f"horse_{safe_name}", record)
        return record

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self):
        # Build horse list
        horses = []
        if getattr(self.args, "horse", None):
            horses = [self.args.horse]
        elif getattr(self.args, "horse_list", None):
            horses = self.load_horse_list(self.args.horse_list)
        else:
            # Try default location
            default_list = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "..",
                "output", "horse_names.txt",
            )
            if os.path.exists(default_list):
                horses = self.load_horse_list(default_list)
            else:
                log.error("No horse list provided. Use --horse or --horse-list.")
                return

        if not horses:
            log.error("Empty horse list, nothing to scrape.")
            return

        log.info("=" * 60)
        log.info("SCRIPT 115 (Playwright) -- PedigreeQuery Pedigree Scraper")
        log.info("  Horses to scrape: %d", len(horses))
        log.info("=" * 60)

        # Checkpoint / resume
        checkpoint = self.load_checkpoint()
        done_set = set(checkpoint.get("done_horses", []))
        if self.args.resume and done_set:
            log.info("  Resuming: %d already done", len(done_set))

        output_file = os.path.join(self.output_dir, "pedigree_query_data.jsonl")
        self.launch_browser()

        try:
            total_scraped = 0
            total_found = 0
            errors = 0
            consecutive_errors = 0

            for i, horse_name in enumerate(horses):
                clean = self._clean_horse_name(horse_name)
                if clean in done_set:
                    continue

                record = self.scrape_horse(horse_name)

                if record:
                    self.append_jsonl(output_file, record)
                    total_scraped += 1
                    consecutive_errors = 0
                    if record.get("status") != "not_found":
                        total_found += 1
                else:
                    errors += 1
                    consecutive_errors += 1

                done_set.add(clean)

                # Progress logging
                if (i + 1) % 50 == 0:
                    log.info(
                        "  Progress: %d/%d | found=%d | errors=%d",
                        i + 1, len(horses), total_found, errors,
                    )
                    self.save_checkpoint({
                        "done_horses": list(done_set),
                        "total_scraped": total_scraped,
                        "total_found": total_found,
                    })

                # Consecutive error handling
                if consecutive_errors >= 10:
                    log.warning("  10 consecutive errors -- long pause 120s")
                    time.sleep(120)
                    consecutive_errors = 0

                # Browser rotation every 80 horses
                if total_scraped > 0 and total_scraped % 80 == 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(10.0, 5.0)
                    self.launch_browser()

                self.smart_pause(self.DEFAULT_PAUSE_BASE, self.DEFAULT_PAUSE_JITTER)

            # Final checkpoint
            self.save_checkpoint({
                "done_horses": list(done_set),
                "total_scraped": total_scraped,
                "total_found": total_found,
                "status": "done",
            })

            log.info("=" * 60)
            log.info(
                "DONE: %d scraped, %d pedigrees found, %d errors -> %s",
                total_scraped, total_found, errors, output_file,
            )
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description="Script 115 (Playwright) -- PedigreeQuery Pedigree Scraper (4+ generations)"
    )
    PlaywrightScraperBase.add_common_args(parser)
    parser.add_argument(
        "--horse", type=str, default=None,
        help="Single horse name to scrape",
    )
    parser.add_argument(
        "--horse-list", type=str, default=None,
        help="Path to text file with horse names (one per line)",
    )
    args = parser.parse_args()

    scraper = PedigreeQueryPlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
