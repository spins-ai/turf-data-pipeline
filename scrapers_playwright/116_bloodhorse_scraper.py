#!/usr/bin/env python3
"""
Script 116 (Playwright) -- Scraping BloodHorse.com via headless Chromium.
Bypasses Cloudflare/anti-bot by rendering JS in a real browser engine.
Replaces the requests-based portion of script 87 for bloodhorse.com.

Source : bloodhorse.com
Collecte : stallion register (sire lists), auction results, breeding stats
CRITIQUE pour : Breeding Features, Stallion Model, Bloodstock Market Intelligence (etape 7P)

Usage:
    pip install playwright
    playwright install chromium
    python 116_bloodhorse_scraper.py --start-year 2015 --end-year 2025
    python 116_bloodhorse_scraper.py --start-year 2020 --resume
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

log = setup_logging("116_bloodhorse_pw")


# Sire list categories available on BloodHorse
SIRE_LIST_CATEGORIES = [
    "leading-sires-by-progeny-earnings",
    "leading-broodmare-sires",
    "leading-freshman-sires",
    "leading-juvenile-sires",
    "leading-turf-sires",
    "leading-dirt-sires",
]

# Auction sale names commonly listed on BloodHorse
AUCTION_KEYWORDS = [
    "keeneland", "fasig-tipton", "ocala", "barretts",
    "tattersalls", "goffs", "arqana", "inglis",
]


class BloodHorsePlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "116_bloodhorse_pw"
    BASE_URL = "https://www.bloodhorse.com"
    DEFAULT_PAUSE_BASE = 7.0
    DEFAULT_PAUSE_JITTER = 4.0

    # ------------------------------------------------------------------
    # Override browser launch to use en-US locale
    # ------------------------------------------------------------------

    def launch_browser(self):
        """Launch headless Chromium with en-US locale for bloodhorse.com."""
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
    # Stallion register / sire lists
    # ------------------------------------------------------------------

    def scrape_sire_list(self, year, category):
        """Scrape a sire list page for a given year and category."""
        cache_key = f"sires_{year}_{category}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        url = (
            f"{self.BASE_URL}/horse-racing/thoroughbred-breeding"
            f"/sire-lists/{year}/{category}"
        )
        if not self.navigate(url, wait_until="domcontentloaded"):
            self.screenshot_on_error(f"sire_{year}_{category}")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # --- Sire ranking tables ---
        table_records = self.extract_tables(
            str(year), "bloodhorse", "sire_ranking",
        )
        for rec in table_records:
            rec["category"] = category
            rec["year"] = str(year)
            # Try to extract earnings from cell values
            for key, val in list(rec.items()):
                if isinstance(val, str):
                    earnings_m = re.search(r'\$[\d,]+', val)
                    if earnings_m:
                        rec["earnings_raw"] = earnings_m.group(0)
                        break
        records.extend(table_records)

        # --- Stallion detail divs ---
        detail_els = self.page.query_selector_all(
            "[class*='stallion'], [class*='sire'], [class*='stud'], "
            "[class*='breeding'], [class*='progeny'], [class*='offspring'], "
            "[class*='horse-detail'], [class*='entry']"
        )
        for el in detail_els:
            text = (el.inner_text() or "").strip()
            if text and 20 < len(text) < 3000:
                records.append({
                    "year": str(year),
                    "category": category,
                    "source": "bloodhorse",
                    "type": "stallion_detail",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

        # --- Embedded JSON ---
        records.extend(self.extract_embedded_json(str(year), "bloodhorse"))

        # --- Data attributes ---
        records.extend(self.extract_data_attributes(
            str(year), "bloodhorse",
            keywords=["sire", "stallion", "horse", "breeding",
                       "earnings", "rank", "progeny", "stud"],
        ))

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Auction results
    # ------------------------------------------------------------------

    def scrape_auction_results(self, year):
        """Scrape auction/sales results from BloodHorse for a given year."""
        cache_key = f"auctions_{year}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        records = []

        # Try the main auction results page
        url = f"{self.BASE_URL}/horse-racing/thoroughbred-breeding/auction-results"
        if not self.navigate(url, wait_until="domcontentloaded"):
            # Try alternate URL patterns
            url = f"{self.BASE_URL}/auction-results/{year}"
            if not self.navigate(url, wait_until="domcontentloaded"):
                self.screenshot_on_error(f"auction_{year}")
                return []

        self.accept_cookies()
        time.sleep(2)

        # --- Auction result tables ---
        table_records = self.extract_tables(str(year), "bloodhorse", "auction_result")
        for rec in table_records:
            rec["year"] = str(year)
            # Parse price
            for key, val in list(rec.items()):
                if isinstance(val, str):
                    price_m = re.search(r'\$[\d,]+', val)
                    if price_m:
                        rec["price_raw"] = price_m.group(0)
                        try:
                            rec["price_usd"] = int(
                                price_m.group(0).replace("$", "").replace(",", "")
                            )
                        except ValueError:
                            pass
                        break
        records.extend(table_records)

        # --- Sale-specific sections ---
        sale_els = self.page.query_selector_all(
            "[class*='sale'], [class*='auction'], [class*='result'], "
            "[class*='lot'], [class*='catalog'], [class*='consign']"
        )
        for el in sale_els:
            text = (el.inner_text() or "").strip()
            if text and 15 < len(text) < 3000:
                rec = {
                    "year": str(year),
                    "source": "bloodhorse",
                    "type": "auction_detail",
                    "contenu": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                }
                # Try to identify sale name
                text_lower = text.lower()
                for kw in AUCTION_KEYWORDS:
                    if kw in text_lower:
                        rec["sale_name"] = kw
                        break
                records.append(rec)

        # --- Links to individual sale pages ---
        sale_links = []
        for a in self.page.query_selector_all("a[href]"):
            href = a.get_attribute("href") or ""
            if re.search(r'auction|sale|results', href, re.I):
                full = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                if str(year) in full or "result" in full.lower():
                    sale_links.append(full)

        # Scrape individual sale pages (limit to 10)
        for link in list(set(sale_links))[:10]:
            detail = self._scrape_sale_detail(link, year)
            if detail:
                records.extend(detail)
            self.smart_pause(5.0, 3.0)

        # --- Embedded JSON ---
        records.extend(self.extract_embedded_json(str(year), "bloodhorse"))

        self.save_cache(cache_key, records)
        return records

    def _scrape_sale_detail(self, url, year):
        """Scrape a single sale/auction detail page."""
        safe_key = re.sub(r'[^a-zA-Z0-9]', '_', url[-80:])
        cache_key = f"sale_detail_{safe_key}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(url, wait_until="domcontentloaded"):
            return []

        time.sleep(2)
        records = []

        # Tables (lot details, results)
        table_records = self.extract_tables(str(year), "bloodhorse", "sale_lot")
        for rec in table_records:
            rec["url_sale"] = url
            rec["year"] = str(year)
            # Parse price
            for key, val in list(rec.items()):
                if isinstance(val, str):
                    price_m = re.search(r'\$[\d,]+', val)
                    if price_m:
                        rec["price_raw"] = price_m.group(0)
                        try:
                            rec["price_usd"] = int(
                                price_m.group(0).replace("$", "").replace(",", "")
                            )
                        except ValueError:
                            pass
                        break
        records.extend(table_records)

        # Lot details
        lot_els = self.page.query_selector_all(
            "[class*='lot'], [class*='hip'], [class*='catalog-entry'], "
            "[class*='horse-info'], [class*='consign']"
        )
        for el in lot_els:
            text = (el.inner_text() or "").strip()
            if text and 10 < len(text) < 2000:
                records.append({
                    "year": str(year),
                    "source": "bloodhorse",
                    "type": "lot_detail",
                    "url_sale": url,
                    "contenu": text[:1500],
                    "scraped_at": datetime.now().isoformat(),
                })

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Breeding stats / stallion directory
    # ------------------------------------------------------------------

    def scrape_breeding_stats(self, year):
        """Scrape general breeding statistics from BloodHorse."""
        cache_key = f"breeding_stats_{year}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        records = []

        # Breeding stats overview
        urls_to_try = [
            f"{self.BASE_URL}/horse-racing/thoroughbred-breeding/statistics/{year}",
            f"{self.BASE_URL}/horse-racing/thoroughbred-breeding/statistics",
            f"{self.BASE_URL}/stallion-register/{year}",
            f"{self.BASE_URL}/stallion-register",
        ]

        for url in urls_to_try:
            if not self.navigate(url, wait_until="domcontentloaded", retries=1):
                continue

            self.accept_cookies()
            time.sleep(2)

            # Tables
            table_records = self.extract_tables(
                str(year), "bloodhorse", "breeding_stat",
            )
            for rec in table_records:
                rec["year"] = str(year)
                rec["url_source"] = url
            records.extend(table_records)

            # Breeding sections
            breed_els = self.page.query_selector_all(
                "[class*='breed'], [class*='stat'], [class*='register'], "
                "[class*='stallion'], [class*='stud-fee'], [class*='fee'], "
                "[class*='mare'], [class*='foal'], [class*='crop']"
            )
            for el in breed_els:
                text = (el.inner_text() or "").strip()
                if text and 15 < len(text) < 3000:
                    rec = {
                        "year": str(year),
                        "source": "bloodhorse",
                        "type": "breeding_stat_detail",
                        "url_source": url,
                        "contenu": text[:2500],
                        "scraped_at": datetime.now().isoformat(),
                    }
                    # Parse stud fee
                    fee_m = re.search(r'(?:stud\s*fee|fee)\s*:?\s*\$?([\d,]+)', text, re.I)
                    if fee_m:
                        rec["stud_fee_raw"] = fee_m.group(1)
                    records.append(rec)

            # Embedded JSON
            records.extend(self.extract_embedded_json(str(year), "bloodhorse"))

            # Data attributes
            records.extend(self.extract_data_attributes(
                str(year), "bloodhorse",
                keywords=["stallion", "fee", "mare", "foal", "breed",
                           "crop", "stud", "register"],
            ))

            if records:
                break  # Got data from one URL, no need to try others

            self.smart_pause(3.0, 2.0)

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Stallion directory / individual stallion pages
    # ------------------------------------------------------------------

    def scrape_stallion_directory(self, year):
        """Scrape the stallion register/directory listing."""
        cache_key = f"stallion_dir_{year}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        records = []
        stallion_links = []

        url = f"{self.BASE_URL}/stallion-register/{year}"
        if not self.navigate(url, wait_until="domcontentloaded"):
            url = f"{self.BASE_URL}/stallion-register"
            if not self.navigate(url, wait_until="domcontentloaded"):
                return []

        self.accept_cookies()
        time.sleep(2)

        # Collect stallion links
        for a in self.page.query_selector_all("a[href]"):
            href = a.get_attribute("href") or ""
            text = (a.inner_text() or "").strip()
            if re.search(r'stallion|sire|stud', href, re.I) and text and len(text) > 2:
                full = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                stallion_links.append({"url": full, "name": text})

        # Table-based directory
        table_records = self.extract_tables(str(year), "bloodhorse", "stallion_register")
        for rec in table_records:
            rec["year"] = str(year)
        records.extend(table_records)

        # Scrape individual stallion pages (limit to 50 per year)
        for item in stallion_links[:50]:
            detail = self._scrape_stallion_detail(item["url"], item["name"], year)
            if detail:
                records.append(detail)
            self.smart_pause(5.0, 3.0)

        self.save_cache(cache_key, records)
        return records

    def _scrape_stallion_detail(self, url, stallion_name, year):
        """Scrape an individual stallion page."""
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', stallion_name)[:50]
        cache_key = f"stallion_{year}_{safe_name}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(url, wait_until="domcontentloaded", retries=2):
            return None

        time.sleep(2)

        record = {
            "name": stallion_name,
            "year": str(year),
            "source": "bloodhorse",
            "type": "stallion_profile",
            "url": url,
            "scraped_at": datetime.now().isoformat(),
        }

        page_text = self.page.inner_text("body") or ""

        # Stud fee
        fee_m = re.search(r'(?:stud\s*fee|fee|stands?\s*(?:for|at))\s*:?\s*\$?([\d,]+)',
                          page_text, re.I)
        if fee_m:
            record["stud_fee_raw"] = fee_m.group(1)
            try:
                record["stud_fee_usd"] = int(fee_m.group(1).replace(",", ""))
            except ValueError:
                pass

        # Sire / dam
        sire_m = re.search(r'(?:sire|by)\s*:?\s*([A-Z][A-Za-z\s\'-]+)', page_text[:2000])
        if sire_m:
            record["sire"] = sire_m.group(1).strip()

        dam_m = re.search(r'(?:dam|out of)\s*:?\s*([A-Z][A-Za-z\s\'-]+)', page_text[:2000])
        if dam_m:
            record["dam"] = dam_m.group(1).strip()

        # Race record
        record_m = re.search(
            r'(\d+)\s*(?:starts?|runs?)\s*[-,]\s*(\d+)\s*(?:wins?)',
            page_text[:3000], re.I,
        )
        if record_m:
            record["starts"] = int(record_m.group(1))
            record["wins"] = int(record_m.group(2))

        # Earnings
        earnings_m = re.search(r'(?:earnings?|earned)\s*:?\s*\$?([\d,]+)', page_text[:3000], re.I)
        if earnings_m:
            record["earnings_raw"] = earnings_m.group(1)

        # Crop / foal stats
        crop_m = re.search(r'(\d+)\s*(?:foals?|crops?|starters?)', page_text[:3000], re.I)
        if crop_m:
            record["foals_count"] = int(crop_m.group(1))

        # Winners ratio
        winners_m = re.search(r'(\d+)\s*(?:winners?)\s*(?:from|of)\s*(\d+)', page_text[:3000], re.I)
        if winners_m:
            record["winners"] = int(winners_m.group(1))
            record["from_starters"] = int(winners_m.group(2))

        # Progeny tables
        prog_tables = self.extract_tables(str(year), "bloodhorse", "progeny")
        if prog_tables:
            record["progeny_data"] = prog_tables

        self.save_cache(cache_key, record)
        return record

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self):
        start_year = self.args.start_year
        end_year = self.args.end_year

        log.info("=" * 60)
        log.info("SCRIPT 116 (Playwright) -- BloodHorse Stallion/Auction Scraper")
        log.info("  Years: %d -> %d", start_year, end_year)
        log.info("=" * 60)

        # Checkpoint / resume
        checkpoint = self.load_checkpoint()
        done_keys = set(checkpoint.get("done_keys", []))
        if self.args.resume and done_keys:
            log.info("  Resuming: %d tasks already done", len(done_keys))

        output_file = os.path.join(self.output_dir, "bloodhorse_data.jsonl")
        self.launch_browser()

        try:
            total_records = 0
            errors = 0

            for year in range(start_year, end_year + 1):
                log.info("--- Year %d ---", year)

                # 1) Sire lists (all categories)
                for cat in SIRE_LIST_CATEGORIES:
                    task_key = f"sires_{year}_{cat}"
                    if task_key in done_keys:
                        continue

                    records = self.scrape_sire_list(year, cat)
                    if records:
                        for rec in records:
                            self.append_jsonl(output_file, rec)
                            total_records += 1
                        log.info("  Sires %s %d: %d records", cat, year, len(records))
                    else:
                        errors += 1

                    done_keys.add(task_key)
                    self.smart_pause(self.DEFAULT_PAUSE_BASE, self.DEFAULT_PAUSE_JITTER)

                # 2) Auction results
                task_key = f"auctions_{year}"
                if task_key not in done_keys:
                    records = self.scrape_auction_results(year)
                    if records:
                        for rec in records:
                            self.append_jsonl(output_file, rec)
                            total_records += 1
                        log.info("  Auctions %d: %d records", year, len(records))
                    done_keys.add(task_key)
                    self.smart_pause(self.DEFAULT_PAUSE_BASE, self.DEFAULT_PAUSE_JITTER)

                # 3) Breeding stats
                task_key = f"breeding_{year}"
                if task_key not in done_keys:
                    records = self.scrape_breeding_stats(year)
                    if records:
                        for rec in records:
                            self.append_jsonl(output_file, rec)
                            total_records += 1
                        log.info("  Breeding stats %d: %d records", year, len(records))
                    done_keys.add(task_key)
                    self.smart_pause(self.DEFAULT_PAUSE_BASE, self.DEFAULT_PAUSE_JITTER)

                # 4) Stallion directory
                task_key = f"stallion_dir_{year}"
                if task_key not in done_keys:
                    records = self.scrape_stallion_directory(year)
                    if records:
                        for rec in records:
                            self.append_jsonl(output_file, rec)
                            total_records += 1
                        log.info("  Stallion dir %d: %d records", year, len(records))
                    done_keys.add(task_key)
                    self.smart_pause(self.DEFAULT_PAUSE_BASE, self.DEFAULT_PAUSE_JITTER)

                # Checkpoint after each year
                self.save_checkpoint({
                    "done_keys": list(done_keys),
                    "total_records": total_records,
                    "last_year": year,
                })

                # Browser rotation between years
                log.info("  Rotating browser context after year %d...", year)
                self.close_browser()
                self.smart_pause(8.0, 4.0)
                self.launch_browser()

            # Final checkpoint
            self.save_checkpoint({
                "done_keys": list(done_keys),
                "total_records": total_records,
                "status": "done",
            })

            log.info("=" * 60)
            log.info(
                "DONE: %d total records, %d errors -> %s",
                total_records, errors, output_file,
            )
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description="Script 116 (Playwright) -- BloodHorse Stallion Register & Auction Scraper"
    )
    PlaywrightScraperBase.add_common_args(parser)
    parser.add_argument(
        "--start-year", type=int, default=2015,
        help="First year to scrape (default: 2015)",
    )
    parser.add_argument(
        "--end-year", type=int, default=datetime.now().year,
        help="Last year to scrape (default: current year)",
    )
    args = parser.parse_args()

    scraper = BloodHorsePlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
