#!/usr/bin/env python3
"""
Script 60 (Playwright) -- Scraping Oddschecker via headless Chromium.
Bypasses Cloudflare/anti-bot by rendering JS in a real browser engine.

Source : oddschecker.com/horse-racing
Collecte : odds comparison across bookmakers, market movers, best odds
CRITIQUE pour : Odds Model, Value Detection, Market Efficiency, Bookmaker Comparison

Usage:
    pip install playwright
    playwright install chromium
    python 60_oddschecker_playwright.py --start 2024-01-01 --end 2024-03-31
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


class OddscheckerPlaywright(PlaywrightScraperBase):
    SCRIPT_NAME = "60_oddschecker_pw"
    BASE_URL = "https://www.oddschecker.com"
    DEFAULT_PAUSE_BASE = 7.0
    DEFAULT_PAUSE_JITTER = 4.0

    # ------------------------------------------------------------------
    # Odds parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def parse_odds_decimal(text):
        """Convert fractional (5/1), decimal (6.0), or 'evens' to decimal odds."""
        text = text.strip()
        frac = re.match(r'^(\d+)/(\d+)$', text)
        if frac:
            num, den = int(frac.group(1)), int(frac.group(2))
            if den > 0:
                return round(num / den + 1.0, 2)
        if re.match(r'^evens?$', text, re.I):
            return 2.0
        dec = re.match(r'^(\d+\.\d+)$', text)
        if dec:
            return float(dec.group(1))
        return None

    # ------------------------------------------------------------------
    # Meetings list
    # ------------------------------------------------------------------

    def scrape_meetings(self, date_str):
        cache_key = f"meetings_{date_str}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        url = f"{self.BASE_URL}/horse-racing/{date_str}"
        if not self.navigate(url):
            self.screenshot_on_error(f"oddschecker_meetings_{date_str}")
            return []

        self.accept_cookies()
        time.sleep(2)

        records = []

        # Race links
        links = self.page.query_selector_all("a[href*='/horse-racing/']")
        seen_urls = set()
        for link in links:
            href = link.get_attribute("href") or ""
            if href == f"/horse-racing/{date_str}" or not href:
                continue
            full_url = href if href.startswith("http") else self.BASE_URL + href
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)
            text = (link.inner_text() or "").strip()
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "race_link",
                    "text": text,
                    "url": full_url,
                    "scraped_at": datetime.now().isoformat(),
                })

        # Meeting sections
        section_els = self.page.query_selector_all(
            "[class*='meeting'], [class*='event'], [class*='fixture'], "
            "[class*='race-card']"
        )
        for section in section_els:
            title_el = section.query_selector("h2, h3, h4, a")
            if title_el:
                rec = {
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "meeting",
                    "meeting_name": (title_el.inner_text() or "").strip(),
                    "scraped_at": datetime.now().isoformat(),
                }
                href = title_el.get_attribute("href")
                if href:
                    rec["url"] = href if href.startswith("http") else self.BASE_URL + href
                records.append(rec)

        self.save_cache(cache_key, records)
        return records

    # ------------------------------------------------------------------
    # Race odds
    # ------------------------------------------------------------------

    def scrape_race_odds(self, race_url, date_str):
        if not race_url.startswith("http"):
            race_url = self.BASE_URL + race_url

        cache_key = f"odds_{re.sub(r'[^a-zA-Z0-9]', '_', race_url[-60:])}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        if not self.navigate(race_url):
            self.screenshot_on_error(f"oddschecker_odds_{date_str}")
            return []

        time.sleep(3)  # Odds tables often load via XHR
        records = []

        # Race name
        race_name = ""
        for sel in ["h1", "h2"]:
            el = self.page.query_selector(sel)
            if el:
                race_name = (el.inner_text() or "").strip()
                if race_name:
                    break

        # Race time
        race_time = ""
        time_el = self.page.query_selector(
            "[class*='time'], [class*='clock'], [class*='race-time'], time"
        )
        if time_el:
            race_time = (time_el.inner_text() or "").strip()

        # --- Extract bookmaker headers ---
        bookmaker_names = []
        header_row = self.page.query_selector(
            "tr[class*='header'], tr[class*='bookmaker'], tr[class*='bookie']"
        )
        if header_row:
            cells = header_row.query_selector_all("th, td")
            for cell in cells:
                bk = (cell.get_attribute("data-bk")
                      or cell.get_attribute("title")
                      or (cell.inner_text() or "").strip())
                if bk:
                    bookmaker_names.append(bk)

        # --- Extract odds table via JS for better reliability ---
        odds_data = self.page.evaluate("""() => {
            const results = [];
            const rows = document.querySelectorAll('tr');
            for (const row of rows) {
                const cells = row.querySelectorAll('td, th');
                if (cells.length < 3) continue;
                const nameEl = row.querySelector(
                    '[class*="runner"], [class*="horse"], [class*="name"], [class*="sel"]'
                );
                if (!nameEl) continue;
                const horseName = nameEl.textContent.trim();
                if (!horseName || horseName.length < 2) continue;
                const oddsMap = {};
                for (const cell of cells) {
                    const bk = cell.getAttribute('data-bk') || '';
                    const odig = cell.getAttribute('data-odig') || '';
                    const display = cell.textContent.trim();
                    if (bk && (odig || display)) {
                        oddsMap[bk] = {display: display, decimal: odig || ''};
                    }
                }
                if (Object.keys(oddsMap).length > 0) {
                    results.push({horseName, odds: oddsMap});
                }
            }
            return results;
        }""")

        for entry in (odds_data or []):
            horse = entry.get("horseName", "")
            odds_map = entry.get("odds", {})
            if not horse or not odds_map:
                continue

            rec = {
                "date": date_str,
                "source": "oddschecker",
                "type": "odds_comparison",
                "race_name": race_name,
                "race_time": race_time,
                "horse_name": horse,
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }

            odds_by_bk = {}
            best_decimal = 0.0
            best_odds = None

            for bk, info in odds_map.items():
                display = info.get("display", "")
                dec_str = info.get("decimal", "")
                dec_val = None
                if dec_str:
                    try:
                        dec_val = float(dec_str)
                    except ValueError:
                        pass
                if dec_val is None:
                    dec_val = self.parse_odds_decimal(display)
                if dec_val:
                    odds_by_bk[bk] = {"fractional": display, "decimal": dec_val}
                    if dec_val > best_decimal:
                        best_decimal = dec_val
                        best_odds = {"bookmaker": bk, "fractional": display,
                                     "decimal": dec_val}

            if odds_by_bk:
                rec["odds_by_bookmaker"] = odds_by_bk
                rec["num_bookmakers"] = len(odds_by_bk)
                if best_odds:
                    rec["best_odds"] = best_odds
                records.append(rec)

        # --- Market percentage / overround ---
        all_best = [r["best_odds"]["decimal"] for r in records
                    if r.get("type") == "odds_comparison"
                    and r.get("best_odds", {}).get("decimal", 0) > 1.0]
        if all_best:
            mkt_pct = sum(1.0 / d for d in all_best) * 100
            records.append({
                "date": date_str,
                "source": "oddschecker",
                "type": "market_percentage",
                "race_name": race_name,
                "market_percentage": round(mkt_pct, 2),
                "overround": round(mkt_pct - 100, 2),
                "num_runners_priced": len(all_best),
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            })

        # --- Market movers ---
        mover_els = self.page.query_selector_all(
            "[class*='mover'], [class*='market-mover'], "
            "[class*='steamer'], [class*='drifter']"
        )
        for el in mover_els:
            text = (el.inner_text() or "").strip()
            if text and 5 < len(text) < 500:
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "market_mover",
                    "race_name": race_name,
                    "content": text[:400],
                    "scraped_at": datetime.now().isoformat(),
                })

        # --- Tips / predictions ---
        tip_els = self.page.query_selector_all(
            "[class*='tip'], [class*='prediction'], [class*='verdict'], "
            "[class*='nap'], [class*='best-bet'], [class*='each-way']"
        )
        for el in tip_els:
            text = (el.inner_text() or "").strip()
            if text and 10 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "tip_prediction",
                    "race_name": race_name,
                    "content": text[:1500],
                    "scraped_at": datetime.now().isoformat(),
                })

        # --- Embedded JSON ---
        records.extend(self.extract_embedded_json(date_str, "oddschecker"))

        # --- Odds history sections ---
        hist_els = self.page.query_selector_all(
            "[class*='odds-history'], [class*='price-history'], "
            "[class*='movement'], [class*='timeline'], [class*='chart-data']"
        )
        for el in hist_els:
            text = (el.inner_text() or "").strip()
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "oddschecker",
                    "type": "odds_history",
                    "race_name": race_name,
                    "content": text[:2500],
                    "url": race_url,
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
        log.info("SCRIPT 60 (Playwright) -- Oddschecker Scraper")
        log.info("  Period : %s -> %s", start_date.date(), end_date.date())
        log.info("=" * 60)

        checkpoint = self.load_checkpoint()
        if self.args.resume and checkpoint.get("last_date"):
            resume_dt = datetime.strptime(checkpoint["last_date"], "%Y-%m-%d") + timedelta(days=1)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("  Resuming from checkpoint: %s", start_date.date())

        output_file = os.path.join(self.output_dir, "oddschecker_data.jsonl")
        self.launch_browser()

        try:
            current = start_date
            day_count = 0
            total_records = 0

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")

                meeting_records = self.scrape_meetings(date_str)
                if meeting_records:
                    # Scrape odds for each race
                    race_urls = list({
                        r.get("url") for r in meeting_records
                        if r.get("url") and "/horse-racing/" in r.get("url", "")
                    })
                    for rurl in race_urls[:20]:
                        odds = self.scrape_race_odds(rurl, date_str)
                        if odds:
                            meeting_records.extend(odds)
                        self.smart_pause(7.0, 3.0)

                    for rec in meeting_records:
                        self.append_jsonl(output_file, rec)
                        total_records += 1

                day_count += 1

                if day_count % 30 == 0:
                    log.info("  %s | days=%d records=%d",
                             date_str, day_count, total_records)
                    self.save_checkpoint({
                        "last_date": date_str,
                        "total_records": total_records,
                    })

                if day_count % 60 == 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(15.0, 8.0)
                    self.launch_browser()

                current += timedelta(days=1)
                self.smart_pause(7.0, 3.0)

            self.save_checkpoint({
                "last_date": end_date.strftime("%Y-%m-%d"),
                "total_records": total_records,
                "status": "done",
            })
            log.info("=" * 60)
            log.info("DONE: %d days, %d records -> %s",
                     day_count, total_records, output_file)
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description="Script 60 (Playwright) -- Oddschecker Odds Comparison"
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = OddscheckerPlaywright(args)
    scraper.run()


if __name__ == "__main__":
    main()
