#!/usr/bin/env python3
"""
Base class for Playwright-based scrapers.
Handles: browser launch/close, page navigation with retry, cookie acceptance,
screenshot on error, proxy support, checkpoint/resume, JSONL output.

Usage:
    pip install playwright
    playwright install chromium
"""

import json
import logging
import os
import random
import re
import time
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

log = logging.getLogger(__name__)


class PlaywrightScraperBase:
    """Base class with common Playwright logic for all scrapers."""

    SCRIPT_NAME = "base_playwright"
    DEFAULT_PAUSE_BASE = 5.0
    DEFAULT_PAUSE_JITTER = 3.0
    DEFAULT_TIMEOUT_MS = 60_000
    MAX_RETRIES = 3

    # Common cookie-consent button selectors (FR + EN sites)
    COOKIE_SELECTORS = [
        "button:has-text('Accepter')",
        "button:has-text('Tout accepter')",
        "button:has-text('Accept')",
        "button:has-text('Accept All')",
        "button:has-text('Accept all')",
        "button:has-text('Agree')",
        "button:has-text('OK')",
        "button:has-text('J\\'accepte')",
        "button:has-text('Continuer')",
        "[id*='accept']",
        "[id*='consent'] button",
        "[class*='accept']",
        "[class*='consent'] button",
        "[data-testid*='accept']",
        "#onetrust-accept-btn-handler",
        "#didomi-notice-agree-button",
        ".cc-accept",
        ".cookie-accept",
        ".js-accept-cookies",
    ]

    def __init__(self, args):
        self.args = args
        self.output_dir = os.path.join("output", self.SCRIPT_NAME)
        self.cache_dir = os.path.join(self.output_dir, "cache")
        self.screenshot_dir = os.path.join(self.output_dir, "screenshots")
        self.checkpoint_file = os.path.join(self.output_dir, ".checkpoint.json")

        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.screenshot_dir, exist_ok=True)

        self._playwright = None
        self._browser = None
        self._context = None
        self.page = None

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    def launch_browser(self):
        """Launch headless Chromium with optional proxy."""
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

        # Context with realistic viewport + locale
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            java_script_enabled=True,
            ignore_https_errors=True,
        )
        # Stealth tweaks
        self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            window.chrome = {runtime: {}};
        """)
        self.page = self._context.new_page()
        self.page.set_default_timeout(self.DEFAULT_TIMEOUT_MS)
        log.info("Browser launched (headless Chromium)")

    def close_browser(self):
        """Graceful shutdown."""
        try:
            if self.page and not self.page.is_closed():
                self.page.close()
        except Exception:
            pass
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright:
                self._playwright.stop()
        except Exception:
            pass
        log.info("Browser closed")

    # ------------------------------------------------------------------
    # Navigation with retry
    # ------------------------------------------------------------------

    def navigate(self, url, wait_until="networkidle", retries=None):
        """Navigate to url, wait for load, retry on failure.
        Returns True on success, False if all retries exhausted.
        """
        retries = retries or self.MAX_RETRIES
        for attempt in range(1, retries + 1):
            try:
                resp = self.page.goto(url, wait_until=wait_until,
                                      timeout=self.DEFAULT_TIMEOUT_MS)
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
                # Extra wait for JS rendering
                self.page.wait_for_load_state("domcontentloaded")
                time.sleep(1.5)
                return True
            except PlaywrightTimeout:
                log.warning("  Timeout on %s (attempt %d/%d)",
                            url, attempt, retries)
                time.sleep(10 * attempt)
            except Exception as exc:
                log.warning("  Navigation error: %s (attempt %d/%d)",
                            str(exc)[:200], attempt, retries)
                time.sleep(5 * attempt)
        log.error("  Failed after %d retries: %s", retries, url)
        return False

    # ------------------------------------------------------------------
    # Cookie consent
    # ------------------------------------------------------------------

    def accept_cookies(self):
        """Try to click a cookie-consent button. Silent if none found."""
        for sel in self.COOKIE_SELECTORS:
            try:
                btn = self.page.locator(sel).first
                if btn.is_visible(timeout=1500):
                    btn.click(timeout=3000)
                    log.info("  Cookies accepted via: %s", sel)
                    time.sleep(1)
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # Screenshot on error
    # ------------------------------------------------------------------

    def screenshot_on_error(self, label="error"):
        """Save a debug screenshot when something goes wrong."""
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_label = re.sub(r'[^a-zA-Z0-9_-]', '_', label)[:60]
            path = os.path.join(self.screenshot_dir,
                                f"{safe_label}_{ts}.png")
            self.page.screenshot(path=path, full_page=True)
            log.info("  Screenshot saved: %s", path)
        except Exception as exc:
            log.warning("  Could not save screenshot: %s", exc)

    # ------------------------------------------------------------------
    # DOM helpers
    # ------------------------------------------------------------------

    def get_page_html(self):
        """Return the full rendered HTML of the current page."""
        return self.page.content()

    def query_all_text(self, selector):
        """Return a list of stripped text from all matching elements."""
        elements = self.page.query_selector_all(selector)
        return [el.inner_text().strip() for el in elements if el.inner_text().strip()]

    def extract_tables(self, date_str, source, record_type="table_row"):
        """Extract all <table> rows as dicts from the current page."""
        records = []
        tables = self.page.query_selector_all("table")
        for table in tables:
            rows = table.query_selector_all("tr")
            if not rows:
                continue
            header_cells = rows[0].query_selector_all("th, td")
            headers = [h.inner_text().strip().lower().replace(" ", "_")
                       for h in header_cells]
            for row in rows[1:]:
                cells = row.query_selector_all("td, th")
                values = [c.inner_text().strip() for c in cells]
                if len(values) < 2:
                    continue
                rec = {
                    "date": date_str,
                    "source": source,
                    "type": record_type,
                    "scraped_at": datetime.now().isoformat(),
                }
                for j, val in enumerate(values):
                    key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                    rec[key] = val
                records.append(rec)
        return records

    def extract_embedded_json(self, date_str, source):
        """Extract JSON from <script> tags in the rendered page."""
        records = []
        scripts = self.page.query_selector_all("script")
        for script in scripts:
            stype = script.get_attribute("type") or ""
            text = script.inner_text() or ""

            # application/json or ld+json
            if "json" in stype.lower():
                try:
                    data = json.loads(text)
                    records.append({
                        "date": date_str,
                        "source": source,
                        "type": "script_json",
                        "script_type": stype,
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except (json.JSONDecodeError, ValueError):
                    pass
                continue

            if len(text) < 50:
                continue

            # window.__DATA = {...}
            for m in re.finditer(
                r'window\[?[\'"]?(\w+)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                text, re.DOTALL
            ):
                try:
                    data = json.loads(m.group(2))
                    records.append({
                        "date": date_str,
                        "source": source,
                        "type": "embedded_window_data",
                        "var_name": m.group(1),
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass

        return records

    def extract_data_attributes(self, date_str, source, keywords=None):
        """Extract elements with data-* attributes matching keywords."""
        keywords = keywords or [
            "cote", "odd", "cheval", "horse", "runner", "race",
            "pari", "bet", "terrain", "track",
        ]
        records = []
        selector = ", ".join(f"[data-{kw}]" for kw in keywords[:8])
        try:
            elements = self.page.query_selector_all(selector)
        except Exception:
            elements = []
        seen = set()
        for el in elements:
            attrs = self.page.evaluate(
                """(el) => {
                    const result = {};
                    for (const attr of el.attributes) {
                        if (attr.name.startsWith('data-')) result[attr.name] = attr.value;
                    }
                    return result;
                }""", el
            )
            key = json.dumps(attrs, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            text = (el.inner_text() or "").strip()[:200]
            records.append({
                "date": date_str,
                "source": source,
                "type": "data_attribute",
                "tag": el.evaluate("el => el.tagName.toLowerCase()"),
                "text": text,
                "attributes": attrs,
                "scraped_at": datetime.now().isoformat(),
            })
        return records

    # ------------------------------------------------------------------
    # JSONL / checkpoint helpers
    # ------------------------------------------------------------------

    @staticmethod
    def append_jsonl(filepath, record):
        """Append one record to a JSONL file."""
        with open(filepath, "a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def load_checkpoint(self):
        """Load checkpoint from disk."""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def save_checkpoint(self, data):
        """Save checkpoint to disk."""
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def load_cache(self, key):
        """Load a cached JSON result. Returns None if not cached."""
        safe_key = re.sub(r'[^a-zA-Z0-9_-]', '_', key)[:120]
        path = os.path.join(self.cache_dir, f"{safe_key}.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def save_cache(self, key, data):
        """Save a JSON result to cache."""
        safe_key = re.sub(r'[^a-zA-Z0-9_-]', '_', key)[:120]
        path = os.path.join(self.cache_dir, f"{safe_key}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    @staticmethod
    def smart_pause(base=5.0, jitter=3.0):
        """Polite delay with random jitter and occasional long pause."""
        pause = base + random.uniform(-jitter, jitter)
        if random.random() < 0.08:
            pause += random.uniform(10, 25)
        time.sleep(max(2.0, pause))

    # ------------------------------------------------------------------
    # argparse helper (static, for subclasses to call)
    # ------------------------------------------------------------------

    @staticmethod
    def add_common_args(parser):
        """Add --start, --end, --resume, --proxy to an ArgumentParser."""
        parser.add_argument("--start", type=str, default="2022-01-01",
                            help="Start date YYYY-MM-DD")
        parser.add_argument("--end", type=str, default=None,
                            help="End date YYYY-MM-DD (default: today)")
        parser.add_argument("--resume", action="store_true", default=True,
                            help="Resume from last checkpoint")
        parser.add_argument("--proxy", type=str, default=None,
                            help="Optional proxy (e.g. http://user:pass@host:port)")
        return parser
