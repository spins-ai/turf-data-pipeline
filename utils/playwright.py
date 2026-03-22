#!/usr/bin/env python3
"""
Shared Playwright helper functions for headless-browser scrapers.

Centralises the browser launch, navigation-with-retry, and cookie-consent
logic that is duplicated across 13+ Playwright scraper files.

Usage::

    from playwright.sync_api import sync_playwright
    from utils.playwright import launch_browser, navigate_with_retry, accept_cookies

    with sync_playwright() as pw:
        browser, context, page = launch_browser(pw)
        navigate_with_retry(page, "https://example.com")
        accept_cookies(page)
        # ... scrape ...
        browser.close()

Requirements:
    pip install playwright
    playwright install chromium
"""

import logging
import time

from playwright.sync_api import Playwright, Browser, BrowserContext, Page
from playwright.sync_api import TimeoutError as PlaywrightTimeout

log = logging.getLogger(__name__)

# ======================================================================
# Common cookie-consent selectors (FR + EN sites)
# ======================================================================

COOKIE_SELECTORS: list[str] = [
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

# Stealth init-script injected into every browser context
_STEALTH_SCRIPT: str = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    window.chrome = {runtime: {}};
"""

# Default user-agent used for all browser contexts
DEFAULT_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ======================================================================
# launch_browser
# ======================================================================

def launch_browser(
    pw: Playwright,
    *,
    locale: str = "fr-FR",
    timezone: str = "Europe/Paris",
    headless: bool = True,
    proxy: str | None = None,
    user_agent: str | None = None,
    viewport_width: int = 1920,
    viewport_height: int = 1080,
    default_timeout_ms: int = 60_000,
) -> tuple[Browser, BrowserContext, Page]:
    """Launch headless Chromium with stealth settings.

    Parameters
    ----------
    pw : Playwright
        The Playwright instance obtained from ``sync_playwright().start()``
        or used inside a ``with sync_playwright() as pw:`` block.
    locale : str
        Browser locale (e.g. ``"fr-FR"``).
    timezone : str
        IANA timezone id (e.g. ``"Europe/Paris"``).
    headless : bool
        Run browser in headless mode (default ``True``).
    proxy : str | None
        Optional proxy URL, e.g. ``"http://user:pass@host:port"``.
    user_agent : str | None
        Custom user-agent string. Falls back to a recent Chrome UA.
    viewport_width : int
        Browser viewport width in pixels.
    viewport_height : int
        Browser viewport height in pixels.
    default_timeout_ms : int
        Default timeout for all Playwright actions on the page (ms).

    Returns
    -------
    tuple[Browser, BrowserContext, Page]
        A ready-to-use ``(browser, context, page)`` triple.
    """
    launch_args: dict = {
        "headless": headless,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    }
    if proxy:
        launch_args["proxy"] = {"server": proxy}

    browser = pw.chromium.launch(**launch_args)

    context = browser.new_context(
        viewport={"width": viewport_width, "height": viewport_height},
        locale=locale,
        timezone_id=timezone,
        user_agent=user_agent or DEFAULT_USER_AGENT,
        java_script_enabled=True,
        ignore_https_errors=True,
    )
    context.add_init_script(_STEALTH_SCRIPT)

    page = context.new_page()
    page.set_default_timeout(default_timeout_ms)

    log.info("Browser launched (headless=%s, locale=%s, tz=%s)", headless, locale, timezone)
    return browser, context, page


# ======================================================================
# navigate_with_retry
# ======================================================================

def navigate_with_retry(
    page: Page,
    url: str,
    *,
    max_retries: int = 3,
    timeout: int = 30_000,
    wait_until: str = "networkidle",
) -> bool:
    """Navigate to *url* with automatic retry on failure.

    Handles HTTP error codes (429 rate-limit, 403 blocked, etc.) and
    Playwright timeouts by backing off and retrying.

    Parameters
    ----------
    page : Page
        The Playwright page to navigate.
    url : str
        Target URL.
    max_retries : int
        Maximum number of attempts (default 3).
    timeout : int
        Navigation timeout in milliseconds (default 30 000).
    wait_until : str
        Playwright load-state to wait for (``"networkidle"``, ``"load"``,
        ``"domcontentloaded"``, ``"commit"``).

    Returns
    -------
    bool
        ``True`` if the page loaded successfully, ``False`` if all retries
        were exhausted.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = page.goto(url, wait_until=wait_until, timeout=timeout)
            if resp and resp.status >= 400:
                log.warning(
                    "HTTP %d on %s (attempt %d/%d)",
                    resp.status, url, attempt, max_retries,
                )
                if resp.status == 429:
                    time.sleep(60 * attempt)
                elif resp.status == 403:
                    time.sleep(30 * attempt)
                else:
                    time.sleep(5 * attempt)
                continue
            # Extra wait for JS rendering
            page.wait_for_load_state("domcontentloaded")
            time.sleep(1.5)
            return True
        except PlaywrightTimeout:
            log.warning("Timeout on %s (attempt %d/%d)", url, attempt, max_retries)
            time.sleep(10 * attempt)
        except Exception as exc:
            log.warning(
                "Navigation error: %s (attempt %d/%d)",
                str(exc)[:200], attempt, max_retries,
            )
            time.sleep(5 * attempt)

    log.error("Failed after %d retries: %s", max_retries, url)
    return False


# ======================================================================
# accept_cookies
# ======================================================================

def accept_cookies(
    page: Page,
    *,
    selectors: list[str] | None = None,
    visible_timeout: int = 1500,
    click_timeout: int = 3000,
) -> bool:
    """Dismiss a cookie-consent banner if one is visible.

    Iterates over a list of common cookie-accept button selectors and
    clicks the first one that is visible.

    Parameters
    ----------
    page : Page
        The Playwright page.
    selectors : list[str] | None
        Custom list of CSS / Playwright selectors to try. Falls back to
        the built-in :data:`COOKIE_SELECTORS` list.
    visible_timeout : int
        Timeout (ms) when checking whether a button is visible.
    click_timeout : int
        Timeout (ms) for the click action.

    Returns
    -------
    bool
        ``True`` if a cookie button was found and clicked, ``False``
        otherwise.
    """
    for sel in (selectors or COOKIE_SELECTORS):
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=visible_timeout):
                btn.click(timeout=click_timeout)
                log.info("Cookies accepted via: %s", sel)
                time.sleep(1)
                return True
        except Exception:
            continue
    return False
