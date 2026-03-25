#!/usr/bin/env python3
"""
Script 146 (Playwright) -- Scraping PMU.fr web pages for data NOT in the API.

PMU.fr is a Single-Page Application (SPA). Individual course URLs like
/turf/programme/DDMMYYYY/R1/C1/ return 404 when navigated to directly.

Strategy:
  1. Navigate to the HUB page /turf/programme/{DDMMYYYY}/ and wait for SPA render
  2. Intercept API calls (XHR/fetch) the SPA makes to discover course data
  3. Extract course links from the rendered DOM
  4. Navigate to courses by clicking links or using URLs found in the DOM/API
  5. Also try known PMU internal API endpoints for structured data

Source : https://www.pmu.fr/turf/programme/{date}/
         (SPA then loads individual courses dynamically)

Collecte :
  - Ferrage / deferre details per horse
  - Avis entraineur (trainer advice)
  - Commentaire apres course (post-race analysis)
  - Poids conditions detailles
  - Oeilleres detaillees
  - Pronostics experts PMU

Join keys : date, reunion, course, numPmu  (+ partant_uid when available)
Output    : output/146_pmu_web_detail/pmu_web_detail.jsonl

Usage:
    pip install playwright
    playwright install chromium
    python 146_pmu_web_detail_scraper.py --start 2024-01-01 --end 2024-03-31
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

log = setup_logging("146_pmu_web_detail")


class PMUWebDetailScraper(PlaywrightScraperBase):
    SCRIPT_NAME = "146_pmu_web_detail"
    BASE_URL = "https://www.pmu.fr"

    # PMU programme hub: /turf/programme/{DDMMYYYY}/
    HUB_URL = BASE_URL + "/turf/programme/{date}/"

    # PMU results hub: /turf/resultats/{DDMMYYYY}/
    RESULTATS_HUB_URL = BASE_URL + "/turf/resultats/{date}/"

    # Known PMU internal API patterns (the SPA fetches these via XHR)
    PMU_API_PATTERNS = [
        "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date}",
        "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date}/R{reunion}/C{course}",
        "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date}/R{reunion}/C{course}/participants",
    ]

    # ----------------------------------------------------------------
    # Regex helpers
    # ----------------------------------------------------------------

    FERRAGE_PATTERN = re.compile(
        r'(d[eé]ferr[eé]\s+des\s+(ant[eé]rieurs|post[eé]rieurs|4\s*pieds)'
        r'|ferr[eé]'
        r'|d[eé]ferr[eé]\s+de\s+l.ant[eé]rieur\s+(droit|gauche)'
        r'|d[eé]ferr[eé]\s+du\s+post[eé]rieur\s+(droit|gauche)'
        r'|d[eé]ferr[eé]\s+des\s+ant[eé]rieurs\s+et\s+du\s+post[eé]rieur'
        r'|pieds\s+nus)',
        re.IGNORECASE,
    )

    OEILLERES_PATTERN = re.compile(
        r'(oeill[eè]res\s*(australiennes|am[eé]ricaines|normales|fran[cç]aises)?)',
        re.IGNORECASE,
    )

    # ----------------------------------------------------------------
    # API interception: capture XHR responses the SPA makes
    # ----------------------------------------------------------------

    def _setup_api_interception(self):
        """Set up response interception to capture API calls the SPA makes."""
        self._intercepted_responses = []
        self._intercepted_api_data = []

        def on_response(response):
            url = response.url
            # Capture any turfinfo API call or programme/resultats JSON
            if any(pattern in url for pattern in [
                "turfinfo.api.pmu.fr",
                "/rest/client/",
                "programme",
                "resultats",
                "participants",
            ]):
                try:
                    if "json" in (response.headers.get("content-type", "") or ""):
                        body = response.json()
                        self._intercepted_api_data.append({
                            "url": url,
                            "status": response.status,
                            "data": body,
                        })
                        log.debug("  Intercepted API: %s (status %d)", url[:120], response.status)
                except Exception:
                    pass

        self.page.on("response", on_response)

    def _clear_intercepted(self):
        """Clear intercepted data for next navigation."""
        self._intercepted_responses = []
        self._intercepted_api_data = []

    # ----------------------------------------------------------------
    # Day-level: discover reunions and courses from the programme hub
    # ----------------------------------------------------------------

    def discover_courses(self, date_pmu, date_iso):
        """Navigate to the PMU programme hub and discover R/C combos.

        PMU.fr is a SPA -- direct URLs to /R1/C1/ return 404.
        We must load the hub page first, wait for the SPA to render,
        and extract course info from the rendered DOM and API calls.

        Args:
            date_pmu: Date in DDMMYYYY format (for PMU URLs)
            date_iso: Date in YYYY-MM-DD format (for logging/keys)

        Returns:
            List of dicts with keys: reunion, course, url (if found), api_data (if any)
        """
        hub_url = self.HUB_URL.format(date=date_pmu)
        courses = []
        course_urls = {}  # (r, c) -> url
        course_set = set()

        # Clear any previous interception data
        self._clear_intercepted()

        log.info("  Navigating to hub: %s", hub_url)

        if not self.navigate(hub_url, wait_until="networkidle"):
            # Try with ISO date format as fallback
            hub_url_iso = self.HUB_URL.format(date=date_iso)
            log.info("  Hub failed with DDMMYYYY, trying ISO: %s", hub_url_iso)
            if not self.navigate(hub_url_iso, wait_until="networkidle"):
                log.warning("  Could not load hub page for %s", date_iso)
                return []

        self.accept_cookies()
        # Give extra time for SPA to fully render + API calls to complete
        time.sleep(4)

        # --- Strategy 1: Extract links from rendered DOM ---
        self._discover_from_dom_links(date_pmu, date_iso, course_set, course_urls)

        # --- Strategy 2: Extract from embedded JSON (__NEXT_DATA__, etc.) ---
        self._discover_from_embedded_json(date_pmu, date_iso, course_set, course_urls)

        # --- Strategy 3: Extract from data attributes (data-reunion, data-course) ---
        self._discover_from_data_attributes(course_set, course_urls)

        # --- Strategy 4: Extract from intercepted API responses ---
        self._discover_from_api_interceptions(course_set, course_urls)

        # --- Strategy 5: Try the known PMU turfinfo API directly ---
        if not course_set:
            self._discover_from_turfinfo_api(date_pmu, date_iso, course_set, course_urls)

        # --- Strategy 6: Extract from page text with regex ---
        if not course_set:
            self._discover_from_page_text(course_set, course_urls)

        # Build result list
        for r_num, c_num in sorted(course_set):
            entry = {
                "reunion": r_num,
                "course": c_num,
                "url": course_urls.get((r_num, c_num)),
            }
            courses.append(entry)

        log.info("  %s: %d courses discovered", date_iso, len(courses))
        return courses

    def _discover_from_dom_links(self, date_pmu, date_iso, course_set, course_urls):
        """Extract reunion/course combos from <a href> in the rendered DOM."""
        try:
            links = self.page.query_selector_all("a[href]")
        except Exception:
            links = []

        for link in links:
            href = link.get_attribute("href") or ""
            # Match patterns like /turf/programme/25032025/R1/C1/
            # or /turf/course/... or any R{n}/C{n} pattern
            for pattern in [
                r'/turf/(?:programme|course|resultats)/[^/]+/R(\d+)/C(\d+)',
                r'/R(\d+)/C(\d+)',
            ]:
                m = re.search(pattern, href)
                if m:
                    r_num = int(m.group(1))
                    c_num = int(m.group(2))
                    course_set.add((r_num, c_num))
                    # Build absolute URL
                    if href.startswith("/"):
                        full_url = self.BASE_URL + href
                    elif href.startswith("http"):
                        full_url = href
                    else:
                        full_url = None
                    if full_url and (r_num, c_num) not in course_urls:
                        course_urls[(r_num, c_num)] = full_url
                    break

        if course_set:
            log.info("    DOM links: found %d courses", len(course_set))

    def _discover_from_embedded_json(self, date_pmu, date_iso, course_set, course_urls):
        """Extract reunion/course combos from embedded JSON in script tags."""
        try:
            scripts = self.page.query_selector_all("script")
        except Exception:
            return

        for script in scripts:
            try:
                text = script.inner_text() or ""
            except Exception:
                continue

            if len(text) < 50:
                continue

            stype = (script.get_attribute("type") or "").lower()

            # __NEXT_DATA__ (Next.js)
            if script.get_attribute("id") == "__NEXT_DATA__" or "__NEXT_DATA__" in text:
                try:
                    # For id="__NEXT_DATA__", text is pure JSON
                    data = json.loads(text)
                    self._extract_courses_from_json(data, course_set, course_urls)
                except (json.JSONDecodeError, ValueError):
                    pass
                continue

            # application/json blocks
            if "json" in stype:
                try:
                    data = json.loads(text)
                    self._extract_courses_from_json(data, course_set, course_urls)
                except (json.JSONDecodeError, ValueError):
                    pass
                continue

            # window.__DATA__, window.__INITIAL_STATE__, etc.
            for var_match in re.finditer(
                r'window\[?[\'"]?(__\w+__|\w+State\w*|\w+Data\w*|\w+Config\w*)[\'"]?\]?'
                r'\s*=\s*(\{.+?\});',
                text,
                re.DOTALL,
            ):
                try:
                    data = json.loads(var_match.group(2))
                    self._extract_courses_from_json(data, course_set, course_urls)
                except json.JSONDecodeError:
                    pass

            # Also look for inline JSON with numReunion/numOrdre
            for match in re.finditer(
                r'"numReunion"\s*:\s*(\d+).*?"numOrdre"\s*:\s*(\d+)',
                text,
            ):
                r_num = int(match.group(1))
                c_num = int(match.group(2))
                course_set.add((r_num, c_num))

        if course_set:
            log.info("    Embedded JSON: found %d courses total", len(course_set))

    def _extract_courses_from_json(self, data, course_set, course_urls):
        """Recursively walk JSON data looking for reunion/course info."""
        if isinstance(data, dict):
            # Look for direct reunion/course fields
            r_num = data.get("numReunion") or data.get("reunion") or data.get("numR")
            c_num = data.get("numOrdre") or data.get("numCourse") or data.get("course") or data.get("numC")
            if r_num is not None and c_num is not None:
                try:
                    r_num = int(r_num)
                    c_num = int(c_num)
                    if 1 <= r_num <= 20 and 1 <= c_num <= 20:
                        course_set.add((r_num, c_num))
                except (ValueError, TypeError):
                    pass

            # Look for "reunions" or "courses" arrays
            for key in ("reunions", "courses", "programme", "races",
                        "data", "props", "pageProps", "results"):
                if key in data:
                    self._extract_courses_from_json(data[key], course_set, course_urls)

            # Recurse into all dict values
            for key, val in data.items():
                if isinstance(val, (dict, list)):
                    self._extract_courses_from_json(val, course_set, course_urls)

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self._extract_courses_from_json(item, course_set, course_urls)

    def _discover_from_data_attributes(self, course_set, course_urls):
        """Extract from elements with data-reunion/data-course attributes."""
        count_before = len(course_set)
        for attr_pattern in [
            "[data-reunion]",
            "[data-num-reunion]",
            "[data-numreunion]",
        ]:
            try:
                els = self.page.query_selector_all(attr_pattern)
            except Exception:
                continue
            for el in els:
                r_val = (
                    el.get_attribute("data-reunion")
                    or el.get_attribute("data-num-reunion")
                    or el.get_attribute("data-numreunion")
                    or ""
                )
                c_val = (
                    el.get_attribute("data-course")
                    or el.get_attribute("data-num-course")
                    or el.get_attribute("data-numcourse")
                    or el.get_attribute("data-numordre")
                    or ""
                )
                try:
                    r_num = int(r_val)
                    c_num = int(c_val)
                    if 1 <= r_num <= 20 and 1 <= c_num <= 20:
                        course_set.add((r_num, c_num))
                except (ValueError, TypeError):
                    pass

        added = len(course_set) - count_before
        if added:
            log.info("    Data attributes: found %d new courses", added)

    def _discover_from_api_interceptions(self, course_set, course_urls):
        """Extract courses from intercepted API responses."""
        count_before = len(course_set)
        for resp in self._intercepted_api_data:
            data = resp.get("data")
            if data:
                self._extract_courses_from_json(data, course_set, course_urls)

        added = len(course_set) - count_before
        if added:
            log.info("    API interceptions: found %d new courses", added)

    def _discover_from_turfinfo_api(self, date_pmu, date_iso, course_set, course_urls):
        """Try the known PMU turfinfo REST API directly."""
        api_url = f"https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date_pmu}"
        log.info("    Trying turfinfo API: %s", api_url)

        try:
            resp = self.page.request.get(api_url, timeout=15000)
            if resp.ok:
                data = resp.json()
                self._extract_courses_from_json(data, course_set, course_urls)
                if course_set:
                    log.info("    Turfinfo API: found %d courses", len(course_set))
        except Exception as exc:
            log.debug("    Turfinfo API failed: %s", str(exc)[:100])

        # Try alternate date format
        if not course_set:
            api_url_iso = f"https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date_iso}"
            try:
                resp = self.page.request.get(api_url_iso, timeout=15000)
                if resp.ok:
                    data = resp.json()
                    self._extract_courses_from_json(data, course_set, course_urls)
            except Exception:
                pass

    def _discover_from_page_text(self, course_set, course_urls):
        """Last resort: extract R/C combos from visible page text."""
        try:
            body_text = self.page.inner_text("body") or ""
        except Exception:
            return

        # Match patterns like "R1 C1", "R1C2", "Reunion 1 Course 3"
        for m in re.finditer(r'R(\d+)\s*[/-]?\s*C(\d+)', body_text):
            try:
                r_num = int(m.group(1))
                c_num = int(m.group(2))
                if 1 <= r_num <= 20 and 1 <= c_num <= 20:
                    course_set.add((r_num, c_num))
            except (ValueError, TypeError):
                pass

        if course_set:
            log.info("    Page text regex: found %d courses", len(course_set))

    # ----------------------------------------------------------------
    # Navigate to a specific course page via the SPA
    # ----------------------------------------------------------------

    def _navigate_to_course(self, date_pmu, reunion, course, course_url=None):
        """Navigate to a specific course page within the SPA.

        Strategy:
          1. If we have a direct URL from discovery, try navigating to it
          2. If that fails, go to the hub and click the course link
          3. If that fails, try constructing common URL patterns

        Returns True if we successfully loaded a page with content.
        """
        # Strategy 1: Use the discovered URL
        if course_url:
            log.debug("    Navigating to discovered URL: %s", course_url[:120])
            if self.navigate(course_url, wait_until="networkidle"):
                time.sleep(2)
                body = self.page.inner_text("body") or ""
                if len(body) > 300 and "404" not in body[:200]:
                    return True
                log.debug("    Discovered URL had no content, trying click strategy")

        # Strategy 2: Navigate to hub, then click the course link
        hub_url = self.HUB_URL.format(date=date_pmu)
        current_url = self.page.url or ""
        # Only navigate to hub if we're not already there
        if date_pmu not in current_url or "programme" not in current_url:
            if not self.navigate(hub_url, wait_until="networkidle"):
                return False
            self.accept_cookies()
            time.sleep(3)

        # Try clicking a link that contains R{reunion}/C{course} or similar
        click_selectors = [
            f"a[href*='R{reunion}/C{course}']",
            f"a[href*='R{reunion}'][href*='C{course}']",
            f"[data-reunion='{reunion}'][data-course='{course}']",
            f"[data-numreunion='{reunion}'][data-numcourse='{course}']",
        ]
        for sel in click_selectors:
            try:
                el = self.page.query_selector(sel)
                if el:
                    el.click()
                    self.page.wait_for_load_state("networkidle", timeout=15000)
                    time.sleep(2)
                    body = self.page.inner_text("body") or ""
                    if len(body) > 300 and "404" not in body[:200]:
                        log.debug("    Clicked into course R%d C%d", reunion, course)
                        return True
            except Exception as exc:
                log.debug("    Click failed for %s: %s", sel, str(exc)[:80])

        # Strategy 3: Try common URL patterns
        url_patterns = [
            f"{self.BASE_URL}/turf/programme/{date_pmu}/R{reunion}/C{course}/",
            f"{self.BASE_URL}/turf/course/{date_pmu}/R{reunion}/C{course}/",
        ]
        for url in url_patterns:
            log.debug("    Trying URL pattern: %s", url[:120])
            if self.navigate(url, wait_until="networkidle", retries=1):
                time.sleep(2)
                body = self.page.inner_text("body") or ""
                if len(body) > 300 and "404" not in body[:200]:
                    return True

        log.debug("    Could not navigate to R%d C%d", reunion, course)
        return False

    # ----------------------------------------------------------------
    # Programme page scraping (pre-race data)
    # ----------------------------------------------------------------

    def scrape_programme(self, date_pmu, date_iso, reunion, course, course_url=None):
        """Scrape the programme page for a single course.

        Instead of navigating directly to /R1/C1/ (which returns 404 on the SPA),
        we navigate through the hub or use discovered URLs.

        Returns a list of records (one per partant + one course-level).
        """
        cache_key = f"prog_{date_iso}_R{reunion}_C{course}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        # Clear intercepted data for this course
        self._clear_intercepted()

        if not self._navigate_to_course(date_pmu, reunion, course, course_url):
            self.screenshot_on_error(f"pmu_prog_{date_iso}_R{reunion}_C{course}")
            self.save_cache(cache_key, [])
            return []

        actual_url = self.page.url

        body_text = self.page.inner_text("body") or ""
        if len(body_text) < 300 or "404" in body_text[:150]:
            log.debug("  No content for programme %s R%d C%d", date_iso, reunion, course)
            self.save_cache(cache_key, [])
            return []

        records = []
        base_keys = {
            "date": date_pmu,
            "date_iso": date_iso,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": actual_url,
        }

        # --- 1. Course-level info ---
        course_rec = {
            **base_keys,
            "type": "course_info",
            "scraped_at": datetime.now().isoformat(),
        }

        # Course name
        h1 = self.page.query_selector("h1")
        if h1:
            course_rec["nom_prix"] = (h1.inner_text() or "").strip()

        # Conditions text
        for sel in [
            "[class*='condition']", "[class*='Condition']",
            "[class*='info-course']", "[class*='infoCourse']",
            "[class*='race-info']", "[class*='raceInfo']",
        ]:
            els = self.page.query_selector_all(sel)
            for el in els:
                text = (el.inner_text() or "").strip()
                if text and len(text) > 10:
                    course_rec["conditions_text"] = text[:2000]
                    break
            if "conditions_text" in course_rec:
                break

        # Pronostics experts
        pronostics = self._extract_pronostics()
        if pronostics:
            course_rec["pronostics_experts"] = pronostics

        records.append(course_rec)

        # --- 2. Partant-level data from DOM ---
        partant_records = self._extract_partants_programme(
            date_pmu, date_iso, reunion, course, actual_url,
        )
        records.extend(partant_records)

        # --- 3. Embedded JSON ---
        embedded = self._extract_pmu_embedded_json(date_iso, reunion, course)
        records.extend(embedded)

        # --- 4. Intercepted API data for this course ---
        api_records = self._extract_intercepted_api_data(date_iso, reunion, course)
        records.extend(api_records)

        # --- 5. Try turfinfo API directly for this course ---
        api_direct = self._fetch_turfinfo_course(date_pmu, date_iso, reunion, course)
        records.extend(api_direct)

        self.save_cache(cache_key, records)
        return records

    # ----------------------------------------------------------------
    # Results page scraping (post-race data)
    # ----------------------------------------------------------------

    def scrape_resultats(self, date_pmu, date_iso, reunion, course):
        """Scrape the results page for a single course.

        Returns a list of records with commentaires, incidents, temps.
        """
        cache_key = f"res_{date_iso}_R{reunion}_C{course}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        self._clear_intercepted()

        # Try the results hub page approach
        results_hub = self.RESULTATS_HUB_URL.format(date=date_pmu)

        # First try direct navigation (results pages may work differently)
        result_url = f"{self.BASE_URL}/turf/resultats/{date_pmu}/R{reunion}/C{course}/"
        loaded = False

        if self.navigate(result_url, wait_until="networkidle", retries=1):
            time.sleep(2)
            body = self.page.inner_text("body") or ""
            if len(body) > 300 and "404" not in body[:200]:
                loaded = True

        # If direct URL failed, try via results hub
        if not loaded:
            if self.navigate(results_hub, wait_until="networkidle", retries=1):
                self.accept_cookies()
                time.sleep(3)
                # Try clicking into the specific course
                click_selectors = [
                    f"a[href*='R{reunion}/C{course}']",
                    f"a[href*='R{reunion}'][href*='C{course}']",
                ]
                for sel in click_selectors:
                    try:
                        el = self.page.query_selector(sel)
                        if el:
                            el.click()
                            self.page.wait_for_load_state("networkidle", timeout=15000)
                            time.sleep(2)
                            body = self.page.inner_text("body") or ""
                            if len(body) > 300 and "404" not in body[:200]:
                                loaded = True
                                break
                    except Exception:
                        continue

        if not loaded:
            log.debug("  No results page for %s R%d C%d", date_iso, reunion, course)
            self.save_cache(cache_key, [])
            return []

        actual_url = self.page.url
        body_text = self.page.inner_text("body") or ""
        if len(body_text) < 300 or "404" in body_text[:150]:
            self.save_cache(cache_key, [])
            return []

        records = []
        base_keys = {
            "date": date_pmu,
            "date_iso": date_iso,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": actual_url,
        }

        # --- Commentaire apres course ---
        for sel in [
            "[class*='comment']", "[class*='Comment']",
            "[class*='analyse']", "[class*='Analyse']",
            "[class*='recap']", "[class*='Recap']",
            "[class*='resume']", "[class*='Resume']",
            "[class*='editorial']",
        ]:
            els = self.page.query_selector_all(sel)
            for el in els:
                text = (el.inner_text() or "").strip()
                if text and 30 < len(text) < 5000:
                    records.append({
                        **base_keys,
                        "type": "commentaire_apres_course",
                        "contenu": text[:3000],
                        "scraped_at": datetime.now().isoformat(),
                    })

        # --- Incidents ---
        for sel in [
            "[class*='incident']", "[class*='Incident']",
            "[class*='reclamation']", "[class*='Reclamation']",
        ]:
            els = self.page.query_selector_all(sel)
            for el in els:
                text = (el.inner_text() or "").strip()
                if text and len(text) > 5:
                    records.append({
                        **base_keys,
                        "type": "incident",
                        "contenu": text[:1000],
                        "scraped_at": datetime.now().isoformat(),
                    })

        # --- Temps de course ---
        for sel in [
            "[class*='temps']", "[class*='Temps']",
            "[class*='time']", "[class*='chrono']",
        ]:
            els = self.page.query_selector_all(sel)
            for el in els:
                text = (el.inner_text() or "").strip()
                if text and re.search(r"\d+[':]\d{2}", text):
                    records.append({
                        **base_keys,
                        "type": "temps_course",
                        "contenu": text[:200],
                        "scraped_at": datetime.now().isoformat(),
                    })

        # --- Result partants ---
        result_partants = self._extract_partants_resultats(
            date_pmu, date_iso, reunion, course, actual_url,
        )
        records.extend(result_partants)

        # --- Embedded JSON from results page ---
        embedded = self._extract_pmu_embedded_json(date_iso, reunion, course)
        records.extend(embedded)

        # --- Intercepted API data ---
        api_records = self._extract_intercepted_api_data(date_iso, reunion, course)
        records.extend(api_records)

        self.save_cache(cache_key, records)
        return records

    # ----------------------------------------------------------------
    # Partant extraction: programme page
    # ----------------------------------------------------------------

    def _extract_partants_programme(self, date_pmu, date_iso, reunion, course, url):
        """Extract per-horse details from the programme page."""
        records = []
        base_keys = {
            "date": date_pmu,
            "date_iso": date_iso,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": url,
        }

        partant_selectors = [
            "[class*='partant']", "[class*='Partant']",
            "[class*='runner']", "[class*='Runner']",
            "[class*='horse']", "[class*='Horse']",
            "[class*='participant']",
            "tr[data-num]", "tr[data-numPmu]",
            "[data-numPmu]", "[data-num-pmu]",
        ]

        partant_els = []
        for sel in partant_selectors:
            partant_els = self.page.query_selector_all(sel)
            if partant_els:
                break

        for el in partant_els:
            rec = {
                **base_keys,
                "type": "partant_detail",
                "scraped_at": datetime.now().isoformat(),
            }

            # Extract numPmu
            num_pmu = (
                el.get_attribute("data-numPmu")
                or el.get_attribute("data-num-pmu")
                or el.get_attribute("data-num")
                or ""
            )
            if not num_pmu:
                num_el = el.query_selector(
                    "[class*='numero'], [class*='num'], .numPmu, .num"
                )
                if num_el:
                    num_pmu = (num_el.inner_text() or "").strip()
            rec["numPmu"] = num_pmu

            # Horse name
            name_el = el.query_selector(
                "[class*='nom'], [class*='name'], [class*='cheval'], "
                "[class*='horse'], h3, h4, strong"
            )
            if name_el:
                rec["nom_cheval"] = (name_el.inner_text() or "").strip()

            # Build partant_uid
            if num_pmu:
                rec["partant_uid"] = (
                    f"{date_iso}_R{reunion}_C{course}_N{num_pmu}"
                )

            full_text = (el.inner_text() or "").strip()

            # --- Ferrage ---
            ferrage_el = el.query_selector(
                "[class*='ferrage'], [class*='Ferrage'], "
                "[class*='deferre'], [class*='Deferre'], "
                "[class*='shoe'], [class*='Shoe']"
            )
            if ferrage_el:
                rec["ferrage"] = (ferrage_el.inner_text() or "").strip()
            else:
                fm = self.FERRAGE_PATTERN.search(full_text)
                if fm:
                    rec["ferrage"] = fm.group(0).strip()

            # --- Oeilleres ---
            oeil_el = el.query_selector(
                "[class*='oeillere'], [class*='Oeillere'], "
                "[class*='blinker'], [class*='Blinker']"
            )
            if oeil_el:
                rec["oeilleres"] = (oeil_el.inner_text() or "").strip()
            else:
                om = self.OEILLERES_PATTERN.search(full_text)
                if om:
                    rec["oeilleres"] = om.group(0).strip()

            # --- Poids / Handicap ---
            poids_el = el.query_selector(
                "[class*='poids'], [class*='Poids'], "
                "[class*='weight'], [class*='Weight'], "
                "[class*='handicap'], [class*='Handicap']"
            )
            if poids_el:
                rec["poids"] = (poids_el.inner_text() or "").strip()

            # --- Avis entraineur ---
            avis_el = el.query_selector(
                "[class*='avis'], [class*='Avis'], "
                "[class*='advice'], [class*='trainer-comment'], "
                "[class*='entraineur'], [class*='Entraineur']"
            )
            if avis_el:
                rec["avis_entraineur"] = (avis_el.inner_text() or "").strip()

            # --- Jockey / Driver ---
            jockey_el = el.query_selector(
                "[class*='jockey'], [class*='Jockey'], "
                "[class*='driver'], [class*='Driver']"
            )
            if jockey_el:
                rec["jockey"] = (jockey_el.inner_text() or "").strip()

            # --- Entraineur ---
            trainer_el = el.query_selector(
                "[class*='entraineur'], [class*='Entraineur'], "
                "[class*='trainer'], [class*='Trainer']"
            )
            if trainer_el:
                rec["entraineur"] = (trainer_el.inner_text() or "").strip()

            # --- Cote probable ---
            cote_el = el.query_selector(
                "[class*='cote'], [class*='Cote'], "
                "[class*='odds'], [class*='Odds']"
            )
            if cote_el:
                rec["cote_probable"] = (cote_el.inner_text() or "").strip()

            if full_text and len(full_text) > 10:
                rec["raw_text"] = full_text[:2000]

            records.append(rec)

        # Fallback: table extraction
        if not records:
            table_records = self.extract_tables(
                date_iso, "pmu_web", "partant_table_row",
            )
            for rec in table_records:
                rec["reunion"] = reunion
                rec["course"] = course
                rec["url"] = url
            records.extend(table_records)

        return records

    # ----------------------------------------------------------------
    # Partant extraction: results page
    # ----------------------------------------------------------------

    def _extract_partants_resultats(self, date_pmu, date_iso, reunion, course, url):
        """Extract per-horse results (classement, ecarts, etc.)."""
        records = []
        base_keys = {
            "date": date_pmu,
            "date_iso": date_iso,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": url,
        }

        result_selectors = [
            "[class*='classement']", "[class*='Classement']",
            "[class*='result']", "[class*='Result']",
            "[class*='arrivee']", "[class*='Arrivee']",
        ]

        container = None
        for sel in result_selectors:
            container = self.page.query_selector(sel)
            if container:
                break

        if not container:
            table_records = self.extract_tables(
                date_iso, "pmu_web", "resultat_table_row",
            )
            for rec in table_records:
                rec["reunion"] = reunion
                rec["course"] = course
                rec["url"] = url
            return table_records

        rows = container.query_selector_all(
            "[class*='partant'], [class*='runner'], tr, [class*='ligne']"
        )
        for row in rows:
            text = (row.inner_text() or "").strip()
            if not text or len(text) < 3:
                continue

            rec = {
                **base_keys,
                "type": "resultat_partant",
                "scraped_at": datetime.now().isoformat(),
            }

            num_el = row.query_selector(
                "[class*='numero'], [class*='num'], .numPmu"
            )
            if num_el:
                rec["numPmu"] = (num_el.inner_text() or "").strip()

            place_el = row.query_selector(
                "[class*='place'], [class*='rang'], [class*='classement']"
            )
            if place_el:
                rec["place"] = (place_el.inner_text() or "").strip()

            ecart_el = row.query_selector(
                "[class*='ecart'], [class*='Ecart'], [class*='distance']"
            )
            if ecart_el:
                rec["ecart"] = (ecart_el.inner_text() or "").strip()

            if rec.get("numPmu"):
                rec["partant_uid"] = (
                    f"{date_iso}_R{reunion}_C{course}_N{rec['numPmu']}"
                )

            rec["raw_text"] = text[:1000]
            records.append(rec)

        return records

    # ----------------------------------------------------------------
    # Pronostics experts
    # ----------------------------------------------------------------

    def _extract_pronostics(self):
        """Extract PMU expert pronostics from the current page."""
        pronostics = []

        prono_selectors = [
            "[class*='pronostic']", "[class*='Pronostic']",
            "[class*='prono']", "[class*='Prono']",
            "[class*='expert']", "[class*='Expert']",
            "[class*='prediction']",
        ]

        for sel in prono_selectors:
            els = self.page.query_selector_all(sel)
            for el in els:
                text = (el.inner_text() or "").strip()
                if text and 5 < len(text) < 2000:
                    pronostics.append(text)

        return pronostics if pronostics else None

    # ----------------------------------------------------------------
    # Embedded JSON extraction (PMU-specific)
    # ----------------------------------------------------------------

    def _extract_pmu_embedded_json(self, date_iso, reunion, course):
        """Extract PMU-specific structured data from script tags."""
        records = []
        scripts = self.page.query_selector_all("script")

        for script in scripts:
            stype = script.get_attribute("type") or ""
            text = script.inner_text() or ""

            if len(text) < 50:
                continue

            # application/json or ld+json blocks
            if "json" in stype.lower():
                try:
                    data = json.loads(text)
                    records.append({
                        "date_iso": date_iso,
                        "reunion": reunion,
                        "course": course,
                        "source": "pmu_web",
                        "type": "embedded_json",
                        "script_type": stype,
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except (json.JSONDecodeError, ValueError):
                    pass
                continue

            # window.__INITIAL_STATE__ or similar
            for var_match in re.finditer(
                r'window\[?[\'"]?(__\w+__|\w+State\w*|\w+Data\w*)[\'"]?\]?'
                r'\s*=\s*(\{.+?\});',
                text,
                re.DOTALL,
            ):
                try:
                    data = json.loads(var_match.group(2))
                    records.append({
                        "date_iso": date_iso,
                        "reunion": reunion,
                        "course": course,
                        "source": "pmu_web",
                        "type": "embedded_window_data",
                        "var_name": var_match.group(1),
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass

            # Look for ferrage data in script text
            if re.search(r'ferr|deferre|oeill', text, re.IGNORECASE):
                for json_match in re.finditer(r'(\[.*?\]|\{.*?\})', text, re.DOTALL):
                    blob = json_match.group(1)
                    if len(blob) > 50 and re.search(
                        r'ferr|deferre|oeill', blob, re.IGNORECASE
                    ):
                        try:
                            data = json.loads(blob)
                            records.append({
                                "date_iso": date_iso,
                                "reunion": reunion,
                                "course": course,
                                "source": "pmu_web",
                                "type": "embedded_ferrage_data",
                                "data": data,
                                "scraped_at": datetime.now().isoformat(),
                            })
                        except json.JSONDecodeError:
                            pass

        return records

    # ----------------------------------------------------------------
    # Intercepted API data extraction
    # ----------------------------------------------------------------

    def _extract_intercepted_api_data(self, date_iso, reunion, course):
        """Convert intercepted API responses into records."""
        records = []
        for resp in self._intercepted_api_data:
            data = resp.get("data")
            if not data:
                continue
            records.append({
                "date_iso": date_iso,
                "reunion": reunion,
                "course": course,
                "source": "pmu_web_api_intercept",
                "type": "api_response",
                "api_url": resp.get("url", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        return records

    # ----------------------------------------------------------------
    # Direct turfinfo API fetch for a course
    # ----------------------------------------------------------------

    def _fetch_turfinfo_course(self, date_pmu, date_iso, reunion, course):
        """Try fetching structured data from the turfinfo REST API."""
        records = []

        endpoints = [
            f"https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date_pmu}/R{reunion}/C{course}",
            f"https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date_pmu}/R{reunion}/C{course}/participants",
        ]

        for api_url in endpoints:
            try:
                resp = self.page.request.get(api_url, timeout=10000)
                if resp.ok:
                    data = resp.json()
                    records.append({
                        "date_iso": date_iso,
                        "reunion": reunion,
                        "course": course,
                        "source": "pmu_turfinfo_api",
                        "type": "api_direct",
                        "api_url": api_url,
                        "data": data,
                        "scraped_at": datetime.now().isoformat(),
                    })
                    log.debug("    Turfinfo API OK: %s", api_url[:100])
            except Exception as exc:
                log.debug("    Turfinfo API error: %s -- %s", api_url[:80], str(exc)[:80])

        return records

    # ----------------------------------------------------------------
    # Main loop
    # ----------------------------------------------------------------

    def run(self):
        start_date = datetime.strptime(self.args.start, "%Y-%m-%d")
        end_date = (
            datetime.strptime(self.args.end, "%Y-%m-%d")
            if self.args.end
            else datetime.now()
        )

        log.info("=" * 60)
        log.info("SCRIPT 146 -- PMU Web Detail Scraper (SPA-aware)")
        log.info("  Periode : %s -> %s", start_date.date(), end_date.date())
        log.info("=" * 60)

        # Checkpoint / resume
        checkpoint = self.load_checkpoint()
        if self.args.resume and checkpoint.get("last_date"):
            resume_dt = (
                datetime.strptime(checkpoint["last_date"], "%Y-%m-%d")
                + timedelta(days=1)
            )
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("  Reprise au checkpoint : %s", start_date.date())

        output_file = os.path.join(self.output_dir, "pmu_web_detail.jsonl")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        self.launch_browser()

        try:
            # Set up API interception
            self._setup_api_interception()

            current = start_date
            day_count = 0
            total_records = 0

            while current <= end_date:
                date_iso = current.strftime("%Y-%m-%d")
                date_pmu = current.strftime("%d%m%Y")

                log.info("  Scraping %s ...", date_iso)

                # Discover R/C combos for this date via the hub page
                courses = self.discover_courses(date_pmu, date_iso)

                for course_info in courses:
                    r_num = course_info["reunion"]
                    c_num = course_info["course"]
                    course_url = course_info.get("url")

                    # Programme page
                    prog_records = self.scrape_programme(
                        date_pmu, date_iso, r_num, c_num, course_url,
                    )
                    for rec in prog_records:
                        rec.setdefault("date_iso", date_iso)
                        self.append_jsonl(output_file, rec)
                        total_records += 1

                    self.smart_pause(4.0, 2.0)

                    # Results page
                    res_records = self.scrape_resultats(
                        date_pmu, date_iso, r_num, c_num,
                    )
                    for rec in res_records:
                        rec.setdefault("date_iso", date_iso)
                        self.append_jsonl(output_file, rec)
                        total_records += 1

                    self.smart_pause(4.0, 2.0)

                day_count += 1

                # Periodic logging + checkpoint
                if day_count % 10 == 0:
                    log.info(
                        "  %s | jours=%d records=%d",
                        date_iso, day_count, total_records,
                    )
                    self.save_checkpoint({
                        "last_date": date_iso,
                        "total_records": total_records,
                    })

                # Browser rotation
                if day_count % 50 == 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(8.0, 4.0)
                    self.launch_browser()
                    self._setup_api_interception()

                current += timedelta(days=1)
                self.smart_pause(3.0, 1.5)

            # Final checkpoint
            self.save_checkpoint({
                "last_date": end_date.strftime("%Y-%m-%d"),
                "total_records": total_records,
                "status": "done",
            })

            log.info("=" * 60)
            log.info(
                "TERMINE: %d jours, %d records -> %s",
                day_count, total_records, output_file,
            )
            log.info("=" * 60)

        finally:
            self.close_browser()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Script 146 -- PMU.fr Web Detail Scraper (SPA-aware) "
            "(ferrage, avis, commentaires, oeilleres, pronostics)"
        ),
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = PMUWebDetailScraper(args)
    scraper.run()


if __name__ == "__main__":
    main()
