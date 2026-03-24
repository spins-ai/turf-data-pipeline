#!/usr/bin/env python3
"""
Script 146 (Playwright) -- Scraping PMU.fr web pages for data NOT in the API.

Source : https://www.pmu.fr/turf/programme/{date}/R{reunion}/C{course}/
         https://www.pmu.fr/turf/resultats/{date}/R{reunion}/C{course}/

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

    # PMU programme page: /turf/programme/{date}/R{reunion}/C{course}/
    PROGRAMME_URL = BASE_URL + "/turf/programme/{date}/R{reunion}/C{course}/"
    # PMU results page: /turf/resultats/{date}/R{reunion}/C{course}/
    RESULTATS_URL = BASE_URL + "/turf/resultats/{date}/R{reunion}/C{course}/"

    # Max reunions and courses per day (PMU rarely exceeds these)
    MAX_REUNIONS = 10
    MAX_COURSES = 12

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
    # Day-level: discover reunions and courses from the programme hub
    # ----------------------------------------------------------------

    def discover_courses(self, date_str):
        """Navigate to the PMU programme hub for a date and discover R/C combos.

        Returns a list of (reunion_num, course_num) tuples.
        Falls back to brute-force probing if the hub page does not load.
        """
        hub_url = f"{self.BASE_URL}/turf/programme/{date_str}/"
        courses = []

        if self.navigate(hub_url, wait_until="domcontentloaded"):
            self.accept_cookies()
            time.sleep(2)

            # Try to extract reunion/course links from the page
            links = self.page.query_selector_all("a[href]")
            for link in links:
                href = link.get_attribute("href") or ""
                m = re.search(
                    r'/turf/programme/' + re.escape(date_str)
                    + r'/R(\d+)/C(\d+)',
                    href,
                )
                if m:
                    r_num = int(m.group(1))
                    c_num = int(m.group(2))
                    if (r_num, c_num) not in courses:
                        courses.append((r_num, c_num))

            # Also look for embedded JSON that may contain the full programme
            scripts = self.page.query_selector_all("script")
            for script in scripts:
                text = script.inner_text() or ""
                if len(text) < 50:
                    continue
                # Look for JSON blobs with reunion/course data
                for match in re.finditer(
                    r'"numReunion"\s*:\s*(\d+).*?"numOrdre"\s*:\s*(\d+)',
                    text,
                ):
                    r_num = int(match.group(1))
                    c_num = int(match.group(2))
                    if (r_num, c_num) not in courses:
                        courses.append((r_num, c_num))

        if not courses:
            # Brute-force: probe R1..MAX_REUNIONS, C1..MAX_COURSES
            log.info("  Hub discovery failed for %s, probing R/C combos", date_str)
            for r in range(1, self.MAX_REUNIONS + 1):
                url = self.PROGRAMME_URL.format(
                    date=date_str, reunion=r, course=1,
                )
                if self.navigate(url, wait_until="domcontentloaded", retries=1):
                    body = self.page.inner_text("body") or ""
                    if len(body) < 200 or "404" in body[:100]:
                        break
                    # This reunion exists; find its courses
                    for c in range(1, self.MAX_COURSES + 1):
                        courses.append((r, c))
                    self.smart_pause(2.0, 1.0)
                else:
                    break

        courses.sort()
        log.info("  %s: %d courses discovered", date_str, len(courses))
        return courses

    # ----------------------------------------------------------------
    # Programme page scraping (pre-race data)
    # ----------------------------------------------------------------

    def scrape_programme(self, date_str, reunion, course):
        """Scrape the programme page for a single course.

        Returns a list of records (one per partant + one course-level).
        """
        cache_key = f"prog_{date_str}_R{reunion}_C{course}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        url = self.PROGRAMME_URL.format(
            date=date_str, reunion=reunion, course=course,
        )
        if not self.navigate(url, wait_until="domcontentloaded"):
            self.screenshot_on_error(f"pmu_prog_{date_str}_R{reunion}_C{course}")
            return []

        self.accept_cookies()
        time.sleep(2)

        body_text = self.page.inner_text("body") or ""
        # Quick check: is there actual content?
        if len(body_text) < 300 or "404" in body_text[:150]:
            log.debug("  No content for programme %s R%d C%d", date_str, reunion, course)
            self.save_cache(cache_key, [])
            return []

        records = []
        base_keys = {
            "date": date_str,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": url,
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

        # Conditions text (often in a dedicated block)
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

        # --- 2. Partant-level data ---
        partant_records = self._extract_partants_programme(
            date_str, reunion, course, url,
        )
        records.extend(partant_records)

        # --- 3. Embedded JSON (may contain structured partant data) ---
        embedded = self._extract_pmu_embedded_json(date_str, reunion, course)
        records.extend(embedded)

        self.save_cache(cache_key, records)
        return records

    # ----------------------------------------------------------------
    # Results page scraping (post-race data)
    # ----------------------------------------------------------------

    def scrape_resultats(self, date_str, reunion, course):
        """Scrape the results page for a single course.

        Returns a list of records with commentaires, incidents, temps.
        """
        cache_key = f"res_{date_str}_R{reunion}_C{course}"
        cached = self.load_cache(cache_key)
        if cached is not None:
            return cached

        url = self.RESULTATS_URL.format(
            date=date_str, reunion=reunion, course=course,
        )
        if not self.navigate(url, wait_until="domcontentloaded"):
            # Results may not exist yet (future race) -- not an error
            log.debug("  No results page for %s R%d C%d", date_str, reunion, course)
            self.save_cache(cache_key, [])
            return []

        self.accept_cookies()
        time.sleep(2)

        body_text = self.page.inner_text("body") or ""
        if len(body_text) < 300 or "404" in body_text[:150]:
            self.save_cache(cache_key, [])
            return []

        records = []
        base_keys = {
            "date": date_str,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": url,
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
                # Match pattern like 1'23"45 or 1:23.45
                if text and re.search(r"\d+[':]\d{2}", text):
                    records.append({
                        **base_keys,
                        "type": "temps_course",
                        "contenu": text[:200],
                        "scraped_at": datetime.now().isoformat(),
                    })

        # --- Result partants (classement, ecarts) ---
        result_partants = self._extract_partants_resultats(
            date_str, reunion, course, url,
        )
        records.extend(result_partants)

        # --- Embedded JSON from results page ---
        embedded = self._extract_pmu_embedded_json(date_str, reunion, course)
        records.extend(embedded)

        self.save_cache(cache_key, records)
        return records

    # ----------------------------------------------------------------
    # Partant extraction: programme page
    # ----------------------------------------------------------------

    def _extract_partants_programme(self, date_str, reunion, course, url):
        """Extract per-horse details from the programme page."""
        records = []
        base_keys = {
            "date": date_str,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": url,
        }

        # PMU typically renders partants in rows or cards
        # Try multiple possible selectors for the partant container
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

            # Extract numPmu from data attribute or text
            num_pmu = (
                el.get_attribute("data-numPmu")
                or el.get_attribute("data-num-pmu")
                or el.get_attribute("data-num")
                or ""
            )
            if not num_pmu:
                # Try to find a number element inside
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
                    f"{date_str}_R{reunion}_C{course}_N{num_pmu}"
                )

            # Full text of the partant block (for regex extraction)
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

            # Store raw text for fields we might have missed
            if full_text and len(full_text) > 10:
                rec["raw_text"] = full_text[:2000]

            records.append(rec)

        # If no partant elements found, try table extraction
        if not records:
            table_records = self.extract_tables(
                date_str, "pmu_web", "partant_table_row",
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

    def _extract_partants_resultats(self, date_str, reunion, course, url):
        """Extract per-horse results (classement, ecarts, etc.)."""
        records = []
        base_keys = {
            "date": date_str,
            "reunion": reunion,
            "course": course,
            "source": "pmu_web",
            "url": url,
        }

        # Results page often has a classement table or card layout
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
            # Fall back to tables
            table_records = self.extract_tables(
                date_str, "pmu_web", "resultat_table_row",
            )
            for rec in table_records:
                rec["reunion"] = reunion
                rec["course"] = course
                rec["url"] = url
            return table_records

        # Extract rows within the classement container
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

            # numPmu
            num_el = row.query_selector(
                "[class*='numero'], [class*='num'], .numPmu"
            )
            if num_el:
                rec["numPmu"] = (num_el.inner_text() or "").strip()

            # Classement / place
            place_el = row.query_selector(
                "[class*='place'], [class*='rang'], [class*='classement']"
            )
            if place_el:
                rec["place"] = (place_el.inner_text() or "").strip()

            # Ecart
            ecart_el = row.query_selector(
                "[class*='ecart'], [class*='Ecart'], [class*='distance']"
            )
            if ecart_el:
                rec["ecart"] = (ecart_el.inner_text() or "").strip()

            # Build partant_uid
            if rec.get("numPmu"):
                rec["partant_uid"] = (
                    f"{date_str}_R{reunion}_C{course}_N{rec['numPmu']}"
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

    def _extract_pmu_embedded_json(self, date_str, reunion, course):
        """Extract PMU-specific structured data from script tags.

        PMU.fr often embeds race/partant data in script tags as JSON
        or as JS variables like window.__INITIAL_STATE__.
        """
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
                        "date": date_str,
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
                        "date": date_str,
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
                # Try to extract JSON arrays/objects containing ferrage
                for json_match in re.finditer(r'(\[.*?\]|\{.*?\})', text, re.DOTALL):
                    blob = json_match.group(1)
                    if len(blob) > 50 and re.search(
                        r'ferr|deferre|oeill', blob, re.IGNORECASE
                    ):
                        try:
                            data = json.loads(blob)
                            records.append({
                                "date": date_str,
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
        log.info("SCRIPT 146 -- PMU Web Detail Scraper")
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
            current = start_date
            day_count = 0
            total_records = 0

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")
                # PMU uses dd/mm/yyyy in URLs -- check which format works
                date_pmu = current.strftime("%d%m%Y")

                log.info("  Scraping %s ...", date_str)

                # Discover R/C combos for this date
                courses = self.discover_courses(date_pmu)
                if not courses:
                    # Try alternate date format (YYYY-MM-DD)
                    courses = self.discover_courses(date_str)

                for r_num, c_num in courses:
                    # Programme page
                    prog_records = self.scrape_programme(
                        date_pmu, r_num, c_num,
                    )
                    for rec in prog_records:
                        # Ensure ISO date is always present for join
                        rec["date_iso"] = date_str
                        self.append_jsonl(output_file, rec)
                        total_records += 1

                    self.smart_pause(4.0, 2.0)

                    # Results page
                    res_records = self.scrape_resultats(
                        date_pmu, r_num, c_num,
                    )
                    for rec in res_records:
                        rec["date_iso"] = date_str
                        self.append_jsonl(output_file, rec)
                        total_records += 1

                    self.smart_pause(4.0, 2.0)

                day_count += 1

                # Periodic logging + checkpoint
                if day_count % 10 == 0:
                    log.info(
                        "  %s | jours=%d records=%d",
                        date_str, day_count, total_records,
                    )
                    self.save_checkpoint({
                        "last_date": date_str,
                        "total_records": total_records,
                    })

                # Browser rotation to avoid memory leaks / detection
                if day_count % 50 == 0:
                    log.info("  Rotating browser context...")
                    self.close_browser()
                    self.smart_pause(8.0, 4.0)
                    self.launch_browser()

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
            "Script 146 -- PMU.fr Web Detail Scraper "
            "(ferrage, avis, commentaires, oeilleres, pronostics)"
        ),
    )
    PlaywrightScraperBase.add_common_args(parser)
    args = parser.parse_args()

    scraper = PMUWebDetailScraper(args)
    scraper.run()


if __name__ == "__main__":
    main()
