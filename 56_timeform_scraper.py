#!/usr/bin/env python3
"""
Script 56 — Scraping Timeform.com (UK Racing)
Source : timeform.com/horse-racing
Collecte : ratings, speed figures, race analysis, form data
CRITIQUE pour : Performance Ratings, Speed Model, Form Analysis
"""

import argparse
import json
import os
import sys
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "56_timeform"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint, create_session

log = setup_logging("56_timeform")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://www.timeform.com"



def scrape_timeform_racecards(session, date_str):
    """Scrape Timeform race cards for a given date — ratings and speed figures."""
    cache_file = os.path.join(CACHE_DIR, f"racecards_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/horse-racing/racecards/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract meetings / race links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/horse-racing/" in href and ("racecard" in href.lower() or "result" in href.lower()):
            text = link.get_text(strip=True)
            if text and len(text) > 2:
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "race_link",
                    "text": text,
                    "url": href if href.startswith("http") else BASE_URL + href,
                    "scraped_at": datetime.now().isoformat(),
                })

    # Extract race cards with ratings from tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            record = {
                "date": date_str,
                "source": "timeform",
                "type": "racecard_entry",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract Timeform ratings from data attributes
    for el in soup.find_all(attrs={"data-rating": True}):
        records.append({
            "date": date_str,
            "source": "timeform",
            "type": "tf_rating",
            "rating": el.get("data-rating"),
            "horse": el.get("data-horse", el.get_text(strip=True)),
            "scraped_at": datetime.now().isoformat(),
        })

    # Extract speed figures from spans/divs with relevant classes
    for el in soup.find_all(["span", "div"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["speed", "figure", "rating", "tf-rating", "master"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d', text):
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "speed_figure",
                    "value": text,
                    "classes": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extract embedded JSON data from scripts ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        # JSON.parse patterns
        for m in re.finditer(r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)', script_text, re.DOTALL):
            try:
                raw = m.group(1).encode().decode('unicode_escape')
                data = json.loads(raw)
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "embedded_json_parse",
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        # window.__DATA, __NEXT_DATA__, raceData, etc.
        for m in re.finditer(r'window\[?[\'"]?(__\w+|__NEXT_DATA__|raceData|cardData|formData)[\'"]?\]?\s*=\s*(\{.+?\});',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "embedded_window_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    # --- Extract script type="application/json" / application/ld+json ---
    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "timeform",
                "type": "script_application_json",
                "script_type": script.get("type", ""),
                "data_id": script.get("id", ""),
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Extract all data-attributes with racing relevance ---
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in
            ["rating", "speed", "horse", "runner", "race", "form", "pace", "section", "master"])
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            records.append({
                "date": date_str,
                "source": "timeform",
                "type": "data_attributes",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            })

    # --- Extract pace/sectionals from class-based elements ---
    for el in soup.find_all(["span", "div", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["pace", "sectional", "split", "tempo",
                                                  "closing", "early", "late"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d', text):
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "pace_sectional",
                    "value": text,
                    "classes": classes,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extract trainer/jockey stats tabs content ---
    for div in soup.find_all(["div", "section"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["trainer-stats", "jockey-stats", "tab-content",
                                                  "statistics", "stat-panel", "form-tab"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "stats_tab_content",
                    "classes": classes,
                    "content": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_timeform_results(session, date_str):
    """Scrape Timeform results page — post-race ratings and analysis."""
    cache_file = os.path.join(CACHE_DIR, f"results_{date_str}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/horse-racing/results/{date_str}"
    resp = fetch_with_retry(session, url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Extract result tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "date": date_str,
                "source": "timeform",
                "type": "result",
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell
            records.append(record)

    # Extract race analysis / race report sections
    for div in soup.find_all(["div", "section", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["analysis", "report", "comment", "verdict", "review"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 2000:
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "race_analysis",
                    "content": text[:1500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extract race comments (in-running, post-race) ---
    for div in soup.find_all(["div", "p", "span", "article"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["race-comment", "in-running", "comment-text",
                                                  "notebook", "eye-catcher", "horse-comment"]):
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "race_comment",
                    "classes": classes,
                    "content": text[:2500],
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Extract sectional times from results ---
    for el in soup.find_all(["div", "table", "section"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["sectional", "split", "furlong-time",
                                                  "section-time", "timing"]):
            if el.name == "table":
                rows = el.find_all("tr")
                sec_headers = []
                if rows:
                    sec_headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells:
                        entry = {
                            "date": date_str,
                            "source": "timeform",
                            "type": "sectional_time",
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = sec_headers[j].lower().replace(" ", "_") if j < len(sec_headers) else f"col_{j}"
                            entry[key] = cell
                        records.append(entry)
            else:
                text = el.get_text(strip=True)
                if text and re.search(r'\d+\.\d+', text):
                    records.append({
                        "date": date_str,
                        "source": "timeform",
                        "type": "sectional_text",
                        "content": text[:1500],
                        "scraped_at": datetime.now().isoformat(),
                    })

    # --- Extract embedded JSON from results page ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'window\[?[\'"]?(__\w+|resultData|raceResult|performanceData)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "result_embedded_data",
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "timeform",
                "type": "result_script_json",
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Extract video/media metadata ---
    for el in soup.find_all(["video", "source", "iframe", "a"]):
        src = el.get("src") or el.get("data-src") or el.get("href", "")
        if src and any(kw in src.lower() for kw in ["replay", "video", "race-video", "stream", "mp4", "m3u8"]):
            records.append({
                "date": date_str,
                "source": "timeform",
                "type": "video_metadata",
                "media_url": src,
                "media_tag": el.name,
                "poster": el.get("poster", ""),
                "scraped_at": datetime.now().isoformat(),
            })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def scrape_race_detail(session, race_url, date_str):
    """Scrape individual race detail for full ratings breakdown."""
    if not race_url.startswith("http"):
        race_url = BASE_URL + race_url

    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', race_url[-60:])
    cache_file = os.path.join(CACHE_DIR, f"detail_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, race_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    records = []

    # Race title
    race_name = ""
    for h in soup.find_all(["h1", "h2"]):
        text = h.get_text(strip=True)
        if text and len(text) > 3:
            race_name = text
            break

    # Extract runner details with ratings
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "date": date_str,
                "source": "timeform",
                "type": "runner_detail",
                "race_name": race_name,
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract numeric rating if present
            for cell in cells:
                rating_match = re.search(r'(\d{2,3})[pP]?$', cell.strip())
                if rating_match:
                    record["tf_rating_parsed"] = rating_match.group(1)
                    break

            records.append(record)

    # --- Detailed pace figures (early, mid, late) ---
    for el in soup.find_all(["span", "div", "td"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["pace-fig", "early-pace", "late-pace",
                                                  "finishing-speed", "pace-rating"]):
            text = el.get_text(strip=True)
            if text and re.search(r'\d', text):
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "pace_figure_detail",
                    "race_name": race_name,
                    "value": text,
                    "classes": classes,
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Form history per horse (last 10+ races) ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["form-history", "form-figures", "previous-form",
                                                  "race-record", "past-perf", "form-line"]):
            if div.name == "table":
                rows = div.find_all("tr")
                form_headers = []
                if rows:
                    form_headers = [th.get_text(strip=True).lower().replace(" ", "_")
                                    for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells and len(cells) >= 2:
                        entry = {
                            "date": date_str,
                            "source": "timeform",
                            "type": "form_history_entry",
                            "race_name": race_name,
                            "url": race_url,
                            "scraped_at": datetime.now().isoformat(),
                        }
                        for j, cell in enumerate(cells):
                            key = form_headers[j] if j < len(form_headers) and form_headers[j] else f"col_{j}"
                            entry[key] = cell
                        records.append(entry)
            else:
                text = div.get_text(strip=True)
                if text and 5 < len(text) < 3000:
                    records.append({
                        "date": date_str,
                        "source": "timeform",
                        "type": "form_history_text",
                        "race_name": race_name,
                        "content": text[:2500],
                        "url": race_url,
                        "scraped_at": datetime.now().isoformat(),
                    })

    # --- Stats by terrain/distance/course ---
    for div in soup.find_all(["div", "section", "table"], class_=True):
        classes = " ".join(div.get("class", []))
        if any(kw in classes.lower() for kw in ["course-stats", "distance-stats", "going-stats",
                                                  "track-record", "course-distance", "stats-breakdown"]):
            text = div.get_text(strip=True)
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "stats_breakdown",
                    "race_name": race_name,
                    "classes": classes,
                    "content": text[:2500],
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    # --- Race detail embedded JSON ---
    for script in soup.find_all("script"):
        script_text = script.string or ""
        for m in re.finditer(r'window\[?[\'"]?(__\w+|raceDetail|runnerData|ratingData|formData)[\'"]?\]?\s*=\s*(\{.+?\}|\[.+?\]);',
                             script_text, re.DOTALL):
            try:
                data = json.loads(m.group(2))
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "detail_embedded_data",
                    "race_name": race_name,
                    "var_name": m.group(1),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                })
            except json.JSONDecodeError:
                pass

    for script in soup.find_all("script", {"type": re.compile(r'application/(ld\+)?json')}):
        try:
            data = json.loads(script.string or "")
            records.append({
                "date": date_str,
                "source": "timeform",
                "type": "detail_script_json",
                "race_name": race_name,
                "data": data,
                "scraped_at": datetime.now().isoformat(),
            })
        except json.JSONDecodeError:
            pass

    # --- Horse comments / notebook entries ---
    for div in soup.find_all(["div", "p", "span"], class_=True):
        classes = " ".join(div.get("class", []))
        text = div.get_text(strip=True)
        if any(kw in classes.lower() for kw in ["horse-comment", "notebook", "trainer-comment",
                                                  "jockey-comment", "in-running-comment"]):
            if text and 10 < len(text) < 3000:
                records.append({
                    "date": date_str,
                    "source": "timeform",
                    "type": "horse_comment",
                    "race_name": race_name,
                    "content": text[:2500],
                    "url": race_url,
                    "scraped_at": datetime.now().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return records


def main():
    parser = argparse.ArgumentParser(description="Script 56 — Timeform Scraper (ratings, speed figures, analysis)")
    parser.add_argument("--start", type=str, default="2022-01-01",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD), default=today")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()

    log.info("=" * 60)
    log.info("SCRIPT 56 — Timeform Scraper (UK Racing)")
    log.info(f"  Period : {start_date.date()} -> {end_date.date()}")
    log.info("=" * 60)

    # Checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    last_date = checkpoint.get("last_date")
    if args.resume and last_date:
        resume_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        start_date = resume_date
        log.info(f"  Resuming from checkpoint: {start_date.date()}")

    session = create_session(USER_AGENTS)
    output_file = os.path.join(OUTPUT_DIR, "timeform_data.jsonl")

    current = start_date
    day_count = 0
    total_records = 0

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # Scrape race cards (pre-race ratings)
        racecard_records = scrape_timeform_racecards(session, date_str)
        if racecard_records:
            # Scrape detail pages for races found
            race_urls = [r.get("url") for r in racecard_records
                         if r.get("type") == "race_link" and r.get("url")]
            for rurl in list(set(race_urls))[:15]:
                detail = scrape_race_detail(session, rurl, date_str)
                if detail:
                    racecard_records.extend(detail)
                smart_pause(1.5, 0.8)

            for rec in racecard_records:
                append_jsonl(output_file, rec)
                total_records += 1

        smart_pause(2.0, 1.0)

        # Scrape results (post-race ratings)
        result_records = scrape_timeform_results(session, date_str)
        if result_records:
            for rec in result_records:
                append_jsonl(output_file, rec)
                total_records += 1

        day_count += 1

        if day_count % 30 == 0:
            log.info(f"  {date_str} | days={day_count} records={total_records}")
            save_checkpoint(CHECKPOINT_FILE, {"last_date": date_str, "total_records": total_records})

        if day_count % 80 == 0:
            session.close()
            session = create_session(USER_AGENTS)
            time.sleep(random.uniform(5, 15))

        current += timedelta(days=1)
        smart_pause(1.0, 0.5)

    save_checkpoint(CHECKPOINT_FILE, {"last_date": end_date.strftime("%Y-%m-%d"),
                     "total_records": total_records, "status": "done"})

    log.info("=" * 60)
    log.info(f"DONE: {day_count} days, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
