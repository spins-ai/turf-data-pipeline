"""Shared HTML parsing helpers for BeautifulSoup-based scrapers.

Provides functions that were previously duplicated across scrapers:
  - extract_embedded_json()        -- finds JSON objects embedded in <script> tags (54-68 scrapers)
  - extract_data_attributes()      -- extracts data-* attributes from HTML elements (54-68 scrapers)
  - extract_embedded_json_data()   -- extracts application/json + __NEXT_DATA__ (105+ scrapers)
  - extract_scraper_data_attributes() -- keyword-filtered data-* extraction (105+ scrapers)
  - extract_runners_table()        -- extracts runner data from HTML tables (105+ scrapers)
  - extract_race_links()           -- extracts race card/result links (105+ scrapers)
"""

import json
import re
from datetime import datetime

# ---------------------------------------------------------------------------
# Default keyword list used by extract_embedded_json to decide whether a
# <script> block is worth scanning for JSON fragments.  Scrapers can pass
# their own domain-specific list via the *keywords* parameter.
# ---------------------------------------------------------------------------
DEFAULT_KEYWORDS = [
    # French racing terms
    "course", "cheval", "partant", "musique", "cote", "resultat",
    "pronostic", "reunion", "hippodrome", "terrain", "piste",
    # English racing terms
    "race", "runner", "horse", "jockey", "trainer", "odds", "form",
    "tip", "selection", "result", "meeting", "going", "verdict",
    "sectional", "speed", "beyer", "bris", "chart", "workout",
    "entry", "track", "field", "barrier",
    # Exchange / betting terms
    "market", "price", "volume", "matched", "traded",
    "back", "lay", "exchange", "depth",
    # Media
    "video", "replay", "programme",
]


def extract_embedded_json(soup, date_str, source, keywords=None):
    """Extract all embedded JSON from ``<script>`` tags.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        Parsed HTML document.
    date_str : str
        Date string to attach to every record (e.g. ``"2025-05-01"``).
    source : str
        Source identifier (e.g. ``"turfinfo"``, ``"racenet_au"``).
    keywords : list[str] | None
        Domain-specific keywords that trigger JSON extraction from a
        ``<script>`` block.  Falls back to :data:`DEFAULT_KEYWORDS` when
        *None*.

    Returns
    -------
    list[dict]
        Records found, each containing *date*, *source*, *type*, *data*
        and *scraped_at* keys.
    """
    if keywords is None:
        keywords = DEFAULT_KEYWORDS

    records = []
    for script in soup.find_all("script"):
        script_text = script.string or ""

        # --- JSON-LD blocks --------------------------------------------------
        if script.get("type") == "application/ld+json":
            try:
                ld = json.loads(script_text)
                records.append({
                    "date": date_str,
                    "source": source,
                    "type": "json_ld",
                    "ld_type": ld.get("@type", "") if isinstance(ld, dict) else "array",
                    "data": ld if isinstance(ld, dict) else ld[:20],
                    "scraped_at": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, TypeError):
                pass
            continue

        # --- Inline scripts containing racing-related data -------------------
        if len(script_text) < 50:
            continue

        for kw in keywords:
            if kw in script_text.lower():
                # Flat JSON objects
                json_matches = re.findall(r'\{[^{}]{30,}\}', script_text)
                for jm in json_matches[:15]:
                    try:
                        data = json.loads(jm)
                        records.append({
                            "date": date_str,
                            "source": source,
                            "type": "embedded_json",
                            "data": data,
                            "scraped_at": datetime.now().isoformat(),
                        })
                    except json.JSONDecodeError:
                        pass

                # JSON arrays
                array_matches = re.findall(r'\[[^\[\]]{30,}\]', script_text)
                for am in array_matches[:10]:
                    try:
                        data = json.loads(am)
                        if isinstance(data, list) and len(data) > 0:
                            records.append({
                                "date": date_str,
                                "source": source,
                                "type": "embedded_json_array",
                                "data": data[:30],
                                "scraped_at": datetime.now().isoformat(),
                            })
                    except json.JSONDecodeError:
                        pass
                break

    return records


def extract_data_attributes(soup, date_str, source):
    """Extract all ``data-*`` attributes from DOM elements.

    Scans the parsed HTML for elements that carry at least two ``data-``
    attributes and returns one record per unique attribute set.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        Parsed HTML document.
    date_str : str
        Date string to attach to every record.
    source : str
        Source identifier.

    Returns
    -------
    list[dict]
        One dict per unique element, with normalised attribute names
        (``data-foo-bar`` becomes ``foo_bar``).
    """
    records = []
    seen = set()
    for el in soup.find_all(True):
        data_attrs = {k: v for k, v in el.attrs.items()
                      if isinstance(k, str) and k.startswith("data-") and v}
        if len(data_attrs) >= 2:
            key = frozenset(data_attrs.items())
            if key in seen:
                continue
            seen.add(key)
            record = {
                "date": date_str,
                "source": source,
                "type": "data_attribute",
                "tag": el.name,
                "scraped_at": datetime.now().isoformat(),
            }
            for attr_name, attr_val in data_attrs.items():
                clean_name = attr_name.replace("data-", "").replace("-", "_")
                record[clean_name] = attr_val
            text = el.get_text(strip=True)
            if text and len(text) < 300:
                record["text_content"] = text
            records.append(record)
    return records


# ---------------------------------------------------------------------------
# Scraper-style helpers (105+ scrapers)
# ---------------------------------------------------------------------------

def extract_embedded_json_data(soup, source, date_str=None):
    """Extract JSON from ``<script type="application/json">`` and ``__NEXT_DATA__``.

    This is the lighter variant used by 105+ scrapers.  Unlike
    :func:`extract_embedded_json` it does **not** scan inline scripts for
    regex-matched JSON fragments.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        Parsed HTML document.
    source : str
        Source identifier (e.g. ``"geegeez"``, ``"proform"``).
    date_str : str or None
        Date string to attach to every record.  When *None* the ``date``
        key is omitted from records.

    Returns
    -------
    list[dict]
    """
    records = []

    for script in soup.find_all("script", {"type": "application/json"}):
        try:
            data = json.loads(script.string or "")
            if data and isinstance(data, dict):
                rec = {
                    "source": source,
                    "type": "embedded_json",
                    "data_id": script.get("id", ""),
                    "data": data,
                    "scraped_at": datetime.now().isoformat(),
                }
                if date_str is not None:
                    rec["date"] = date_str
                records.append(rec)
        except (json.JSONDecodeError, TypeError):
            pass

    # __NEXT_DATA__ or similar SSR payloads
    for script in soup.find_all("script", {"id": "__NEXT_DATA__"}):
        try:
            data = json.loads(script.string or "")
            page_props = data.get("props", {}).get("pageProps", {})
            if page_props:
                rec = {
                    "source": source,
                    "type": "next_data",
                    "data": page_props,
                    "scraped_at": datetime.now().isoformat(),
                }
                if date_str is not None:
                    rec["date"] = date_str
                records.append(rec)
        except (json.JSONDecodeError, TypeError):
            pass

    return records


_DATA_ATTR_KEYWORDS = [
    "horse", "runner", "jockey", "trainer", "odds", "sp",
    "result", "position", "speed", "rating", "form",
]


def extract_scraper_data_attributes(soup, source, date_str=None):
    """Extract racing-related ``data-*`` attributes (keyword-filtered).

    Unlike :func:`extract_data_attributes` which collects *all* elements
    with 2+ data-attributes, this version only keeps elements whose
    ``data-*`` attribute names contain a racing keyword.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        Parsed HTML document.
    source : str
        Source identifier.
    date_str : str or None
        Date string.  Omitted from records when *None*.

    Returns
    -------
    list[dict]
    """
    records = []
    for el in soup.find_all(attrs=lambda attrs: attrs and any(
            k.startswith("data-") and any(kw in k for kw in _DATA_ATTR_KEYWORDS)
            for k in attrs)):
        data_attrs = {k: v for k, v in el.attrs.items() if k.startswith("data-")}
        if data_attrs:
            rec = {
                "source": source,
                "type": "data_attrs",
                "tag": el.name,
                "text": el.get_text(strip=True)[:200],
                "attributes": data_attrs,
                "scraped_at": datetime.now().isoformat(),
            }
            if date_str is not None:
                rec["date"] = date_str
            records.append(rec)
    return records


def extract_runners_table(soup, source, date_str=None, race_url="", race_name=""):
    """Extract runner data from race card or result HTML tables.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        Parsed HTML document.
    source : str
        Source identifier.
    date_str : str or None
        Date string.  Omitted from records when *None*.
    race_url : str
        URL of the race page (for provenance).
    race_name : str
        Optional race name to include in records.

    Returns
    -------
    list[dict]
    """
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_").replace(".", "")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 3:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 3:
                continue
            record = {
                "source": source,
                "type": "runner",
                "url": race_url,
                "scraped_at": datetime.now().isoformat(),
            }
            if date_str is not None:
                record["date"] = date_str
            if race_name:
                record["race_name"] = race_name
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                record[key] = cell

            # Extract data-attributes from row
            for attr_name, attr_val in row.attrs.items():
                if attr_name.startswith("data-"):
                    clean = attr_name.replace("data-", "").replace("-", "_")
                    record[clean] = attr_val

            records.append(record)
    return records


def extract_race_links(soup, base_url=""):
    """Extract links to individual race cards or results from a day page.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
        Parsed HTML document.
    base_url : str
        Base URL to prepend to relative links.

    Returns
    -------
    list[str]
        Sorted, deduplicated list of absolute race URLs.
    """
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/(racecard|racecards|results?|race)/', href, re.I):
            full_url = href if href.startswith("http") else f"{base_url}{href}"
            links.add(full_url)
    return sorted(links)
