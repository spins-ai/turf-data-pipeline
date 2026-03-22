"""Shared HTML parsing helpers for BeautifulSoup-based scrapers.

Provides two functions that were previously duplicated across scrapers
54, 55, 57, 58, 61, 64, 65, 68:
  - extract_embedded_json()  -- finds JSON objects embedded in <script> tags
  - extract_data_attributes() -- extracts data-* attributes from HTML elements
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
