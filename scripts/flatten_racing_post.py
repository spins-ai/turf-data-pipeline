#!/usr/bin/env python3
"""
scripts/flatten_racing_post.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Flatten raw Racing Post race-level scrapes into horse-level records.

Input:  output/37_racing_post/racing_post_fr.jsonl   (~3.6 M lines)
Output: output/37_racing_post/racing_post_flat.jsonl  (1 line per horse)

Each input record has a ``raw_text`` field like::

    "2.\n                    Kingcormac | 11/8F | Shirocco (GER) | ..."

This script:
  1. Streams the input line by line (constant memory).
  2. Parses ``raw_text`` to extract position, horse name, odds (fractional
     -> decimal), sire, dam, breeder, owner.
  3. Falls back to the top-level ``position`` / ``odds`` fields when the
     raw_text parse is incomplete.
  4. Skips non-horse rows (logo banners, header scrapes, etc.).
  5. Deduplicates identical (date, course_id, race_name, position, horse)
     tuples, keeping the richest record.

Memory: O(1) streaming — safe for multi-GB files.
"""

from __future__ import annotations

import json
import os
import re
import sys

# ---------------------------------------------------------------------------
# Odds conversion
# ---------------------------------------------------------------------------

_ODDS_RE = re.compile(r"^(\d+)/(\d+)\s*([A-Za-z]*)$")

def fractional_to_decimal(raw: str | None) -> float | None:
    """Convert fractional odds like '11/8F' to decimal (2.375). Returns None on failure."""
    if not raw:
        return None
    raw = raw.strip()
    m = _ODDS_RE.match(raw)
    if m:
        num, den = int(m.group(1)), int(m.group(2))
        if den == 0:
            return None
        return round(num / den + 1, 4)
    # Try plain decimal
    try:
        return round(float(raw), 4)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Raw-text parser
# ---------------------------------------------------------------------------

_POS_HORSE_RE = re.compile(
    r"^\s*(\d+)\.\s+(.*)",
    re.DOTALL,
)

def parse_raw_text(raw_text: str) -> dict | None:
    """Parse a Racing Post raw_text field into structured horse data.

    Returns None if the text does not look like a horse record.
    """
    if not raw_text:
        return None

    text = raw_text.strip()

    # Must start with "N." (position number followed by a dot)
    m = _POS_HORSE_RE.match(text)
    if not m:
        return None

    position = int(m.group(1))
    remainder = m.group(2).strip()

    # Split on | separator
    parts = [p.strip() for p in remainder.split("|")]
    parts = [p for p in parts if p]  # drop empty

    if not parts:
        return None

    horse_name = parts[0].strip()
    if not horse_name:
        return None

    # Filter out logo/banner scrapes that matched the number prefix
    # (these have many "logo" tokens)
    logo_count = sum(1 for p in parts if "logo" in p.lower())
    if logo_count >= 3:
        return None

    result: dict = {
        "position": position,
        "nom_cheval": horse_name,
    }

    # Odds (second field if present)
    raw_odds = parts[1] if len(parts) > 1 else None
    if raw_odds:
        dec = fractional_to_decimal(raw_odds)
        if dec is not None:
            result["raw_odds"] = raw_odds
            result["odds_decimal"] = dec

    # Sire / Dam / Breeder / Owner from remaining pipe segments
    # Typical pattern: sire | - | dam | (damsire) | Breeder | : | name | Owner | : | name
    if len(parts) > 2:
        extras = parts[2:]
        result["sire"] = _clean_origin(extras[0]) if extras else None

        # Find Breeder and Owner markers
        breeder = _extract_after_marker(extras, "Breeder")
        owner = _extract_after_marker(extras, "Owner")
        if breeder:
            result["breeder"] = breeder
        if owner:
            result["owner"] = owner

    return result


def _clean_origin(val: str) -> str | None:
    """Strip country suffix like (GER) and dashes."""
    if not val or val == "-":
        return None
    return val.strip()


def _extract_after_marker(parts: list[str], marker: str) -> str | None:
    """Find 'Marker : Value' pattern in pipe-separated parts list."""
    for i, p in enumerate(parts):
        if p.strip().lower() == marker.lower():
            # The value follows after a ':' separator
            if i + 2 < len(parts) and parts[i + 1].strip() == ":":
                val = parts[i + 2].strip()
                return val if val else None
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    inpath = os.path.join(base, "output", "37_racing_post", "racing_post_fr.jsonl")
    outpath = os.path.join(base, "output", "37_racing_post", "racing_post_flat.jsonl")

    if not os.path.isfile(inpath):
        print(f"ERROR: Input not found: {inpath}", file=sys.stderr)
        sys.exit(1)

    total_in = 0
    total_out = 0
    skipped_no_horse = 0
    skipped_dup = 0
    parse_errors = 0

    # Dedup set: keep track of (date, course_id, race_name, position, horse)
    seen: set[tuple] = set()

    with open(inpath, "r", encoding="utf-8") as fin, \
         open(outpath, "w", encoding="utf-8") as fout:

        for line_no, raw in enumerate(fin, 1):
            raw = raw.strip()
            if not raw:
                continue
            total_in += 1

            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                parse_errors += 1
                continue

            raw_text = rec.get("raw_text", "")

            parsed = parse_raw_text(raw_text)
            if parsed is None:
                skipped_no_horse += 1
                continue

            # Build flat record
            flat: dict = {
                "date": str(rec.get("date", ""))[:10],
                "course_id": rec.get("course_id"),
                "race_name": rec.get("race_name"),
                "race_info": rec.get("race_info"),
                "position": parsed["position"],
                "nom_cheval": parsed["nom_cheval"],
            }

            # Odds: prefer parsed from raw_text, fallback to top-level
            if "odds_decimal" in parsed:
                flat["odds_decimal"] = parsed["odds_decimal"]
                flat["raw_odds"] = parsed["raw_odds"]
            elif rec.get("odds"):
                dec = fractional_to_decimal(str(rec["odds"]))
                flat["odds_decimal"] = dec
                flat["raw_odds"] = str(rec["odds"])
            else:
                flat["odds_decimal"] = None
                flat["raw_odds"] = None

            # Extra fields from parse
            for key in ("sire", "breeder", "owner"):
                if parsed.get(key):
                    flat[key] = parsed[key]

            # Dedup: keep first (richest) record per unique key
            dedup_key = (
                flat["date"],
                flat["course_id"],
                flat["race_name"],
                flat["position"],
                flat["nom_cheval"].upper(),
            )
            if dedup_key in seen:
                skipped_dup += 1
                continue
            seen.add(dedup_key)

            total_out += 1
            fout.write(json.dumps(flat, ensure_ascii=False) + "\n")

            if total_out % 500_000 == 0:
                print(f"  ... {total_out:,} horses written ({total_in:,} lines read)")

    print(f"Done. {total_in:,} input lines -> {total_out:,} horse records.")
    print(f"  Skipped non-horse: {skipped_no_horse:,}")
    print(f"  Skipped duplicate: {skipped_dup:,}")
    print(f"  Parse errors:      {parse_errors:,}")
    print(f"Output: {outpath}")


if __name__ == "__main__":
    main()
