#!/usr/bin/env python3
"""
Script 30 — Smarkets Exchange : Cotes back/lay courses FR
Source : api.smarkets.com (API gratuite)
CRITIQUE pour : Value Detection, Market Analysis, Outsider Detection
"""

import requests
import json
import time
import os
import logging
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "30_smarkets_exchange")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("30_smarkets_exchange")

BASE_URL = "https://api.smarkets.com/v3"

session = requests.Session()
session.headers.update({
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
})

# --- French track detection ---
# Primary: slug contains "-fra" suffix (reliable)
# Fallback: known French track names in slug or event name
FR_TRACK_KEYWORDS = [
    "longchamp", "chantilly", "deauville", "saint-cloud", "auteuil",
    "vincennes", "lyon", "marseille", "bordeaux", "toulouse",
    "strasbourg", "compiegne", "fontainebleau", "maisons-laffitte",
    "enghien", "cagnes", "vichy", "clairefontaine", "craon",
    "la-teste", "le-lion", "les-sables", "mont-de-marsan", "nantes",
    "pau", "reims", "royan", "salon-de-provence", "tarbes",
    "vittel", "argentan", "cabourg", "chatillon", "dieppe",
    "evreux", "le-croise-laroche", "le-mans", "moulins",
    "nancy", "pontchateau", "pornichet", "senonnes",
    "laval", "graignes", "agen", "aix-les-bains",
]


def smart_pause(base=0.5, jitter=0.3):
    time.sleep(base + jitter * (2 * __import__('random').random() - 1))


def is_french_event(event):
    """Check if event is French horse racing."""
    slug = event.get("full_slug", "").lower()
    name = event.get("name", "").lower()
    combined = slug + " " + name

    # Primary check: -fra in slug (e.g., auteuil-fra, reims-fra)
    if "-fra" in slug or "-fra/" in slug:
        return True

    # Fallback: known track keywords
    for kw in FR_TRACK_KEYWORDS:
        if kw in combined:
            return True

    # Also check for "france" explicitly
    if "france" in combined:
        return True

    return False


def get_horse_racing_events(date_from, date_to):
    """Récupérer les événements hippiques"""
    events = []
    url = f"{BASE_URL}/events/"
    params = {
        "type_domain": "horse_racing",
        "type_scope": "single_event",
        "start_datetime_min": date_from,
        "start_datetime_max": date_to,
        "limit": 200,
        "sort": "id",
    }

    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            events = data.get("events", [])
            log.info(f"  {len(events)} événements trouvés")

            # Pagination
            while data.get("pagination", {}).get("next_page"):
                smart_pause(0.3, 0.1)
                params["after"] = data["pagination"]["next_page"]
                resp = session.get(url, params=params, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    new_events = data.get("events", [])
                    events.extend(new_events)
                    if not new_events:
                        break
                else:
                    break
        else:
            log.warning(f"  HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"  Erreur events: {e}")

    return events


def get_markets_for_event(event_id):
    """Récupérer les marchés d'un événement"""
    url = f"{BASE_URL}/events/{event_id}/markets/"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("markets", [])
    except Exception as e:
        log.debug(f"  Erreur markets {event_id}: {e}")
    return []


def get_contracts_for_market(market_id):
    """Récupérer les contrats (chevaux) d'un marché"""
    url = f"{BASE_URL}/markets/{market_id}/contracts/"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("contracts", [])
    except Exception as e:
        log.debug(f"  Erreur contracts {market_id}: {e}")
    return []


def get_quotes_for_market(market_id):
    """Récupérer les meilleures cotes back/lay pour un marché.
    Returns dict keyed by contract_id with best bid (back) and offer (lay).
    Prices are in basis points (e.g., 571 = 5.71% implied probability).
    """
    url = f"{BASE_URL}/markets/{market_id}/quotes/"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log.debug(f"  Erreur quotes {market_id}: {e}")
    return {}


def get_last_executed_prices(market_id):
    """Récupérer les derniers prix exécutés pour un marché.
    Returns dict keyed by contract_id -> {last_executed_price, timestamp}.
    """
    url = f"{BASE_URL}/markets/{market_id}/last_executed_prices/"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            prices = {}
            for item in data.get("last_executed_prices", {}).get(str(market_id), []):
                cid = str(item.get("contract_id", ""))
                prices[cid] = {
                    "last_executed_price": item.get("last_executed_price"),
                    "timestamp": item.get("timestamp"),
                }
            return prices
    except Exception as e:
        log.debug(f"  Erreur last_executed_prices {market_id}: {e}")
    return {}


def get_market_volume(market_id):
    """Récupérer le volume échangé sur un marché."""
    url = f"{BASE_URL}/markets/{market_id}/volumes/"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            vols = resp.json().get("volumes", [])
            for v in vols:
                if str(v.get("market_id")) == str(market_id):
                    return v.get("volume", 0)
    except Exception as e:
        log.debug(f"  Erreur volumes {market_id}: {e}")
    return 0


def bp_to_decimal_odds(bp_price):
    """Convert basis-point price to decimal odds.
    Smarkets prices are implied probability in basis points:
    571 = 5.71% -> decimal odds = 100 / 5.71 = 17.51
    Returns None if price is 0 or invalid.
    """
    if bp_price is None or bp_price == 0:
        return None
    pct = bp_price / 100.0
    if pct <= 0:
        return None
    return round(10000.0 / bp_price, 2)


def main():
    log.info("=" * 60)
    log.info("SCRIPT 30 — Smarkets Exchange Cotes")
    log.info("=" * 60)

    all_records = []
    output_file = os.path.join(OUTPUT_DIR, "smarkets_exchange.json")

    # Checkpoint
    # Note: Smarkets API only serves events ~1-2 weeks around today.
    # Historical data is NOT available. We start from 4 weeks ago to
    # catch any edge cases, but most weeks will return 0 events.
    checkpoint_file = os.path.join(OUTPUT_DIR, ".checkpoint_30.json")
    default_start = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")
    last_date = default_start
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, encoding="utf-8") as f:
            cp = json.load(f)
        saved_date = cp.get("last_date", default_start)
        # Don't go further back than 4 weeks (API won't have data)
        if saved_date > default_start:
            last_date = saved_date
        log.info(f"Reprise depuis {last_date}")

    if os.path.exists(output_file):
        with open(output_file, encoding="utf-8") as f:
            all_records = json.load(f)
        log.info(f"Chargé {len(all_records)} records existants")

    # Parcourir par semaines (include upcoming week for active markets)
    current = datetime.strptime(last_date, "%Y-%m-%d")
    end = datetime.now() + timedelta(days=7)

    while current < end:
        week_end = min(current + timedelta(days=7), end)
        date_from = current.strftime("%Y-%m-%dT00:00:00Z")
        date_to = week_end.strftime("%Y-%m-%dT23:59:59Z")

        cache_key = current.strftime("%Y-%m-%d")
        cache_file = os.path.join(CACHE_DIR, f"week_{cache_key}.json")

        if os.path.exists(cache_file):
            with open(cache_file, encoding="utf-8") as f:
                week_records = json.load(f)
            all_records.extend(week_records)
            log.info(f"Semaine {cache_key} (cache): {len(week_records)} records")
            current = week_end
            continue

        log.info(f"Semaine {cache_key}...")
        events = get_horse_racing_events(date_from, date_to)

        # Filtrer événements français
        fr_events = [e for e in events if is_french_event(e)]

        week_records = []

        for event in fr_events:
            event_id = event.get("id")
            event_name = event.get("name", "")
            event_date = event.get("start_datetime", "")
            event_slug = event.get("full_slug", "")

            # Extract track name from slug: /sport/horse-racing/TRACK/...
            track_name = ""
            slug_parts = event_slug.split("/")
            if len(slug_parts) > 3:
                track_name = slug_parts[3]

            smart_pause(0.3, 0.1)
            markets = get_markets_for_event(event_id)

            for market in markets:
                market_id = market.get("id")
                market_name = market.get("name", "")

                smart_pause(0.2, 0.1)
                contracts = get_contracts_for_market(market_id)

                # Fetch prices for this market
                smart_pause(0.2, 0.1)
                quotes = get_quotes_for_market(market_id)

                smart_pause(0.2, 0.1)
                last_prices = get_last_executed_prices(market_id)

                smart_pause(0.2, 0.1)
                market_volume = get_market_volume(market_id)

                for contract in contracts:
                    contract_id = str(contract.get("id", ""))

                    # Extract quote data for this contract
                    contract_quotes = quotes.get(contract_id, {})
                    bids = contract_quotes.get("bids", [])
                    offers = contract_quotes.get("offers", [])

                    # Best back = highest bid (what you can sell/back at)
                    best_back_bp = bids[0]["price"] if bids else None
                    best_back_qty = bids[0]["quantity"] if bids else None

                    # Best lay = lowest offer (what you can buy/lay at)
                    best_lay_bp = offers[0]["price"] if offers else None
                    best_lay_qty = offers[0]["quantity"] if offers else None

                    # Last executed price
                    lp_data = last_prices.get(contract_id, {})
                    last_exec_bp = lp_data.get("last_executed_price")
                    if last_exec_bp is not None:
                        try:
                            last_exec_bp = int(last_exec_bp)
                        except (ValueError, TypeError):
                            last_exec_bp = None

                    record = {
                        "event_id": event_id,
                        "event_name": event_name,
                        "event_date": event_date,
                        "track": track_name,
                        "market_id": market_id,
                        "market_name": market_name,
                        "contract_id": contract_id,
                        "runner_name": contract.get("name", ""),
                        "runner_slug": contract.get("slug", ""),
                        # Prices in basis points (raw)
                        "best_back_bp": best_back_bp,
                        "best_lay_bp": best_lay_bp,
                        "last_executed_bp": last_exec_bp,
                        # Decimal odds (converted)
                        "best_back_odds": bp_to_decimal_odds(best_back_bp),
                        "best_lay_odds": bp_to_decimal_odds(best_lay_bp),
                        "last_executed_odds": bp_to_decimal_odds(last_exec_bp),
                        # Quantities available (in cents)
                        "best_back_qty": best_back_qty,
                        "best_lay_qty": best_lay_qty,
                        # Market volume
                        "market_volume": market_volume,
                        "source": "smarkets",
                    }
                    week_records.append(record)

        if week_records:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(week_records, f, ensure_ascii=False)

        all_records.extend(week_records)
        log.info(f"  → {len(fr_events)} événements FR, {len(week_records)} records")

        # Sauvegarder périodiquement
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False)
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump({"last_date": cache_key, "total": len(all_records)}, f)

        current = week_end
        smart_pause(1.0, 0.5)

    # Sauvegarde finale
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False)

    log.info("=" * 60)
    log.info(f"TERMINÉ: {len(all_records)} records Smarkets")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
