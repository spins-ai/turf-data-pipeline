#!/usr/bin/env python3
"""
Systematic test of ALL possible PMU API endpoints to discover available data.
Tests both offline and online servers, multiple client IDs, and various endpoint suffixes.
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import logging
import requests
import time

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

DATES = ["14032026", "13032026"]
REUNION = 1
COURSE = 1

SERVERS = {
    "offline": "https://offline.turfinfo.api.pmu.fr/rest/client/{client}/programme/{date}/R{r}/C{c}/",
    "online": "https://online.turfinfo.api.pmu.fr/rest/client/{client}/programme/{date}/R{r}/C{c}/",
}

ENDPOINTS = [
    "",  # base course info
    "participants",
    "rapports-definitifs",
    "performances-detaillees/pretty",
    "citations",
    "combinaisons",
    "pronostics",
    "rapports",
    "rapports-definitifs/simple-gagnant",
    "rapports-definitifs/simple-place",
    "rapports-definitifs/couple-gagnant",
    "rapports-definitifs/couple-place",
    "rapports-definitifs/couple-ordre",
    "rapports-definitifs/trio",
    "rapports-definitifs/trio-ordre",
    "rapports-definitifs/tierce",
    "rapports-definitifs/quarte",
    "rapports-definitifs/quinte",
    "rapports-definitifs/multi",
    "rapports-definitifs/2sur4",
    "rapports-instantanes",
    "paris",
    "arrivee",
    "incidents",
    "ecuries",
    "musiques",
    "derniers-rapports",
    "rapports-direct",
    "rapports-reference",
    "gains",
    "pedigree",
    "historique",
    "forme",
    "statistiques",
    "entraineurs",
    "jockeys",
    "conditions",
    "meteo",
    "parcours",
    "tracking",
    "sectionals",
    "temps-intermediaires",
]

CLIENT_IDS = [1, 7, 61]


def test_endpoint(url, label=""):
    """Test a single endpoint and return results."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        status = resp.status_code
        if status == 200:
            text = resp.text[:500]
            try:
                data = resp.json()
                if isinstance(data, dict):
                    keys = list(data.keys())
                elif isinstance(data, list):
                    keys = f"[list with {len(data)} items]"
                    if len(data) > 0 and isinstance(data[0], dict):
                        keys = f"[list with {len(data)} items, first item keys: {list(data[0].keys())}]"
                else:
                    keys = f"[{type(data).__name__}]"
            except Exception as e:
                log.debug("Failed to parse JSON response: %s", e)
                keys = "[not JSON]"
            return status, text, keys
        else:
            return status, None, None
    except requests.exceptions.Timeout:
        return "TIMEOUT", None, None
    except requests.exceptions.ConnectionError as e:
        return f"CONN_ERR", None, None
    except Exception as e:
        return f"ERR: {e}", None, None


def main():
    # First, find a working date
    working_date = None
    working_server = None

    print("=" * 80)
    print("STEP 1: Finding a working date/server combination")
    print("=" * 80)

    for date in DATES:
        for server_name, base_url in SERVERS.items():
            url = base_url.format(client=1, date=date, r=REUNION, c=COURSE)
            status, text, keys = test_endpoint(url)
            print(f"  {server_name} | date={date} | status={status}")
            if status == 200:
                working_date = date
                working_server = server_name
                print(f"    -> FOUND working combo: {server_name} / {date}")
                print(f"    -> Keys: {keys}")
                print(f"    -> Preview: {text[:200]}...")

    if not working_date:
        print("\nNo working date found with R1/C1. Trying to find valid reunions...")
        # Try programme endpoint to find what's available
        for date in DATES:
            for server_name in ["offline", "online"]:
                prog_url = f"https://{server_name}.turfinfo.api.pmu.fr/rest/client/1/programme/{date}"
                status, text, keys = test_endpoint(prog_url)
                print(f"  Programme {server_name} | date={date} | status={status}")
                if status == 200:
                    print(f"    -> Keys: {keys}")
                    print(f"    -> Preview: {text[:300]}...")

    print()
    print("=" * 80)
    print("STEP 2: Testing ALL endpoints on BOTH servers")
    print("=" * 80)

    test_date = working_date or DATES[0]

    results_200 = []
    results_other = []

    for server_name, base_url in SERVERS.items():
        print(f"\n{'─' * 40}")
        print(f"SERVER: {server_name} | DATE: {test_date} | R{REUNION}/C{COURSE}")
        print(f"{'─' * 40}")

        for endpoint in ENDPOINTS:
            url = base_url.format(client=1, date=test_date, r=REUNION, c=COURSE) + endpoint
            label = endpoint if endpoint else "(base course info)"
            status, text, keys = test_endpoint(url)

            if status == 200:
                print(f"\n  [200] {label}")
                print(f"    URL: {url}")
                print(f"    Top-level keys: {keys}")
                print(f"    Preview: {text[:500]}")
                results_200.append((server_name, label, keys))
            else:
                print(f"  [{status}] {label}")
                results_other.append((server_name, label, status))

            time.sleep(0.15)  # polite delay

    # Also test the second date if different results expected
    if len(DATES) > 1:
        alt_date = DATES[1] if test_date == DATES[0] else DATES[0]
        print(f"\n{'─' * 40}")
        print(f"QUICK CHECK: alternate date {alt_date} on offline (base + participants + arrivee)")
        print(f"{'─' * 40}")
        for ep in ["", "participants", "arrivee", "rapports-definitifs"]:
            url = SERVERS["offline"].format(client=1, date=alt_date, r=REUNION, c=COURSE) + ep
            label = ep if ep else "(base)"
            status, text, keys = test_endpoint(url)
            if status == 200:
                print(f"  [200] {label} -> keys: {keys}")
                print(f"    Preview: {text[:300]}")
            else:
                print(f"  [{status}] {label}")

    print()
    print("=" * 80)
    print("STEP 3: Testing different CLIENT IDs on offline server")
    print("=" * 80)

    for client_id in CLIENT_IDS:
        url = SERVERS["offline"].format(client=client_id, date=test_date, r=REUNION, c=COURSE)
        status, text, keys = test_endpoint(url)
        print(f"\n  Client {client_id} | status={status}")
        if status == 200:
            print(f"    Keys: {keys}")
            print(f"    Preview: {text[:500]}")

        # Also test participants with different clients
        url_p = url + "participants"
        status_p, text_p, keys_p = test_endpoint(url_p)
        print(f"  Client {client_id} /participants | status={status_p}")
        if status_p == 200:
            print(f"    Keys: {keys_p}")
            print(f"    Preview: {text_p[:500]}")

        time.sleep(0.2)

    # Try some additional programme-level endpoints
    print()
    print("=" * 80)
    print("STEP 4: Testing programme-level endpoints (no course)")
    print("=" * 80)

    programme_endpoints = [
        f"https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{test_date}",
        f"https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{test_date}/R{REUNION}",
        f"https://online.turfinfo.api.pmu.fr/rest/client/1/programme/{test_date}",
        f"https://online.turfinfo.api.pmu.fr/rest/client/1/programme/{test_date}/R{REUNION}",
    ]

    for url in programme_endpoints:
        status, text, keys = test_endpoint(url)
        short_url = url.split("pmu.fr")[1]
        if status == 200:
            print(f"\n  [200] {short_url}")
            print(f"    Keys: {keys}")
            print(f"    Preview: {text[:400]}")
        else:
            print(f"  [{status}] {short_url}")
        time.sleep(0.15)

    # Summary
    print()
    print("=" * 80)
    print("SUMMARY: Endpoints returning 200")
    print("=" * 80)
    for server, label, keys in results_200:
        print(f"  {server:8s} | {label:40s} | keys: {keys}")

    print()
    print("SUMMARY: Endpoints NOT returning 200")
    print("=" * 80)
    for server, label, status in results_other:
        print(f"  {server:8s} | {label:40s} | {status}")


if __name__ == "__main__":
    main()
