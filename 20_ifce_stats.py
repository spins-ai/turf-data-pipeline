#!/usr/bin/env python3
"""
Script 20 — IFCE Stats & Cartes
Source : https://statscartes.ifce.fr/
Collecte les statistiques officielles de la filière courses (JSON API interne)
"""

import requests
import json
import time
import random
import os
from datetime import datetime

OUTPUT_DIR = "output/20_ifce_stats"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Referer": "https://statscartes.ifce.fr/",
    })
    return s

def smart_pause(base=2.0, jitter=1.0):
    pause = base + random.uniform(-jitter, jitter)
    time.sleep(max(0.5, pause))

def fetch_json(session, url, cache_name):
    """Récupérer un JSON avec cache"""
    cache_file = os.path.join(CACHE_DIR, f"{cache_name}.json")
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            return json.load(f)

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            try:
                data = resp.json()
            except:
                data = {"html": resp.text[:5000], "status": resp.status_code}

            with open(cache_file, "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return data
        else:
            print(f"  {url} -> {resp.status_code}")
            return None
    except Exception as e:
        print(f"  Erreur {url}: {e}")
        return None

def main():
    print("=" * 60)
    print("SCRIPT 20 — IFCE Stats & Cartes")
    print("=" * 60)

    session = new_session()

    # Dashboard IDs connus (courses hippiques)
    dashboards = {
        "courses_trot": 47,
        "courses_galop": 48,
        "elevage": 49,
        "general": 1,
    }

    # URLs à tester
    base_urls = [
        "https://statscartes.ifce.fr",
        "https://statscartes.ifce.fr/api",
        "https://statscartes.ifce.fr/dashboard",
    ]

    all_data = {}

    # [1/4] Explorer les dashboards
    print("\n[1/4] Exploration des dashboards IFCE...")
    for name, dash_id in dashboards.items():
        # Tester différents formats d'URL
        urls_to_try = [
            f"https://statscartes.ifce.fr/dashboard/{dash_id}",
            f"https://statscartes.ifce.fr/api/dashboard/{dash_id}",
            f"https://statscartes.ifce.fr/api/v1/dashboard/{dash_id}",
        ]

        for url in urls_to_try:
            data = fetch_json(session, url, f"dashboard_{name}_{dash_id}")
            if data:
                all_data[f"dashboard_{name}"] = data
                print(f"  ✓ {name} (ID {dash_id}): {type(data).__name__}")
                break
            smart_pause(1, 0.5)

    # [2/4] Chercher les APIs de données
    print("\n[2/4] Recherche d'APIs de données...")
    api_endpoints = [
        ("hippodromes", "/api/hippodromes"),
        ("entraineurs", "/api/entraineurs"),
        ("jockeys", "/api/jockeys"),
        ("eleveurs", "/api/eleveurs"),
        ("proprietaires", "/api/proprietaires"),
        ("courses_stats", "/api/courses/stats"),
        ("regions", "/api/regions"),
        ("departements", "/api/departements"),
        # Endpoints Metabase (souvent utilisé par les dashboards IFCE)
        ("metabase_datasets", "/api/dataset"),
        ("metabase_cards", "/api/card"),
    ]

    for name, endpoint in api_endpoints:
        url = f"https://statscartes.ifce.fr{endpoint}"
        data = fetch_json(session, url, f"api_{name}")
        if data:
            all_data[f"api_{name}"] = data
            if isinstance(data, list):
                print(f"  ✓ {name}: {len(data)} entrées")
            elif isinstance(data, dict):
                print(f"  ✓ {name}: {len(data)} clés")
        smart_pause(1.5, 0.8)

    # [3/4] Page principale pour découvrir d'autres endpoints
    print("\n[3/4] Analyse de la page principale...")
    resp = session.get("https://statscartes.ifce.fr/", timeout=30)
    if resp.status_code == 200:
        # Chercher des URLs d'API dans le HTML/JS
        import re
        api_urls = re.findall(r'(?:api|data|json)["\s]*:\s*["\']([^"\']+)["\']', resp.text)
        fetch_urls = re.findall(r'fetch\(["\']([^"\']+)["\']', resp.text)
        xhr_urls = re.findall(r'(?:get|post)\(["\']([^"\']+)["\']', resp.text, re.IGNORECASE)

        all_found_urls = set(api_urls + fetch_urls + xhr_urls)
        if all_found_urls:
            print(f"  URLs trouvées dans le code: {len(all_found_urls)}")
            for found_url in list(all_found_urls)[:20]:
                if found_url.startswith("/"):
                    full_url = f"https://statscartes.ifce.fr{found_url}"
                    data = fetch_json(session, full_url, f"discovered_{found_url.replace('/', '_')}")
                    if data:
                        all_data[f"discovered_{found_url}"] = data
                        print(f"  ✓ {found_url}")
                    smart_pause(1, 0.5)

        # Sauver la page pour analyse manuelle
        with open(os.path.join(OUTPUT_DIR, "page_principale.html"), "w") as f:
            f.write(resp.text)

    # [4/4] Info Chevaux IFCE - explorer l'API
    print("\n[4/4] Exploration Info Chevaux IFCE...")
    info_endpoints = [
        ("infochevaux_search", "https://infochevaux.ifce.fr/api/search"),
        ("infochevaux_breeds", "https://infochevaux.ifce.fr/api/breeds"),
        ("infochevaux_races", "https://infochevaux.ifce.fr/api/races"),
    ]

    for name, url in info_endpoints:
        data = fetch_json(session, url, name)
        if data:
            all_data[name] = data
            print(f"  ✓ {name}")
        smart_pause(2, 1)

    # Sauvegarde finale
    print(f"\n=== SAUVEGARDE ===")

    output_file = os.path.join(OUTPUT_DIR, "ifce_stats_all.json")
    with open(output_file, "w") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    # Résumé
    print(f"\nRésumé:")
    for key, val in all_data.items():
        if isinstance(val, list):
            print(f"  {key}: {len(val)} entrées")
        elif isinstance(val, dict):
            print(f"  {key}: {len(val)} clés")
        else:
            print(f"  {key}: {type(val).__name__}")

    print(f"\nTERMINÉ: {len(all_data)} datasets sauvegardés dans {output_file}")

if __name__ == "__main__":
    main()
