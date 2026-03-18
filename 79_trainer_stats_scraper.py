#!/usr/bin/env python3
"""
Script 79 — Scraping Trainer Performance Statistics
Sources : racingpost.com, attheraces.com, sportinglife.com, timeform.com, france-galop.com
Collecte : statistiques entraineurs (win%, place%, ROI, par hippodrome,
           par distance, par type de terrain, saisonnalite, strike rate)
CRITIQUE pour : Trainer Model, Form Analysis, Stable Confidence
"""

import argparse
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

SCRIPT_NAME = "79_trainer_stats"
OUTPUT_DIR = os.path.join("output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Sources et leurs URLs de base
SOURCES_CONFIG = {
    "racing_post": {
        "base": "https://www.racingpost.com",
        "trainers_url": "/trainers",
        "lang": "en-GB",
    },
    "sporting_life": {
        "base": "https://www.sportinglife.com",
        "trainers_url": "/racing/trainers",
        "lang": "en-GB",
    },
    "at_the_races": {
        "base": "https://www.attheraces.com",
        "trainers_url": "/trainers",
        "lang": "en-GB",
    },
    "timeform": {
        "base": "https://www.timeform.com",
        "trainers_url": "/horse-racing/trainers",
        "lang": "en-GB",
    },
    "france_galop": {
        "base": "https://www.france-galop.com",
        "trainers_url": "/fr/entraineurs",
        "lang": "fr-FR",
    },
}

# Entraineurs majeurs a scraper en priorite (FR + UK + IRE)
TOP_TRAINERS_FR = [
    "andre-fabre", "jean-claude-rouget", "christophe-ferland", "francis-graffard",
    "frederic-head", "alain-de-royer-dupre", "pascal-bary", "carlos-laffon-parias",
    "yann-barberot", "philippe-sogorb", "stephane-wattel", "didier-guillemin",
    "fabrice-chappet", "jerome-reynier", "cedric-rossi", "mikel-delzangles",
    "nicolas-clement", "henri-alex-pantall", "charley-rossi", "david-menuisier",
]

TOP_TRAINERS_UK = [
    "john-gosden", "aidan-obrien", "charlie-appleby", "william-haggas",
    "andrew-balding", "roger-varian", "mark-johnston", "sir-michael-stoute",
    "ralph-beckett", "richard-hannon", "clive-cox", "richard-fahey",
    "charlie-hills", "hugo-palmer", "simon-crisford", "james-fanshawe",
    "nicky-henderson", "paul-nicholls", "willie-mullins", "gordon-elliott",
    "dan-skelton", "henry-de-bromhead", "nigel-twiston-davies", "philip-hobbs",
]


def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9,fr-FR;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    })
    return s


def smart_pause(base=2.5, jitter=1.5):
    pause = base + random.uniform(-jitter, jitter)
    if random.random() < 0.08:
        pause += random.uniform(5, 15)
    time.sleep(max(1.0, pause))


def fetch_with_retry(session, url, max_retries=3, timeout=30):
    """GET avec retry automatique (3 essais puis skip)."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning(f"  429 Too Many Requests, pause {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                log.warning(f"  403 Forbidden sur {url}, pause 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} sur {url} (essai {attempt}/{max_retries})")
                time.sleep(5 * attempt)
                continue
            return resp
        except requests.RequestException as e:
            log.warning(f"  Erreur reseau: {e} (essai {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    log.error(f"  Echec apres {max_retries} essais: {url}")
    return None


def append_jsonl(filepath, record):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape_trainer_list(session, source_key):
    """Scraper la liste des entraineurs depuis une source."""
    config = SOURCES_CONFIG[source_key]
    url = f"{config['base']}{config['trainers_url']}"

    cache_file = os.path.join(CACHE_DIR, f"list_{source_key}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    resp = fetch_with_retry(session, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    trainers = []

    # Extraire depuis les tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            trainer = {
                "source": source_key,
                "type": "trainer_list",
                "scraped_at": datetime.utcnow().isoformat(),
            }
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                trainer[key] = cell

            link = row.find("a", href=True)
            if link:
                trainer["url_profil"] = link["href"] if link["href"].startswith("http") else f"{config['base']}{link['href']}"
                trainer["nom"] = link.get_text(strip=True)

            if trainer.get("nom"):
                trainers.append(trainer)

    # Extraire depuis les liens
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)
        if any(kw in href.lower() for kw in ["trainer", "entraineur"]) and text and len(text) > 3:
            full_url = href if href.startswith("http") else f"{config['base']}{href}"
            if not any(t.get("url_profil") == full_url for t in trainers):
                trainers.append({
                    "source": source_key,
                    "type": "trainer_link",
                    "nom": text,
                    "url_profil": full_url,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(trainers, f, ensure_ascii=False, indent=2)

    return trainers


def scrape_trainer_profile(session, trainer_url, trainer_name, source_key):
    """Scraper le profil complet d'un entraineur."""
    url_hash = re.sub(r'[^a-zA-Z0-9]', '_', trainer_url[-80:])
    cache_file = os.path.join(CACHE_DIR, f"profile_{source_key}_{url_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    if not trainer_url.startswith("http"):
        config = SOURCES_CONFIG.get(source_key, {})
        trainer_url = f"{config.get('base', '')}{trainer_url}"

    resp = fetch_with_retry(session, trainer_url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    profile = {
        "source": source_key,
        "type": "trainer_profile",
        "nom": trainer_name,
        "url": trainer_url,
        "scraped_at": datetime.utcnow().isoformat(),
    }

    # Extraire toutes les paires cle/valeur
    for dt in soup.find_all(["dt", "th", "label", "strong"]):
        dd = dt.find_next_sibling(["dd", "td", "span", "div"])
        if dd:
            key = dt.get_text(strip=True).lower().replace(" ", "_").replace(":", "")
            val = dd.get_text(strip=True)
            if key and val and len(key) < 50:
                profile[key] = val

    # Statistiques globales
    stats_patterns = {
        "wins": r'(\d+)\s*win',
        "runs": r'(\d+)\s*run',
        "strike_rate": r'strike\s*rate[:\s]*(\d+\.?\d*)%?',
        "win_pct": r'win[:\s]*(\d+\.?\d*)%',
        "place_pct": r'place[:\s]*(\d+\.?\d*)%',
        "roi": r'roi[:\s]*([+-]?\d+\.?\d*)',
        "prize_money": r'prize\s*money[:\s]*[\$\xa3]?([\d,]+)',
        "earnings": r'(gains|earnings)[:\s]*[\$\xa3]?([\d,]+)',
    }

    page_text = soup.get_text()
    for stat_key, pattern in stats_patterns.items():
        match = re.search(pattern, page_text, re.I)
        if match:
            profile[stat_key] = match.group(1) if stat_key != "earnings" else match.group(2)

    # Tables de stats detaillees (par distance, terrain, hippodrome, etc.)
    stats_tables = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        headers = []
        if rows:
            headers = [th.get_text(strip=True).lower().replace(" ", "_")
                       for th in rows[0].find_all(["th", "td"])]
        if len(headers) < 2:
            continue

        # Determiner le type de table
        header_text = " ".join(headers).lower()
        table_type = "unknown"
        if any(kw in header_text for kw in ["distance", "furlong", "mile"]):
            table_type = "by_distance"
        elif any(kw in header_text for kw in ["going", "ground", "terrain"]):
            table_type = "by_going"
        elif any(kw in header_text for kw in ["course", "track", "hippodrome"]):
            table_type = "by_course"
        elif any(kw in header_text for kw in ["class", "grade", "group"]):
            table_type = "by_class"
        elif any(kw in header_text for kw in ["month", "season", "year"]):
            table_type = "by_period"
        elif any(kw in header_text for kw in ["jockey", "rider"]):
            table_type = "by_jockey"

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells or len(cells) < 2:
                continue
            stat_row = {"table_type": table_type}
            for j, cell in enumerate(cells):
                key = headers[j] if j < len(headers) and headers[j] else f"col_{j}"
                stat_row[key] = cell

            # Extraire win%
            for cell in cells:
                pct_match = re.search(r'(\d+\.?\d*)%', cell)
                if pct_match:
                    stat_row["pct_value"] = float(pct_match.group(1))
                    break

            stats_tables.append(stat_row)

    if stats_tables:
        profile["stats_detail"] = stats_tables

    # Derniers resultats / forme recente
    recent = []
    for el in soup.find_all(["div", "li", "tr"], class_=True):
        classes = " ".join(el.get("class", []))
        if any(kw in classes.lower() for kw in ["result", "recent", "form", "runner"]):
            text = el.get_text(strip=True)
            if text and 5 < len(text) < 500:
                recent.append(text)
    if recent:
        profile["recent_results"] = recent[:30]  # Max 30

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)

    return profile


def scrape_trainer_by_slug(session, slug, source_key):
    """Scraper un entraineur par son slug (nom dans l'URL)."""
    config = SOURCES_CONFIG[source_key]
    url = f"{config['base']}{config['trainers_url']}/{slug}"
    return scrape_trainer_profile(session, url, slug.replace("-", " ").title(), source_key)


def main():
    parser = argparse.ArgumentParser(description="Script 79 — Trainer Stats Scraper")
    parser.add_argument("--sources", nargs="+",
                        default=["racing_post", "sporting_life", "at_the_races", "timeform", "france_galop"],
                        help="Sources a scraper")
    parser.add_argument("--mode", choices=["list", "profiles", "all"], default="all",
                        help="Mode: list (listes seulement), profiles (profils detailles), all")
    parser.add_argument("--top-only", action="store_true", default=False,
                        help="Scraper uniquement les top trainers connus")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 79 — Trainer Stats Scraper")
    log.info(f"  Sources : {args.sources}")
    log.info(f"  Mode : {args.mode}")
    log.info(f"  Top only : {args.top_only}")
    log.info("=" * 60)

    checkpoint = load_checkpoint()
    session = new_session()
    output_file = os.path.join(OUTPUT_DIR, "trainer_stats.jsonl")

    total_records = checkpoint.get("total_records", 0)
    processed_trainers = set(checkpoint.get("processed_trainers", []))

    # --- Phase 1: Listes ---
    all_trainer_urls = []

    if args.mode in ("list", "all"):
        log.info("--- Phase 1: Listes d'entraineurs ---")
        for source_key in args.sources:
            if source_key not in SOURCES_CONFIG:
                log.warning(f"  Source inconnue: {source_key}")
                continue

            log.info(f"  Source: {source_key}")
            trainers = scrape_trainer_list(session, source_key)
            for t in trainers:
                append_jsonl(output_file, t)
                total_records += 1
                if t.get("url_profil"):
                    all_trainer_urls.append({
                        "url": t["url_profil"],
                        "nom": t.get("nom", ""),
                        "source": source_key,
                    })
            log.info(f"    -> {len(trainers)} entraineurs")
            smart_pause(2.0, 1.0)

    # --- Phase 2: Profils detailles ---
    if args.mode in ("profiles", "all"):
        log.info("--- Phase 2: Profils detailles ---")

        # Ajouter les top trainers connus
        if args.top_only or not all_trainer_urls:
            for slug in TOP_TRAINERS_FR:
                all_trainer_urls.append({
                    "url": f"{SOURCES_CONFIG['france_galop']['base']}/fr/entraineurs/{slug}",
                    "nom": slug.replace("-", " ").title(),
                    "source": "france_galop",
                })
            for slug in TOP_TRAINERS_UK:
                for src in ["racing_post", "sporting_life"]:
                    if src in SOURCES_CONFIG:
                        all_trainer_urls.append({
                            "url": f"{SOURCES_CONFIG[src]['base']}{SOURCES_CONFIG[src]['trainers_url']}/{slug}",
                            "nom": slug.replace("-", " ").title(),
                            "source": src,
                        })
                        break

        # Aussi essayer les slugs directement sur chaque source
        for source_key in args.sources:
            if source_key not in SOURCES_CONFIG:
                continue
            trainers_list = TOP_TRAINERS_FR if "france" in source_key else TOP_TRAINERS_UK
            for slug in trainers_list:
                key = f"{source_key}_{slug}"
                if key not in processed_trainers:
                    all_trainer_urls.append({
                        "url": f"{SOURCES_CONFIG[source_key]['base']}{SOURCES_CONFIG[source_key]['trainers_url']}/{slug}",
                        "nom": slug.replace("-", " ").title(),
                        "source": source_key,
                    })

        # Deduplication par URL
        seen_urls = set()
        unique_trainers = []
        for t in all_trainer_urls:
            if t["url"] not in seen_urls:
                seen_urls.add(t["url"])
                unique_trainers.append(t)

        log.info(f"  {len(unique_trainers)} profils a scraper")

        trainer_count = 0
        for t in unique_trainers:
            key = f"{t['source']}_{t['nom']}"
            if key in processed_trainers:
                continue

            log.info(f"  [{trainer_count + 1}/{len(unique_trainers)}] {t['nom']} ({t['source']})")
            profile = scrape_trainer_profile(session, t["url"], t["nom"], t["source"])

            if profile:
                append_jsonl(output_file, profile)
                total_records += 1
                processed_trainers.add(key)

            trainer_count += 1
            smart_pause(2.0, 1.0)

            if trainer_count % 20 == 0:
                log.info(f"  Progression: {trainer_count} profils, {total_records} records")
                save_checkpoint({
                    "processed_trainers": list(processed_trainers),
                    "total_records": total_records,
                })

            if trainer_count % 50 == 0:
                session.close()
                session = new_session()
                time.sleep(random.uniform(5, 15))

    save_checkpoint({
        "processed_trainers": list(processed_trainers),
        "total_records": total_records,
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINE: {len(processed_trainers)} entraineurs, {total_records} records -> {output_file}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
