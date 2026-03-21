#!/usr/bin/env python3
"""
14_pedigree_scraper.py
======================
Scrape les pedigrees complets des pur-sang depuis PedigreeQuery.com.

Pour chaque cheval PUR-SANG unique dans partants_normalises.json, recherche
sur PedigreeQuery.com et extrait l'arbre genealogique complet (4 generations).

PATCH JSONL : streaming des partants + append JSONL, ~15 MB RAM au lieu de 2.7 GB

Input :
  - output/02_liste_courses/partants_normalises.json (ou .jsonl)

Output : output/14_pedigree/
  - pedigrees_pq.jsonl (append mode)
  - cache/{nom_normalise}.json  (un fichier par cheval)
  - checkpoint.json              (progression)

Usage :
    python3 14_pedigree_scraper.py
    python3 14_pedigree_scraper.py --max 100       # tester sur 100 chevaux
    python3 14_pedigree_scraper.py --pause 2.0     # pause 2s entre requetes
    python3 14_pedigree_scraper.py --batch 200      # checkpoint tous les 200
    python3 14_pedigree_scraper.py --help
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from bs4 import BeautifulSoup

    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False


# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_PATH = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "partants_normalises.json"
PARTANTS_JSONL_PATH = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "partants_normalises.jsonl"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "14_pedigree"
CACHE_DIR = OUTPUT_DIR / "cache"
CHECKPOINT_PATH = OUTPUT_DIR / "checkpoint.json"
OUTPUT_JSONL = OUTPUT_DIR / "pedigrees_pq.jsonl"

from utils.logging_setup import setup_logging

BASE_URL = "https://www.pedigreequery.com"
REQUEST_PAUSE_S = 1.0
REQUEST_TIMEOUT_S = 15
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 "
    "(research project)"
)


# ===========================================================================
# DATACLASS
# ===========================================================================

@dataclass
class PedigreeRecord:
    nom_cheval: str
    horse_id: str            # MD5 hash from nom+pere+mere
    # Parents
    pere: str = ""
    mere: str = ""
    pere_mere: str = ""      # maternal grandsire (pere de la mere)
    # Grands-parents paternels
    grand_pere_paternel: str = ""   # pere du pere
    grand_mere_paternelle: str = "" # mere du pere
    # Grands-parents maternels
    grand_pere_maternel: str = ""   # pere de la mere (= pere_mere)
    grand_mere_maternelle: str = "" # mere de la mere
    # Arriere-grands-parents paternels (branche pere)
    arriere_gpp_pp: str = ""   # pere du pere du pere
    arriere_gpm_pp: str = ""   # mere du pere du pere
    arriere_gpp_mp: str = ""   # pere de la mere du pere
    arriere_gpm_mp: str = ""   # mere de la mere du pere
    # Arriere-grands-parents maternels (branche mere)
    arriere_gpp_pm: str = ""   # pere du pere de la mere
    arriere_gpm_pm: str = ""   # mere du pere de la mere
    arriere_gpp_mm: str = ""   # pere de la mere de la mere
    arriere_gpm_mm: str = ""   # mere de la mere de la mere
    # Meta
    source: str = "pedigreequery"
    found: bool = False
    timestamp_collecte: str = ""


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normaliser_texte(texte: str) -> str:
    """Normalise en minuscules sans accents."""
    if not texte:
        return ""
    texte = texte.strip().lower()
    nfkd = unicodedata.normalize("NFKD", texte)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def make_horse_id(nom: str, pere: str, mere: str) -> str:
    """Hash MD5 stable pour identifier un cheval unique."""
    key = "|".join(normaliser_texte(s) for s in [nom, pere, mere])
    return hashlib.md5(key.encode()).hexdigest()


def slugify_pq(name: str) -> str:
    """Convertit un nom de cheval en slug pour PedigreeQuery."""
    name = normaliser_texte(name)
    name = re.sub(r"[''`]", "", name)
    name = re.sub(r"[^a-z0-9]+", "+", name)
    name = re.sub(r"\++", "+", name).strip("+")
    return name


def cache_key_for_horse(nom: str, pere: str, mere: str) -> str:
    """Cle de cache basee sur le nom normalise + hash court pour unicite."""
    slug = slugify_pq(nom)
    hid = make_horse_id(nom, pere, mere)[:8]
    return f"{slug}_{hid}"


# ===========================================================================
# HTTP SESSION
# ===========================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,fr;q=0.5",
    })
    return session


def safe_get(
    session: requests.Session,
    url: str,
    logger: logging.Logger,
    timeout: int = REQUEST_TIMEOUT_S,
) -> Optional[requests.Response]:
    """GET avec gestion des erreurs. Retourne None si echec."""
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code == 404:
            logger.debug("404 Not Found: %s", url)
            return None
        if resp.status_code == 429:
            logger.warning("429 Too Many Requests: %s — pause 30s", url)
            time.sleep(30)
            return None
        resp.raise_for_status()
        return resp
    except requests.exceptions.ConnectionError as e:
        logger.warning("Connection error: %s — %s", url, e)
        return None
    except requests.exceptions.Timeout:
        logger.warning("Timeout: %s", url)
        return None
    except requests.exceptions.HTTPError as e:
        logger.warning("HTTP error: %s — %s", url, e)
        return None
    except Exception as e:
        logger.warning("Erreur inattendue GET %s: %s", url, e)
        return None


# ===========================================================================
# CACHE
# ===========================================================================

def load_cache_entry(cache_key: str) -> Optional[dict]:
    """Charge un enregistrement depuis le cache disque."""
    path = CACHE_DIR / f"{cache_key}.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
    return None


def save_cache_entry(cache_key: str, record: dict):
    """Sauvegarde un enregistrement dans le cache disque."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{cache_key}.json"
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)


# ===========================================================================
# CHECKPOINT
# ===========================================================================

def load_checkpoint() -> dict:
    """Charge l'etat du checkpoint."""
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"processed_ids": [], "last_index": 0, "total_records": 0, "started_at": "", "updated_at": ""}


def save_checkpoint(state: dict):
    """Sauvegarde le checkpoint."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = utc_now_iso()
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(CHECKPOINT_PATH)


# ===========================================================================
# PARSER PEDIGREEQUERY.COM
# ===========================================================================

def _clean_name(text: str) -> str:
    """Nettoie un nom de cheval extrait du HTML."""
    if not text:
        return ""
    name = re.sub(r"\s*\([A-Z]{2,3}\)\s*$", "", text.strip())
    name = re.sub(r"^\d+\.\s*", "", name)
    return name.strip().upper()


def parse_pedigreequery_page(
    html: str,
    nom: str,
    pere: str,
    mere: str,
    horse_id: str,
    logger: logging.Logger,
) -> PedigreeRecord:
    """Parse la page PedigreeQuery.com pour extraire le pedigree complet."""
    record = PedigreeRecord(
        nom_cheval=nom,
        horse_id=horse_id,
        pere=pere,
        mere=mere,
        source="pedigreequery",
        found=False,
        timestamp_collecte=utc_now_iso(),
    )

    soup = BeautifulSoup(html, "html.parser")

    page_text = soup.get_text(separator=" ", strip=True).lower()
    if "no horse found" in page_text or "did not match any" in page_text:
        logger.debug("Cheval non trouve sur PedigreeQuery: %s", nom)
        return record

    pedigree_table = _find_pedigree_table(soup, logger)

    if pedigree_table is None:
        logger.debug("Aucun tableau de pedigree trouve pour: %s", nom)
        return record

    names = _extract_names_from_table(pedigree_table, logger)

    if not names:
        logger.debug("Aucun nom extrait du tableau pour: %s", nom)
        return record

    _assign_pedigree_positions(names, record, pere, mere, logger)

    if record.pere or record.grand_pere_paternel or record.pere_mere:
        record.found = True

    return record


def _find_pedigree_table(soup: BeautifulSoup, logger: logging.Logger):
    """Trouve le tableau de pedigree dans la page."""
    for attr in ("class", "id"):
        table = soup.find("table", attrs={attr: re.compile(r"pedigree|ped|chart|tree", re.I)})
        if table:
            return table

    tables = soup.find_all("table")
    best_table = None
    best_score = 0

    for table in tables:
        cells_with_rowspan = table.find_all(["td", "th"], attrs={"rowspan": True})
        score = len(cells_with_rowspan)
        links = table.find_all("a")
        score += len(links) * 0.5
        text = table.get_text(separator=" ", strip=True).lower()
        if len(text) > 50:
            score += 1

        if score > best_score:
            best_score = score
            best_table = table

    if best_score >= 3:
        return best_table

    if tables:
        largest = max(tables, key=lambda t: len(t.get_text(strip=True)))
        if len(largest.get_text(strip=True)) > 50:
            return largest

    return None


def _extract_names_from_table(table, logger: logging.Logger) -> list[list]:
    """Extrait les noms de chevaux depuis le tableau de pedigree."""
    rows_data = []

    for row in table.find_all("tr"):
        row_names = []
        for cell in row.find_all(["td", "th"]):
            link = cell.find("a")
            if link:
                name = _clean_name(link.get_text(strip=True))
                if name and len(name) > 1:
                    rowspan = int(cell.get("rowspan", 1))
                    row_names.append((name, rowspan))
                    continue

            text = cell.get_text(strip=True)
            name = _clean_name(text)
            if name and len(name) > 1 and not re.match(r"^(sire|dam|pedigree|of|for)$", name, re.I):
                rowspan = int(cell.get("rowspan", 1))
                row_names.append((name, rowspan))

        if row_names:
            rows_data.append(row_names)

    return rows_data


def _assign_pedigree_positions(
    rows_data: list[list[tuple[str, int]]],
    record: PedigreeRecord,
    pere_connu: str,
    mere_connue: str,
    logger: logging.Logger,
):
    """Assigne les noms extraits aux positions du pedigree."""
    all_cells: list[tuple[str, int]] = []
    for row in rows_data:
        for name, rowspan in row:
            all_cells.append((name, rowspan))

    if not all_cells:
        return

    rowspan_values = sorted(set(rs for _, rs in all_cells), reverse=True)

    gen_map: dict[int, list[str]] = {}
    for name, rowspan in all_cells:
        gen = rowspan_values.index(rowspan)
        gen_map.setdefault(gen, []).append(name)

    logger.debug("Generations detectees: %s",
                 {g: len(names) for g, names in gen_map.items()})

    horse_norm = normaliser_texte(record.nom_cheval)
    pere_norm = normaliser_texte(pere_connu)
    mere_norm = normaliser_texte(mere_connue)

    offset = 0
    if gen_map.get(0):
        first_names_norm = [normaliser_texte(n) for n in gen_map[0]]
        if horse_norm in first_names_norm:
            offset = 1
        elif pere_norm and pere_norm in first_names_norm:
            offset = 0
        elif len(gen_map.get(0, [])) == 1 and len(gen_map) >= 4:
            offset = 1

    # Parents
    parents = gen_map.get(offset, [])
    if len(parents) >= 2:
        p0_norm = normaliser_texte(parents[0])
        if mere_norm and p0_norm == mere_norm:
            record.pere = parents[1] if not record.pere else record.pere
            record.mere = parents[0] if not record.mere else record.mere
        else:
            record.pere = parents[0] if not record.pere else record.pere
            record.mere = parents[1] if not record.mere else record.mere
    elif len(parents) == 1:
        if not record.pere:
            record.pere = parents[0]

    # Grands-parents
    grandparents = gen_map.get(offset + 1, [])
    if len(grandparents) >= 4:
        record.grand_pere_paternel = grandparents[0]
        record.grand_mere_paternelle = grandparents[1]
        record.grand_pere_maternel = grandparents[2]
        record.grand_mere_maternelle = grandparents[3]
        record.pere_mere = grandparents[2]
    elif len(grandparents) >= 2:
        record.grand_pere_paternel = grandparents[0]
        record.grand_mere_paternelle = grandparents[1]
        if len(grandparents) >= 3:
            record.grand_pere_maternel = grandparents[2]
            record.pere_mere = grandparents[2]
        if len(grandparents) >= 4:
            record.grand_mere_maternelle = grandparents[3]

    # Arriere-grands-parents
    great_gp = gen_map.get(offset + 2, [])
    if len(great_gp) >= 8:
        record.arriere_gpp_pp = great_gp[0]
        record.arriere_gpm_pp = great_gp[1]
        record.arriere_gpp_mp = great_gp[2]
        record.arriere_gpm_mp = great_gp[3]
        record.arriere_gpp_pm = great_gp[4]
        record.arriere_gpm_pm = great_gp[5]
        record.arriere_gpp_mm = great_gp[6]
        record.arriere_gpm_mm = great_gp[7]
    elif len(great_gp) >= 4:
        for idx, attr in enumerate([
            "arriere_gpp_pp", "arriere_gpm_pp", "arriere_gpp_mp", "arriere_gpm_mp",
            "arriere_gpp_pm", "arriere_gpm_pm", "arriere_gpp_mm", "arriere_gpm_mm",
        ]):
            if idx < len(great_gp):
                setattr(record, attr, great_gp[idx])

    logger.debug(
        "Pedigree %s: sire=%s dam=%s gps=%s gpm=%s ggpp=%s",
        record.nom_cheval,
        record.pere, record.mere,
        record.grand_pere_paternel, record.grand_pere_maternel,
        record.arriere_gpp_pp,
    )


# ===========================================================================
# FETCH PEDIGREE
# ===========================================================================

def fetch_pedigree(
    session: requests.Session,
    nom: str,
    pere: str,
    mere: str,
    horse_id: str,
    logger: logging.Logger,
) -> PedigreeRecord:
    """Recupere le pedigree d'un cheval depuis PedigreeQuery.com."""
    if not HAS_BS4:
        logger.error("beautifulsoup4 non installe — pip install beautifulsoup4")
        return PedigreeRecord(
            nom_cheval=nom, horse_id=horse_id, pere=pere, mere=mere,
            timestamp_collecte=utc_now_iso(),
        )

    slug = slugify_pq(nom)
    url = f"{BASE_URL}/{slug}"
    logger.debug("PedigreeQuery: %s", url)

    resp = safe_get(session, url, logger)
    if resp is None:
        return PedigreeRecord(
            nom_cheval=nom, horse_id=horse_id, pere=pere, mere=mere,
            timestamp_collecte=utc_now_iso(),
        )

    record = parse_pedigreequery_page(resp.text, nom, pere, mere, horse_id, logger)
    return record


# ===========================================================================
# EXTRACTION DES CHEVAUX UNIQUES PUR-SANG (STREAMING)
# ===========================================================================

def extract_unique_thoroughbreds_streaming(
    partants_path: Path, logger: logging.Logger
) -> list[dict]:
    """Extrait les chevaux PUR-SANG uniques depuis les partants — mode streaming.

    Supporte JSON et JSONL. Ne garde en mémoire que les chevaux uniques (~250K),
    pas les 2.7M partants.
    """
    seen: dict[str, dict] = {}
    total_ps = 0
    total_partants = 0

    # Essayer JSONL d'abord (plus leger)
    jsonl_path = partants_path.with_suffix(".jsonl")
    if jsonl_path.exists():
        logger.info("Lecture JSONL streaming: %s", jsonl_path)
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    p = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total_partants += 1
                _process_partant(p, seen, total_ps_counter=[total_ps])
                total_ps = _process_partant.last_count
    elif partants_path.exists():
        # Fallback JSON — lecture streaming ligne par ligne
        logger.info("Lecture JSON streaming: %s", partants_path)
        total_ps = _stream_json_partants(partants_path, seen, logger)
    else:
        logger.error("Aucun fichier partants trouvé: %s", partants_path)
        return []

    horses = sorted(seen.values(), key=lambda h: h["nom"])

    logger.info("Chevaux PUR-SANG uniques: %d", len(horses))
    with_pm = sum(1 for h in horses if h.get("pere_mere_pmu"))
    logger.info("  pere_mere deja connu (PMU): %d / %d (%.1f%%)",
                with_pm, len(horses), 100 * with_pm / len(horses) if horses else 0)

    return horses


def _stream_json_partants(path: Path, seen: dict, logger: logging.Logger) -> int:
    """Lit un gros JSON array en streaming sans charger tout en mémoire.

    Utilise ijson si disponible, sinon charge par chunks.
    """
    total_ps = 0
    try:
        import ijson
        logger.info("  Utilisation de ijson pour streaming JSON")
        with open(path, "rb") as f:
            for p in ijson.items(f, "item"):
                race = (p.get("race") or "").strip().upper()
                if race != "PUR-SANG":
                    continue
                total_ps += 1
                _add_horse(p, seen)
        return total_ps
    except ImportError:
        pass

    # Fallback: charger le JSON complet mais ne garder que les clés nécessaires
    logger.info("  ijson non disponible, chargement JSON complet (lent)...")
    logger.info("  Conseil: pip install ijson pour économiser la RAM")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for p in data:
        race = (p.get("race") or "").strip().upper()
        if race != "PUR-SANG":
            continue
        total_ps += 1
        _add_horse(p, seen)
    del data  # Libérer immédiatement
    return total_ps


def _add_horse(p: dict, seen: dict):
    """Ajoute un cheval PUR-SANG unique au dict seen."""
    nom = (p.get("nom_cheval") or "").strip()
    pere = (p.get("pere") or "").strip()
    mere = (p.get("mere") or "").strip()
    pere_mere_pmu = (p.get("pere_mere") or "").strip()

    if not nom:
        return

    horse_id = make_horse_id(nom, pere, mere)
    if horse_id not in seen:
        seen[horse_id] = {
            "nom": nom,
            "pere": pere,
            "mere": mere,
            "horse_id": horse_id,
            "pere_mere_pmu": pere_mere_pmu,
        }


# Wrapper pour compatibilité
def _process_partant(p: dict, seen: dict, total_ps_counter: list):
    race = (p.get("race") or "").strip().upper()
    if race == "PUR-SANG":
        total_ps_counter[0] += 1
        _add_horse(p, seen)
    _process_partant.last_count = total_ps_counter[0]
_process_partant.last_count = 0


# ===========================================================================
# MAIN LOOP — JSONL APPEND
# ===========================================================================

def run_scraping(
    horses: list[dict],
    session: requests.Session,
    logger: logging.Logger,
    pause: float = REQUEST_PAUSE_S,
    checkpoint_every: int = 500,
) -> dict:
    """Boucle principale de scraping — écrit en JSONL append, pas d'accumulation."""
    total = len(horses)
    stats = {
        "total": total,
        "from_cache": 0,
        "scraped_ok": 0,
        "scraped_fail": 0,
        "errors": 0,
    }

    # Charger le checkpoint
    checkpoint = load_checkpoint()
    processed_set = set(checkpoint.get("processed_ids", []))
    total_records = checkpoint.get("total_records", 0)
    if not checkpoint.get("started_at"):
        checkpoint["started_at"] = utc_now_iso()

    logger.info(
        "Demarrage du scraping: %d chevaux, %d deja traites dans le checkpoint",
        total, len(processed_set),
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    for i, horse in enumerate(horses):
        horse_id = horse["horse_id"]
        nom = horse["nom"]
        pere = horse["pere"]
        mere = horse["mere"]
        cache_key = cache_key_for_horse(nom, pere, mere)

        # Progression
        if (i + 1) % 500 == 0 or i == 0:
            elapsed = time.time() - t0
            processed_this_run = sum(
                stats[k] for k in ["from_cache", "scraped_ok", "scraped_fail", "errors"]
            )
            speed = processed_this_run / elapsed if elapsed > 0 else 0
            remaining = total - i - 1
            eta = remaining / speed if speed > 0 else 0
            logger.info(
                "Progression: %d/%d (%.1f%%) | cache=%d ok=%d fail=%d err=%d | "
                "%.1f chev/min | ETA: %.0f min",
                i + 1, total, 100 * (i + 1) / total,
                stats["from_cache"], stats["scraped_ok"], stats["scraped_fail"],
                stats["errors"],
                speed * 60, eta / 60,
            )

        # Verifier le cache disque
        cached = load_cache_entry(cache_key)
        if cached is not None:
            # Écrire en JSONL si pas encore dans le checkpoint
            if horse_id not in processed_set:
                _append_jsonl(cached)
                total_records += 1
                processed_set.add(horse_id)
            stats["from_cache"] += 1
            continue

        # Skip si deja traite dans cette session (checkpoint)
        if horse_id in processed_set:
            stats["from_cache"] += 1
            continue

        # Scraper
        try:
            record = fetch_pedigree(session, nom, pere, mere, horse_id, logger)
            record_dict = asdict(record)

            # Si PQ n'a pas trouve le pere_mere, utiliser celui du PMU
            if not record_dict.get("pere_mere") and horse.get("pere_mere_pmu"):
                record_dict["pere_mere"] = horse["pere_mere_pmu"]
                record_dict["grand_pere_maternel"] = horse["pere_mere_pmu"]

            # Sauver dans le cache
            save_cache_entry(cache_key, record_dict)

            # Append JSONL — pas d'accumulation en mémoire
            _append_jsonl(record_dict)
            total_records += 1

            if record.found:
                stats["scraped_ok"] += 1
            else:
                stats["scraped_fail"] += 1

        except Exception as e:
            logger.error("Erreur sur %s (id=%s): %s", nom, horse_id, e)
            stats["errors"] += 1
            fallback = PedigreeRecord(
                nom_cheval=nom, horse_id=horse_id, pere=pere, mere=mere,
                pere_mere=horse.get("pere_mere_pmu", ""),
                grand_pere_maternel=horse.get("pere_mere_pmu", ""),
                timestamp_collecte=utc_now_iso(),
            )
            fb_dict = asdict(fallback)
            save_cache_entry(cache_key, fb_dict)
            _append_jsonl(fb_dict)
            total_records += 1

        # Mettre a jour le checkpoint
        processed_set.add(horse_id)
        if (i + 1) % checkpoint_every == 0:
            checkpoint["processed_ids"] = list(processed_set)
            checkpoint["last_index"] = i + 1
            checkpoint["total_records"] = total_records
            save_checkpoint(checkpoint)
            logger.debug("Checkpoint sauve a l'index %d", i + 1)

        # Pause entre les requetes
        time.sleep(pause)

    # Checkpoint final
    checkpoint["processed_ids"] = list(processed_set)
    checkpoint["last_index"] = total
    checkpoint["total_records"] = total_records
    save_checkpoint(checkpoint)

    # Log final
    elapsed = time.time() - t0
    logger.info("-" * 50)
    logger.info("SCRAPING TERMINE en %.1f min", elapsed / 60)
    logger.info("  Total chevaux         : %d", stats["total"])
    logger.info("  Depuis le cache       : %d", stats["from_cache"])
    logger.info("  Scrapes reussis       : %d", stats["scraped_ok"])
    logger.info("  Scrapes echoues       : %d", stats["scraped_fail"])
    logger.info("  Erreurs               : %d", stats["errors"])
    logger.info("  Total records JSONL   : %d", total_records)
    attempted = stats["scraped_ok"] + stats["scraped_fail"]
    taux = stats["scraped_ok"] / attempted if attempted > 0 else 0
    logger.info("  Taux de succes scrape : %.1f%%", 100 * taux)

    return stats


def _append_jsonl(record: dict):
    """Append un record au fichier JSONL."""
    with open(OUTPUT_JSONL, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape les pedigrees des pur-sang depuis PedigreeQuery.com."
    )
    parser.add_argument(
        "--partants", type=str, default=str(PARTANTS_PATH),
        help=f"Chemin vers partants_normalises.json (defaut: {PARTANTS_PATH})",
    )
    parser.add_argument(
        "--pause", type=float, default=REQUEST_PAUSE_S,
        help=f"Pause entre requetes en secondes (defaut: {REQUEST_PAUSE_S})",
    )
    parser.add_argument(
        "--batch", type=int, default=500,
        help="Frequence de sauvegarde du checkpoint (defaut: 500)",
    )
    parser.add_argument(
        "--max", type=int, default=0,
        help="Limiter le nombre de chevaux a traiter (0 = tous)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Mode debug",
    )
    args = parser.parse_args()

    logger = setup_logging("14_pedigree_scraper")
    logger.info("=" * 70)
    logger.info("14 — PEDIGREE SCRAPER PEDIGREEQUERY.COM (PUR-SANG) — MODE JSONL")
    logger.info("=" * 70)

    # Verifier les dependances
    if not HAS_BS4:
        logger.error("beautifulsoup4 requis: pip install beautifulsoup4")
        sys.exit(1)

    # Charger partants en streaming
    partants_path = Path(args.partants)
    horses = extract_unique_thoroughbreds_streaming(partants_path, logger)

    if args.max > 0:
        horses = horses[:args.max]
        logger.info("Limite appliquee: %d chevaux", len(horses))

    if not horses:
        logger.warning("Aucun cheval PUR-SANG a traiter. Fin.")
        sys.exit(0)

    # Creer la session HTTP
    session = create_session()

    # Scraping
    logger.info("-" * 50)
    logger.info("Source: PedigreeQuery.com")
    logger.info("Pause: %.1fs | Timeout: %ds | Retries: %d",
                args.pause, REQUEST_TIMEOUT_S, MAX_RETRIES)
    logger.info("Checkpoint: tous les %d chevaux", args.batch)
    logger.info("Output: %s (JSONL append)", OUTPUT_JSONL)
    logger.info("-" * 50)

    stats = run_scraping(
        horses=horses,
        session=session,
        logger=logger,
        pause=args.pause,
        checkpoint_every=args.batch,
    )

    logger.info("=" * 70)
    logger.info("TERMINE — %d records ecrits dans %s", stats.get("total", 0), OUTPUT_JSONL)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
