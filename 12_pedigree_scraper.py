#!/usr/bin/env python3
"""
12_pedigree_scraper.py
======================
Enrichit les donnees de pedigree des chevaux en scrappant des sources en ligne.

Sources (par ordre de priorite) :
  1. Le Trot — fiche cheval (trot)
  2. IFCE InfoChevaux (trot + galop)
  3. France Sire (galop)

Input :
  - output/02_liste_courses/partants_normalises.json

Output : output/12_pedigree/
  - pedigrees_enrichis.json / .parquet / .csv
  - cache/{horse_hash}.json  (un fichier par cheval)
  - checkpoint.json           (progression)

Usage :
    python3 12_pedigree_scraper.py
    python3 12_pedigree_scraper.py --limit 100        # tester sur 100 chevaux
    python3 12_pedigree_scraper.py --source letrot     # forcer une source
    python3 12_pedigree_scraper.py --force-refresh     # ignorer le cache
    python3 12_pedigree_scraper.py --resume             # reprendre depuis le checkpoint
    python3 12_pedigree_scraper.py --help
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from bs4 import BeautifulSoup

    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False


# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_PATH = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "partants_normalises.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "12_pedigree"
CACHE_DIR = OUTPUT_DIR / "cache"
CHECKPOINT_PATH = OUTPUT_DIR / "checkpoint.json"

from utils.logging_setup import setup_logging

# Scraping
REQUEST_PAUSE_S = 1.0          # pause entre requetes
REQUEST_TIMEOUT_S = 15         # timeout par requete
MAX_RETRIES = 3                # retries HTTP
BACKOFF_FACTOR = 1.0           # backoff exponentiel
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Sources
SOURCE_LETROT = "letrot"
SOURCE_IFCE = "ifce"
SOURCE_FRANCESIRE = "francesire"
ALL_SOURCES = [SOURCE_LETROT, SOURCE_IFCE, SOURCE_FRANCESIRE]


# ===========================================================================
# SAUVEGARDE
# ===========================================================================

def sauver_json(data: list[dict], path: Path, logger: logging.Logger):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)
    logger.info("Sauve: %s (%d entrees)", path.name, len(data))


def sauver_parquet(data: list[dict], path: Path, logger: logging.Logger):
    if not HAS_PARQUET or not data:
        return
    try:
        flat = []
        for row in data:
            r = {}
            for k, v in row.items():
                if isinstance(v, (set, frozenset)):
                    r[k] = sorted(v)
                elif isinstance(v, dict):
                    r[k] = json.dumps(v, ensure_ascii=False, default=str)
                else:
                    r[k] = v
            flat.append(r)
        table = pa.Table.from_pylist(flat)
        pq.write_table(table, path)
        logger.info("Sauve: %s", path.name)
    except Exception as e:
        logger.warning("Parquet ignore: %s", e)


def sauver_csv(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(data[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            flat_row = {}
            for k, v in row.items():
                if isinstance(v, (list, set, frozenset, dict)):
                    flat_row[k] = json.dumps(
                        sorted(v) if isinstance(v, (set, frozenset)) else v,
                        ensure_ascii=False, default=str,
                    )
                else:
                    flat_row[k] = v
            writer.writerow(flat_row)
    logger.info("Sauve: %s", path.name)


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
    """Hash stable pour identifier un cheval unique."""
    key = "|".join(normaliser_texte(s) for s in [nom, pere, mere])
    return hashlib.blake2b(key.encode(), digest_size=8).hexdigest()


def slugify_letrot(name: str) -> str:
    """Convertit un nom de cheval en slug pour l'URL Le Trot.

    Ex: 'FACE TIME BOURBON' -> 'face-time-bourbon'
         'IDEE DU LARGE'    -> 'idee-du-large'
    """
    name = normaliser_texte(name)
    # Remplacer les apostrophes et caracteres speciaux
    name = re.sub(r"[''`]", "", name)
    # Remplacer tout non-alphanum par un tiret
    name = re.sub(r"[^a-z0-9]+", "-", name)
    # Nettoyer les tirets multiples et en debut/fin
    name = re.sub(r"-+", "-", name).strip("-")
    return name


def slugify_francesire(name: str) -> str:
    """Convertit un nom de cheval en slug pour l'URL France Sire.

    Ex: 'FACE TIME BOURBON' -> 'face+time+bourbon'
    """
    name = normaliser_texte(name)
    name = re.sub(r"[''`]", "", name)
    name = re.sub(r"[^a-z0-9]+", "+", name)
    name = re.sub(r"\++", "+", name).strip("+")
    return name


def empty_pedigree_record(nom: str, pere: str, mere: str, horse_id: str) -> dict:
    """Enregistrement pedigree vide avec les champs de base."""
    return {
        "horse_id": horse_id,
        "nom": nom,
        "pere": pere,
        "mere": mere,
        "pere_mere": "",
        "mere_pere": "",
        "pere_pere": "",
        "mere_mere": "",
        "lignee_male": "",
        "pays_naissance": "",
        "annee_naissance": None,
        "sexe": "",
        "race": "",
        "studfee": None,
        "nb_produits": None,
        "stats_produits": None,
        "source": "",
        "source_url": "",
        "scrape_date": "",
        "scrape_success": False,
    }


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
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
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

def load_cache_entry(horse_id: str) -> Optional[dict]:
    """Charge un enregistrement depuis le cache disque."""
    path = CACHE_DIR / f"{horse_id}.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
    return None


def save_cache_entry(horse_id: str, record: dict):
    """Sauvegarde un enregistrement dans le cache disque."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{horse_id}.json"
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)


# ===========================================================================
# CHECKPOINT
# ===========================================================================

# NOTE: Not migrated to utils.scraping.load_checkpoint/save_checkpoint because
# these use a hardcoded CHECKPOINT_PATH, return script-specific default keys
# (processed_ids, last_index, etc.), and save_checkpoint does atomic write
# (tmp+replace) with automatic updated_at timestamping.
def load_checkpoint() -> dict:
    """Charge l'etat du checkpoint."""
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"processed_ids": [], "last_index": 0, "started_at": "", "updated_at": ""}


def save_checkpoint(state: dict):
    """Sauvegarde le checkpoint."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = utc_now_iso()
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(CHECKPOINT_PATH)


# ===========================================================================
# PARSERS — LE TROT
# ===========================================================================

def parse_letrot_fiche(html: str, nom: str, pere: str, mere: str,
                       horse_id: str, url: str, logger: logging.Logger) -> dict:
    """Parse la fiche cheval Le Trot pour extraire le pedigree.

    La page Le Trot contient typiquement un arbre genealogique dans un
    tableau ou une section dediee. On cherche les ancetres dans les
    differentes sections de la page.
    """
    record = empty_pedigree_record(nom, pere, mere, horse_id)
    record["source"] = SOURCE_LETROT
    record["source_url"] = url
    record["scrape_date"] = utc_now_iso()

    soup = BeautifulSoup(html, "html.parser")

    # --- Extraction des informations generales ---
    # Chercher les infos dans les sections "fiche-identite" ou similaires
    _extract_letrot_identity(soup, record, logger)

    # --- Extraction de l'arbre genealogique ---
    _extract_letrot_pedigree_tree(soup, record, logger)

    # Si on a reussi a extraire au moins pere_mere, c'est un succes
    if record.get("pere_mere"):
        record["scrape_success"] = True

    return record


def _extract_letrot_identity(soup: BeautifulSoup, record: dict, logger: logging.Logger):
    """Extrait les infos d'identite depuis la fiche Le Trot."""
    # Chercher les blocs d'information (sexe, race, naissance, etc.)
    # Les fiches Le Trot ont souvent des <dt>/<dd> ou des <span class="label">/<span class="value">

    text = soup.get_text(separator=" ", strip=True).lower()

    # Sexe
    for pattern in [r'\b(male|femelle|hongre)\b', r'\b(m\.|f\.|h\.)\b']:
        m = re.search(pattern, text)
        if m:
            val = m.group(1)
            sexe_map = {"male": "M", "m.": "M", "femelle": "F", "f.": "F", "hongre": "H", "h.": "H"}
            record["sexe"] = sexe_map.get(val, val.upper())
            break

    # Race — trotteur francais est la plus courante sur Le Trot
    for race_pat in [r'trotteur fran[cç]ais', r'trotteur am[eé]ricain', r'pur[- ]?sang']:
        if re.search(race_pat, text):
            record["race"] = re.search(race_pat, text).group(0).title()
            break

    # Annee de naissance
    # Chercher un pattern "ne(e) en YYYY" ou "YYYY" pres de "naissance"
    m = re.search(r'n[eé]e?\s+(?:le\s+\d{1,2}[/.\-]\d{1,2}[/.\-])?\s*(\d{4})', text)
    if m:
        record["annee_naissance"] = int(m.group(1))
    else:
        m = re.search(r'naissance\s*:?\s*.*?(\d{4})', text)
        if m:
            record["annee_naissance"] = int(m.group(1))

    # Pays de naissance
    m = re.search(r'pays\s*:?\s*([a-z]+)', text)
    if m:
        record["pays_naissance"] = m.group(1).upper()

    # Chercher dans les elements structures
    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        val = dd.get_text(strip=True)
        if not val:
            continue

        if "sexe" in label:
            sexe_map = {"male": "M", "femelle": "F", "hongre": "H"}
            record["sexe"] = sexe_map.get(val.lower(), val[:1].upper())
        elif "race" in label:
            record["race"] = val
        elif "naissance" in label:
            m = re.search(r'(\d{4})', val)
            if m:
                record["annee_naissance"] = int(m.group(1))
        elif "pays" in label:
            record["pays_naissance"] = val.upper()[:3]


def _extract_letrot_pedigree_tree(soup: BeautifulSoup, record: dict, logger: logging.Logger):
    """Extrait l'arbre genealogique depuis la fiche Le Trot.

    Le Trot organise le pedigree sous forme d'arbre :
        Pere
            Pere du pere
            Mere du pere
        Mere
            Pere de la mere
            Mere de la mere

    On cherche ce pattern dans plusieurs structures HTML possibles.
    """
    # Strategie 1 : Chercher un tableau de pedigree
    pedigree_table = soup.find("table", class_=re.compile(r"pedigree|genealog|arbre", re.I))
    if pedigree_table:
        _parse_pedigree_table(pedigree_table, record, logger)
        return

    # Strategie 2 : Chercher une section pedigree avec des divs imbriques
    pedigree_section = soup.find(
        ["section", "div"],
        class_=re.compile(r"pedigree|genealog|arbre|origines|lineage", re.I),
    )
    if pedigree_section:
        _parse_pedigree_section(pedigree_section, record, logger)
        return

    # Strategie 3 : Chercher les liens vers les fiches des parents
    # Sur Le Trot, les noms des ancetres sont souvent des liens
    _parse_pedigree_from_links(soup, record, logger)


def _parse_pedigree_table(table, record: dict, logger: logging.Logger):
    """Parse un tableau de pedigree standard.

    Format typique : tableau avec lignes pour chaque generation.
    Les cellules avec rowspan representent les ancetres de generation superieure.
    """
    cells = []
    for row in table.find_all("tr"):
        for cell in row.find_all(["td", "th"]):
            text = cell.get_text(strip=True)
            if text and not text.isspace():
                cells.append(normaliser_texte(text))

    # Essayer de mapper les cellules aux positions de l'arbre
    # L'ordre typique dans un tableau de pedigree (4 gen) est :
    # Pere, Pere du Pere, Mere du Pere, Mere, Pere de la Mere, Mere de la Mere
    pere_norm = normaliser_texte(record["pere"])
    mere_norm = normaliser_texte(record["mere"])

    # Chercher pere et mere dans les cellules pour valider
    pere_idx = None
    mere_idx = None
    for i, c in enumerate(cells):
        if c == pere_norm and pere_idx is None:
            pere_idx = i
        elif c == mere_norm and mere_idx is None:
            mere_idx = i

    if pere_idx is not None and mere_idx is not None:
        # Recuperer les cellules entre pere et mere (ancetres du pere)
        # et apres mere (ancetres de la mere)
        between = cells[pere_idx + 1: mere_idx]
        after = cells[mere_idx + 1:]

        if len(between) >= 2:
            record["pere_pere"] = between[0].upper()
            record["mere_pere"] = between[1].upper() if len(between) > 1 else ""
        elif len(between) == 1:
            record["pere_pere"] = between[0].upper()

        if len(after) >= 2:
            record["pere_mere"] = after[0].upper()
            record["mere_mere"] = after[1].upper() if len(after) > 1 else ""
        elif len(after) == 1:
            record["pere_mere"] = after[0].upper()

    logger.debug("Table pedigree: pere_pere=%s, pere_mere=%s",
                 record.get("pere_pere"), record.get("pere_mere"))


def _parse_pedigree_section(section, record: dict, logger: logging.Logger):
    """Parse une section pedigree avec divs imbriques."""
    # Chercher tous les elements contenant des noms de chevaux
    # (souvent des <a> ou <span> avec class specifique)
    links = section.find_all("a")
    if not links:
        spans = section.find_all("span")
        names = [s.get_text(strip=True) for s in spans if s.get_text(strip=True)]
    else:
        names = [a.get_text(strip=True) for a in links if a.get_text(strip=True)]

    if len(names) < 3:
        return

    _assign_pedigree_from_names(names, record, logger)


def _parse_pedigree_from_links(soup: BeautifulSoup, record: dict, logger: logging.Logger):
    """Cherche les noms des ancetres dans les liens de la page."""
    # Sur Le Trot, les fiches cheval contiennent des liens vers les parents
    # du type /fiche-cheval/nom-du-parent
    pedigree_links = []
    for a in soup.find_all("a", href=re.compile(r"/fiche-cheval/", re.I)):
        name = a.get_text(strip=True)
        if name:
            pedigree_links.append(name)

    if len(pedigree_links) >= 3:
        _assign_pedigree_from_names(pedigree_links, record, logger)


def _assign_pedigree_from_names(names: list[str], record: dict, logger: logging.Logger):
    """Assigne les noms d'ancetres a partir d'une liste ordonnee.

    L'ordre standard dans un arbre de pedigree est :
    [Pere, Pere_du_Pere, Mere_du_Pere, Mere, Pere_de_la_Mere, Mere_de_la_Mere]
    Ou parfois :
    [Pere, Mere, Pere_du_Pere, Mere_du_Pere, Pere_de_la_Mere, Mere_de_la_Mere]
    """
    pere_norm = normaliser_texte(record["pere"])
    mere_norm = normaliser_texte(record["mere"])
    names_norm = [normaliser_texte(n) for n in names]

    # Trouver les positions du pere et de la mere connus
    pere_idx = None
    mere_idx = None
    for i, n in enumerate(names_norm):
        if n == pere_norm and pere_idx is None:
            pere_idx = i
        elif n == mere_norm and mere_idx is None:
            mere_idx = i

    if pere_idx is not None and mere_idx is not None:
        # Extraire les descendants de chaque branche
        if pere_idx < mere_idx:
            # Format : Pere, [ancetres du pere...], Mere, [ancetres de la mere...]
            pere_descendants = names[pere_idx + 1: mere_idx]
            mere_descendants = names[mere_idx + 1:]
        else:
            # Format inverse
            mere_descendants = names[mere_idx + 1: pere_idx]
            pere_descendants = names[pere_idx + 1:]

        if len(pere_descendants) >= 1:
            record["pere_pere"] = pere_descendants[0].strip().upper()
        if len(pere_descendants) >= 2:
            record["mere_pere"] = pere_descendants[1].strip().upper()

        if len(mere_descendants) >= 1:
            record["pere_mere"] = mere_descendants[0].strip().upper()
        if len(mere_descendants) >= 2:
            record["mere_mere"] = mere_descendants[1].strip().upper()
    else:
        # Fallback : assigner par position brute en excluant pere/mere connus
        remaining = [n for i, n in enumerate(names)
                     if normaliser_texte(n) not in (pere_norm, mere_norm)]
        if len(remaining) >= 1:
            record["pere_pere"] = remaining[0].strip().upper()
        if len(remaining) >= 2:
            record["pere_mere"] = remaining[1].strip().upper()
        if len(remaining) >= 3:
            record["mere_pere"] = remaining[2].strip().upper()
        if len(remaining) >= 4:
            record["mere_mere"] = remaining[3].strip().upper()

    logger.debug("Pedigree assigne: pp=%s, pm=%s, mp=%s, mm=%s",
                 record.get("pere_pere"), record.get("pere_mere"),
                 record.get("mere_pere"), record.get("mere_mere"))


# ===========================================================================
# PARSERS — IFCE
# ===========================================================================

def parse_ifce_page(html: str, nom: str, pere: str, mere: str,
                    horse_id: str, url: str, logger: logging.Logger) -> dict:
    """Parse la page IFCE InfoChevaux."""
    record = empty_pedigree_record(nom, pere, mere, horse_id)
    record["source"] = SOURCE_IFCE
    record["source_url"] = url
    record["scrape_date"] = utc_now_iso()

    soup = BeautifulSoup(html, "html.parser")

    # IFCE a des sections structurees avec les infos du cheval
    text = soup.get_text(separator="\n", strip=True)

    # Chercher les blocs d'information
    _extract_ifce_identity(soup, text, record, logger)
    _extract_ifce_pedigree(soup, text, record, logger)

    if record.get("pere_mere"):
        record["scrape_success"] = True

    return record


def _extract_ifce_identity(soup: BeautifulSoup, text: str, record: dict,
                           logger: logging.Logger):
    """Extrait l'identite depuis IFCE."""
    text_lower = text.lower()

    # Sexe
    m = re.search(r'sexe\s*:?\s*(male|femelle|hongre|m|f|h)\b', text_lower)
    if m:
        val = m.group(1)
        sexe_map = {"male": "M", "m": "M", "femelle": "F", "f": "F", "hongre": "H", "h": "H"}
        record["sexe"] = sexe_map.get(val, val[:1].upper())

    # Race
    m = re.search(r'race\s*:?\s*([^\n]+)', text_lower)
    if m:
        record["race"] = m.group(1).strip().title()

    # Naissance
    m = re.search(r'(\d{2}[/.\-]\d{2}[/.\-]\d{4})', text)
    if m:
        try:
            for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
                try:
                    dt = datetime.strptime(m.group(1), fmt)
                    record["annee_naissance"] = dt.year
                    break
                except ValueError:
                    continue
        except Exception as e:
            log.debug(f"  Erreur parsing date naissance: {e}")

    if not record["annee_naissance"]:
        m = re.search(r'n[eé]e?\s+en\s+(\d{4})', text_lower)
        if m:
            record["annee_naissance"] = int(m.group(1))

    # Pays
    m = re.search(r'pays\s*(?:de\s+naissance)?\s*:?\s*([A-Z]{2,3})', text)
    if m:
        record["pays_naissance"] = m.group(1)


def _extract_ifce_pedigree(soup: BeautifulSoup, text: str, record: dict,
                           logger: logging.Logger):
    """Extrait le pedigree depuis IFCE."""
    # Chercher une section pedigree / origines
    pedigree_section = soup.find(
        ["section", "div", "table"],
        class_=re.compile(r"pedigree|genealog|origines|arbre|parents", re.I),
    )
    if pedigree_section:
        links = pedigree_section.find_all("a")
        if links:
            names = [a.get_text(strip=True) for a in links if a.get_text(strip=True)]
            if len(names) >= 3:
                _assign_pedigree_from_names(names, record, logger)
                return

    # Chercher dans le texte brut
    # Pattern : "Pere de la mere : NOM" ou "Grand-pere maternel : NOM"
    patterns = {
        "pere_mere": [
            r'p[eè]re\s+de\s+(?:la\s+)?m[eè]re\s*:?\s*([A-Z][A-Z\s\'-]+)',
            r'grand[- ]?p[eè]re\s+maternel\s*:?\s*([A-Z][A-Z\s\'-]+)',
            r'BMS\s*:?\s*([A-Z][A-Z\s\'-]+)',
        ],
        "pere_pere": [
            r'p[eè]re\s+du\s+p[eè]re\s*:?\s*([A-Z][A-Z\s\'-]+)',
            r'grand[- ]?p[eè]re\s+paternel\s*:?\s*([A-Z][A-Z\s\'-]+)',
        ],
        "mere_mere": [
            r'm[eè]re\s+de\s+(?:la\s+)?m[eè]re\s*:?\s*([A-Z][A-Z\s\'-]+)',
            r'grand[- ]?m[eè]re\s+maternelle\s*:?\s*([A-Z][A-Z\s\'-]+)',
        ],
        "mere_pere": [
            r'm[eè]re\s+du\s+p[eè]re\s*:?\s*([A-Z][A-Z\s\'-]+)',
            r'grand[- ]?m[eè]re\s+paternelle\s*:?\s*([A-Z][A-Z\s\'-]+)',
        ],
    }

    for field, pats in patterns.items():
        for pat in pats:
            m = re.search(pat, text)
            if m:
                record[field] = m.group(1).strip().upper()
                break


# ===========================================================================
# PARSERS — FRANCE SIRE
# ===========================================================================

def parse_francesire_page(html: str, nom: str, pere: str, mere: str,
                          horse_id: str, url: str, logger: logging.Logger) -> dict:
    """Parse la page France Sire."""
    record = empty_pedigree_record(nom, pere, mere, horse_id)
    record["source"] = SOURCE_FRANCESIRE
    record["source_url"] = url
    record["scrape_date"] = utc_now_iso()

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    # France Sire est oriente galop / etalon
    _extract_francesire_identity(soup, text, record, logger)
    _extract_francesire_pedigree(soup, text, record, logger)
    _extract_francesire_stats(soup, text, record, logger)

    if record.get("pere_mere"):
        record["scrape_success"] = True

    return record


def _extract_francesire_identity(soup: BeautifulSoup, text: str, record: dict,
                                 logger: logging.Logger):
    """Extrait l'identite depuis France Sire."""
    text_lower = text.lower()

    # Race
    if "pur-sang" in text_lower or "pur sang" in text_lower:
        record["race"] = "Pur-Sang"
    elif "trotteur" in text_lower:
        record["race"] = "Trotteur Francais"

    # Annee de naissance
    m = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text)
    if m:
        year = int(m.group(1))
        if 1970 <= year <= 2026:
            record["annee_naissance"] = year

    # Pays
    m = re.search(r'\(([A-Z]{2,3})\)', text)
    if m:
        record["pays_naissance"] = m.group(1)


def _extract_francesire_pedigree(soup: BeautifulSoup, text: str, record: dict,
                                 logger: logging.Logger):
    """Extrait le pedigree depuis France Sire."""
    # France Sire a souvent un arbre dans un tableau ou div structure
    pedigree_el = soup.find(
        ["table", "div", "section"],
        class_=re.compile(r"pedigree|genealog|origines|arbre|tree", re.I),
    )
    if pedigree_el:
        links = pedigree_el.find_all("a")
        names = [a.get_text(strip=True) for a in links if a.get_text(strip=True)]
        if len(names) >= 3:
            _assign_pedigree_from_names(names, record, logger)
            return

    # Fallback : regex sur le texte
    _extract_ifce_pedigree(soup, text, record, logger)


def _extract_francesire_stats(soup: BeautifulSoup, text: str, record: dict,
                              logger: logging.Logger):
    """Extrait les stats de reproduction depuis France Sire."""
    text_lower = text.lower()

    # Prix de saillie
    m = re.search(r'prix\s+de\s+saillie\s*:?\s*([\d\s.]+)\s*(?:€|eur)', text_lower)
    if m:
        try:
            record["studfee"] = int(re.sub(r'[\s.]', '', m.group(1)))
        except ValueError:
            pass

    # Nombre de produits
    m = re.search(r'(\d+)\s*produits?\b', text_lower)
    if m:
        record["nb_produits"] = int(m.group(1))

    # Taux de victoire des produits
    m = re.search(r'(\d+[.,]?\d*)\s*%\s*(?:de\s+)?(?:gagnants?|winners?)', text_lower)
    if m:
        record["stats_produits"] = float(m.group(1).replace(",", ".")) / 100.0


# ===========================================================================
# FETCH ORCHESTRATION
# ===========================================================================

def fetch_horse_pedigree(
    session: requests.Session,
    nom: str,
    pere: str,
    mere: str,
    horse_id: str,
    discipline: str,
    sources: list[str],
    logger: logging.Logger,
) -> dict:
    """Tente de recuperer le pedigree d'un cheval depuis les sources configurees.

    Args:
        session: Session HTTP
        nom: Nom du cheval
        pere: Nom du pere
        mere: Nom de la mere
        horse_id: ID unique du cheval
        discipline: 'trot' ou 'galop' (pour choisir la source)
        sources: Liste de sources a essayer
        logger: Logger

    Returns:
        dict: Enregistrement pedigree (avec scrape_success=True/False)
    """
    if not HAS_BS4:
        logger.error("beautifulsoup4 non installe — pip install beautifulsoup4")
        return empty_pedigree_record(nom, pere, mere, horse_id)

    record = None

    for source in sources:
        if source == SOURCE_LETROT:
            record = _try_letrot(session, nom, pere, mere, horse_id, logger)
        elif source == SOURCE_IFCE:
            record = _try_ifce(session, nom, pere, mere, horse_id, logger)
        elif source == SOURCE_FRANCESIRE:
            record = _try_francesire(session, nom, pere, mere, horse_id, logger)

        if record and record.get("scrape_success"):
            return record

        # Pause entre les sources
        time.sleep(REQUEST_PAUSE_S * 0.5)

    # Aucune source n'a fonctionne — retourner le dernier resultat ou un vide
    return record or empty_pedigree_record(nom, pere, mere, horse_id)


def _try_letrot(session: requests.Session, nom: str, pere: str, mere: str,
                horse_id: str, logger: logging.Logger) -> Optional[dict]:
    """Tente Le Trot."""
    slug = slugify_letrot(nom)
    url = f"https://www.letrot.com/fiche-cheval/{slug}"
    logger.debug("Le Trot: %s", url)

    resp = safe_get(session, url, logger)
    if resp is None:
        return None

    return parse_letrot_fiche(resp.text, nom, pere, mere, horse_id, url, logger)


def _try_ifce(session: requests.Session, nom: str, pere: str, mere: str,
              horse_id: str, logger: logging.Logger) -> Optional[dict]:
    """Tente IFCE InfoChevaux.

    IFCE necessite souvent une recherche avant d'acceder a la fiche.
    On tente d'abord une URL directe, puis une recherche.
    """
    # Essayer une recherche
    search_url = f"https://infochevaux.ifce.fr/fr/info-chevaux?search={requests.utils.quote(nom)}"
    logger.debug("IFCE search: %s", search_url)

    resp = safe_get(session, search_url, logger)
    if resp is None:
        return None

    # Verifier si la page contient des resultats avec le bon cheval
    soup = BeautifulSoup(resp.text, "html.parser")

    # Chercher un lien vers la fiche du cheval
    for a in soup.find_all("a", href=True):
        link_text = normaliser_texte(a.get_text(strip=True))
        if normaliser_texte(nom) in link_text:
            fiche_url = a["href"]
            if not fiche_url.startswith("http"):
                fiche_url = f"https://infochevaux.ifce.fr{fiche_url}"

            time.sleep(REQUEST_PAUSE_S)
            resp2 = safe_get(session, fiche_url, logger)
            if resp2:
                return parse_ifce_page(resp2.text, nom, pere, mere, horse_id,
                                       fiche_url, logger)
            break

    # Si pas de lien specifique, tenter de parser la page de recherche directement
    return parse_ifce_page(resp.text, nom, pere, mere, horse_id, search_url, logger)


def _try_francesire(session: requests.Session, nom: str, pere: str, mere: str,
                    horse_id: str, logger: logging.Logger) -> Optional[dict]:
    """Tente France Sire."""
    slug = slugify_francesire(nom)
    search_url = f"https://www.france-sire.com/recherche.php?search={slug}"
    logger.debug("France Sire: %s", search_url)

    resp = safe_get(session, search_url, logger)
    if resp is None:
        return None

    # Chercher un lien vers la fiche
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        link_text = normaliser_texte(a.get_text(strip=True))
        if normaliser_texte(nom) in link_text:
            fiche_url = a["href"]
            if not fiche_url.startswith("http"):
                fiche_url = f"https://www.france-sire.com/{fiche_url.lstrip('/')}"

            time.sleep(REQUEST_PAUSE_S)
            resp2 = safe_get(session, fiche_url, logger)
            if resp2:
                return parse_francesire_page(resp2.text, nom, pere, mere,
                                             horse_id, fiche_url, logger)
            break

    return parse_francesire_page(resp.text, nom, pere, mere, horse_id,
                                 search_url, logger)


# ===========================================================================
# LIGNEE MALE
# ===========================================================================

def compute_lignee_male(record: dict) -> str:
    """Reconstruit la lignee male (patrilineaire) a partir du pedigree.

    La lignee male est : cheval -> pere -> pere_pere -> ...
    On s'arrete quand on n'a plus d'information.
    """
    parts = []
    if record.get("pere"):
        parts.append(record["pere"])
    if record.get("pere_pere"):
        parts.append(record["pere_pere"])
    return " > ".join(parts) if parts else ""


# ===========================================================================
# EXTRACTION DES CHEVAUX UNIQUES
# ===========================================================================

def extract_unique_horses(partants: list[dict], logger: logging.Logger) -> list[dict]:
    """Extrait les chevaux uniques depuis les partants normalises.

    Returns:
        list[dict]: Liste de dicts avec nom, pere, mere, horse_id, discipline
    """
    seen = {}  # horse_id -> dict
    no_pere = 0
    no_mere = 0

    for p in partants:
        nom = (p.get("nom_cheval") or "").strip()
        pere = (p.get("pere") or "").strip()
        mere = (p.get("mere") or "").strip()

        if not nom:
            continue
        if not pere:
            no_pere += 1
        if not mere:
            no_mere += 1

        horse_id = make_horse_id(nom, pere, mere)
        if horse_id not in seen:
            disc = (p.get("discipline") or "").lower()
            seen[horse_id] = {
                "nom": nom,
                "pere": pere,
                "mere": mere,
                "horse_id": horse_id,
                "discipline": disc,
                "pere_mere_connu": bool(p.get("pere_mere", "").strip()),
            }

    horses = sorted(seen.values(), key=lambda h: h["nom"])

    logger.info("Chevaux uniques: %d", len(horses))
    logger.info("  dont sans pere: %d partants", no_pere)
    logger.info("  dont sans mere: %d partants", no_mere)

    # Stats pere_mere deja connu
    with_pm = sum(1 for h in horses if h["pere_mere_connu"])
    logger.info("  pere_mere deja connu: %d / %d (%.1f%%)",
                with_pm, len(horses), 100 * with_pm / len(horses) if horses else 0)

    return horses


# ===========================================================================
# DETERMINE SOURCE ORDER
# ===========================================================================

def get_source_order(discipline: str, preferred_source: Optional[str]) -> list[str]:
    """Determine l'ordre des sources a essayer selon la discipline."""
    if preferred_source:
        return [preferred_source]

    if discipline == "trot" or discipline == "attele" or discipline == "monte":
        return [SOURCE_LETROT, SOURCE_IFCE, SOURCE_FRANCESIRE]
    elif discipline == "galop" or discipline == "plat" or discipline == "obstacle":
        return [SOURCE_FRANCESIRE, SOURCE_IFCE, SOURCE_LETROT]
    else:
        # Inconnu — essayer toutes les sources
        return [SOURCE_LETROT, SOURCE_IFCE, SOURCE_FRANCESIRE]


# ===========================================================================
# MAIN LOOP
# ===========================================================================

def run_scraping(
    horses: list[dict],
    session: requests.Session,
    logger: logging.Logger,
    force_refresh: bool = False,
    use_checkpoint: bool = False,
    preferred_source: Optional[str] = None,
    checkpoint_every: int = 100,
) -> list[dict]:
    """Boucle principale de scraping.

    Args:
        horses: Liste de chevaux uniques a traiter
        session: Session HTTP
        logger: Logger
        force_refresh: Ignorer le cache
        use_checkpoint: Reprendre depuis le checkpoint
        preferred_source: Forcer une source specifique
        checkpoint_every: Sauver le checkpoint tous les N chevaux

    Returns:
        list[dict]: Enregistrements pedigree enrichis
    """
    total = len(horses)
    results = []
    stats = {
        "total": total,
        "from_cache": 0,
        "scraped_ok": 0,
        "scraped_fail": 0,
        "skipped": 0,
        "errors": 0,
    }

    # Charger le checkpoint si demande
    checkpoint = load_checkpoint() if use_checkpoint else {
        "processed_ids": [], "last_index": 0,
        "started_at": utc_now_iso(), "updated_at": "",
    }
    processed_set = set(checkpoint.get("processed_ids", []))
    start_idx = checkpoint.get("last_index", 0) if use_checkpoint else 0

    logger.info("Demarrage du scraping: %d chevaux, start_idx=%d, cache=%d deja traites",
                total, start_idx, len(processed_set))

    t0 = time.time()

    for i, horse in enumerate(horses):
        if i < start_idx and use_checkpoint:
            continue

        horse_id = horse["horse_id"]
        nom = horse["nom"]
        pere = horse["pere"]
        mere = horse["mere"]
        discipline = horse["discipline"]

        # Progression
        if (i + 1) % 500 == 0 or i == 0:
            elapsed = time.time() - t0
            speed = (i - start_idx + 1) / elapsed if elapsed > 0 else 0
            eta = (total - i - 1) / speed if speed > 0 else 0
            logger.info(
                "Progression: %d/%d (%.1f%%) | cache=%d ok=%d fail=%d | "
                "%.1f chev/min | ETA: %.0f min",
                i + 1, total, 100 * (i + 1) / total,
                stats["from_cache"], stats["scraped_ok"], stats["scraped_fail"],
                speed * 60, eta / 60,
            )

        # Verifier le cache
        if not force_refresh:
            cached = load_cache_entry(horse_id)
            if cached is not None:
                results.append(cached)
                stats["from_cache"] += 1
                continue

        # Skip si deja traite dans cette session (via checkpoint)
        if horse_id in processed_set and not force_refresh:
            stats["skipped"] += 1
            continue

        # Determiner l'ordre des sources
        sources = get_source_order(discipline, preferred_source)

        # Scraper
        try:
            record = fetch_horse_pedigree(
                session, nom, pere, mere, horse_id, discipline, sources, logger,
            )

            # Lignee male
            record["lignee_male"] = compute_lignee_male(record)

            # Sauver dans le cache
            save_cache_entry(horse_id, record)
            results.append(record)

            if record.get("scrape_success"):
                stats["scraped_ok"] += 1
            else:
                stats["scraped_fail"] += 1

        except Exception as e:
            logger.error("Erreur sur %s (id=%s): %s", nom, horse_id, e)
            stats["errors"] += 1
            # Sauver un record vide dans le cache pour ne pas re-essayer
            record = empty_pedigree_record(nom, pere, mere, horse_id)
            record["scrape_date"] = utc_now_iso()
            save_cache_entry(horse_id, record)
            results.append(record)

        # Checkpoint periodique
        processed_set.add(horse_id)
        if (i + 1) % checkpoint_every == 0:
            checkpoint["processed_ids"] = list(processed_set)
            checkpoint["last_index"] = i + 1
            save_checkpoint(checkpoint)
            logger.debug("Checkpoint sauve a l'index %d", i + 1)

        # Pause entre les requetes
        time.sleep(REQUEST_PAUSE_S)

    # Checkpoint final
    checkpoint["processed_ids"] = list(processed_set)
    checkpoint["last_index"] = total
    save_checkpoint(checkpoint)

    # Log final
    elapsed = time.time() - t0
    logger.info("-" * 50)
    logger.info("SCRAPING TERMINE en %.1f min", elapsed / 60)
    logger.info("  Total chevaux         : %d", stats["total"])
    logger.info("  Depuis le cache       : %d", stats["from_cache"])
    logger.info("  Scrapes reussis       : %d", stats["scraped_ok"])
    logger.info("  Scrapes echoues       : %d", stats["scraped_fail"])
    logger.info("  Sautes (checkpoint)   : %d", stats["skipped"])
    logger.info("  Erreurs               : %d", stats["errors"])
    taux = (
        stats["scraped_ok"] / (stats["scraped_ok"] + stats["scraped_fail"])
        if (stats["scraped_ok"] + stats["scraped_fail"]) > 0
        else 0
    )
    logger.info("  Taux de succes scrape : %.1f%%", 100 * taux)

    return results


# ===========================================================================
# MERGE AVEC DONNEES EXISTANTES
# ===========================================================================

def merge_with_existing(
    pedigree_records: list[dict],
    partants: list[dict],
    logger: logging.Logger,
) -> list[dict]:
    """Enrichit les enregistrements pedigree avec les infos deja presentes
    dans partants_normalises (pere_mere, sexe, etc.) quand le scraping n'a
    pas reussi.
    """
    # Indexer les partants par horse_id pour recuperer pere_mere existant
    partant_by_horse: dict[str, dict] = {}
    for p in partants:
        nom = (p.get("nom_cheval") or "").strip()
        pere = (p.get("pere") or "").strip()
        mere = (p.get("mere") or "").strip()
        if nom:
            hid = make_horse_id(nom, pere, mere)
            if hid not in partant_by_horse:
                partant_by_horse[hid] = p

    enriched = 0
    for rec in pedigree_records:
        hid = rec.get("horse_id", "")
        p = partant_by_horse.get(hid)
        if not p:
            continue

        # Si pere_mere non trouve par le scraping, utiliser celui des partants
        if not rec.get("pere_mere") and p.get("pere_mere"):
            rec["pere_mere"] = p["pere_mere"]
            enriched += 1

        # Completer les champs manquants
        if not rec.get("sexe") and p.get("sexe"):
            rec["sexe"] = p["sexe"]
        if not rec.get("race") and p.get("race"):
            rec["race"] = p["race"]
        if not rec.get("pays_naissance") and p.get("pays_cheval"):
            rec["pays_naissance"] = p["pays_cheval"]

    logger.info("Enrichissement depuis partants existants: %d pere_mere recuperes", enriched)
    return pedigree_records


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    global REQUEST_PAUSE_S, OUTPUT_DIR, CACHE_DIR, CHECKPOINT_PATH
    parser = argparse.ArgumentParser(
        description="Scrape les pedigrees des chevaux depuis des sources en ligne."
    )
    parser.add_argument(
        "--partants", type=str, default=str(PARTANTS_PATH),
        help=f"Chemin vers partants_normalises.json (defaut: {PARTANTS_PATH})",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Limiter le nombre de chevaux a traiter (0 = tous)",
    )
    parser.add_argument(
        "--source", type=str, default=None, choices=ALL_SOURCES,
        help="Forcer une source specifique (defaut: auto selon discipline)",
    )
    parser.add_argument(
        "--force-refresh", action="store_true",
        help="Ignorer le cache et re-scraper tous les chevaux",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Reprendre depuis le dernier checkpoint",
    )
    parser.add_argument(
        "--checkpoint-every", type=int, default=100,
        help="Frequence de sauvegarde du checkpoint (defaut: 100)",
    )
    parser.add_argument(
        "--pause", type=float, default=REQUEST_PAUSE_S,
        help=f"Pause entre requetes en secondes (defaut: {REQUEST_PAUSE_S})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Mode debug",
    )
    args = parser.parse_args()

    # Config globale
    REQUEST_PAUSE_S = args.pause
    OUTPUT_DIR = Path(args.output_dir)
    CACHE_DIR = OUTPUT_DIR / "cache"
    CHECKPOINT_PATH = OUTPUT_DIR / "checkpoint.json"

    logger = setup_logging("12_pedigree_scraper")
    logger.info("=" * 70)
    logger.info("12 — PEDIGREE SCRAPER (ENRICHISSEMENT LIGNEES)")
    logger.info("=" * 70)

    # Verifier les dependances
    if not HAS_BS4:
        logger.error("beautifulsoup4 requis: pip install beautifulsoup4")
        sys.exit(1)

    # Charger partants
    partants_path = Path(args.partants)
    if not partants_path.exists():
        logger.error("Fichier introuvable: %s", partants_path)
        sys.exit(1)
    with open(partants_path, "r", encoding="utf-8") as f:
        partants = json.load(f)
    logger.info("Partants charges: %d", len(partants))

    # Extraire les chevaux uniques
    horses = extract_unique_horses(partants, logger)

    if args.limit > 0:
        horses = horses[:args.limit]
        logger.info("Limite appliquee: %d chevaux", len(horses))

    # Creer la session HTTP
    session = create_session()

    # Scraping
    logger.info("-" * 50)
    logger.info("Sources: %s", args.source or "auto (trot: letrot>ifce>francesire, galop: francesire>ifce>letrot)")
    logger.info("Pause: %.1fs | Timeout: %ds | Retries: %d",
                REQUEST_PAUSE_S, REQUEST_TIMEOUT_S, MAX_RETRIES)
    logger.info("-" * 50)

    results = run_scraping(
        horses=horses,
        session=session,
        logger=logger,
        force_refresh=args.force_refresh,
        use_checkpoint=args.resume,
        preferred_source=args.source,
        checkpoint_every=args.checkpoint_every,
    )

    # Merge avec donnees existantes
    results = merge_with_existing(results, partants, logger)

    # Stats finales
    total = len(results)
    with_pm = sum(1 for r in results if r.get("pere_mere"))
    with_pp = sum(1 for r in results if r.get("pere_pere"))
    with_mm = sum(1 for r in results if r.get("mere_mere"))
    with_mp = sum(1 for r in results if r.get("mere_pere"))
    success = sum(1 for r in results if r.get("scrape_success"))

    logger.info("-" * 50)
    logger.info("RESUME FINAL:")
    logger.info("  Chevaux uniques       : %d", total)
    logger.info("  Scrape succes         : %d (%.1f%%)", success, 100 * success / total if total else 0)
    logger.info("  pere_mere renseignes  : %d (%.1f%%)", with_pm, 100 * with_pm / total if total else 0)
    logger.info("  pere_pere renseignes  : %d (%.1f%%)", with_pp, 100 * with_pp / total if total else 0)
    logger.info("  mere_pere renseignes  : %d (%.1f%%)", with_mp, 100 * with_mp / total if total else 0)
    logger.info("  mere_mere renseignes  : %d (%.1f%%)", with_mm, 100 * with_mm / total if total else 0)

    # Sauvegarder
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    sauver_json(results, OUTPUT_DIR / "pedigrees_enrichis.json", logger)
    sauver_parquet(results, OUTPUT_DIR / "pedigrees_enrichis.parquet", logger)
    sauver_csv(results, OUTPUT_DIR / "pedigrees_enrichis.csv", logger)

    logger.info("=" * 70)
    logger.info("TERMINE")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
