#!/usr/bin/env python3
"""
02b_scraper_letrot.py
=====================
Collecte des courses et partants de trot hors-PMU depuis Le Trot.

Cible les ~7 567 reunions presentes sur Le Trot mais SANS couverture PMU
(qualifications, regionales sans enjeux PMU).  Produit ~36K courses et
~435K partants supplementaires.

Sources :
  - Le Trot (HTML) : https://www.letrot.com/courses/programme/{date}/{id_letrot}

Architecture (identique a 02_liste_courses.py) :
  - Filtre reunions_references_02.json : url_letrot presente, url_pmu absente
  - Cache JSON par reunion dans output/02b_scraper_letrot/cache/
  - Checkpoint par reunion
  - Memes dataclasses CourseBrute / PartantBrut
  - Memes normalisations + exports JSON / Parquet / CSV

Usage :
    python3 02b_scraper_letrot.py
    python3 02b_scraper_letrot.py --pause 0.5 --batch 100
    python3 02b_scraper_letrot.py --date-debut 2020-01-01 --date-fin 2023-12-31
    python3 02b_scraper_letrot.py --max-reunions 50
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
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERREUR: beautifulsoup4 requis.  pip install beautifulsoup4 lxml")
    sys.exit(1)

# Imports optionnels
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

# ===========================================================================
# CONFIG
# ===========================================================================

REFERENCES_PATH = Path(__file__).resolve().parent / "output" / "01_calendrier_reunions" / "reunions_references_02.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "02b_scraper_letrot"
CACHE_DIR = OUTPUT_DIR / "cache"

from utils.logging_setup import setup_logging
from utils.normalize import normaliser_texte
from utils.output import sauver_json, sauver_csv
from utils.types import utc_now_iso

LETROT_BASE = "https://www.letrot.com"
LETROT_PROGRAMME = f"{LETROT_BASE}/courses/programme"
# Tentative JSON API (resultat par course)
LETROT_API_RESULTATS = f"{LETROT_BASE}/stats/courses/programme/resultats"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ===========================================================================
# DATACLASSES -- COURSE
# ===========================================================================

@dataclass
class CourseBrute:
    """Course telle que collectee depuis Le Trot."""
    # Tracabilite
    source: str = ""
    reunion_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    url_source: str = ""
    timestamp_collecte: str = ""

    # Donnees course
    libelle: str = ""
    libelle_court: str = ""
    distance: Optional[int] = None
    distance_unit: str = ""
    parcours: str = ""
    corde: str = ""
    discipline: str = ""
    specialite: str = ""
    condition_sexe: str = ""
    categorie_particularite: str = ""
    condition_age: str = ""
    conditions_texte: str = ""
    nombre_partants: Optional[int] = None
    heure_depart: Optional[int] = None  # timestamp ms
    montant_prix: Optional[int] = None  # centimes
    montant_1er: Optional[int] = None
    montant_2eme: Optional[int] = None
    montant_3eme: Optional[int] = None
    montant_4eme: Optional[int] = None
    montant_5eme: Optional[int] = None

    # Resultat course
    statut: str = ""
    categorie_statut: str = ""
    ordre_arrivee: list = field(default_factory=list)
    duree_course: Optional[int] = None  # ms
    incidents: list = field(default_factory=list)
    arrivee_definitive: bool = False

    # Piste
    type_piste: str = ""
    penetrometre: str = ""
    penetrometre_valeur: str = ""

    # Paris
    paris_disponibles: list = field(default_factory=list)

    # Hippodrome (from reunion)
    hippodrome: str = ""

    # Extras
    replay_disponible: bool = False
    course_trackee: bool = False
    extras: dict = field(default_factory=dict)


@dataclass
class CourseNormalisee:
    """Course normalisee pour le pipeline aval."""
    course_uid: str = ""
    reunion_uid: str = ""
    cle_course: str = ""

    source: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    hippodrome: str = ""
    pays: str = ""
    numero_reunion: int = 0
    numero_course: int = 0

    libelle: str = ""
    distance: Optional[int] = None
    parcours: str = ""
    corde: str = ""
    discipline: str = ""
    specialite: str = ""
    conditions_texte: str = ""
    condition_sexe: str = ""
    condition_age: str = ""
    categorie: str = ""
    mode_depart: str = ""
    nombre_partants: Optional[int] = None
    heure_depart: str = ""
    allocation_totale: Optional[int] = None  # euros
    allocation_1er: Optional[int] = None

    type_piste: str = ""
    penetrometre: str = ""

    statut: str = ""
    ordre_arrivee: list = field(default_factory=list)
    duree_course_ms: Optional[int] = None
    incidents: list = field(default_factory=list)

    paris_types: list = field(default_factory=list)
    replay_disponible: bool = False
    course_trackee: bool = False

    timestamp_collecte: str = ""
    url_source: str = ""


# ===========================================================================
# DATACLASSES -- PARTANT
# ===========================================================================

@dataclass
class PartantBrut:
    """Partant tel que collecte depuis Le Trot."""
    source: str = ""
    course_uid: str = ""
    reunion_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    timestamp_collecte: str = ""

    # Cheval
    nom: str = ""
    num_pmu: Optional[int] = None
    age: Optional[int] = None
    sexe: str = ""
    race: str = ""
    robe: str = ""
    musique: str = ""
    nombre_courses: Optional[int] = None
    nombre_victoires: Optional[int] = None
    nombre_places: Optional[int] = None
    nombre_places_second: Optional[int] = None
    nombre_places_troisieme: Optional[int] = None
    gains_carriere: Optional[int] = None  # centimes
    gains_victoires: Optional[int] = None
    gains_place: Optional[int] = None
    gains_annee_en_cours: Optional[int] = None
    gains_annee_precedente: Optional[int] = None
    indicateur_inedit: bool = False

    # Jockey / Driver
    driver: str = ""
    driver_change: bool = False

    # Entraineur
    entraineur: str = ""

    # Proprietaire
    proprietaire: str = ""

    # Pedigree
    nom_pere: str = ""
    nom_mere: str = ""
    eleveur: str = ""

    # Equipement
    oeilleres: str = ""
    deferre: str = ""

    # Course
    statut_partant: str = ""
    engagement: bool = False
    supplement: Optional[int] = None
    handicap_distance: Optional[int] = None
    handicap_poids: Optional[int] = None
    handicap_valeur: Optional[float] = None
    poids_condition_monte: Optional[int] = None
    poids_condition_monte_change: bool = False
    taux_reclamation: Optional[int] = None
    place_corde: Optional[int] = None
    allure: str = ""

    # Infos supplementaires
    pays: str = ""
    pays_entrainement: str = ""
    nom_pere_mere: str = ""
    incident: str = ""
    distance_cheval_precedent: str = ""
    commentaire_apres_course: str = ""
    avis_entraineur: str = ""
    jument_pleine: bool = False

    # Resultat individuel
    ordre_arrivee: Optional[int] = None
    temps_obtenu: Optional[int] = None  # ms
    reduction_kilometrique: Optional[int] = None  # ms/km

    # Cotes (pas de cotes pour les courses hors-PMU)
    cote_direct: Optional[float] = None
    cote_reference: Optional[float] = None

    url_casaque: str = ""
    extras: dict = field(default_factory=dict)


@dataclass
class PartantNormalise:
    """Partant normalise pour le pipeline aval."""
    partant_uid: str = ""
    course_uid: str = ""
    reunion_uid: str = ""
    cle_partant: str = ""

    source: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    distance: Optional[int] = None
    discipline: str = ""

    horse_id: str = ""
    nom_cheval: str = ""
    num_pmu: Optional[int] = None
    age: Optional[int] = None
    sexe: str = ""
    race: str = ""
    robe: str = ""
    musique: str = ""
    nb_courses_carriere: Optional[int] = None
    nb_victoires_carriere: Optional[int] = None
    nb_places_carriere: Optional[int] = None
    nb_places_2eme: Optional[int] = None
    nb_places_3eme: Optional[int] = None
    gains_carriere_euros: Optional[float] = None
    gains_annee_euros: Optional[float] = None
    is_inedit: bool = False

    jockey_driver: str = ""
    jockey_driver_change: bool = False

    entraineur: str = ""
    proprietaire: str = ""

    pere: str = ""
    mere: str = ""
    eleveur: str = ""

    oeilleres: str = ""
    deferre: str = ""

    statut: str = ""
    engagement: bool = False
    supplement_euros: Optional[float] = None
    handicap_distance_m: Optional[int] = None
    poids_porte_kg: Optional[float] = None
    poids_base_kg: Optional[float] = None
    surcharge_decharge_kg: Optional[float] = None
    handicap_valeur: Optional[float] = None
    poids_monte_change: bool = False
    taux_reclamation_euros: Optional[float] = None
    place_corde: Optional[int] = None
    allure: str = ""

    pays_cheval: str = ""
    pays_entrainement: str = ""
    pere_mere: str = ""
    incident: str = ""
    ecart_precedent: str = ""
    commentaire_apres_course: str = ""
    avis_entraineur: str = ""
    jument_pleine: bool = False

    position_arrivee: Optional[int] = None
    temps_ms: Optional[int] = None
    reduction_km_ms: Optional[int] = None
    is_gagnant: bool = False
    is_place: bool = False
    is_disqualifie: bool = False

    cote_finale: Optional[float] = None
    cote_reference: Optional[float] = None
    proba_implicite: Optional[float] = None

    timestamp_collecte: str = ""


# ===========================================================================
# HTTP
# ===========================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
    })
    return session


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def make_uid(*parts: str) -> str:
    h = hashlib.blake2b("|".join(str(p) for p in parts).encode(), digest_size=8)
    return h.hexdigest()


def ms_to_hhmm(ts_ms: Optional[int]) -> str:
    if not ts_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%H:%M")
    except (OSError, ValueError):
        return ""


def centimes_to_euros(centimes: Optional[int]) -> Optional[float]:
    if centimes is None:
        return None
    return centimes / 100.0


# NOTE: Incompatible with utils.types.safe_int — this version does regex
# extraction from strings, handles \xa0, commas as decimal separators, etc.
def safe_int(val: Any) -> Optional[int]:
    """Extrait un entier depuis une valeur potentiellement textuelle."""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        val = val.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        if not val:
            return None
        # Extraire les chiffres
        m = re.search(r"[\d]+(?:[.,]\d+)?", val)
        if m:
            try:
                return int(float(m.group().replace(",", ".")))
            except ValueError:
                return None
    return None


# NOTE: Incompatible with utils.types.safe_float — this version does regex
# extraction from strings, handles \xa0, commas as decimal separators, etc.
def safe_float(val: Any) -> Optional[float]:
    """Extrait un float depuis une valeur potentiellement textuelle."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        val = val.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        if not val:
            return None
        m = re.search(r"[\d]+(?:\.\d+)?", val)
        if m:
            try:
                return float(m.group())
            except ValueError:
                return None
    return None


def parse_time_to_ms(time_str: str) -> Optional[int]:
    """Parse un temps au format M'SS\"CC ou similaire en millisecondes.

    Exemples: 1'13\"5 -> 73500, 1'15\"2 -> 75200
    """
    if not time_str or not time_str.strip():
        return None
    s = time_str.strip()
    # Format M'SS"CC ou M'SS"C
    m = re.match(r"(\d+)['\u2019](\d{1,2})[\"″\u201d](\d{1,2})", s)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        centis = m.group(3)
        # Si 1 chiffre -> dixiemes, si 2 chiffres -> centiemes
        if len(centis) == 1:
            ms = int(centis) * 100
        else:
            ms = int(centis) * 10
        return (minutes * 60 + seconds) * 1000 + ms

    # Format simple M:SS.CC
    m = re.match(r"(\d+):(\d{1,2})\.(\d{1,3})", s)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        frac = m.group(3)
        if len(frac) == 1:
            ms = int(frac) * 100
        elif len(frac) == 2:
            ms = int(frac) * 10
        else:
            ms = int(frac)
        return (minutes * 60 + seconds) * 1000 + ms

    return None


def parse_rk_to_ms_per_km(rk_str: str) -> Optional[int]:
    """Parse une reduction kilometrique au format 1'13\"5 en ms/km."""
    return parse_time_to_ms(rk_str)


def parse_allocation_euros(text: str) -> Optional[int]:
    """Parse un montant du type '12 500 EUR' ou '12500' en euros (int)."""
    if not text:
        return None
    cleaned = text.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    cleaned = re.sub(r"[^\d.]", "", cleaned)
    if not cleaned:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def _make_horse_id(nom: str, pere: str, mere: str) -> str:
    parts = [
        (nom or "").strip().upper(),
        (pere or "").strip().upper(),
        (mere or "").strip().upper(),
    ]
    key = "|".join(parts)
    if not any(parts):
        return ""
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


# ===========================================================================
# CACHE
# ===========================================================================

class ReunionCache:
    """Cache des reponses HTML brutes par reunion Le Trot."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, date_iso: str, id_letrot: str) -> Path:
        return self.cache_dir / f"{date_iso}_{id_letrot}.json"

    def has(self, date_iso: str, id_letrot: str) -> bool:
        return self._path(date_iso, id_letrot).exists()

    def get(self, date_iso: str, id_letrot: str) -> Optional[dict]:
        p = self._path(date_iso, id_letrot)
        if not p.exists():
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def put(self, date_iso: str, id_letrot: str, data: dict):
        tmp = self._path(date_iso, id_letrot).with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        tmp.replace(self._path(date_iso, id_letrot))


# ===========================================================================
# CHECKPOINT
# ===========================================================================

class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self._data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        return {"completed_reunions": []}

    def is_done(self, reunion_uid: str) -> bool:
        return reunion_uid in self._data.get("completed_reunions", [])

    def mark_done(self, reunion_uid: str):
        self._data.setdefault("completed_reunions", []).append(reunion_uid)

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False))
        tmp.replace(self.path)

    @property
    def count_done(self) -> int:
        return len(self._data.get("completed_reunions", []))


# ===========================================================================
# NORMALISATION
# ===========================================================================

def normaliser_corde(corde_raw: str) -> str:
    if not corde_raw:
        return ""
    c = corde_raw.upper()
    if "GAUCHE" in c:
        return "gauche"
    if "DROITE" in c:
        return "droite"
    return corde_raw.strip().lower()


def normaliser_oeilleres(raw: str) -> str:
    if not raw:
        return ""
    r = raw.upper()
    if "SANS" in r:
        return "sans"
    if "AUSTRALIEN" in r:
        return "australiennes"
    if "OEILLERE" in r or "AVEC" in r:
        return "avec"
    return raw.strip().lower()


def normaliser_deferre(raw: str) -> str:
    if not raw:
        return ""
    r = raw.upper()
    if "ANTERIEURS_POSTERIEURS" in r or "4" in r or ("ANT" in r and "POST" in r):
        return "4_pieds"
    if "ANTERIEURS" in r or "ANT" in r:
        return "anterieurs"
    if "POSTERIEURS" in r or "POST" in r:
        return "posterieurs"
    if "NON_DEFERRE" in r or "FERRE" in r:
        return "ferre"
    return raw.strip().lower()


def normaliser_statut_partant(raw: str) -> str:
    if not raw:
        return ""
    r = raw.upper()
    if "NON" in r:
        return "non_partant"
    if "PARTANT" in r:
        return "partant"
    return raw.strip().lower()


def normaliser_discipline_course(raw: str) -> str:
    if not raw:
        return ""
    r = raw.strip().lower()
    aliases = {
        "attele": "trot_attele",
        "trot_attele": "trot_attele",
        "trot attele": "trot_attele",
        "monte": "trot_monte",
        "trot_monte": "trot_monte",
        "trot monte": "trot_monte",
        "trot": "trot_attele",  # defaut trot = attele sauf si monte specifie
        "plat": "plat",
        "galop": "plat",
        "obstacle": "obstacle",
    }
    return aliases.get(r, r)


def _deduire_mode_depart(categorie_raw: str, discipline_raw: str) -> str:
    cat = (categorie_raw or "").upper()
    disc = (discipline_raw or "").upper()
    if "AUTOSTART" in cat:
        return "autostart"
    if disc in ("ATTELE", "MONTE", "TROT_ATTELE", "TROT_MONTE", "TROT"):
        return "volte"
    return ""


def _deduire_specialite(text: str) -> str:
    """Deduit la specialite (attele/monte) depuis le texte de la course."""
    t = (text or "").lower()
    if "monte" in t:
        return "trot_monte"
    if "attele" in t or "attelé" in t:
        return "trot_attele"
    # Defaut pour le trot
    return "trot_attele"


# ===========================================================================
# FETCH LE TROT
# ===========================================================================

def fetch_letrot_programme(
    session: requests.Session,
    date_iso: str,
    id_letrot: str,
    logger: logging.Logger,
) -> Optional[str]:
    """Recupere la page HTML du programme d'une reunion Le Trot."""
    url = f"{LETROT_PROGRAMME}/{date_iso}/{id_letrot}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            logger.debug("  404 pour %s", url)
            return None
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.RequestException as e:
        logger.warning("  HTTP error Le Trot %s: %s", id_letrot, str(e)[:120])
        return None


def fetch_letrot_json_course(
    session: requests.Session,
    date_iso: str,
    id_letrot: str,
    num_course: int,
    logger: logging.Logger,
) -> Optional[dict]:
    """Tente de recuperer les donnees JSON d'une course individuelle."""
    url = f"{LETROT_API_RESULTATS}/{date_iso}/{id_letrot}/{num_course}"
    try:
        resp = session.get(url, timeout=20, headers={"Accept": "application/json"})
        if resp.status_code in (404, 403):
            return None
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else None
    except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError):
        return None


def detect_json_api(session: requests.Session, logger: logging.Logger) -> bool:
    """Teste si l'API JSON Le Trot est accessible avec une course connue."""
    test_url = f"{LETROT_API_RESULTATS}/2023-06-01/7500/1"
    try:
        resp = session.get(test_url, timeout=10, headers={"Accept": "application/json"})
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and data:
                logger.info("API JSON Le Trot detectee et fonctionnelle")
                return True
    except Exception:
        pass
    logger.info("API JSON Le Trot non disponible, utilisation du parsing HTML")
    return False


# ===========================================================================
# PARSING HTML LE TROT
# ===========================================================================

def _clean_text(el) -> str:
    """Extrait le texte nettoye d'un element BS4."""
    if el is None:
        return ""
    return " ".join(el.get_text(separator=" ").split()).strip()


def _find_text_after_label(soup, label: str) -> str:
    """Cherche un texte apres un label specifique dans le HTML."""
    for el in soup.find_all(string=re.compile(re.escape(label), re.I)):
        parent = el.parent
        if parent:
            # Texte du frere suivant ou du parent
            sibling = parent.find_next_sibling()
            if sibling:
                return _clean_text(sibling)
            # Essayer le texte restant dans le parent
            full = _clean_text(parent.parent) if parent.parent else ""
            idx = full.lower().find(label.lower())
            if idx >= 0:
                rest = full[idx + len(label):].strip().strip(":").strip()
                if rest:
                    return rest
    return ""


def parse_reunion_html(
    html: str,
    reunion_ref: dict,
    timestamp: str,
    logger: logging.Logger,
) -> tuple[list[CourseBrute], list[PartantBrut]]:
    """Parse une page de reunion Le Trot et retourne les courses et partants."""
    soup = BeautifulSoup(html, "lxml")
    courses = []
    partants = []

    reunion_uid = reunion_ref.get("reunion_uid", "")
    date_iso = reunion_ref.get("date_reunion_iso", "")
    hippo = reunion_ref.get("hippodrome_normalise", "")
    hippodrome_raw = reunion_ref.get("hippodrome", "")
    num_reunion = reunion_ref.get("numero_reunion", 0)
    id_letrot = reunion_ref.get("id_letrot", "")

    # Chercher les blocs de course
    # Le Trot utilise differents formats selon les periodes.
    # Strategie : chercher les sections/blocs de course par differents selecteurs

    # ------------------------------------------------------------------
    # Strategy 1: Try extracting structured JSON from Vue.js component
    # Le Trot embeds race data as JSON in a :current-meeting attribute
    # on a <meeting-detail> custom element.
    # ------------------------------------------------------------------
    vue_courses, vue_partants = _extract_vue_meeting_data(
        soup, reunion_uid, date_iso, hippo, hippodrome_raw,
        num_reunion, id_letrot, timestamp, logger,
    )
    if vue_courses:
        return vue_courses, vue_partants

    # ------------------------------------------------------------------
    # Strategy 2: Classical HTML block parsing (older page formats)
    # ------------------------------------------------------------------
    course_blocks = _find_course_blocks(soup)

    if not course_blocks:
        logger.debug("  Aucun bloc de course trouve dans le HTML pour %s/%s", date_iso, id_letrot)
        # Fallback : tenter un parsing plus generique
        course_blocks = _find_course_blocks_fallback(soup)

    for idx, block in enumerate(course_blocks, 1):
        course, block_partants = _parse_course_block(
            block, idx, reunion_uid, date_iso, hippo, hippodrome_raw,
            num_reunion, id_letrot, timestamp, logger
        )
        if course:
            courses.append(course)
            partants.extend(block_partants)

    return courses, partants


def _extract_vue_meeting_data(
    soup: BeautifulSoup,
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    hippodrome_raw: str,
    num_reunion: int,
    id_letrot: str,
    timestamp: str,
    logger: logging.Logger,
) -> tuple[list[CourseBrute], list[PartantBrut]]:
    """Extract race data from Vue.js :current-meeting attribute on <meeting-detail>.

    Le Trot pages are Vue.js SPAs that embed the full meeting/race/runner data
    as a JSON string in the :current-meeting attribute of a <meeting-detail>
    custom element.  This function extracts that JSON and converts it to
    CourseBrute / PartantBrut records using the existing _parse_json_course
    helpers.
    """
    meeting_el = soup.select_one("meeting-detail")
    if not meeting_el:
        return [], []

    raw = meeting_el.get(":current-meeting", "")
    if not raw:
        return [], []

    try:
        meeting_data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.debug("  Impossible de parser :current-meeting JSON pour %s/%s", date_iso, id_letrot)
        return [], []

    races = meeting_data.get("races", [])
    if not races:
        return [], []

    # Use hippodrome info from Vue data when ref has none
    if not hippodrome_raw:
        hippodrome_raw = meeting_data.get("nomHippodrome", "")
    if not hippo:
        hippo = normaliser_texte(hippodrome_raw)
    if not num_reunion:
        num_reunion = safe_int(meeting_data.get("numReunion")) or 0

    courses: list[CourseBrute] = []
    partants: list[PartantBrut] = []

    for race in races:
        nc = safe_int(race.get("numCourse")) or safe_int(race.get("raceNbr")) or 0

        # Map Vue race fields to the dict format expected by _parse_json_course
        course_dict = {
            "course": {
                "libelle": race.get("raceName", ""),
                "distance": race.get("distance"),
                "discipline": race.get("discipline", ""),
                "allocation": race.get("allocation"),
                "corde": race.get("corde", ""),
                "conditions": race.get("conditionAge", ""),
                "specialite": race.get("discipline", ""),
                "typePiste": race.get("typePiste", ""),
            },
            "partants": [],
        }

        # Map Vue partant fields to the dict format expected by _parse_json_partant
        for p in race.get("partants", []):
            mapped = {
                "nom": p.get("name", ""),
                "nomCheval": p.get("name", ""),
                "sexe": p.get("sexe", ""),
                "age": p.get("age"),
                "ferrure": p.get("ferrure", ""),
                "deferre": p.get("ferrure", ""),
                "driver": p.get("driver", ""),
                "entraineur": p.get("coach", ""),
                "proprietaire": p.get("owner", ""),
                "pere": p.get("father", ""),
                "nomPere": p.get("father", ""),
                "mere": p.get("mother", ""),
                "nomMere": p.get("mother", ""),
                "eleveur": p.get("breeder", ""),
                "musique": p.get("song", ""),
                "gains": safe_int(p.get("earnings")),
                "numero": p.get("leavingNumber"),
                "nonPartant": p.get("nonPartant", False),
                "arrivee": _parse_rang(p.get("rang")),
                "temps": None,  # parsed below
                "reductionKilometrique": None,  # parsed below
                "handicapDistance": safe_int(p.get("distance", 0)) - safe_int(race.get("distance", 0))
                    if safe_int(p.get("distance")) and safe_int(race.get("distance"))
                    and safe_int(p.get("distance")) != safe_int(race.get("distance"))
                    else None,
                "oeilleres": "",
            }

            # Parse temps (e.g. "1'19\"30" or "TNC")
            temps_raw = p.get("temps", "")
            if temps_raw and temps_raw not in ("TNC", "NP", ""):
                mapped["temps"] = temps_raw

            # Parse reduction kilometrique
            rk_raw = p.get("reduction", "")
            if rk_raw and rk_raw != "-":
                mapped["reductionKilometrique"] = rk_raw

            course_dict["partants"].append(mapped)

        course_obj, course_partants = _parse_json_course(
            course_dict, reunion_uid, date_iso, hippo, hippodrome_raw,
            num_reunion, nc, id_letrot, timestamp, logger,
        )
        if course_obj:
            # Enrich with Vue-specific fields
            statut_raw = (race.get("statut", "") or race.get("status", "")).upper()
            if statut_raw in ("DEFINITIVE", "DEFINITE"):
                course_obj.statut = "terminee"
                course_obj.arrivee_definitive = True
            elif statut_raw == "ANNULEE":
                course_obj.statut = "annulee"
            course_obj.type_piste = race.get("typePiste", "") or "herbe"
            if race.get("videoUrl"):
                course_obj.replay_disponible = True
            if race.get("autostart"):
                course_obj.categorie_particularite = "AUTOSTART"
            courses.append(course_obj)
            partants.extend(course_partants)

    if courses:
        logger.debug(
            "  Vue.js extraction: %d courses, %d partants pour %s/%s",
            len(courses), len(partants), date_iso, id_letrot,
        )

    return courses, partants


def _parse_rang(rang_str) -> Optional[int]:
    """Parse a rank string like '1 ', '02', 'DA', 'NP' into an integer position or None."""
    if not rang_str:
        return None
    rang_str = str(rang_str).strip()
    if rang_str.isdigit():
        return int(rang_str)
    # Try to extract leading digits
    m = re.match(r"(\d+)", rang_str)
    if m:
        return int(m.group(1))
    return None


def _find_course_blocks(soup: BeautifulSoup) -> list:
    """Trouve les blocs de course dans la page Le Trot."""
    blocks = []

    # Methode 1 : sections avec class contenant "course" ou "race"
    for selector in [
        "section.course", "div.course", "div.race-card",
        "article.course", "div[class*='course']",
        "div.programme-course", "div.race",
        "section[class*='course']", "div[class*='Race']",
        "div.bloc-course", "div.course-bloc",
    ]:
        found = soup.select(selector)
        if found:
            blocks = found
            break

    # Methode 2 : chercher par data-attributes
    if not blocks:
        blocks = soup.select("[data-course]") or soup.select("[data-num-course]")

    # Methode 3 : headers de course (h2, h3 avec numero de course)
    if not blocks:
        headers = soup.find_all(["h2", "h3", "h4"],
                                string=re.compile(r"course\s*n?\s*[°o]?\s*\d", re.I))
        if headers:
            # Chaque header = debut d'un bloc, fin = header suivant ou fin de parent
            for i, header in enumerate(headers):
                block_content = [header]
                sibling = header.find_next_sibling()
                while sibling:
                    # Arreter si on tombe sur le header de la course suivante
                    if sibling in headers:
                        break
                    block_content.append(sibling)
                    sibling = sibling.find_next_sibling()
                # Wrapper dans un element virtuel
                wrapper = BeautifulSoup("<div></div>", "lxml").div
                for el in block_content:
                    wrapper.append(el.__copy__() if hasattr(el, '__copy__') else el)
                blocks.append(wrapper)

    return blocks


def _find_course_blocks_fallback(soup: BeautifulSoup) -> list:
    """Fallback: tente de decouper la page en blocs de course de maniere heuristique."""
    blocks = []

    # Chercher toutes les tables qui pourraient etre des listes de partants
    tables = soup.find_all("table")
    for table in tables:
        # Verifier que la table contient des lignes avec des noms de chevaux
        rows = table.find_all("tr")
        if len(rows) >= 3:  # Au moins en-tete + 2 partants
            # Trouver le contexte (titre de course au dessus)
            context = table.find_previous(["h2", "h3", "h4", "h5", "div", "p"],
                                          string=re.compile(r"course|prix|c\d", re.I))
            wrapper = BeautifulSoup("<div></div>", "lxml").div
            if context:
                wrapper.append(context.__copy__() if hasattr(context, '__copy__') else context)
            wrapper.append(table.__copy__() if hasattr(table, '__copy__') else table)
            blocks.append(wrapper)

    return blocks


def _parse_course_block(
    block,
    default_num: int,
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    hippodrome_raw: str,
    num_reunion: int,
    id_letrot: str,
    timestamp: str,
    logger: logging.Logger,
) -> tuple[Optional[CourseBrute], list[PartantBrut]]:
    """Parse un bloc HTML correspondant a une course."""
    # Extraire le numero de course
    num_course = default_num
    num_match = re.search(
        r"(?:course|c)\s*n?\s*[°o]?\s*(\d+)",
        _clean_text(block) if hasattr(block, 'get_text') else str(block),
        re.I,
    )
    if num_match:
        num_course = int(num_match.group(1))

    # Extraire le titre / libelle de la course
    libelle = ""
    for tag in ["h2", "h3", "h4", "h5"]:
        title_el = block.find(tag)
        if title_el:
            libelle = _clean_text(title_el)
            break
    if not libelle:
        # Chercher un element avec class contenant "titre" ou "title" ou "nom"
        title_el = block.select_one("[class*='titre'], [class*='title'], [class*='nom-course']")
        if title_el:
            libelle = _clean_text(title_el)

    # Extraire la distance
    distance = None
    dist_match = re.search(r"(\d[\d\s\.]*)\s*(?:m(?:etres?|\.)?|M)\b", _clean_text(block))
    if dist_match:
        d = dist_match.group(1).replace(" ", "").replace(".", "")
        distance = safe_int(d)

    # Specialite (attele/monte)
    block_text = _clean_text(block).lower() if hasattr(block, 'get_text') else str(block).lower()
    specialite = _deduire_specialite(block_text)

    # Allocation
    allocation = None
    alloc_match = re.search(
        r"(?:allocation|dotation|prix)\s*(?::|de)?\s*([\d\s\.]+)\s*(?:€|EUR|euros?)?",
        block_text, re.I
    )
    if alloc_match:
        allocation = parse_allocation_euros(alloc_match.group(1))

    # Heure de depart
    heure_depart = ""
    heure_match = re.search(r"(\d{1,2})\s*[hH:]\s*(\d{2})", _clean_text(block))
    if heure_match:
        heure_depart = f"{int(heure_match.group(1)):02d}:{heure_match.group(2)}"

    # Conditions
    conditions_texte = ""
    for cls in ["conditions", "condition", "criteres", "resume"]:
        cond_el = block.select_one(f"[class*='{cls}']")
        if cond_el:
            conditions_texte = _clean_text(cond_el)
            break

    # Corde
    corde = ""
    corde_match = re.search(r"corde\s*(?:a|à)?\s*(gauche|droite)", block_text, re.I)
    if corde_match:
        corde = corde_match.group(1)

    # Ordre d'arrivee
    ordre_arrivee = []
    arrivee_section = block.select_one("[class*='arrivee'], [class*='resultat'], [class*='arrival']")
    if arrivee_section:
        nums = re.findall(r"\b(\d{1,2})\b", _clean_text(arrivee_section))
        if nums:
            ordre_arrivee = [int(n) for n in nums[:10]]

    # Duree course
    duree_course = None
    duree_match = re.search(
        r"(?:temps|duree|dur[ée]e)\s*(?::|de)?\s*(\d+['\u2019]\d{1,2}[\"″\u201d]\d{1,2})",
        block_text, re.I
    )
    if duree_match:
        duree_course = parse_time_to_ms(duree_match.group(1))

    # Nombre de partants
    partants_list = _parse_partants_from_block(
        block, reunion_uid, date_iso, hippo, num_reunion, num_course,
        id_letrot, timestamp, logger
    )
    nombre_partants = len([p for p in partants_list if p.statut_partant != "NON_PARTANT"])

    url_source = f"{LETROT_PROGRAMME}/{date_iso}/{id_letrot}"

    course = CourseBrute(
        source="letrot",
        reunion_uid=reunion_uid,
        date_reunion_iso=date_iso,
        hippodrome_normalise=hippo,
        numero_reunion=num_reunion,
        numero_course=num_course,
        url_source=url_source,
        timestamp_collecte=timestamp,
        libelle=libelle,
        distance=distance,
        discipline="trot",
        specialite=specialite,
        corde=corde,
        conditions_texte=conditions_texte,
        nombre_partants=nombre_partants if nombre_partants > 0 else None,
        montant_prix=allocation * 100 if allocation else None,  # convertir en centimes
        statut="terminee" if ordre_arrivee else "programme",
        ordre_arrivee=ordre_arrivee,
        duree_course=duree_course,
        hippodrome=hippodrome_raw,
    )

    return course, partants_list


def _parse_partants_from_block(
    block,
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    num_reunion: int,
    num_course: int,
    id_letrot: str,
    timestamp: str,
    logger: logging.Logger,
) -> list[PartantBrut]:
    """Extrait les partants depuis un bloc de course."""
    partants = []

    # Chercher un tableau de partants
    table = block.find("table")
    if table:
        partants = _parse_partants_table(
            table, reunion_uid, date_iso, hippo, num_reunion, num_course,
            id_letrot, timestamp, logger
        )

    # Si pas de tableau, chercher des lignes de partants dans des divs
    if not partants:
        partants = _parse_partants_divs(
            block, reunion_uid, date_iso, hippo, num_reunion, num_course,
            id_letrot, timestamp, logger
        )

    return partants


def _parse_partants_table(
    table,
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    num_reunion: int,
    num_course: int,
    id_letrot: str,
    timestamp: str,
    logger: logging.Logger,
) -> list[PartantBrut]:
    """Parse un tableau HTML de partants."""
    partants = []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return partants

    # Detecter les en-tetes pour savoir quelle colonne correspond a quoi
    header_row = rows[0]
    headers = [_clean_text(th).lower() for th in header_row.find_all(["th", "td"])]

    # Mapper les colonnes
    col_map = _detect_column_mapping(headers)

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        # Extraire les donnees en fonction du mapping
        partant = _extract_partant_from_row(
            cells, col_map, headers,
            reunion_uid, date_iso, hippo, num_reunion, num_course,
            id_letrot, timestamp
        )
        if partant and partant.nom:
            partants.append(partant)

    return partants


def _detect_column_mapping(headers: list[str]) -> dict:
    """Detecte le mapping des colonnes a partir des en-tetes."""
    mapping = {}

    for idx, h in enumerate(headers):
        h_norm = normaliser_texte(h)
        if not h_norm:
            continue

        # Numero
        if h_norm in ("n", "n°", "no", "num", "numero", "n?") or h_norm == "#":
            mapping["num"] = idx
        # Nom du cheval
        elif any(k in h_norm for k in ("cheval", "nom", "horse", "partant", "engag")):
            mapping["nom"] = idx
        # Driver
        elif any(k in h_norm for k in ("driver", "jockey", "jock", "driv")):
            mapping["driver"] = idx
        # Entraineur
        elif any(k in h_norm for k in ("entraineur", "entr", "trainer")):
            mapping["entraineur"] = idx
        # Distance / Recul
        elif any(k in h_norm for k in ("dist", "recul", "hand")):
            mapping["distance_handicap"] = idx
        # Gains
        elif any(k in h_norm for k in ("gain", "alloc")):
            mapping["gains"] = idx
        # Musique
        elif any(k in h_norm for k in ("musique", "music", "perf")):
            mapping["musique"] = idx
        # Record / RK
        elif any(k in h_norm for k in ("record", "rk", "reduc")):
            mapping["record"] = idx
        # Proprietaire
        elif any(k in h_norm for k in ("proprio", "proprietaire", "owner")):
            mapping["proprietaire"] = idx
        # Age
        elif h_norm in ("age", "a"):
            mapping["age"] = idx
        # Sexe
        elif h_norm in ("sexe", "s", "sx"):
            mapping["sexe"] = idx
        # Origine / Pere / Mere
        elif any(k in h_norm for k in ("origine", "pere", "mere", "pedigree", "genealog")):
            mapping["origine"] = idx
        # Oeilleres
        elif any(k in h_norm for k in ("oeil", "oe")):
            mapping["oeilleres"] = idx
        # Deferre
        elif any(k in h_norm for k in ("def", "fer")):
            mapping["deferre"] = idx
        # Place arrivee
        elif any(k in h_norm for k in ("arr", "place", "pos", "cl")):
            mapping["arrivee"] = idx
        # Temps
        elif any(k in h_norm for k in ("temps", "time", "chrono")):
            mapping["temps"] = idx
        # RK
        elif any(k in h_norm for k in ("rk", "r.k", "red")):
            mapping["rk"] = idx
        # Eleveur
        elif any(k in h_norm for k in ("elev", "breed")):
            mapping["eleveur"] = idx
        # Courses / Victoires
        elif any(k in h_norm for k in ("courses", "crs", "vict", "v")):
            if "vict" in h_norm or h_norm == "v":
                mapping["victoires"] = idx
            else:
                mapping["nb_courses"] = idx

    # Si aucun mapping nom, prendre la colonne 1 ou 2 par defaut
    if "nom" not in mapping:
        mapping["nom"] = min(1, len(headers) - 1)
    if "num" not in mapping:
        mapping["num"] = 0

    return mapping


def _extract_partant_from_row(
    cells: list,
    col_map: dict,
    headers: list[str],
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    num_reunion: int,
    num_course: int,
    id_letrot: str,
    timestamp: str,
) -> Optional[PartantBrut]:
    """Extrait un PartantBrut d'une ligne de tableau."""
    def cell_text(idx: Optional[int]) -> str:
        if idx is None or idx >= len(cells):
            return ""
        return _clean_text(cells[idx])

    # Numero
    num_text = cell_text(col_map.get("num"))
    num = safe_int(num_text)

    # Nom
    nom_raw = cell_text(col_map.get("nom"))
    if not nom_raw:
        return None

    # Nettoyer le nom (peut contenir des infos supplementaires)
    # Le nom du cheval est souvent en majuscules
    nom = nom_raw.split("\n")[0].strip()
    # Retirer les indicateurs de non-partant
    is_non_partant = False
    if "(NP)" in nom.upper() or "NON PARTANT" in nom.upper():
        is_non_partant = True
        nom = re.sub(r"\(?\s*NP\s*\)?", "", nom, flags=re.I).strip()
        nom = re.sub(r"NON\s*PARTANT", "", nom, flags=re.I).strip()

    # Extraire le sexe et l'age du nom si present (ex: "CHEVAL (H5)" ou "CHEVAL M6")
    age = None
    sexe = ""
    age_sexe_match = re.search(r"\(?([HMFG])\s*(\d{1,2})\)?", nom_raw)
    if age_sexe_match:
        sexe = age_sexe_match.group(1).upper()
        age = safe_int(age_sexe_match.group(2))
        nom = re.sub(r"\(?\s*[HMFG]\s*\d{1,2}\s*\)?", "", nom).strip()

    # Age explicite
    if age is None:
        age = safe_int(cell_text(col_map.get("age")))

    # Sexe explicite
    if not sexe:
        sexe_text = cell_text(col_map.get("sexe"))
        if sexe_text:
            sexe = sexe_text.strip().upper()[:1]

    # Driver
    driver = cell_text(col_map.get("driver"))

    # Entraineur
    entraineur = cell_text(col_map.get("entraineur"))

    # Musique
    musique = cell_text(col_map.get("musique"))

    # Gains
    gains_text = cell_text(col_map.get("gains"))
    gains_carriere = None
    if gains_text:
        g = parse_allocation_euros(gains_text)
        if g is not None:
            gains_carriere = g * 100  # en centimes

    # Origine / Pedigree
    origine_text = cell_text(col_map.get("origine"))
    nom_pere = ""
    nom_mere = ""
    if origine_text:
        # Formats: "PERE x MERE", "PERE et MERE", "PERE - MERE"
        parts = re.split(r"\s*[x×\-]\s*", origine_text, maxsplit=1)
        if len(parts) >= 2:
            nom_pere = parts[0].strip()
            nom_mere = parts[1].strip()
        elif len(parts) == 1:
            nom_pere = parts[0].strip()

    # Proprietaire
    proprietaire = cell_text(col_map.get("proprietaire"))

    # Oeilleres
    oeilleres = cell_text(col_map.get("oeilleres"))

    # Deferre
    deferre = cell_text(col_map.get("deferre"))

    # Arrivee
    position_arrivee = safe_int(cell_text(col_map.get("arrivee")))

    # Temps
    temps_text = cell_text(col_map.get("temps"))
    temps_obtenu = parse_time_to_ms(temps_text)

    # Reduction kilometrique
    rk_text = cell_text(col_map.get("rk")) or cell_text(col_map.get("record"))
    reduction_km = parse_rk_to_ms_per_km(rk_text)

    # Nombre de courses / victoires
    nb_courses = safe_int(cell_text(col_map.get("nb_courses")))
    nb_victoires = safe_int(cell_text(col_map.get("victoires")))

    # Eleveur
    eleveur = cell_text(col_map.get("eleveur"))

    # Distance handicap
    handicap_dist = safe_int(cell_text(col_map.get("distance_handicap")))

    course_uid = make_uid(date_iso, hippo, f"R{num_reunion}", f"C{num_course}")

    return PartantBrut(
        source="letrot",
        course_uid=course_uid,
        reunion_uid=reunion_uid,
        date_reunion_iso=date_iso,
        hippodrome_normalise=hippo,
        numero_reunion=num_reunion,
        numero_course=num_course,
        timestamp_collecte=timestamp,
        nom=nom,
        num_pmu=num,
        age=age,
        sexe=sexe,
        musique=musique,
        gains_carriere=gains_carriere,
        nombre_courses=nb_courses,
        nombre_victoires=nb_victoires,
        driver=driver,
        entraineur=entraineur,
        proprietaire=proprietaire,
        nom_pere=nom_pere,
        nom_mere=nom_mere,
        eleveur=eleveur,
        oeilleres=oeilleres,
        deferre=deferre,
        statut_partant="NON_PARTANT" if is_non_partant else "PARTANT",
        handicap_distance=handicap_dist,
        ordre_arrivee=position_arrivee,
        temps_obtenu=temps_obtenu,
        reduction_kilometrique=reduction_km,
    )


def _parse_partants_divs(
    block,
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    num_reunion: int,
    num_course: int,
    id_letrot: str,
    timestamp: str,
    logger: logging.Logger,
) -> list[PartantBrut]:
    """Parse les partants depuis des elements div/li (layout non-table)."""
    partants = []

    # Chercher des elements individuels de partant
    runner_elements = block.select(
        "[class*='partant'], [class*='runner'], [class*='cheval'], "
        "[class*='horse'], li[class*='part']"
    )

    for el in runner_elements:
        text = _clean_text(el)
        if not text or len(text) < 3:
            continue

        # Extraire le numero et le nom
        num_match = re.match(r"(\d{1,2})\s*[-.\s]+\s*(\S+.*)", text)
        if not num_match:
            continue

        num = int(num_match.group(1))
        rest = num_match.group(2)

        # Le nom est generalement le premier mot en majuscules
        nom_match = re.match(r"([A-Z\s\-']+)", rest)
        nom = nom_match.group(1).strip() if nom_match else rest.split()[0]

        if not nom or len(nom) < 2:
            continue

        course_uid = make_uid(date_iso, hippo, f"R{num_reunion}", f"C{num_course}")

        partant = PartantBrut(
            source="letrot",
            course_uid=course_uid,
            reunion_uid=reunion_uid,
            date_reunion_iso=date_iso,
            hippodrome_normalise=hippo,
            numero_reunion=num_reunion,
            numero_course=num_course,
            timestamp_collecte=timestamp,
            nom=nom,
            num_pmu=num,
            statut_partant="PARTANT",
        )
        partants.append(partant)

    return partants


# ===========================================================================
# PARSING JSON LE TROT
# ===========================================================================

def parse_reunion_json(
    session: requests.Session,
    reunion_ref: dict,
    nb_courses: int,
    timestamp: str,
    pause: float,
    logger: logging.Logger,
) -> tuple[list[CourseBrute], list[PartantBrut], dict]:
    """Recupere et parse les donnees JSON par course pour une reunion."""
    courses = []
    partants = []
    raw_data = {"courses": {}}

    reunion_uid = reunion_ref.get("reunion_uid", "")
    date_iso = reunion_ref.get("date_reunion_iso", "")
    hippo = reunion_ref.get("hippodrome_normalise", "")
    hippodrome_raw = reunion_ref.get("hippodrome", "")
    num_reunion = reunion_ref.get("numero_reunion", 0)
    id_letrot = reunion_ref.get("id_letrot", "")

    # Essayer les courses de 1 a nb_courses + quelques extras au cas ou
    max_course = max(nb_courses + 3, 12) if nb_courses else 12

    for num_course in range(1, max_course + 1):
        data = fetch_letrot_json_course(session, date_iso, id_letrot, num_course, logger)
        if data is None:
            if num_course > nb_courses + 1:
                break  # Plus de courses
            continue

        raw_data["courses"][str(num_course)] = data
        course, course_partants = _parse_json_course(
            data, reunion_uid, date_iso, hippo, hippodrome_raw,
            num_reunion, num_course, id_letrot, timestamp, logger
        )
        if course:
            courses.append(course)
            partants.extend(course_partants)

        time.sleep(pause * 0.3)

    return courses, partants, raw_data


def _parse_json_course(
    data: dict,
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    hippodrome_raw: str,
    num_reunion: int,
    num_course: int,
    id_letrot: str,
    timestamp: str,
    logger: logging.Logger,
) -> tuple[Optional[CourseBrute], list[PartantBrut]]:
    """Parse les donnees JSON d'une course Le Trot."""
    course_info = data.get("course", data)
    if not course_info:
        return None, []

    libelle = course_info.get("libelle", "") or course_info.get("nom", "") or ""
    distance = safe_int(course_info.get("distance"))
    specialite_raw = course_info.get("specialite", "") or course_info.get("discipline", "")
    specialite = _deduire_specialite(specialite_raw) if specialite_raw else "trot_attele"
    conditions = course_info.get("conditions", "") or course_info.get("conditionsTexte", "") or ""
    allocation = safe_int(course_info.get("allocation")) or safe_int(course_info.get("montantPrix"))
    corde = course_info.get("corde", "")

    # Ordre d'arrivee
    arrivee = course_info.get("arrivee", []) or course_info.get("ordreArrivee", [])
    if isinstance(arrivee, str):
        arrivee = [safe_int(x) for x in re.findall(r"\d+", arrivee) if safe_int(x)]

    # Duree
    duree = None
    duree_raw = course_info.get("tempsTotal") or course_info.get("dureeCourse")
    if duree_raw:
        if isinstance(duree_raw, (int, float)):
            duree = int(duree_raw)
        else:
            duree = parse_time_to_ms(str(duree_raw))

    url_source = f"{LETROT_PROGRAMME}/{date_iso}/{id_letrot}"

    # Partants
    partants_data = data.get("partants", []) or data.get("participants", []) or []
    partants_list = []
    for p in partants_data:
        partant = _parse_json_partant(
            p, reunion_uid, date_iso, hippo, num_reunion, num_course,
            id_letrot, timestamp
        )
        if partant:
            partants_list.append(partant)

    nb_partants = len([p for p in partants_list if p.statut_partant != "NON_PARTANT"])

    course = CourseBrute(
        source="letrot",
        reunion_uid=reunion_uid,
        date_reunion_iso=date_iso,
        hippodrome_normalise=hippo,
        numero_reunion=num_reunion,
        numero_course=num_course,
        url_source=url_source,
        timestamp_collecte=timestamp,
        libelle=libelle,
        distance=distance,
        discipline="trot",
        specialite=specialite,
        corde=corde,
        conditions_texte=conditions,
        nombre_partants=nb_partants if nb_partants > 0 else None,
        montant_prix=allocation * 100 if allocation else None,
        statut="terminee" if arrivee else "programme",
        ordre_arrivee=arrivee if isinstance(arrivee, list) else [],
        duree_course=duree,
        hippodrome=hippodrome_raw,
    )

    return course, partants_list


def _parse_json_partant(
    p: dict,
    reunion_uid: str,
    date_iso: str,
    hippo: str,
    num_reunion: int,
    num_course: int,
    id_letrot: str,
    timestamp: str,
) -> Optional[PartantBrut]:
    """Parse un partant depuis les donnees JSON Le Trot."""
    nom = (p.get("nom", "") or p.get("nomCheval", "") or "").strip()
    if not nom:
        return None

    num = safe_int(p.get("numero")) or safe_int(p.get("num"))
    age = safe_int(p.get("age"))
    sexe = (p.get("sexe", "") or "").strip().upper()[:1]
    race = (p.get("race", "") or "").strip()
    musique = (p.get("musique", "") or "").strip()

    gains_raw = safe_int(p.get("gainsCarriere")) or safe_int(p.get("gains"))
    gains_carriere = gains_raw * 100 if gains_raw else None  # en centimes si en euros

    nb_courses = safe_int(p.get("nombreCourses")) or safe_int(p.get("nbCourses"))
    nb_victoires = safe_int(p.get("nombreVictoires")) or safe_int(p.get("nbVictoires"))

    driver = (p.get("driver", "") or p.get("jockey", "") or "").strip()
    entraineur = (p.get("entraineur", "") or p.get("trainer", "") or "").strip()
    proprietaire = (p.get("proprietaire", "") or p.get("owner", "") or "").strip()

    nom_pere = (p.get("pere", "") or p.get("nomPere", "") or "").strip()
    nom_mere = (p.get("mere", "") or p.get("nomMere", "") or "").strip()
    eleveur = (p.get("eleveur", "") or "").strip()

    oeilleres = (p.get("oeilleres", "") or "").strip()
    deferre = (p.get("deferre", "") or p.get("ferrure", "") or "").strip()

    # Statut
    statut_raw = (p.get("statut", "") or "").upper()
    is_non_partant = "NON" in statut_raw or p.get("nonPartant", False)

    # Resultat
    position = safe_int(p.get("arrivee")) or safe_int(p.get("place")) or safe_int(p.get("ordreArrivee"))
    temps_raw = p.get("tempsObtenu") or p.get("temps")
    temps = None
    if temps_raw:
        if isinstance(temps_raw, (int, float)):
            temps = int(temps_raw)
        else:
            temps = parse_time_to_ms(str(temps_raw))

    rk_raw = p.get("reductionKilometrique") or p.get("rk")
    rk = None
    if rk_raw:
        if isinstance(rk_raw, (int, float)):
            rk = int(rk_raw)
        else:
            rk = parse_rk_to_ms_per_km(str(rk_raw))

    handicap_dist = safe_int(p.get("handicapDistance")) or safe_int(p.get("recul"))

    course_uid = make_uid(date_iso, hippo, f"R{num_reunion}", f"C{num_course}")

    return PartantBrut(
        source="letrot",
        course_uid=course_uid,
        reunion_uid=reunion_uid,
        date_reunion_iso=date_iso,
        hippodrome_normalise=hippo,
        numero_reunion=num_reunion,
        numero_course=num_course,
        timestamp_collecte=timestamp,
        nom=nom,
        num_pmu=num,
        age=age,
        sexe=sexe,
        race=race,
        musique=musique,
        gains_carriere=gains_carriere,
        nombre_courses=nb_courses,
        nombre_victoires=nb_victoires,
        driver=driver,
        entraineur=entraineur,
        proprietaire=proprietaire,
        nom_pere=nom_pere,
        nom_mere=nom_mere,
        eleveur=eleveur,
        oeilleres=oeilleres,
        deferre=deferre,
        statut_partant="NON_PARTANT" if is_non_partant else "PARTANT",
        handicap_distance=handicap_dist,
        ordre_arrivee=position,
        temps_obtenu=temps,
        reduction_kilometrique=rk,
    )


# ===========================================================================
# NORMALISATION COURSE + PARTANT
# ===========================================================================

def normaliser_course(brute: CourseBrute, reunion_ref: dict) -> CourseNormalisee:
    """Normalise une course brute."""
    date_iso = brute.date_reunion_iso
    hippo = brute.hippodrome_normalise
    nr = brute.numero_reunion
    nc = brute.numero_course

    return CourseNormalisee(
        course_uid=make_uid(date_iso, hippo, f"R{nr}", f"C{nc}"),
        reunion_uid=brute.reunion_uid,
        cle_course=f"{date_iso}|{hippo}|R{nr}|C{nc}",
        source=brute.source,
        date_reunion_iso=date_iso,
        hippodrome_normalise=hippo,
        hippodrome=reunion_ref.get("hippodrome", ""),
        pays=reunion_ref.get("pays", ""),
        numero_reunion=nr,
        numero_course=nc,
        libelle=brute.libelle,
        distance=brute.distance,
        parcours=brute.parcours,
        corde=normaliser_corde(brute.corde),
        discipline=normaliser_discipline_course(brute.discipline),
        specialite=normaliser_discipline_course(brute.specialite),
        conditions_texte=brute.conditions_texte,
        condition_sexe=brute.condition_sexe.strip().lower() if brute.condition_sexe else "",
        condition_age=brute.condition_age.strip().lower().replace("_", " ") if brute.condition_age else "",
        categorie=brute.categorie_particularite.strip().lower().replace("_", " ") if brute.categorie_particularite else "",
        mode_depart=_deduire_mode_depart(brute.categorie_particularite, brute.discipline),
        nombre_partants=brute.nombre_partants,
        heure_depart=ms_to_hhmm(brute.heure_depart) if isinstance(brute.heure_depart, int) else "",
        allocation_totale=centimes_to_euros(brute.montant_prix),
        allocation_1er=centimes_to_euros(brute.montant_1er),
        type_piste="cendree",  # Trot = cendree par defaut
        penetrometre=brute.penetrometre,
        statut=brute.statut.strip().lower().replace("_", " ") if brute.statut else "",
        ordre_arrivee=brute.ordre_arrivee,
        duree_course_ms=brute.duree_course,
        incidents=brute.incidents,
        paris_types=[],  # Pas de paris sur ces courses
        replay_disponible=brute.replay_disponible,
        course_trackee=brute.course_trackee,
        timestamp_collecte=brute.timestamp_collecte,
        url_source=brute.url_source,
    )


def normaliser_partant(brute: PartantBrut, course_norm: CourseNormalisee) -> PartantNormalise:
    """Normalise un partant brut."""
    date_iso = brute.date_reunion_iso
    hippo = brute.hippodrome_normalise
    nr = brute.numero_reunion
    nc = brute.numero_course
    num = brute.num_pmu or 0

    pos = brute.ordre_arrivee
    is_gagnant = pos == 1 if pos else False
    is_place = pos is not None and 1 <= pos <= 3

    return PartantNormalise(
        partant_uid=make_uid(date_iso, hippo, f"R{nr}", f"C{nc}", str(num)),
        course_uid=course_norm.course_uid,
        reunion_uid=brute.reunion_uid,
        cle_partant=f"{date_iso}|{hippo}|R{nr}|C{nc}|{num}",
        source=brute.source,
        date_reunion_iso=date_iso,
        hippodrome_normalise=hippo,
        numero_reunion=nr,
        numero_course=nc,
        distance=course_norm.distance,
        discipline=course_norm.discipline,
        horse_id=_make_horse_id(brute.nom, brute.nom_pere, brute.nom_mere),
        nom_cheval=brute.nom.strip() if brute.nom else "",
        num_pmu=brute.num_pmu,
        age=brute.age,
        sexe=brute.sexe.strip().lower() if brute.sexe else "",
        race=brute.race.strip() if brute.race else "",
        robe=brute.robe.strip().lower() if brute.robe else "",
        musique=brute.musique,
        nb_courses_carriere=brute.nombre_courses,
        nb_victoires_carriere=brute.nombre_victoires,
        nb_places_carriere=brute.nombre_places,
        nb_places_2eme=brute.nombre_places_second,
        nb_places_3eme=brute.nombre_places_troisieme,
        gains_carriere_euros=centimes_to_euros(brute.gains_carriere),
        gains_annee_euros=centimes_to_euros(brute.gains_annee_en_cours),
        is_inedit=brute.indicateur_inedit,
        jockey_driver=brute.driver.strip() if brute.driver else "",
        jockey_driver_change=brute.driver_change,
        entraineur=brute.entraineur.strip() if brute.entraineur else "",
        proprietaire=brute.proprietaire.strip() if brute.proprietaire else "",
        pere=brute.nom_pere.strip() if brute.nom_pere else "",
        mere=brute.nom_mere.strip() if brute.nom_mere else "",
        eleveur=brute.eleveur.strip() if brute.eleveur else "",
        oeilleres=normaliser_oeilleres(brute.oeilleres),
        deferre=normaliser_deferre(brute.deferre),
        statut=normaliser_statut_partant(brute.statut_partant),
        engagement=brute.engagement,
        supplement_euros=centimes_to_euros(brute.supplement),
        handicap_distance_m=brute.handicap_distance,
        poids_porte_kg=round(brute.handicap_poids / 10.0, 1) if brute.handicap_poids else None,
        poids_base_kg=round(brute.poids_condition_monte / 10.0, 1) if brute.poids_condition_monte else None,
        surcharge_decharge_kg=(
            round((brute.handicap_poids - brute.poids_condition_monte) / 10.0, 1)
            if brute.handicap_poids and brute.poids_condition_monte else None
        ),
        handicap_valeur=brute.handicap_valeur,
        poids_monte_change=brute.poids_condition_monte_change,
        taux_reclamation_euros=centimes_to_euros(brute.taux_reclamation),
        place_corde=brute.place_corde,
        allure=brute.allure.strip().lower() if brute.allure else "",
        pays_cheval=brute.pays.strip() if brute.pays else "",
        pays_entrainement=brute.pays_entrainement.strip() if brute.pays_entrainement else "",
        pere_mere=brute.nom_pere_mere.strip() if brute.nom_pere_mere else "",
        incident=brute.incident.strip().lower().replace("_", " ") if brute.incident else "",
        ecart_precedent=brute.distance_cheval_precedent.strip() if brute.distance_cheval_precedent else "",
        commentaire_apres_course=brute.commentaire_apres_course,
        avis_entraineur=brute.avis_entraineur.strip().lower() if brute.avis_entraineur else "",
        jument_pleine=brute.jument_pleine,
        position_arrivee=pos,
        temps_ms=brute.temps_obtenu,
        reduction_km_ms=brute.reduction_kilometrique,
        is_gagnant=is_gagnant,
        is_place=is_place,
        is_disqualifie=False,
        cote_finale=None,  # Pas de cotes hors-PMU
        cote_reference=None,
        proba_implicite=None,
        timestamp_collecte=brute.timestamp_collecte,
    )


# ===========================================================================
# SAUVEGARDE
# ===========================================================================




def sauver_parquet(data: list[dict], path: Path, logger: logging.Logger):
    if not HAS_PARQUET or not data:
        return
    try:
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path)
        logger.info("Sauve: %s", path.name)
    except Exception as e:
        logger.warning("Parquet ignore: %s", e)





# ===========================================================================
# JSONL EXPORT
# ===========================================================================

def aggregate_cache_to_jsonl():
    """Read all cache files, parse HTML, normalise and write JSONL output."""
    logger = setup_logging("02b_scraper_letrot")
    jsonl_path = OUTPUT_DIR / "letrot_data.jsonl"

    # Build lookup from references file (date_iso, id_letrot) -> ref dict
    ref_lookup: dict[tuple[str, str], dict] = {}
    if REFERENCES_PATH.exists():
        try:
            with open(REFERENCES_PATH, "r", encoding="utf-8") as f:
                all_refs = json.load(f)
            for r in all_refs:
                date_iso = r.get("date_reunion_iso", "")
                id_lt = r.get("id_letrot", "")
                if date_iso and id_lt:
                    ref_lookup[(date_iso, str(id_lt))] = r
            logger.info("References chargees: %d reunions", len(ref_lookup))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Impossible de charger les references: %s", e)

    # Enumerate cache files
    if not CACHE_DIR.exists():
        logger.info("Aucun repertoire cache: %s", CACHE_DIR)
        return

    cache_files = sorted(f for f in CACHE_DIR.iterdir() if f.suffix == ".json")
    if not cache_files:
        logger.info("Aucun fichier cache trouve.")
        return

    logger.info("Fichiers cache a traiter: %d", len(cache_files))
    timestamp = utc_now_iso()
    total_courses = 0
    total_partants = 0
    errors = 0

    with open(jsonl_path, "w", encoding="utf-8", newline="\n") as out:
        for idx, fpath in enumerate(cache_files, 1):
            # Extract date_iso and id_letrot from filename: {date}_{id}.json
            stem = fpath.stem  # e.g. "2016-04-03_4917"
            parts = stem.split("_", 1)
            if len(parts) != 2:
                logger.warning("Nom de fichier cache inattendu: %s", fpath.name)
                errors += 1
                continue

            date_iso, id_letrot = parts[0], parts[1]

            # Load cache data
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    cached = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Erreur lecture cache %s: %s", fpath.name, e)
                errors += 1
                continue

            # Build reunion ref (from references or minimal fallback)
            ref = ref_lookup.get((date_iso, id_letrot), {
                "date_reunion_iso": date_iso,
                "id_letrot": id_letrot,
                "reunion_uid": make_uid(date_iso, id_letrot),
                "hippodrome_normalise": "",
                "hippodrome": "",
                "numero_reunion": 0,
                "nombre_courses_reunion": 0,
            })

            reunion_uid = ref.get("reunion_uid", "")
            hippo = ref.get("hippodrome_normalise", "")
            num_reunion = ref.get("numero_reunion", 0)

            # Parse cached data
            courses_brut: list = []
            partants_brut: list = []

            if "html" in cached:
                courses_brut, partants_brut = parse_reunion_html(
                    cached["html"], ref, timestamp, logger
                )
            elif "courses" in cached:
                for nc_str, course_data in cached.get("courses", {}).items():
                    nc = int(nc_str)
                    course, cpartants = _parse_json_course(
                        course_data, reunion_uid, date_iso, hippo,
                        ref.get("hippodrome", ""), num_reunion, nc,
                        id_letrot, timestamp, logger
                    )
                    if course:
                        courses_brut.append(course)
                        partants_brut.extend(cpartants)

            if not courses_brut:
                continue

            # Normalise and write courses
            courses_norm_list: list[dict] = []
            for course_brute in courses_brut:
                course_norm = normaliser_course(course_brute, ref)
                rec = asdict(course_norm)
                rec["_type"] = "course"
                out.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                courses_norm_list.append(asdict(course_norm))
                total_courses += 1

            # Normalise and write partants
            for pb in partants_brut:
                # Find matching normalised course
                course_norm_data = CourseNormalisee(
                    course_uid=make_uid(date_iso, hippo, f"R{num_reunion}", f"C{pb.numero_course}"),
                    distance=None,
                    discipline=normaliser_discipline_course("trot"),
                )
                for cn_dict in courses_norm_list:
                    if cn_dict.get("numero_course") == pb.numero_course:
                        course_norm_data = CourseNormalisee(**{
                            k: v for k, v in cn_dict.items()
                            if k in CourseNormalisee.__dataclass_fields__
                        })
                        break

                pn = normaliser_partant(pb, course_norm_data)
                rec = asdict(pn)
                rec["_type"] = "partant"
                out.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                total_partants += 1

            if idx % 500 == 0:
                logger.info(
                    "  [%d/%d] courses=%d partants=%d erreurs=%d",
                    idx, len(cache_files), total_courses, total_partants, errors,
                )

    logger.info(
        "JSONL export termine: %d courses, %d partants, %d erreurs -> %s",
        total_courses, total_partants, errors, jsonl_path.name,
    )


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Collecte courses + partants Le Trot (hors-PMU)"
    )
    parser.add_argument("--pause", type=float, default=0.5,
                        help="Pause entre requetes (s)")
    parser.add_argument("--batch", type=int, default=100,
                        help="Sauvegarde intermediaire tous les N reunions")
    parser.add_argument("--date-debut", type=str, default=None,
                        help="Date debut (YYYY-MM-DD)")
    parser.add_argument("--date-fin", type=str, default=None,
                        help="Date fin (YYYY-MM-DD)")
    parser.add_argument("--max-reunions", type=int, default=0,
                        help="Max reunions a traiter (0=toutes)")
    parser.add_argument("--force-html", action="store_true",
                        help="Forcer le parsing HTML meme si JSON disponible")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Repart de zero (ignore le checkpoint)")
    parser.add_argument("--export", action="store_true",
                        help="Exporter le cache en JSONL sans scraper")
    args = parser.parse_args()

    if args.export:
        aggregate_cache_to_jsonl()
        return

    logger = setup_logging("02b_scraper_letrot")
    logger.info("=" * 70)
    logger.info("02b -- COLLECTE COURSES + PARTANTS LE TROT (HORS-PMU)")
    logger.info("=" * 70)

    # Charger references
    if not REFERENCES_PATH.exists():
        logger.error("Fichier references introuvable: %s", REFERENCES_PATH)
        sys.exit(1)

    with open(REFERENCES_PATH, "r", encoding="utf-8") as f:
        all_refs = json.load(f)
    logger.info("References chargees: %d reunions", len(all_refs))

    # Filtrer par date si demande
    refs = all_refs
    if args.date_debut:
        refs = [r for r in refs if r.get("date_reunion_iso", "") >= args.date_debut]
    if args.date_fin:
        refs = [r for r in refs if r.get("date_reunion_iso", "") <= args.date_fin]

    # Filtrer: url_letrot presente ET url_pmu absente (reunions hors-PMU)
    refs_letrot = [
        r for r in refs
        if r.get("url_letrot") and not r.get("url_pmu")
    ]
    refs_letrot.sort(key=lambda r: (r.get("date_reunion_iso", ""), r.get("id_letrot", "")))

    logger.info("Reunions Le Trot hors-PMU a traiter: %d", len(refs_letrot))

    if args.max_reunions > 0:
        refs_letrot = refs_letrot[:args.max_reunions]
        logger.info("Limite a %d reunions", args.max_reunions)

    if not refs_letrot:
        logger.info("Aucune reunion a traiter. Fin.")
        return

    # Checkpoint
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_path = OUTPUT_DIR / ".checkpoint_02b.json"
    if args.reset_checkpoint and checkpoint_path.exists():
        checkpoint_path.unlink()
        logger.info("Checkpoint reinitialise")
    checkpoint = CheckpointManager(checkpoint_path)
    logger.info("Checkpoint: %d reunions deja traitees", checkpoint.count_done)

    # Cache
    cache = ReunionCache(CACHE_DIR)

    # Session HTTP
    session = create_session()

    # Detecter si l'API JSON est disponible
    use_json_api = False
    if not args.force_html:
        use_json_api = detect_json_api(session, logger)

    # Accumulateurs
    all_courses_brut: list[dict] = []
    all_courses_norm: list[dict] = []
    all_partants_brut: list[dict] = []
    all_partants_norm: list[dict] = []

    # Charger les donnees existantes si le fichier existe (reprise apres crash)
    for fname, acc in [
        ("courses_brut.json", all_courses_brut),
        ("courses_normalisees.json", all_courses_norm),
        ("partants_brut.json", all_partants_brut),
        ("partants_normalises.json", all_partants_norm),
    ]:
        fpath = OUTPUT_DIR / fname
        if fpath.exists() and checkpoint.count_done > 0:
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                acc.extend(existing)
                logger.info("Reprise: %d entrees chargees depuis %s", len(existing), fname)
            except (json.JSONDecodeError, OSError):
                pass

    total_courses = len(all_courses_brut)
    total_partants = len(all_partants_brut)
    total_erreurs = 0
    total_requetes = 0
    reunions_traitees = 0

    for idx, ref in enumerate(refs_letrot, 1):
        reunion_uid = ref.get("reunion_uid", "")

        # Skip si deja fait
        if checkpoint.is_done(reunion_uid):
            continue

        date_iso = ref.get("date_reunion_iso", "")
        id_letrot = ref.get("id_letrot", "")
        hippo = ref.get("hippodrome_normalise", "")
        num_reunion = ref.get("numero_reunion", 0)
        nb_courses_ref = ref.get("nombre_courses_reunion", 0)

        if not date_iso or not id_letrot:
            checkpoint.mark_done(reunion_uid)
            continue

        timestamp = utc_now_iso()

        # === Verifier le cache ===
        cached = cache.get(date_iso, id_letrot)
        from_cache = False

        if cached:
            from_cache = True
            courses_brut = []
            partants_brut = []

            # Le cache peut contenir du HTML parse ou des donnees JSON
            if "html" in cached:
                courses_brut, partants_brut = parse_reunion_html(
                    cached["html"], ref, timestamp, logger
                )
            elif "courses" in cached:
                # Re-parse depuis les donnees cachees
                for nc_str, course_data in cached.get("courses", {}).items():
                    nc = int(nc_str)
                    course, cpartants = _parse_json_course(
                        course_data, reunion_uid, date_iso, hippo,
                        ref.get("hippodrome", ""), num_reunion, nc,
                        id_letrot, timestamp, logger
                    )
                    if course:
                        courses_brut.append(course)
                        partants_brut.extend(cpartants)
        else:
            # Fetch depuis Le Trot
            if use_json_api:
                courses_brut, partants_brut, raw_data = parse_reunion_json(
                    session, ref, nb_courses_ref, timestamp, args.pause, logger
                )
                total_requetes += nb_courses_ref + 3
                if courses_brut:
                    cache.put(date_iso, id_letrot, raw_data)
            else:
                html = fetch_letrot_programme(session, date_iso, id_letrot, logger)
                total_requetes += 1
                if html:
                    courses_brut, partants_brut = parse_reunion_html(
                        html, ref, timestamp, logger
                    )
                    # Sauver le HTML dans le cache
                    cache.put(date_iso, id_letrot, {"html": html})
                else:
                    courses_brut = []
                    partants_brut = []
                    total_erreurs += 1

        if not courses_brut:
            if not from_cache:
                total_erreurs += 1
            checkpoint.mark_done(reunion_uid)
            if not from_cache:
                time.sleep(args.pause)
            continue

        # Normaliser
        for course_brute in courses_brut:
            course_norm = normaliser_course(course_brute, ref)
            all_courses_brut.append(asdict(course_brute))
            all_courses_norm.append(asdict(course_norm))
            total_courses += 1

        for pb in partants_brut:
            # Trouver la course normalisee correspondante
            course_norm_data = CourseNormalisee(
                course_uid=make_uid(date_iso, hippo, f"R{num_reunion}", f"C{pb.numero_course}"),
                distance=None,
                discipline=normaliser_discipline_course("trot"),
            )
            # Chercher la bonne course
            for cn_dict in all_courses_norm[-len(courses_brut):]:
                if cn_dict.get("numero_course") == pb.numero_course:
                    course_norm_data = CourseNormalisee(**{
                        k: v for k, v in cn_dict.items()
                        if k in CourseNormalisee.__dataclass_fields__
                    })
                    break

            pn = normaliser_partant(pb, course_norm_data)
            all_partants_brut.append(asdict(pb))
            all_partants_norm.append(asdict(pn))
            total_partants += 1

        checkpoint.mark_done(reunion_uid)
        reunions_traitees += 1

        # Log progression
        if reunions_traitees % 50 == 0 or reunions_traitees <= 5:
            logger.info(
                "  [%d/%d] %s %s | courses=%d partants=%d erreurs=%d req=%d",
                idx, len(refs_letrot), date_iso, hippo[:20],
                total_courses, total_partants, total_erreurs, total_requetes,
            )

        # Sauvegarde intermediaire
        if reunions_traitees % args.batch == 0 and reunions_traitees > 0:
            sauver_json(all_courses_norm, OUTPUT_DIR / "courses_normalisees.json", logger)
            sauver_json(all_partants_norm, OUTPUT_DIR / "partants_normalises.json", logger)
            checkpoint.save()
            logger.info(
                ">>> Sauvegarde intermediaire: %d reunions, %d courses, %d partants <<<",
                reunions_traitees, total_courses, total_partants,
            )

        # Renouveler session tous les 2000 requetes
        if total_requetes > 0 and total_requetes % 2000 == 0:
            session.close()
            session = create_session()
            logger.info("  Session HTTP renouvelee")

        if not from_cache:
            time.sleep(args.pause)

    # === Sauvegarde finale ===
    logger.info("Sauvegarde finale...")

    # Courses
    sauver_json(all_courses_brut, OUTPUT_DIR / "courses_brut.json", logger)
    sauver_json(all_courses_norm, OUTPUT_DIR / "courses_normalisees.json", logger)
    sauver_parquet(all_courses_norm, OUTPUT_DIR / "courses_normalisees.parquet", logger)
    sauver_csv(all_courses_norm, OUTPUT_DIR / "courses_normalisees.csv", logger)

    # Partants
    sauver_json(all_partants_brut, OUTPUT_DIR / "partants_brut.json", logger)
    sauver_json(all_partants_norm, OUTPUT_DIR / "partants_normalises.json", logger)
    sauver_parquet(all_partants_norm, OUTPUT_DIR / "partants_normalises.parquet", logger)
    sauver_csv(all_partants_norm, OUTPUT_DIR / "partants_normalises.csv", logger)

    checkpoint.save()

    logger.info("=" * 70)
    logger.info(
        "TERMINE: %d reunions, %d courses, %d partants, %d erreurs, %d requetes",
        reunions_traitees, total_courses, total_partants, total_erreurs, total_requetes,
    )
    logger.info("Output: %s", OUTPUT_DIR.resolve())
    logger.info("=" * 70)

    session.close()

    # Export cache to JSONL
    aggregate_cache_to_jsonl()


if __name__ == "__main__":
    main()
