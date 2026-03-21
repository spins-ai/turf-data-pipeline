#!/usr/bin/env python3
"""
02_liste_courses.py
====================
Collecte multi-sources des courses et partants par réunion.

Fusionne les étapes 02 (courses) et 03 (partants) car l'API PMU retourne
les deux en une seule requête par réunion.

Sources :
  - PMU (API JSON) : /programme/{date}/R{num} → toutes les courses + participants

Produit (JSONL append — léger en RAM) :
  - courses_brut.jsonl / courses_normalisees.jsonl
  - partants_brut.jsonl / partants_normalises.jsonl
  - courses_references_04.jsonl (interface pour script 04_resultats.py)

PATCH JSONL : ~50 MB RAM au lieu de 5 GB — append mode, pas d'accumulation

Architecture :
  - 1 requête PMU par RÉUNION → toutes les courses + partants + résultats
  - Cache JSON par jour pour reprise
  - Checkpoint par réunion
  - Export JSONL

Usage :
    python3 02_liste_courses.py
    python3 02_liste_courses.py --pause 0.3 --batch 500
    python3 02_liste_courses.py --date-debut 2024-01-01 --date-fin 2024-12-31
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
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

# ===========================================================================
# CONFIG
# ===========================================================================

REFERENCES_PATH = Path(__file__).resolve().parent / "output" / "01_calendrier_reunions" / "reunions_references_02.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "02_liste_courses"
CACHE_DIR = OUTPUT_DIR / "cache"
from utils.logging_setup import setup_logging

PMU_API_BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme"


# ===========================================================================
# DATACLASSES — COURSE
# ===========================================================================

@dataclass
class CourseBrute:
    """Course telle que collectée depuis l'API."""
    source: str = ""
    reunion_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    url_source: str = ""
    timestamp_collecte: str = ""
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
    heure_depart: Optional[int] = None
    montant_prix: Optional[int] = None
    montant_1er: Optional[int] = None
    montant_2eme: Optional[int] = None
    montant_3eme: Optional[int] = None
    montant_4eme: Optional[int] = None
    montant_5eme: Optional[int] = None
    statut: str = ""
    categorie_statut: str = ""
    ordre_arrivee: list = field(default_factory=list)
    duree_course: Optional[int] = None
    incidents: list = field(default_factory=list)
    arrivee_definitive: bool = False
    type_piste: str = ""
    penetrometre: str = ""
    penetrometre_valeur: str = ""
    paris_disponibles: list = field(default_factory=list)
    hippodrome: str = ""
    replay_disponible: bool = False
    course_trackee: bool = False
    extras: dict = field(default_factory=dict)


@dataclass
class CourseNormalisee:
    """Course normalisée pour le pipeline aval."""
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
    allocation_totale: Optional[int] = None
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
# DATACLASSES — PARTANT
# ===========================================================================

@dataclass
class PartantBrut:
    """Partant tel que collecté depuis l'API."""
    source: str = ""
    course_uid: str = ""
    reunion_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    timestamp_collecte: str = ""
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
    gains_carriere: Optional[int] = None
    gains_victoires: Optional[int] = None
    gains_place: Optional[int] = None
    gains_annee_en_cours: Optional[int] = None
    gains_annee_precedente: Optional[int] = None
    indicateur_inedit: bool = False
    driver: str = ""
    driver_change: bool = False
    entraineur: str = ""
    proprietaire: str = ""
    nom_pere: str = ""
    nom_mere: str = ""
    eleveur: str = ""
    oeilleres: str = ""
    deferre: str = ""
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
    pays: str = ""
    pays_entrainement: str = ""
    nom_pere_mere: str = ""
    incident: str = ""
    distance_cheval_precedent: str = ""
    commentaire_apres_course: str = ""
    avis_entraineur: str = ""
    jument_pleine: bool = False
    ordre_arrivee: Optional[int] = None
    temps_obtenu: Optional[int] = None
    reduction_kilometrique: Optional[int] = None
    cote_direct: Optional[float] = None
    cote_reference: Optional[float] = None
    url_casaque: str = ""
    extras: dict = field(default_factory=dict)


@dataclass
class PartantNormalise:
    """Partant normalisé pour le pipeline aval."""
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
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normaliser_texte(texte: str) -> str:
    if not texte:
        return ""
    texte = texte.strip().lower()
    nfkd = unicodedata.normalize("NFKD", texte)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


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


# ===========================================================================
# CACHE
# ===========================================================================

class ReunionCache:
    """Cache des réponses API brutes par réunion."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, date_iso: str, num_reunion: int) -> Path:
        return self.cache_dir / f"{date_iso}_R{num_reunion}.json"

    def has(self, date_iso: str, num_reunion: int) -> bool:
        return self._path(date_iso, num_reunion).exists()

    def get(self, date_iso: str, num_reunion: int) -> Optional[dict]:
        p = self._path(date_iso, num_reunion)
        if not p.exists():
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def put(self, date_iso: str, num_reunion: int, data: dict):
        target = self._path(date_iso, num_reunion)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        tmp.replace(target)


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
        return {"completed_reunions": [], "total_courses": 0, "total_partants": 0}

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

    def update_counts(self, courses: int, partants: int):
        self._data["total_courses"] = courses
        self._data["total_partants"] = partants


# ===========================================================================
# JSONL WRITER
# ===========================================================================

class JsonlWriter:
    """Écrit en mode append dans 4 fichiers JSONL — pas d'accumulation mémoire."""

    def __init__(self, output_dir: Path):
        output_dir.mkdir(parents=True, exist_ok=True)
        self.courses_brut = output_dir / "courses_brut.jsonl"
        self.courses_norm = output_dir / "courses_normalisees.jsonl"
        self.partants_brut = output_dir / "partants_brut.jsonl"
        self.partants_norm = output_dir / "partants_normalises.jsonl"
        self.courses_ref = output_dir / "courses_references_04.jsonl"

    def write_course(self, brut: dict, norm: dict):
        self._append(self.courses_brut, brut)
        self._append(self.courses_norm, norm)
        # Référence pour script 04
        ref = {
            "course_uid": norm["course_uid"],
            "reunion_uid": norm["reunion_uid"],
            "date_reunion_iso": norm["date_reunion_iso"],
            "hippodrome_normalise": norm["hippodrome_normalise"],
            "numero_reunion": norm["numero_reunion"],
            "numero_course": norm["numero_course"],
            "nombre_partants": norm["nombre_partants"],
            "statut": norm["statut"],
            "discipline": norm.get("discipline", ""),
            "distance": norm.get("distance"),
        }
        self._append(self.courses_ref, ref)

    def write_partant(self, brut: dict, norm: dict):
        self._append(self.partants_brut, brut)
        self._append(self.partants_norm, norm)

    @staticmethod
    def _append(path: Path, record: dict):
        with open(path, "a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


# ===========================================================================
# PARSING PMU
# ===========================================================================

def fetch_reunion_pmu(
    session: requests.Session,
    date_ddmmyyyy: str,
    num_reunion: int,
    logger: logging.Logger,
) -> Optional[dict]:
    url = f"{PMU_API_BASE}/{date_ddmmyyyy}/R{num_reunion}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.warning("  HTTP error R%d: %s", num_reunion, str(e)[:80])
        return None
    except json.JSONDecodeError as e:
        logger.warning("  JSON error R%d: %s", num_reunion, str(e)[:80])
        return None


def fetch_participants_pmu(
    session: requests.Session,
    date_ddmmyyyy: str,
    num_reunion: int,
    num_course: int,
    logger: logging.Logger,
) -> Optional[dict]:
    url = f"{PMU_API_BASE}/{date_ddmmyyyy}/R{num_reunion}/C{num_course}/participants"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.warning("  HTTP error R%d/C%d participants: %s", num_reunion, num_course, str(e)[:80])
        return None
    except json.JSONDecodeError:
        return None


def parse_course_pmu(
    course_data: dict,
    participants_data: dict,
    reunion_ref: dict,
    num_course: int,
    timestamp: str,
) -> tuple[CourseBrute, list[PartantBrut]]:
    """Parse les données PMU en CourseBrute + liste de PartantBrut."""

    reunion_uid = reunion_ref.get("reunion_uid", "")
    date_iso = reunion_ref.get("date_reunion_iso", "")
    hippo = reunion_ref.get("hippodrome_normalise", "")
    num_reunion = reunion_ref.get("numero_reunion", 0)

    course = CourseBrute(
        source="pmu",
        reunion_uid=reunion_uid,
        date_reunion_iso=date_iso,
        hippodrome_normalise=hippo,
        numero_reunion=num_reunion,
        numero_course=num_course,
        url_source=f"https://www.pmu.fr/turf/{date.fromisoformat(date_iso).strftime('%d%m%Y')}/R{num_reunion}/C{num_course}/",
        timestamp_collecte=timestamp,
        libelle=course_data.get("libelle", ""),
        libelle_court=course_data.get("libelleCourt", ""),
        distance=course_data.get("distance"),
        distance_unit=course_data.get("distanceUnit", ""),
        parcours=course_data.get("parcours", ""),
        corde=course_data.get("corde", ""),
        discipline=course_data.get("discipline", ""),
        specialite=course_data.get("specialite", ""),
        condition_sexe=course_data.get("conditionSexe", ""),
        categorie_particularite=course_data.get("categorieParticularite", ""),
        condition_age=course_data.get("conditionAge", ""),
        conditions_texte=course_data.get("conditions", ""),
        nombre_partants=course_data.get("nombreDeclaresPartants"),
        heure_depart=course_data.get("heureDepart"),
        montant_prix=course_data.get("montantPrix"),
        montant_1er=course_data.get("montantOffert1er"),
        montant_2eme=course_data.get("montantOffert2eme"),
        montant_3eme=course_data.get("montantOffert3eme"),
        montant_4eme=course_data.get("montantOffert4eme"),
        montant_5eme=course_data.get("montantOffert5eme"),
        statut=course_data.get("statut", ""),
        categorie_statut=course_data.get("categorieStatut", ""),
        ordre_arrivee=course_data.get("ordreArrivee", []),
        duree_course=course_data.get("dureeCourse"),
        incidents=course_data.get("incidents", []),
        arrivee_definitive=course_data.get("isArriveeDefinitive", False)
                          or course_data.get("arriveeDefinitive", False),
        replay_disponible=course_data.get("replayDisponible", False),
        course_trackee=course_data.get("courseTrackee", False),
    )

    penetro = course_data.get("penetrometre", {})
    if isinstance(penetro, dict):
        course.penetrometre = penetro.get("intitule", "")
        course.penetrometre_valeur = str(penetro.get("valeurMesure", ""))
    tp = course_data.get("typePiste", "")
    if tp:
        course.type_piste = tp

    paris = course_data.get("paris", [])
    if paris:
        course.paris_disponibles = [p.get("typePari", "") for p in paris if isinstance(p, dict)]

    participants = participants_data.get("participants", [])
    course_uid = make_uid(date_iso, hippo, f"R{num_reunion}", f"C{num_course}")

    dq_nums = set()
    for inc in course_data.get("incidents", []):
        if isinstance(inc, dict):
            for n in inc.get("numeroParticipants", []):
                dq_nums.add(n)

    partants = []
    for p in participants:
        gains = p.get("gainsParticipant", {}) or {}

        rapport_direct = p.get("dernierRapportDirect", {}) or {}
        rapport_ref = p.get("dernierRapportReference", {}) or {}
        cote_d = rapport_direct.get("rapport") if isinstance(rapport_direct, dict) else None
        cote_r = rapport_ref.get("rapport") if isinstance(rapport_ref, dict) else None

        robe_data = p.get("robe", {})
        robe_str = ""
        if isinstance(robe_data, dict):
            robe_str = robe_data.get("libelleCourt", "") or robe_data.get("libelleLong", "")
        elif isinstance(robe_data, str):
            robe_str = robe_data

        num_pmu = p.get("numPmu")

        dist_prev = p.get("distanceChevalPrecedent", {})
        ecart_str = ""
        if isinstance(dist_prev, dict):
            ecart_str = dist_prev.get("libelleCourt", "") or dist_prev.get("libelleLong", "")
        elif isinstance(dist_prev, str):
            ecart_str = dist_prev

        com = p.get("commentaireApresCourse", {})
        com_texte = ""
        if isinstance(com, dict):
            com_texte = com.get("texte", "")
        elif isinstance(com, str):
            com_texte = com

        partant = PartantBrut(
            source="pmu",
            course_uid=course_uid,
            reunion_uid=reunion_uid,
            date_reunion_iso=date_iso,
            hippodrome_normalise=hippo,
            numero_reunion=num_reunion,
            numero_course=num_course,
            timestamp_collecte=timestamp,
            nom=p.get("nom", ""),
            num_pmu=num_pmu,
            age=p.get("age"),
            sexe=p.get("sexe", ""),
            race=p.get("race", ""),
            robe=robe_str,
            musique=p.get("musique", ""),
            nombre_courses=p.get("nombreCourses"),
            nombre_victoires=p.get("nombreVictoires"),
            nombre_places=p.get("nombrePlaces"),
            nombre_places_second=p.get("nombrePlacesSecond"),
            nombre_places_troisieme=p.get("nombrePlacesTroisieme"),
            gains_carriere=gains.get("gainsCarriere"),
            gains_victoires=gains.get("gainsVictoires"),
            gains_place=gains.get("gainsPlace"),
            gains_annee_en_cours=gains.get("gainsAnneeEnCours"),
            gains_annee_precedente=gains.get("gainsAnneePrecedente"),
            indicateur_inedit=p.get("indicateurInedit", False),
            driver=p.get("driver", "") or p.get("jockey", ""),
            driver_change=p.get("driverChange", False) or p.get("jockeyChange", False),
            entraineur=p.get("entraineur", ""),
            proprietaire=p.get("proprietaire", ""),
            nom_pere=p.get("nomPere", ""),
            nom_mere=p.get("nomMere", ""),
            eleveur=p.get("eleveur", ""),
            oeilleres=p.get("oeilleres", ""),
            deferre=p.get("deferre", ""),
            statut_partant=p.get("statut", ""),
            engagement=p.get("engagement", False),
            supplement=p.get("supplement"),
            handicap_distance=p.get("handicapDistance"),
            handicap_poids=p.get("handicapPoids"),
            handicap_valeur=p.get("handicapValeur"),
            poids_condition_monte=p.get("poidsConditionMonte"),
            poids_condition_monte_change=p.get("poidsConditionMonteChange", False),
            taux_reclamation=p.get("tauxReclamation"),
            place_corde=p.get("placeCorde"),
            allure=p.get("allure", ""),
            pays=p.get("pays", ""),
            pays_entrainement=p.get("paysEntrainement", ""),
            nom_pere_mere=p.get("nomPereMere", ""),
            incident=p.get("incident", ""),
            distance_cheval_precedent=ecart_str,
            commentaire_apres_course=com_texte,
            avis_entraineur=p.get("avisEntraineur", ""),
            jument_pleine=p.get("jumentPleine", False),
            ordre_arrivee=p.get("ordreArrivee"),
            temps_obtenu=p.get("tempsObtenu"),
            reduction_kilometrique=p.get("reductionKilometrique"),
            cote_direct=cote_d,
            cote_reference=cote_r,
            url_casaque=p.get("urlCasaque", ""),
        )
        partants.append(partant)

    return course, partants


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
    if "ANTERIEURS_POSTERIEURS" in r or "4" in r:
        return "4_pieds"
    if "ANTERIEURS" in r:
        return "anterieurs"
    if "POSTERIEURS" in r:
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
        "monte": "trot_monte",
        "trot_monte": "trot_monte",
        "plat": "plat",
        "galop": "plat",
        "obstacle": "obstacle",
        "steeple": "steeple",
        "steeplechase": "steeple",
        "steeple-chase": "steeple",
        "haies": "haies",
        "cross": "cross_country",
        "cross-country": "cross_country",
    }
    return aliases.get(r, r)


def _deduire_mode_depart(categorie_raw: str, discipline_raw: str) -> str:
    cat = (categorie_raw or "").upper()
    disc = (discipline_raw or "").upper()
    if "AUTOSTART" in cat:
        return "autostart"
    if disc in ("ATTELE", "MONTE", "TROT_ATTELE", "TROT_MONTE"):
        return "volte"
    if disc in ("PLAT", "STEEPLECHASE", "HAIE", "CROSS"):
        return "stall"
    return ""


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


_HIPPO_PSF = {
    "pau", "deauville", "chantilly", "lyon-parilly",
    "lyon parilly", "pornichet", "marseille-borely",
    "marseille borely", "salon-de-provence", "agen",
}


def _deduire_type_piste(type_piste_raw: str, discipline: str, parcours: str, hippo: str) -> str:
    tp = (type_piste_raw or "").strip().lower()
    if tp and tp not in ("inconnu", "non_defini"):
        return tp

    parcours_low = (parcours or "").lower()
    if "sable" in parcours_low or "fibre" in parcours_low or "psf" in parcours_low:
        return "psf"
    if "gazon" in parcours_low or "herbe" in parcours_low:
        return "gazon"

    disc = (discipline or "").lower()
    hippo_low = (hippo or "").lower()

    if disc in ("attele", "trot_attele", "monte", "trot_monte"):
        return "cendrée"
    if disc in ("steeplechase", "steeple", "haie", "haies", "cross", "cross_country"):
        return "gazon"
    if disc in ("plat", "galop"):
        if hippo_low in _HIPPO_PSF:
            return "psf"
        return "gazon"

    return ""


def normaliser_course(brute: CourseBrute, reunion_ref: dict) -> CourseNormalisee:
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
        heure_depart=ms_to_hhmm(brute.heure_depart),
        allocation_totale=centimes_to_euros(brute.montant_prix),
        allocation_1er=centimes_to_euros(brute.montant_1er),
        type_piste=_deduire_type_piste(brute.type_piste, brute.discipline, brute.parcours, hippo),
        penetrometre=brute.penetrometre,
        statut=brute.statut.strip().lower().replace("_", " ") if brute.statut else "",
        ordre_arrivee=brute.ordre_arrivee,
        duree_course_ms=brute.duree_course,
        incidents=brute.incidents,
        paris_types=brute.paris_disponibles,
        replay_disponible=brute.replay_disponible,
        course_trackee=brute.course_trackee,
        timestamp_collecte=brute.timestamp_collecte,
        url_source=brute.url_source,
    )


def normaliser_partant(brute: PartantBrut, course_norm: CourseNormalisee) -> PartantNormalise:
    date_iso = brute.date_reunion_iso
    hippo = brute.hippodrome_normalise
    nr = brute.numero_reunion
    nc = brute.numero_course
    num = brute.num_pmu or 0

    pos = brute.ordre_arrivee
    is_gagnant = pos == 1 if pos else False
    is_place = pos is not None and 1 <= pos <= 3

    cote = brute.cote_direct or brute.cote_reference
    proba = round(1.0 / cote, 4) if cote and cote > 0 else None

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
        cote_finale=brute.cote_direct,
        cote_reference=brute.cote_reference,
        proba_implicite=proba,
        timestamp_collecte=brute.timestamp_collecte,
    )


# ===========================================================================
# PROCESS REUNION → JSONL (no accumulation)
# ===========================================================================

def process_reunion_to_jsonl(
    reunion_data: dict,
    participants_by_course: dict,
    ref: dict,
    writer: JsonlWriter,
    logger: logging.Logger,
) -> tuple[int, int]:
    """Traite une réunion et écrit directement en JSONL. Retourne (nb_courses, nb_partants)."""
    courses_list = reunion_data.get("courses", [])
    timestamp = utc_now_iso()
    nb_courses = 0
    nb_partants = 0

    for course_data in courses_list:
        nc = course_data.get("numOrdre") or course_data.get("numExterne", 0)
        if not nc:
            continue

        participants_data = participants_by_course.get(str(nc), {"participants": []})

        try:
            course_brute, partants_bruts = parse_course_pmu(
                course_data, participants_data, ref, nc, timestamp
            )
            course_norm = normaliser_course(course_brute, ref)

            # Marquer les DQ
            dq_nums = set()
            for inc in course_brute.incidents:
                if isinstance(inc, dict):
                    for n in inc.get("numeroParticipants", []):
                        dq_nums.add(n)

            # Écrire course en JSONL
            writer.write_course(asdict(course_brute), asdict(course_norm))
            nb_courses += 1

            # Écrire partants en JSONL
            for pb in partants_bruts:
                pn = normaliser_partant(pb, course_norm)
                if pb.num_pmu in dq_nums:
                    pn.is_disqualifie = True
                writer.write_partant(asdict(pb), asdict(pn))
                nb_partants += 1

        except Exception as e:
            d_iso = ref.get("date_reunion_iso", "?")
            n_reu = ref.get("numero_reunion", "?")
            logger.warning("Erreur parsing %s R%s C%s: %s", d_iso, n_reu, nc, e)

    return nb_courses, nb_partants


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Collecte courses + partants PMU — MODE JSONL")
    parser.add_argument("--pause", type=float, default=0.3, help="Pause entre requêtes (s)")
    parser.add_argument("--batch", type=int, default=500, help="Checkpoint tous les N réunions")
    parser.add_argument("--date-debut", type=str, default=None, help="Date début (YYYY-MM-DD)")
    parser.add_argument("--date-fin", type=str, default=None, help="Date fin (YYYY-MM-DD)")
    parser.add_argument("--max-reunions", type=int, default=0, help="Max réunions (0=toutes)")
    parser.add_argument("--rebuild", action="store_true", help="Forcer reconstruction depuis cache")
    args = parser.parse_args()

    logger = setup_logging("02_liste_courses")
    logger.info("=" * 70)
    logger.info("02 — COLLECTE COURSES + PARTANTS — MODE JSONL")
    logger.info("=" * 70)

    # Charger références (petit fichier ~10 MB)
    if not REFERENCES_PATH.exists():
        logger.error("Fichier références introuvable: %s", REFERENCES_PATH)
        sys.exit(1)

    with open(REFERENCES_PATH, "r", encoding="utf-8") as f:
        all_refs = json.load(f)
    logger.info("Références chargées: %d réunions", len(all_refs))

    # Filtrer par date
    refs = all_refs
    del all_refs  # libérer
    if args.date_debut:
        refs = [r for r in refs if r.get("date_reunion_iso", "") >= args.date_debut]
    if args.date_fin:
        refs = [r for r in refs if r.get("date_reunion_iso", "") <= args.date_fin]

    refs_pmu = [r for r in refs if r.get("url_pmu") and (r.get("numero_reunion") or 0) > 0]
    refs_pmu.sort(key=lambda r: (r.get("date_reunion_iso", ""), r.get("numero_reunion", 0)))
    del refs  # libérer

    logger.info("Réunions PMU à traiter: %d", len(refs_pmu))

    if args.max_reunions > 0:
        refs_pmu = refs_pmu[:args.max_reunions]
        logger.info("Limité à %d réunions", args.max_reunions)

    # Checkpoint
    checkpoint = CheckpointManager(OUTPUT_DIR / ".checkpoint_02.json")
    logger.info("Checkpoint: %d réunions déjà traitées", checkpoint.count_done)

    # Cache API par réunion
    cache = ReunionCache(CACHE_DIR)

    # JSONL Writer — append mode, pas d'accumulation
    writer = JsonlWriter(OUTPUT_DIR)

    # Session HTTP
    session = create_session()

    # === Rebuild depuis cache si demandé ===
    if args.rebuild:
        logger.info("Reconstruction depuis cache demandée...")
        # Vider les JSONL existants
        for p in [writer.courses_brut, writer.courses_norm, writer.partants_brut,
                  writer.partants_norm, writer.courses_ref]:
            if p.exists():
                p.unlink()

        rebuilt_count = 0
        total_c = 0
        total_p = 0
        for ref in refs_pmu:
            r_uid = ref.get("reunion_uid", "")
            if not checkpoint.is_done(r_uid):
                continue
            d_iso = ref.get("date_reunion_iso", "")
            n_reu = ref.get("numero_reunion", 0)
            if not d_iso or n_reu <= 0:
                continue
            cached = cache.get(d_iso, n_reu)
            if not cached:
                continue

            reunion_data = cached.get("reunion_data")
            participants_by_course = cached.get("participants", {})
            if not reunion_data:
                continue

            nc, np = process_reunion_to_jsonl(
                reunion_data, participants_by_course, ref, writer, logger
            )
            total_c += nc
            total_p += np
            rebuilt_count += 1

            if rebuilt_count % 500 == 0:
                logger.info("  Reconstruit: %d réunions, %d courses, %d partants",
                           rebuilt_count, total_c, total_p)

        logger.info("Reconstruction terminée: %d réunions, %d courses, %d partants",
                   rebuilt_count, total_c, total_p)

    # === Boucle principale ===
    total_courses = checkpoint._data.get("total_courses", 0)
    total_partants = checkpoint._data.get("total_partants", 0)
    total_erreurs = 0
    total_requetes = 0
    reunions_traitees = 0

    for idx, ref in enumerate(refs_pmu, 1):
        reunion_uid = ref.get("reunion_uid", "")

        if checkpoint.is_done(reunion_uid):
            continue

        date_iso = ref.get("date_reunion_iso", "")
        num_reunion = ref.get("numero_reunion", 0)

        if not date_iso or num_reunion <= 0:
            continue

        date_obj = date.fromisoformat(date_iso)
        date_ddmmyyyy = date_obj.strftime("%d%m%Y")

        # === Vérifier le cache ===
        cached = cache.get(date_iso, num_reunion)
        if cached:
            reunion_data = cached.get("reunion_data")
            participants_by_course = cached.get("participants", {})
            from_cache = True
        else:
            from_cache = False
            reunion_data = fetch_reunion_pmu(session, date_ddmmyyyy, num_reunion, logger)
            total_requetes += 1
            participants_by_course = {}

        if not reunion_data:
            total_erreurs += 1
            checkpoint.mark_done(reunion_uid)
            time.sleep(args.pause)
            continue

        courses_list = reunion_data.get("courses", [])

        # Fetcher les participants manquants
        if not from_cache:
            for course_data in courses_list:
                nc = course_data.get("numOrdre") or course_data.get("numExterne", 0)
                if not nc:
                    continue
                pdata = fetch_participants_pmu(
                    session, date_ddmmyyyy, num_reunion, nc, logger
                )
                total_requetes += 1
                participants_by_course[str(nc)] = pdata or {"participants": []}
                time.sleep(args.pause * 0.5)

            # Sauver dans le cache
            cache.put(date_iso, num_reunion, {
                "reunion_data": reunion_data,
                "participants": participants_by_course,
            })

        # Traiter et écrire en JSONL — PAS d'accumulation mémoire
        nb_c, nb_p = process_reunion_to_jsonl(
            reunion_data, participants_by_course, ref, writer, logger
        )
        total_courses += nb_c
        total_partants += nb_p

        checkpoint.mark_done(reunion_uid)
        reunions_traitees += 1

        if reunions_traitees % 100 == 0:
            logger.info(
                "  [%d/%d] courses=%d partants=%d erreurs=%d req=%d",
                reunions_traitees, len(refs_pmu), total_courses,
                total_partants, total_erreurs, total_requetes,
            )

        # Checkpoint périodique
        if reunions_traitees % args.batch == 0:
            checkpoint.update_counts(total_courses, total_partants)
            checkpoint.save()
            logger.info(
                ">>> Checkpoint: %d réunions, %d courses, %d partants <<<",
                reunions_traitees, total_courses, total_partants,
            )

        # Renouveler session
        if total_requetes > 0 and total_requetes % 2000 == 0:
            session.close()
            session = create_session()
            logger.info("  Session HTTP renouvelée")

        time.sleep(args.pause)

    # === Checkpoint final ===
    checkpoint.update_counts(total_courses, total_partants)
    checkpoint.save()

    logger.info("=" * 70)
    logger.info("TERMINÉ: %d réunions, %d courses, %d partants, %d erreurs, %d requêtes",
                reunions_traitees, total_courses, total_partants, total_erreurs, total_requetes)
    logger.info("Fichiers JSONL: %s", OUTPUT_DIR)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
