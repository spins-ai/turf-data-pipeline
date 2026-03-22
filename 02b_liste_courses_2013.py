#!/usr/bin/env python3
"""
02_liste_courses.py
====================
Collecte multi-sources des courses et partants par réunion.

Fusionne les étapes 02 (courses) et 03 (partants) car l'API PMU retourne
les deux en une seule requête par réunion.

Sources :
  - PMU (API JSON) : /programme/{date}/R{num} → toutes les courses + participants
  - Le Trot (HTML) : courses de trot hors-PMU (qualifications, régionales)

Produit :
  - courses_brut.json / courses_normalisees.json
  - partants_brut.json / partants_normalises.json
  - courses_references_04.json (interface pour script 04_resultats.py)

Architecture :
  - 1 requête PMU par RÉUNION → toutes les courses + partants + résultats
  - Cache JSON par jour pour reprise
  - Checkpoint par réunion
  - Export JSON + Parquet + CSV

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

REFERENCES_PATH = Path(__file__).resolve().parent / "output" / "01_calendrier_reunions" / "reunions_references_02_2013_2016.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "02b_liste_courses_2013"
CACHE_DIR = OUTPUT_DIR / "cache"

from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv, sauver_parquet
from utils.types import utc_now_iso

PMU_API_BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme"
# Endpoints:
#   {base}/{DDMMYYYY}/R{num}/C{num}              → course details
#   {base}/{DDMMYYYY}/R{num}/C{num}/participants  → partants + résultats


# ===========================================================================
# DATACLASSES — COURSE
# ===========================================================================

@dataclass
class CourseBrute:
    """Course telle que collectée depuis l'API."""
    # Traçabilité
    source: str = ""
    reunion_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    url_source: str = ""
    timestamp_collecte: str = ""

    # Données course
    libelle: str = ""
    libelle_court: str = ""
    distance: Optional[int] = None
    distance_unit: str = ""
    parcours: str = ""
    corde: str = ""
    discipline: str = ""
    specialite: str = ""
    condition_sexe: str = ""
    categorie_particularite: str = ""  # AUTOSTART, HANDICAP, GROUPE_III, etc.
    condition_age: str = ""  # TROIS_ANS, QUATRE_ANS_ET_PLUS, etc.
    conditions_texte: str = ""
    nombre_partants: Optional[int] = None
    heure_depart: Optional[int] = None  # timestamp ms
    montant_prix: Optional[int] = None  # centimes
    montant_1er: Optional[int] = None
    montant_2eme: Optional[int] = None
    montant_3eme: Optional[int] = None
    montant_4eme: Optional[int] = None
    montant_5eme: Optional[int] = None

    # Résultat course
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
    """Course normalisée pour le pipeline aval."""
    # Identifiants
    course_uid: str = ""
    reunion_uid: str = ""
    cle_course: str = ""  # YYYY-MM-DD|hippodrome|RX|CY

    # Contexte
    source: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    hippodrome: str = ""
    pays: str = ""
    numero_reunion: int = 0
    numero_course: int = 0

    # Course
    libelle: str = ""
    distance: Optional[int] = None
    parcours: str = ""
    corde: str = ""
    discipline: str = ""
    specialite: str = ""
    conditions_texte: str = ""
    condition_sexe: str = ""
    condition_age: str = ""  # trois_ans, quatre_ans_et_plus, etc.
    categorie: str = ""  # autostart, handicap, groupe_iii, course_a_conditions, etc.
    mode_depart: str = ""  # autostart, volte, stall (déduit de categorie + discipline)
    nombre_partants: Optional[int] = None
    heure_depart: str = ""  # HH:MM
    allocation_totale: Optional[int] = None  # euros
    allocation_1er: Optional[int] = None

    # Piste
    type_piste: str = ""
    penetrometre: str = ""

    # Résultat
    statut: str = ""
    ordre_arrivee: list = field(default_factory=list)
    duree_course_ms: Optional[int] = None
    incidents: list = field(default_factory=list)

    # Extras
    paris_types: list = field(default_factory=list)
    replay_disponible: bool = False
    course_trackee: bool = False

    # Traçabilité
    timestamp_collecte: str = ""
    url_source: str = ""


# ===========================================================================
# DATACLASSES — PARTANT
# ===========================================================================

@dataclass
class PartantBrut:
    """Partant tel que collecté depuis l'API."""
    # Traçabilité
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

    # Entraîneur
    entraineur: str = ""

    # Propriétaire
    proprietaire: str = ""

    # Pedigree
    nom_pere: str = ""
    nom_mere: str = ""
    eleveur: str = ""

    # Équipement
    oeilleres: str = ""
    deferre: str = ""

    # Course
    statut_partant: str = ""  # PARTANT, NON_PARTANT
    engagement: bool = False
    supplement: Optional[int] = None
    handicap_distance: Optional[int] = None
    handicap_poids: Optional[int] = None  # 10èmes de kg (625 = 62.5 kg)
    handicap_valeur: Optional[float] = None  # valeur handicap
    poids_condition_monte: Optional[int] = None  # poids de base en 10èmes de kg
    poids_condition_monte_change: bool = False
    taux_reclamation: Optional[int] = None  # prix à réclamer en centimes
    place_corde: Optional[int] = None  # numéro de stalle / corde
    allure: str = ""

    # Infos supplémentaires
    pays: str = ""
    pays_entrainement: str = ""
    nom_pere_mere: str = ""  # père de la mère
    incident: str = ""  # DQ, allure irrégulière, etc.
    distance_cheval_precedent: str = ""  # écart avec le précédent à l'arrivée
    commentaire_apres_course: str = ""
    avis_entraineur: str = ""
    jument_pleine: bool = False

    # Résultat individuel
    ordre_arrivee: Optional[int] = None
    temps_obtenu: Optional[int] = None  # ms (ex: 208130 = 2:08.13)
    reduction_kilometrique: Optional[int] = None  # ms/km (ex: 73000 = 1:13.0)

    # Cotes
    cote_direct: Optional[float] = None
    cote_reference: Optional[float] = None

    # Casaque
    url_casaque: str = ""

    # Extras
    extras: dict = field(default_factory=dict)


@dataclass
class PartantNormalise:
    """Partant normalisé pour le pipeline aval."""
    # Identifiants
    partant_uid: str = ""
    course_uid: str = ""
    reunion_uid: str = ""
    cle_partant: str = ""  # YYYY-MM-DD|hippodrome|RX|CY|numPMU

    # Contexte course
    source: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    distance: Optional[int] = None
    discipline: str = ""

    # Cheval
    horse_id: str = ""  # hash stable nom+pere+mere
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

    # Jockey / Driver
    jockey_driver: str = ""
    jockey_driver_change: bool = False

    # Entraîneur
    entraineur: str = ""

    # Propriétaire
    proprietaire: str = ""

    # Pedigree
    pere: str = ""
    mere: str = ""
    eleveur: str = ""

    # Équipement
    oeilleres: str = ""  # sans, avec, australiennes
    deferre: str = ""  # aucun, anterieurs, posterieurs, 4_pieds

    # Statut
    statut: str = ""  # partant, non_partant
    engagement: bool = False
    supplement_euros: Optional[float] = None
    handicap_distance_m: Optional[int] = None
    poids_porte_kg: Optional[float] = None  # handicapPoids / 10
    poids_base_kg: Optional[float] = None  # poidsConditionMonte / 10
    surcharge_decharge_kg: Optional[float] = None  # poids_porte - poids_base
    handicap_valeur: Optional[float] = None
    poids_monte_change: bool = False
    taux_reclamation_euros: Optional[float] = None
    place_corde: Optional[int] = None  # stalle / numéro de corde
    allure: str = ""

    # Infos supplémentaires
    pays_cheval: str = ""
    pays_entrainement: str = ""
    pere_mere: str = ""  # père de la mère
    incident: str = ""
    ecart_precedent: str = ""  # écart avec le cheval précédent à l'arrivée
    commentaire_apres_course: str = ""
    avis_entraineur: str = ""
    jument_pleine: bool = False

    # Résultat
    position_arrivee: Optional[int] = None
    temps_ms: Optional[int] = None
    reduction_km_ms: Optional[int] = None
    is_gagnant: bool = False
    is_place: bool = False  # top 3
    is_disqualifie: bool = False

    # Cotes
    cote_finale: Optional[float] = None
    cote_reference: Optional[float] = None
    proba_implicite: Optional[float] = None  # 1/cote

    # Traçabilité
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

def make_uid(*parts: str) -> str:
    h = hashlib.blake2b("|".join(str(p) for p in parts).encode(), digest_size=8)
    return h.hexdigest()


def ms_to_hhmm(ts_ms: Optional[int]) -> str:
    """Convertit un timestamp ms Unix en HH:MM."""
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
    """Cache des réponses API brutes par réunion.

    Stocke pour chaque réunion :
      - reunion_data : réponse de /programme/{date}/R{num}
      - participants  : dict {num_course: réponse /participants}

    Permet de ne pas re-requêter l'API en cas de crash/relance.
    """

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
# PARSING PMU
# ===========================================================================

def fetch_reunion_pmu(
    session: requests.Session,
    date_ddmmyyyy: str,
    num_reunion: int,
    logger: logging.Logger,
) -> Optional[dict]:
    """Récupère les détails de toutes les courses d'une réunion (sans participants)."""
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
    """Récupère les participants d'une course."""
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

    # === Course ===
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

    # Type piste et pénétromètre
    penetro = course_data.get("penetrometre", {})
    if isinstance(penetro, dict):
        course.penetrometre = penetro.get("intitule", "")
        course.penetrometre_valeur = str(penetro.get("valeurMesure", ""))
    tp = course_data.get("typePiste", "")
    if tp:
        course.type_piste = tp

    # Paris
    paris = course_data.get("paris", [])
    if paris:
        course.paris_disponibles = [p.get("typePari", "") for p in paris if isinstance(p, dict)]

    # === Partants ===
    participants = participants_data.get("participants", [])
    course_uid = make_uid(date_iso, hippo, f"R{num_reunion}", f"C{num_course}")

    # Identifier les disqualifiés
    dq_nums = set()
    for inc in course_data.get("incidents", []):
        if isinstance(inc, dict):
            for n in inc.get("numeroParticipants", []):
                dq_nums.add(n)

    partants = []
    for p in participants:
        # Extraire gains
        gains = p.get("gainsParticipant", {}) or {}

        # Extraire cotes
        rapport_direct = p.get("dernierRapportDirect", {}) or {}
        rapport_ref = p.get("dernierRapportReference", {}) or {}
        cote_d = rapport_direct.get("rapport") if isinstance(rapport_direct, dict) else None
        cote_r = rapport_ref.get("rapport") if isinstance(rapport_ref, dict) else None

        # Robe
        robe_data = p.get("robe", {})
        robe_str = ""
        if isinstance(robe_data, dict):
            robe_str = robe_data.get("libelleCourt", "") or robe_data.get("libelleLong", "")
        elif isinstance(robe_data, str):
            robe_str = robe_data

        num_pmu = p.get("numPmu")

        # Distance cheval précédent
        dist_prev = p.get("distanceChevalPrecedent", {})
        ecart_str = ""
        if isinstance(dist_prev, dict):
            ecart_str = dist_prev.get("libelleCourt", "") or dist_prev.get("libelleLong", "")
        elif isinstance(dist_prev, str):
            ecart_str = dist_prev

        # Commentaire après course
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
    """Déduit le mode de départ depuis categorieParticularite + discipline."""
    cat = (categorie_raw or "").upper()
    disc = (discipline_raw or "").upper()
    if "AUTOSTART" in cat:
        return "autostart"
    if disc in ("ATTELE", "MONTE", "TROT_ATTELE", "TROT_MONTE"):
        return "volte"  # défaut trot si pas autostart
    if disc in ("PLAT", "STEEPLECHASE", "HAIE", "CROSS"):
        return "stall"  # départ en boîtes (stalles) pour le galop
    return ""


def _make_horse_id(nom: str, pere: str, mere: str) -> str:
    """Crée un identifiant stable pour un cheval à partir de nom+père+mère."""
    parts = [
        (nom or "").strip().upper(),
        (pere or "").strip().upper(),
        (mere or "").strip().upper(),
    ]
    key = "|".join(parts)
    if not any(parts):
        return ""
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


# Hippodromes connus avec piste en sable fibré (PSF / polytrack / fibresand)
_HIPPO_PSF = {
    "pau", "deauville", "chantilly", "lyon-parilly",
    "lyon parilly", "pornichet", "marseille-borely",
    "marseille borely", "salon-de-provence", "agen",
}


def _deduire_type_piste(type_piste_raw: str, discipline: str, parcours: str, hippo: str) -> str:
    """Déduit la surface si l'API ne la fournit pas."""
    # Si l'API l'a fourni, on le garde
    tp = (type_piste_raw or "").strip().lower()
    if tp and tp not in ("inconnu", "non_defini"):
        return tp

    # Indices dans le parcours
    parcours_low = (parcours or "").lower()
    if "sable" in parcours_low or "fibre" in parcours_low or "psf" in parcours_low:
        return "psf"
    if "gazon" in parcours_low or "herbe" in parcours_low:
        return "gazon"

    disc = (discipline or "").lower()
    hippo_low = (hippo or "").lower()

    # Trot → cendrée/sable
    if disc in ("attele", "trot_attele", "monte", "trot_monte"):
        return "cendrée"

    # Galop obstacle → gazon
    if disc in ("steeplechase", "steeple", "haie", "haies", "cross", "cross_country"):
        return "gazon"

    # Galop plat → PSF si hippodrome connu, sinon gazon
    if disc in ("plat", "galop"):
        if hippo_low in _HIPPO_PSF:
            return "psf"
        return "gazon"

    return ""


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
    """Normalise un partant brut."""
    date_iso = brute.date_reunion_iso
    hippo = brute.hippodrome_normalise
    nr = brute.numero_reunion
    nc = brute.numero_course
    num = brute.num_pmu or 0

    # Calculer si placé (top 3)
    pos = brute.ordre_arrivee
    is_gagnant = pos == 1 if pos else False
    is_place = pos is not None and 1 <= pos <= 3

    # Probabilité implicite
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
        is_disqualifie=False,  # Sera mis à jour après
        cote_finale=brute.cote_direct,
        cote_reference=brute.cote_reference,
        proba_implicite=proba,
        timestamp_collecte=brute.timestamp_collecte,
    )


# ===========================================================================
# SAUVEGARDE
# ===========================================================================





import csv  # noqa: E402 (already imported above via implicit)


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Collecte courses + partants PMU")
    parser.add_argument("--pause", type=float, default=0.3, help="Pause entre requêtes (s)")
    parser.add_argument("--batch", type=int, default=500, help="Sauvegarde intermédiaire tous les N réunions")
    parser.add_argument("--date-debut", type=str, default=None, help="Date début (YYYY-MM-DD)")
    parser.add_argument("--date-fin", type=str, default=None, help="Date fin (YYYY-MM-DD)")
    parser.add_argument("--max-reunions", type=int, default=0, help="Max réunions à traiter (0=toutes)")
    args = parser.parse_args()

    logger = setup_logging("02_liste_courses")
    logger.info("=" * 70)
    logger.info("02 — COLLECTE COURSES + PARTANTS")
    logger.info("=" * 70)

    # Charger références
    if not REFERENCES_PATH.exists():
        logger.error("Fichier références introuvable: %s", REFERENCES_PATH)
        sys.exit(1)

    with open(REFERENCES_PATH, "r", encoding="utf-8") as f:
        all_refs = json.load(f)
    logger.info("Références chargées: %d réunions", len(all_refs))

    # Filtrer par date si demandé
    refs = all_refs
    if args.date_debut:
        refs = [r for r in refs if r.get("date_reunion_iso", "") >= args.date_debut]
    if args.date_fin:
        refs = [r for r in refs if r.get("date_reunion_iso", "") <= args.date_fin]

    # Filtrer seulement les réunions PMU (avec url_pmu et numero_reunion > 0)
    refs_pmu = [r for r in refs if r.get("url_pmu") and (r.get("numero_reunion") or 0) > 0]
    refs_pmu.sort(key=lambda r: (r.get("date_reunion_iso", ""), r.get("numero_reunion", 0)))

    logger.info("Réunions PMU à traiter: %d", len(refs_pmu))

    if args.max_reunions > 0:
        refs_pmu = refs_pmu[:args.max_reunions]
        logger.info("Limité à %d réunions", args.max_reunions)

    # Checkpoint
    checkpoint = CheckpointManager(OUTPUT_DIR / ".checkpoint_02b.json")
    logger.info("Checkpoint: %d réunions déjà traitées", checkpoint.count_done)

    # Cache API par réunion
    cache = ReunionCache(CACHE_DIR)

    # Session HTTP
    session = create_session()

    # Accumulateurs — charger données existantes si reprise
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def _load_existing(filename):
        p = OUTPUT_DIR / filename
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"Reprise: {len(data)} entrées chargées depuis {filename}")
                return data
            except (json.JSONDecodeError, OSError):
                pass
        return []

    all_courses_brut = _load_existing("courses_brut.json")
    all_courses_norm = _load_existing("courses_normalisees.json")
    all_partants_brut = _load_existing("partants_brut.json")
    all_partants_norm = _load_existing("partants_normalises.json")

    # Détection de données corrompues/tronquées : si on a beaucoup de réunions
    # dans le checkpoint mais peu de partants dans les JSON, reconstruire depuis le cache
    nb_done = len(checkpoint._data.get("completed_reunions", []))
    if nb_done > 100 and len(all_partants_norm) < nb_done * 5:
        logger.info(f"⚠️  Données tronquées détectées: {len(all_partants_norm)} partants "
                    f"pour {nb_done} réunions dans le checkpoint")
        logger.info("Reconstruction depuis le cache...")
        all_courses_brut = []
        all_courses_norm = []
        all_partants_brut = []
        all_partants_norm = []

        rebuilt_count = 0
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
            courses_list = reunion_data.get("courses", [])
            timestamp = ref.get("timestamp", utc_now_iso())
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
                    dq_nums = set()
                    for inc in course_brute.incidents:
                        if isinstance(inc, dict):
                            for n in inc.get("numeroParticipants", []):
                                dq_nums.add(n)
                    for pb in partants_bruts:
                        pn = normaliser_partant(pb, course_norm)
                        if pn.num_pmu in dq_nums:
                            pn.is_disqualifie = True
                        all_partants_brut.append(asdict(pb))
                        all_partants_norm.append(asdict(pn))
                    all_courses_brut.append(asdict(course_brute))
                    all_courses_norm.append(asdict(course_norm))
                except Exception as e:
                    logger.warning(f"Erreur rebuild {d_iso} R{n_reu} C{nc}: {e}")
            rebuilt_count += 1
            if rebuilt_count % 500 == 0:
                logger.info(f"  Reconstruit: {rebuilt_count} réunions, "
                           f"{len(all_courses_norm)} courses, {len(all_partants_norm)} partants")

        logger.info(f"✅ Reconstruction terminée: {rebuilt_count} réunions, "
                   f"{len(all_courses_norm)} courses, {len(all_partants_norm)} partants")
        sauver_json(all_courses_brut, OUTPUT_DIR / "courses_brut.json", logger)
        sauver_json(all_courses_norm, OUTPUT_DIR / "courses_normalisees.json", logger)
        sauver_json(all_partants_brut, OUTPUT_DIR / "partants_brut.json", logger)
        sauver_json(all_partants_norm, OUTPUT_DIR / "partants_normalises.json", logger)

    total_courses = len(all_courses_norm)
    total_partants = len(all_partants_norm)
    total_erreurs = 0
    total_requetes = 0
    reunions_traitees = 0

    for idx, ref in enumerate(refs_pmu, 1):
        reunion_uid = ref.get("reunion_uid", "")

        # Skip si déjà fait
        if checkpoint.is_done(reunion_uid):
            continue

        date_iso = ref.get("date_reunion_iso", "")
        num_reunion = ref.get("numero_reunion", 0)
        hippo = ref.get("hippodrome_normalise", "")

        if not date_iso or num_reunion <= 0:
            continue

        date_obj = date.fromisoformat(date_iso)
        date_ddmmyyyy = date_obj.strftime("%d%m%Y")
        timestamp = utc_now_iso()

        # === Vérifier le cache ===
        cached = cache.get(date_iso, num_reunion)
        if cached:
            reunion_data = cached.get("reunion_data")
            participants_by_course = cached.get("participants", {})
            from_cache = True
        else:
            from_cache = False
            # 1 requête réunion → détails de toutes les courses
            reunion_data = fetch_reunion_pmu(session, date_ddmmyyyy, num_reunion, logger)
            total_requetes += 1
            participants_by_course = {}

        if not reunion_data:
            total_erreurs += 1
            checkpoint.mark_done(reunion_uid)
            time.sleep(args.pause)
            continue

        courses_list = reunion_data.get("courses", [])

        # Fetcher les participants manquants (pas dans le cache)
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

        for course_data in courses_list:
            nc = course_data.get("numOrdre") or course_data.get("numExterne", 0)
            if not nc:
                continue

            participants_data = participants_by_course.get(str(nc), {"participants": []})

            # Parser
            course_brute, partants_bruts = parse_course_pmu(
                course_data, participants_data, ref, nc, timestamp
            )

            # Normaliser
            course_norm = normaliser_course(course_brute, ref)

            # Marquer les DQ dans les partants
            dq_nums = set()
            for inc in course_brute.incidents:
                if isinstance(inc, dict):
                    for n in inc.get("numeroParticipants", []):
                        dq_nums.add(n)

            for pb in partants_bruts:
                pn = normaliser_partant(pb, course_norm)
                if pb.num_pmu in dq_nums:
                    pn.is_disqualifie = True
                all_partants_brut.append(asdict(pb))
                all_partants_norm.append(asdict(pn))

            all_courses_brut.append(asdict(course_brute))
            all_courses_norm.append(asdict(course_norm))

            total_courses += 1
            total_partants += len(partants_bruts)

        checkpoint.mark_done(reunion_uid)
        reunions_traitees += 1

        if reunions_traitees % 100 == 0:
            logger.info(
                "  [%d/%d] courses=%d partants=%d erreurs=%d req=%d",
                reunions_traitees, len(refs_pmu), total_courses,
                total_partants, total_erreurs, total_requetes,
            )

        # Sauvegarde intermédiaire
        if reunions_traitees % args.batch == 0:
            sauver_json(all_courses_norm, OUTPUT_DIR / "courses_normalisees.json", logger)
            sauver_json(all_partants_norm, OUTPUT_DIR / "partants_normalises.json", logger)
            checkpoint.save()
            logger.info(
                ">>> Sauvegarde intermédiaire: %d réunions, %d courses, %d partants <<<",
                reunions_traitees, total_courses, total_partants,
            )

        # Renouveler session tous les 2000 requêtes
        if total_requetes > 0 and total_requetes % 2000 == 0:
            session.close()
            session = create_session()
            logger.info("  Session HTTP renouvelée")

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

    # Références pour script 04
    courses_refs = []
    for cn in all_courses_norm:
        courses_refs.append({
            "course_uid": cn["course_uid"],
            "reunion_uid": cn["reunion_uid"],
            "date_reunion_iso": cn["date_reunion_iso"],
            "hippodrome_normalise": cn["hippodrome_normalise"],
            "numero_reunion": cn["numero_reunion"],
            "numero_course": cn["numero_course"],
            "nombre_partants": cn["nombre_partants"],
            "statut": cn["statut"],
        })
    sauver_json(courses_refs, OUTPUT_DIR / "courses_references_04.json", logger)

    checkpoint.save()

    logger.info("=" * 70)
    logger.info("TERMINÉ: %d réunions, %d courses, %d partants, %d erreurs, %d requêtes",
                reunions_traitees, total_courses, total_partants, total_erreurs, total_requetes)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
