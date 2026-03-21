#!/usr/bin/env python3
"""
01_calendrier_reunions.py
=========================
Script de collecte multi-sources du calendrier des réunions de courses hippiques.

Sources supportées :
  - PMU (API JSON publique turfinfo)
  - Le Trot (HTML avec JSON embarqué dans composant Vue)
  - Geny (HTML server-rendered)

Architecture :
  - Configuration centralisée avec endpoints, headers, field mappings par source
  - Variables brutes séparées des variables normalisées
  - Features dérivées pour le pipeline aval
  - Tracking de provenance et conflits inter-sources
  - Interface claire pour 02_liste_courses.py

Produit des tables brutes et normalisées en JSON, Parquet et CSV,
ainsi qu'un rapport qualité et des logs détaillés.

Usage :
    python 01_calendrier_reunions.py --date-debut 2025-01-01 --date-fin 2025-01-31
    python 01_calendrier_reunions.py --date-debut 2025-03-15
    python 01_calendrier_reunions.py --config config.yaml
    python 01_calendrier_reunions.py --date-debut 2025-03-01 --date-fin 2025-03-31 --sources pmu,letrot
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html as html_module
import json
import logging
import re
import sys
import time
import unicodedata
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hippodromes_db import get_hippodrome_info, HIPPODROME_ALIASES

# ---------------------------------------------------------------------------
# Imports optionnels (dégradation gracieuse)
# ---------------------------------------------------------------------------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False


# ===========================================================================
# ENUMS ET CONSTANTES
# ===========================================================================

class Discipline(str, Enum):
    TROT_ATTELE = "trot_attele"
    TROT_MONTE = "trot_monte"
    TROT = "trot"
    PLAT = "plat"
    OBSTACLE = "obstacle"
    STEEPLE_CHASE = "steeple_chase"
    HAIES = "haies"
    CROSS_COUNTRY = "cross_country"
    INCONNU = "inconnu"


class StatutReunion(str, Enum):
    PROGRAMMEE = "programmee"
    EN_COURS = "en_cours"
    TERMINEE = "terminee"
    ANNULEE = "annulee"
    REPORTEE = "reportee"
    INCONNU = "inconnu"


def _deduire_discipline_par_pays(pays_brut: str, hippo_norm: str) -> Discipline:
    """Déduit la discipline quand elle est inconnue, basée sur le pays et l'hippodrome."""
    if not pays_brut:
        return Discipline.INCONNU
    p = pays_brut.strip().lower()
    # Nettoyer les suffixes parasites Geny
    p = p.replace(" - genybet", "").replace(" (genybet", "").replace("genybet", "").strip()

    # Pays exclusivement galop (plat)
    pays_plat = {
        "australie", "singapour", "hong-kong", "hong kong", "etats-unis", "etats unis",
        "usa", "grande-bretagne", "grande bretagne", "royaume-uni", "royaume uni",
        "irlande", "afrique du sud", "chili", "argentine", "bresil", "brésil",
        "japon", "emirats", "emirats arabes unis", "arabie saoudite", "maroc",
        "uruguay", "perou", "pérou",
    }
    # Pays mixtes (trot + galop) — ne pas deviner
    pays_mixtes = {"france", "allemagne", "belgique", "italie", "suisse"}
    # Pays principalement trot
    pays_trot = {"suede", "suède", "norvege", "norvège", "finlande", "danemark"}

    if p in pays_plat:
        return Discipline.PLAT
    if p in pays_trot:
        return Discipline.TROT
    if p in pays_mixtes:
        # Pour la France, essayer de deviner par l'hippodrome
        if p == "france" and hippo_norm:
            # Hippodromes exclusivement trot
            hippo_trot = {"vincennes", "enghien", "cabourg", "graignes", "laval"}
            if hippo_norm in hippo_trot:
                return Discipline.TROT
        return Discipline.INCONNU
    return Discipline.INCONNU


def _nettoyer_terrain(terrain: str) -> str:
    """Nettoie le terrain brut (surtout Geny qui mélange terrain + non-partants + pénétromètre)."""
    if not terrain:
        return ""
    # Retirer les suffixes parasites
    import re
    t = terrain.strip()
    t = re.sub(r"\s*Non-partants?\b.*$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s*Pénétromètre\b.*$", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s*Penetrometre\b.*$", "", t, flags=re.IGNORECASE).strip()
    return t if t else terrain.strip()


def _deduire_federation(disc: Discipline) -> str:
    """Déduit la fédération à partir de la discipline."""
    if disc in (Discipline.TROT, Discipline.TROT_ATTELE, Discipline.TROT_MONTE):
        return "Le Trot"
    if disc in (Discipline.PLAT, Discipline.OBSTACLE, Discipline.STEEPLE_CHASE,
                Discipline.HAIES, Discipline.CROSS_COUNTRY):
        return "France Galop"
    return ""


class TypeSource(str, Enum):
    API_JSON = "api_json"
    HTML_VUE_EMBEDDED = "html_vue_embedded"
    HTML_SERVER_RENDERED = "html_server_rendered"


class StrategieParsing(str, Enum):
    JSON_DIRECT = "json_direct"
    HTML_VUE_ATTRIBUTE = "html_vue_attribute"
    HTML_CSS_SELECTORS = "html_css_selectors"


DISCIPLINE_ALIASES: dict[str, Discipline] = {
    "trot attelé": Discipline.TROT_ATTELE,
    "trot attele": Discipline.TROT_ATTELE,
    "attelé": Discipline.TROT_ATTELE,
    "attele": Discipline.TROT_ATTELE,
    "trot monté": Discipline.TROT_MONTE,
    "trot monte": Discipline.TROT_MONTE,
    "monté": Discipline.TROT_MONTE,
    "monte": Discipline.TROT_MONTE,
    "trot": Discipline.TROT,
    "plat": Discipline.PLAT,
    "galop plat": Discipline.PLAT,
    "obstacle": Discipline.OBSTACLE,
    "steeple": Discipline.STEEPLE_CHASE,
    "steeple-chase": Discipline.STEEPLE_CHASE,
    "steeplechase": Discipline.STEEPLE_CHASE,
    "haies": Discipline.HAIES,
    "cross": Discipline.CROSS_COUNTRY,
    "cross-country": Discipline.CROSS_COUNTRY,
}

STATUT_ALIASES: dict[str, StatutReunion] = {
    "programme": StatutReunion.PROGRAMMEE,
    "programmee": StatutReunion.PROGRAMMEE,
    "a venir": StatutReunion.PROGRAMMEE,
    "en cours": StatutReunion.EN_COURS,
    "live": StatutReunion.EN_COURS,
    "termine": StatutReunion.TERMINEE,
    "terminee": StatutReunion.TERMINEE,
    "definitive": StatutReunion.TERMINEE,
    "fini": StatutReunion.TERMINEE,
    "annule": StatutReunion.ANNULEE,
    "annulee": StatutReunion.ANNULEE,
    "reporte": StatutReunion.REPORTEE,
    "reportee": StatutReunion.REPORTEE,
}


# ===========================================================================
# CONFIGURATION CENTRALISÉE DES SOURCES
# ===========================================================================

@dataclass(frozen=True)
class FieldMapping:
    """Mapping d'un champ source vers un champ normalisé."""
    source_field: str
    normalized_field: str
    transform: Optional[str] = None  # Nom de la transformation à appliquer


@dataclass(frozen=True)
class SourceEndpointConfig:
    """
    Configuration complète d'un endpoint source.
    Regroupe : identité, endpoint, HTTP, parsing, priorité, field mappings.
    """
    # Identité
    nom_source: str
    code_source: str  # clé unique : pmu, letrot, geny
    type_source: TypeSource
    priorite: int  # 1 = plus haute priorité

    # Endpoint
    url_base: str
    url_calendrier_jour_pattern: str  # Pattern avec {date_*} comme placeholder
    methode_http: str = "GET"
    params_template: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)

    # Parsing
    strategie_parsing: StrategieParsing = StrategieParsing.JSON_DIRECT

    # HTTP
    timeout: int = 30
    retry_max: int = 3
    retry_backoff: float = 1.0

    # Activation
    active: bool = True

    # Field mapping : champs bruts disponibles sur cette source
    champs_disponibles: tuple[str, ...] = ()

    def build_url(self, jour: date) -> str:
        """Construit l'URL pour un jour donné."""
        return self.url_calendrier_jour_pattern.format(
            date_iso=jour.isoformat(),
            date_ddmmyyyy=jour.strftime("%d%m%Y"),
            date_dd_mm_yyyy=jour.strftime("%d-%m-%Y"),
            date_yyyymmdd=jour.strftime("%Y%m%d"),
        )


def default_source_configs() -> dict[str, SourceEndpointConfig]:
    """Retourne la configuration par défaut de toutes les sources."""
    return {
        "pmu": SourceEndpointConfig(
            nom_source="PMU",
            code_source="pmu",
            type_source=TypeSource.API_JSON,
            priorite=1,
            url_base="https://online.turfinfo.api.pmu.fr",
            url_calendrier_jour_pattern=(
                "https://online.turfinfo.api.pmu.fr"
                "/rest/client/7/programme/{date_ddmmyyyy}"
            ),
            methode_http="GET",
            strategie_parsing=StrategieParsing.JSON_DIRECT,
            champs_disponibles=(
                "date_reunion_brut", "hippodrome_brut", "discipline_brut",
                "numero_reunion_brut", "url_reunion_brut",
                "identifiant_source_reunion_brut", "pays_brut",
                "nombre_courses_reunion_brut", "statut_reunion_brut",
                "libelle_reunion_brut", "code_reunion_brut",
                "type_reunion_brut", "specialite_brut",
            ),
        ),
        "letrot": SourceEndpointConfig(
            nom_source="Le Trot",
            code_source="letrot",
            type_source=TypeSource.HTML_VUE_EMBEDDED,
            priorite=2,
            url_base="https://www.letrot.com",
            url_calendrier_jour_pattern=(
                "https://www.letrot.com/courses/{date_iso}"
            ),
            methode_http="GET",
            strategie_parsing=StrategieParsing.HTML_VUE_ATTRIBUTE,
            champs_disponibles=(
                "date_reunion_brut", "hippodrome_brut", "discipline_brut",
                "numero_reunion_brut", "url_reunion_brut",
                "identifiant_source_reunion_brut",
                "nombre_courses_reunion_brut", "statut_reunion_brut",
                "type_reunion_brut",
            ),
        ),
        "geny": SourceEndpointConfig(
            nom_source="Geny",
            code_source="geny",
            type_source=TypeSource.HTML_SERVER_RENDERED,
            priorite=3,
            url_base="https://www.geny.com",
            url_calendrier_jour_pattern=(
                "https://www.geny.com/reunions-courses-pmu/_d{date_iso}"
            ),
            methode_http="GET",
            strategie_parsing=StrategieParsing.HTML_CSS_SELECTORS,
            champs_disponibles=(
                "date_reunion_brut", "hippodrome_brut",
                "numero_reunion_brut", "url_reunion_brut",
                "identifiant_source_reunion_brut", "pays_brut",
                "nombre_courses_reunion_brut",
            ),
        ),
    }


# ===========================================================================
# CARTOGRAPHIE DES VARIABLES PAR SOURCE
# ===========================================================================

SOURCE_FIELD_MAPPINGS: dict[str, dict[str, str]] = {
    "pmu": {
        # champ JSON PMU -> champ brut normalisé interne
        "hippodrome.libelleCourt": "hippodrome_brut",
        "hippodrome.libelleLong": "hippodrome_brut_long",
        "pays.libelle": "pays_brut",
        "pays.code": "pays_code_brut",
        "numOfficiel": "numero_reunion_brut",
        "numExterne": "code_reunion_brut",
        "discipline": "discipline_brut",
        "disciplinesMere": "specialite_brut",
        "statut": "statut_reunion_brut",
        "nombreCourses": "nombre_courses_reunion_brut",
        "nature": "type_reunion_brut",
        "audience": "libelle_reunion_brut",
    },
    "letrot": {
        "nomHippodrome": "hippodrome_brut",
        "numHippodrome": "code_reunion_brut",
        "numReunion": "numero_reunion_brut",
        "nbCourse": "nombre_courses_reunion_brut",
        "status": "statut_reunion_brut",
        "type": "type_reunion_brut",
        "seanceId": "identifiant_source_reunion_brut",
        "dateReunion": "date_reunion_brut",
        "heureReunion": "heure_reunion_brut",
    },
    "geny": {
        ".nomReunion": "hippodrome_brut",
        ".infoReunion heure": "heure_reunion_brut",
        ".infoReunion terrain": "terrain_brut",
        "anchor_name": "identifiant_source_reunion_brut",
        "(RN)": "numero_reunion_brut",
        "(pays)": "pays_brut",
        "count(.courseParis)": "nombre_courses_reunion_brut",
    },
}


# ===========================================================================
# CONFIGURATION PIPELINE
# ===========================================================================

@dataclass
class PipelineConfig:
    """Configuration globale du pipeline."""
    date_debut: date = field(default_factory=date.today)
    date_fin: date = field(default_factory=date.today)
    dossier_sortie: Path = Path(__file__).resolve().parent / "output" / "01_calendrier_reunions"
    dossier_logs: Path = Path("logs")
    mode_reprise: bool = True
    export_csv: bool = True
    export_parquet: bool = True
    export_json: bool = True
    log_level: str = "INFO"
    http_timeout: int = 30
    http_retry_max: int = 3
    http_retry_backoff: float = 1.0
    pause_inter_jour: float = 0.5
    pause_inter_source: float = 0.3  # pause entre chaque source pour éviter ban
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    sources: dict[str, SourceEndpointConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.sources:
            self.sources = default_source_configs()

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        if not HAS_YAML:
            raise ImportError("PyYAML requis pour charger la configuration YAML")
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        raw.pop("sources", None)  # Les sources YAML ne sont pas encore supportées
        return cls(**raw)


# ===========================================================================
# MODÈLES DE DONNÉES — VARIABLES BRUTES
# ===========================================================================

@dataclass
class ReunionBrute:
    """
    Réunion telle que récupérée depuis une source, avant normalisation.
    Contient toutes les variables brutes possibles selon la source.
    """
    # Traçabilité
    source: str
    url_source: str = ""
    timestamp_collecte: str = ""

    # Variables brutes standard
    date_reunion_brut: str = ""           # Date telle que reçue
    hippodrome_brut: str = ""             # Nom brut de l'hippodrome
    discipline_brut: str = ""             # Discipline brute
    numero_reunion_brut: Optional[int] = None
    url_reunion_brut: str = ""
    identifiant_source_reunion_brut: str = ""
    pays_brut: str = ""
    region_brut: str = ""
    nombre_courses_reunion_brut: Optional[int] = None
    statut_reunion_brut: str = ""

    # Variables brutes étendues
    libelle_reunion_brut: str = ""        # Nom complet/audience de la réunion
    code_reunion_brut: str = ""           # Code interne source (numHippodrome, numExterne)
    type_reunion_brut: str = ""           # DIURNE/NOCTURNE, Premium, etc.
    specialite_brut: str = ""             # disciplinesMere, spécialité
    heure_reunion_brut: str = ""          # Heure de début
    terrain_brut: str = ""                # État du terrain
    meteo_brut: str = ""                  # Météo si disponible

    # Données supplémentaires non mappées (catch-all)
    extras: dict[str, Any] = field(default_factory=dict)


# ===========================================================================
# MODÈLES DE DONNÉES — VARIABLES NORMALISÉES + FEATURES DÉRIVÉES
# ===========================================================================

@dataclass
class ReunionNormalisee:
    """
    Réunion après normalisation, avec features dérivées pour le pipeline aval.
    Structure cible unifiée exploitable par 02_liste_courses.py.
    """
    # === Identifiants ===
    reunion_uid: str = ""                     # Hash unique (date + hippo_norm + source)
    cross_uid: str = ""                       # Hash cross-source (date + hippo_norm) pour fusion
    cle_jour_hippodrome_numero: str = ""      # Clé métier lisible

    # === Variables normalisées ===
    source: str = ""
    date_reunion_iso: str = ""                # YYYY-MM-DD garanti
    hippodrome: str = ""                      # Nom original (casse d'origine)
    hippodrome_normalise: str = ""            # Minuscules, sans accents, nettoyé
    discipline_normalisee: str = ""           # Enum Discipline.value
    numero_reunion: Optional[int] = None
    url_reunion: str = ""
    identifiant_source_reunion: str = ""
    pays: str = ""
    region: str = ""
    nombre_courses_reunion: Optional[int] = None
    statut_reunion: str = ""                  # Enum StatutReunion.value

    # === Variables brutes conservées ===
    libelle_reunion: str = ""
    code_reunion: str = ""
    type_reunion: str = ""
    specialite: str = ""
    heure_reunion: str = ""
    terrain: str = ""
    meteo: str = ""                           # Météo textuelle (ex: "Pluie faible | 7°C | vent 9km/h O")

    # === Météo structurée (depuis PMU ou enrichissement) ===
    meteo_temperature: Optional[int] = None
    meteo_nebulosite: str = ""                # Ciel dégagé, Pluie faible, etc.
    meteo_force_vent: Optional[int] = None    # km/h
    meteo_direction_vent: str = ""            # N, NE, E, SE, S, SO, O, NO

    # === Infos paris (depuis PMU + Le Trot) ===
    has_quinte: bool = False
    paris_evenements: list[str] = field(default_factory=list)

    # === Infos complémentaires ===
    corde_piste: str = ""                     # Corde et piste (ex: "Corde à gauche - 1.411m (sable)")
    federation: str = ""                      # Fédération régionale Le Trot (ex: "OUEST")
    condition: str = ""                       # Condition d'accès (ex: "3 ans et +")
    non_partants: str = ""                    # Non-partants (ex: "511 - 701")
    nb_engages: Optional[int] = None          # Nombre d'engagés (Le Trot)
    has_replay: bool = False                  # Replay disponible (Le Trot)

    # === Features dérivées ===
    source_prioritaire: str = ""              # Source de plus haute priorité parmi les matchs
    nb_sources_match: int = 1                 # Nombre de sources ayant cette réunion
    est_duplique_inter_source: bool = False   # True si trouvée sur 2+ sources
    indicateur_donnee_incomplete: bool = False # True si champs critiques manquants
    indicateur_reunion_fusionnee: bool = False # True si fusion inter-sources effectuée
    date_collecte: str = ""                   # Date de collecte (YYYY-MM-DD)
    timestamp_collecte: str = ""
    url_source: str = ""

    # === Provenance et conflits ===
    sources_multiples: list[str] = field(default_factory=list)
    source_origine_principale: str = ""       # Source qui a fourni les données de base
    sources_secondaires: list[str] = field(default_factory=list)
    champs_confirmes_par_plusieurs_sources: list[str] = field(default_factory=list)
    champs_en_conflit: dict[str, dict[str, str]] = field(default_factory=dict)
    # Format champs_en_conflit : {"hippodrome": {"pmu": "CHANTILLY", "geny": "Chantilly"}}


# ===========================================================================
# INTERFACE POUR 02_LISTE_COURSES.PY
# ===========================================================================

@dataclass(frozen=True)
class ReunionReference:
    """
    Structure minimale exposée au script suivant 02_liste_courses.py.
    Contient tout ce qui est nécessaire pour itérer sur les courses d'une réunion.
    """
    reunion_uid: str
    date_reunion_iso: str
    hippodrome_normalise: str
    hippodrome: str
    discipline_normalisee: str
    numero_reunion: Optional[int]
    pays: str
    nombre_courses_reunion: Optional[int]

    # URLs par source pour 02_liste_courses.py
    url_pmu: str = ""
    url_letrot: str = ""
    url_geny: str = ""

    # Identifiants par source
    id_pmu: str = ""
    id_letrot: str = ""
    id_geny: str = ""

    sources: tuple[str, ...] = ()


# ===========================================================================
# RAPPORT QUALITÉ
# ===========================================================================

@dataclass
class RapportQualite:
    date_debut: str = ""
    date_fin: str = ""
    sources_actives: list[str] = field(default_factory=list)
    total_jours_traites: int = 0
    total_jours_ignores_reprise: int = 0
    reunions_brutes_trouvees: int = 0
    reunions_valides: int = 0
    reunions_invalides: int = 0
    reunions_doublons: int = 0
    reunions_normalisees_sauvees: int = 0
    erreurs_http: int = 0
    erreurs_parsing: int = 0
    erreurs_par_source: dict[str, int] = field(default_factory=dict)
    reunions_par_source: dict[str, int] = field(default_factory=dict)
    champs_disponibles_par_source: dict[str, list[str]] = field(default_factory=dict)
    conflits_detectes: int = 0
    reunions_fusionnees: int = 0
    reunions_donnees_incompletes: int = 0
    duree_totale_secondes: float = 0.0
    timestamp_debut: str = ""
    timestamp_fin: str = ""


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging(config: PipelineConfig) -> logging.Logger:
    logger = logging.getLogger("calendrier_reunions")
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    config.dossier_logs.mkdir(parents=True, exist_ok=True)
    log_file = config.dossier_logs / f"01_calendrier_reunions_{date.today().isoformat()}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def normaliser_texte(texte: str) -> str:
    if not texte:
        return ""
    texte = texte.strip().lower()
    nfkd = unicodedata.normalize("NFKD", texte)
    sans_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[\s\-_]+", " ", sans_accents).strip()


def normaliser_hippodrome(nom: str) -> str:
    if not nom:
        return ""
    norm = normaliser_texte(nom)
    for suffixe in (" hippodrome", " racecourse", " turf"):
        norm = norm.replace(suffixe, "")
    norm = re.sub(r"[^a-z0-9 ]", "", norm).strip()
    norm = re.sub(r"\s+", " ", norm)
    # Résolution des aliases (suffixes promo, variantes orthographiques)
    if norm in HIPPODROME_ALIASES:
        norm = HIPPODROME_ALIASES[norm]
    # Nettoyage suffixes promotionnels génériques restants
    norm = re.sub(r"\s*r\d+\s+jouer\s+maintenant\s*$", "", norm).strip()
    norm = re.sub(r"\s+midi$", "", norm).strip()
    norm = re.sub(r"\s+soir$", "", norm).strip()
    norm = re.sub(r"\s+genybet$", "", norm).strip()
    return norm


def normaliser_discipline(raw: str) -> Discipline:
    if not raw:
        return Discipline.INCONNU
    key = normaliser_texte(raw)
    if key in DISCIPLINE_ALIASES:
        return DISCIPLINE_ALIASES[key]
    for alias, disc in DISCIPLINE_ALIASES.items():
        if alias in key or key in alias:
            return disc
    return Discipline.INCONNU


def normaliser_statut(raw: str) -> StatutReunion:
    if not raw:
        return StatutReunion.INCONNU
    key = normaliser_texte(raw)
    for alias, statut in STATUT_ALIASES.items():
        if alias in key:
            return statut
    return StatutReunion.INCONNU


def generer_reunion_uid(date_reunion: str, hippodrome_normalise: str, source: str) -> str:
    base = f"{date_reunion}|{hippodrome_normalise}|{source}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def generer_reunion_uid_cross_source(date_reunion: str, hippodrome_normalise: str) -> str:
    base = f"{date_reunion}|{hippodrome_normalise}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def generer_cle_jour_hippo_numero(
    date_reunion: str, hippodrome_normalise: str, numero: Optional[int]
) -> str:
    num_str = str(numero) if numero else "X"
    return f"{date_reunion}|{hippodrome_normalise}|R{num_str}"


# ===========================================================================
# CLIENT HTTP
# ===========================================================================

class HttpClient:
    def __init__(self, config: PipelineConfig, logger: logging.Logger) -> None:
        self._logger = logger
        self._config = config
        self._session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": self._config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/json,*/*",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        })
        retry_strategy = Retry(
            total=self._config.http_retry_max,
            backoff_factor=self._config.http_retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get(
        self, url: str, src: Optional[SourceEndpointConfig] = None,
        params: Optional[dict[str, str]] = None,
    ) -> Optional[requests.Response]:
        timeout = src.timeout if src else self._config.http_timeout
        headers = dict(src.headers) if src and src.headers else {}
        self._logger.debug("GET %s (timeout=%ds)", url, timeout)
        try:
            resp = self._session.get(url, timeout=timeout, headers=headers, params=params)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            self._logger.warning("HTTP %s pour %s: %s", status, url, e)
        except requests.exceptions.ConnectionError as e:
            self._logger.warning("Connexion échouée pour %s: %s", url, e)
        except requests.exceptions.Timeout:
            self._logger.warning("Timeout pour %s après %ds", url, timeout)
        except requests.exceptions.RequestException as e:
            self._logger.error("Erreur requête pour %s: %s", url, e)
        return None

    def get_json(self, url: str, src: Optional[SourceEndpointConfig] = None) -> Optional[Any]:
        resp = self.get(url, src)
        if resp is None:
            return None
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as e:
            self._logger.warning("Non-JSON depuis %s: %s", url, e)
            return None

    def get_html(self, url: str, src: Optional[SourceEndpointConfig] = None) -> Optional[str]:
        resp = self.get(url, src)
        if resp is None:
            return None
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text


# ===========================================================================
# VALIDATION
# ===========================================================================

class ValidationError:
    def __init__(self, champ: str, message: str) -> None:
        self.champ = champ
        self.message = message

    def __repr__(self) -> str:
        return f"ValidationError({self.champ}: {self.message})"


def valider_reunion_brute(r: ReunionBrute) -> list[ValidationError]:
    erreurs: list[ValidationError] = []
    if not r.source:
        erreurs.append(ValidationError("source", "Source manquante"))
    if not r.date_reunion_brut:
        erreurs.append(ValidationError("date_reunion_brut", "Date manquante"))
    else:
        try:
            date.fromisoformat(r.date_reunion_brut)
        except ValueError:
            erreurs.append(ValidationError("date_reunion_brut", f"Format invalide: {r.date_reunion_brut}"))
    if not r.hippodrome_brut:
        erreurs.append(ValidationError("hippodrome_brut", "Hippodrome manquant"))
    return erreurs


# ===========================================================================
# PARSERS
# ===========================================================================

class BaseParser(ABC):
    def __init__(
        self, http: HttpClient, src_config: SourceEndpointConfig, logger: logging.Logger
    ):
        self.http = http
        self.src = src_config
        self.logger = logger

    @abstractmethod
    def fetch_reunions(self, jour: date) -> list[ReunionBrute]:
        ...


# ---------------------------------------------------------------------------
# PMU — API JSON turfinfo
# ---------------------------------------------------------------------------

class ParserPMU(BaseParser):
    """
    Endpoint : GET https://online.turfinfo.api.pmu.fr/rest/client/7/programme/{DDMMYYYY}
    Type : API JSON publique
    Stratégie : JSON_DIRECT
    """

    def fetch_reunions(self, jour: date) -> list[ReunionBrute]:
        url = self.src.build_url(jour)
        self.logger.info("[PMU] Récupération programme pour %s", jour.isoformat())

        data = self.http.get_json(url, self.src)
        if data is None:
            self.logger.warning("[PMU] Aucune donnée pour %s", jour.isoformat())
            return []
        return self._parse(data, jour, url)

    def _parse(self, data: Any, jour: date, url_source: str) -> list[ReunionBrute]:
        reunions: list[ReunionBrute] = []
        ts = utc_now_iso()

        programme = data.get("programme", data) if isinstance(data, dict) else {}
        liste = programme.get("reunions", [])

        for item in liste:
            try:
                # Extraction via le field mapping PMU
                hippo_data = item.get("hippodrome", {})
                hippo_nom = ""
                hippo_nom_long = ""
                if isinstance(hippo_data, dict):
                    hippo_nom = hippo_data.get("libelleCourt", "") or hippo_data.get("libelle", "")
                    hippo_nom_long = hippo_data.get("libelleLong", "")
                else:
                    hippo_nom = str(hippo_data)

                pays_data = item.get("pays", {})
                pays = ""
                pays_code = ""
                if isinstance(pays_data, dict):
                    pays = pays_data.get("libelle", "")
                    pays_code = pays_data.get("code", "")
                elif isinstance(hippo_data, dict):
                    pi = hippo_data.get("pays", {})
                    pays = pi.get("libelle", "") if isinstance(pi, dict) else ""

                disc_raw = item.get("discipline", "")
                disc_str = disc_raw.get("libelle", "") if isinstance(disc_raw, dict) else str(disc_raw)
                if not disc_str:
                    dm = item.get("disciplinesMere", [])
                    if dm and isinstance(dm, list):
                        disc_str = dm[0] if isinstance(dm[0], str) else str(dm[0])

                numero = item.get("numOfficiel") or item.get("numExterne")
                num_externe = str(item.get("numExterne", ""))
                courses = item.get("courses", [])
                nb_courses = item.get("nombreCourses") or (len(courses) if courses else None)
                statut = str(item.get("statut", ""))
                nature = str(item.get("nature", ""))
                audience = str(item.get("audience", ""))

                # Heure de la première course
                heure_str = ""
                if courses:
                    ts_depart = courses[0].get("heureDepart")
                    if ts_depart and isinstance(ts_depart, (int, float)):
                        from datetime import datetime as dt_cls
                        try:
                            heure_str = dt_cls.fromtimestamp(ts_depart / 1000).strftime("%H:%M")
                        except (OSError, ValueError):
                            pass

                # Terrain PMU (typePiste + penetrometre de la première course)
                terrain_str = ""
                corde_str = ""
                condition_str = ""
                nb_partants = None
                distance_m = None
                parcours_str = ""
                if courses:
                    c0 = courses[0]
                    # Corde
                    raw_corde = c0.get("corde", "")
                    corde_str = str(raw_corde).replace("CORDE_", "").replace("_", " ").title() if raw_corde else ""
                    # Type de piste → terrain
                    type_piste = c0.get("typePiste", "")
                    if type_piste:
                        terrain_str = str(type_piste).upper()  # HERBE, PSF, SABLE, etc.
                    # Pénétromètre → condition du terrain
                    penetro = c0.get("penetrometre", {})
                    if isinstance(penetro, dict) and penetro.get("intitule"):
                        condition_str = penetro["intitule"]  # "Très souple", "Bon", etc.
                    # Nombre de partants
                    np = c0.get("nombreDeclaresPartants")
                    if np is not None:
                        nb_partants = int(np)
                    # Distance et parcours
                    dist = c0.get("distance")
                    if dist is not None:
                        distance_m = int(dist)
                    parcours_str = str(c0.get("parcours", ""))  # "1200 M. (LIGNE DROITE)"

                # Météo PMU
                meteo_raw = item.get("meteo", {})
                meteo_str = ""
                if isinstance(meteo_raw, dict) and meteo_raw:
                    parts = []
                    neb = meteo_raw.get("nebulositeLibelleCourt", "")
                    if neb:
                        parts.append(neb)
                    temp = meteo_raw.get("temperature")
                    if temp is not None:
                        parts.append(f"{temp}°C")
                    vent = meteo_raw.get("forceVent")
                    dir_vent = meteo_raw.get("directionVent", "")
                    if vent is not None:
                        parts.append(f"vent {vent}km/h {dir_vent}".strip())
                    meteo_str = " | ".join(parts)

                # Spécialités complètes
                specialites = item.get("specialites", [])
                disc_mere = item.get("disciplinesMere", [])
                spec_str = disc_str
                if specialites:
                    spec_str = ", ".join(str(s) for s in specialites)
                elif disc_mere:
                    spec_str = ", ".join(str(d) for d in disc_mere)

                url_reunion = ""
                if numero:
                    url_reunion = f"https://www.pmu.fr/turf/{jour.strftime('%d%m%Y')}/R{numero}/"

                # Paris événements (Quinté, Pick5)
                paris_evt = item.get("parisEvenement", [])
                paris_codes = list({p.get("codePari", "") for p in paris_evt if isinstance(p, dict)})
                has_quinte = any("QUINTE" in c for c in paris_codes)

                reunion = ReunionBrute(
                    source="pmu",
                    url_source=url_source,
                    timestamp_collecte=ts,
                    date_reunion_brut=jour.isoformat(),
                    hippodrome_brut=hippo_nom,
                    discipline_brut=disc_str,
                    numero_reunion_brut=int(numero) if numero else None,
                    url_reunion_brut=url_reunion,
                    identifiant_source_reunion_brut=str(numero) if numero else "",
                    pays_brut=pays,
                    nombre_courses_reunion_brut=int(nb_courses) if nb_courses else None,
                    statut_reunion_brut=statut,
                    libelle_reunion_brut=audience,
                    code_reunion_brut=num_externe,
                    type_reunion_brut=nature,
                    specialite_brut=spec_str,
                    heure_reunion_brut=heure_str,
                    terrain_brut=terrain_str,
                    meteo_brut=meteo_str,
                    extras={
                        "hippodrome_brut_long": hippo_nom_long,
                        "pays_code_brut": pays_code,
                        "corde": corde_str,
                        "condition": condition_str,
                        "nb_partants": nb_partants,
                        "distance_m": distance_m,
                        "parcours": parcours_str,
                        "meteo_temperature": meteo_raw.get("temperature") if isinstance(meteo_raw, dict) else None,
                        "meteo_nebulosite": meteo_raw.get("nebulositeLibelleCourt", "") if isinstance(meteo_raw, dict) else "",
                        "meteo_nebulosite_long": meteo_raw.get("nebulositeLibelleLong", "") if isinstance(meteo_raw, dict) else "",
                        "meteo_force_vent": meteo_raw.get("forceVent") if isinstance(meteo_raw, dict) else None,
                        "meteo_direction_vent": meteo_raw.get("directionVent", "") if isinstance(meteo_raw, dict) else "",
                        "has_quinte": has_quinte,
                        "paris_evenements": paris_codes,
                        "specialites_liste": [str(s) for s in specialites],
                        "disciplines_mere": [str(d) for d in disc_mere],
                    },
                )
                reunions.append(reunion)
            except (KeyError, TypeError, ValueError) as e:
                self.logger.warning("[PMU] Erreur parsing: %s | %s", e, str(item)[:200])

        self.logger.info("[PMU] %d réunions pour %s", len(reunions), jour.isoformat())
        return reunions


# ---------------------------------------------------------------------------
# LE TROT — HTML avec JSON embarqué <meeting-day :program="...">
# ---------------------------------------------------------------------------

class ParserLeTrot(BaseParser):
    """
    Endpoint : GET https://www.letrot.com/courses/{YYYY-MM-DD}
    Type : HTML avec JSON dans attribut Vue :program
    Stratégie : HTML_VUE_ATTRIBUTE
    """

    def fetch_reunions(self, jour: date) -> list[ReunionBrute]:
        if not HAS_BS4:
            self.logger.warning("[Le Trot] BeautifulSoup requis")
            return []

        url = self.src.build_url(jour)
        self.logger.info("[Le Trot] Récupération programme pour %s", jour.isoformat())

        html_content = self.http.get_html(url, self.src)
        if html_content is None:
            self.logger.warning("[Le Trot] Aucune donnée pour %s", jour.isoformat())
            return []
        return self._parse(html_content, jour, url)

    def _parse(self, html_content: str, jour: date, url_source: str) -> list[ReunionBrute]:
        reunions: list[ReunionBrute] = []
        ts = utc_now_iso()

        program_data = self._extract_meeting_day_json(html_content)
        if program_data is None:
            self.logger.warning("[Le Trot] Impossible d'extraire meeting-day pour %s", jour.isoformat())
            return []

        meetings = program_data.get("meetings", [])

        for item in meetings:
            try:
                nom_hippo = item.get("nomHippodrome", "")
                num_hippo = item.get("numHippodrome", "")
                nb_courses = item.get("nbCourse")
                numero = item.get("numReunion")
                seance_id = item.get("seanceId")
                statut = item.get("status", "")
                termine = item.get("termine", False)
                annule = item.get("annule", False)
                is_live = item.get("isLive", False)
                type_reunion = item.get("type", "")
                heure = item.get("heureReunion", "")
                corde_piste = item.get("cordePiste", "")
                nom_fede = item.get("nomFede", "")
                condition_txt = item.get("condition", "")
                nb_engages = item.get("nbEngages")
                nb_qualifies = item.get("nbQualifies")

                # Extraire terrain depuis cordePiste (ex: "Corde à gauche - 1.411 mètres environ (sable)")
                terrain_lt = ""
                if corde_piste:
                    m_terrain = re.search(r"\(([^)]+)\)\s*$", corde_piste)
                    if m_terrain:
                        terrain_lt = m_terrain.group(1).strip()

                if annule:
                    statut_str = "annulee"
                elif termine:
                    statut_str = statut if statut else "terminee"
                elif is_live:
                    statut_str = "en_cours"
                else:
                    statut_str = statut if statut else "programmee"

                url_path = item.get("url", "")
                url_reunion = ""
                if url_path:
                    url_reunion = f"{self.src.url_base}{url_path}"
                elif num_hippo:
                    url_reunion = f"{self.src.url_base}/courses/programme/{jour.isoformat()}/{num_hippo}"

                id_source = str(seance_id) if seance_id else str(num_hippo)

                reunion = ReunionBrute(
                    source="letrot",
                    url_source=url_source,
                    timestamp_collecte=ts,
                    date_reunion_brut=jour.isoformat(),
                    hippodrome_brut=nom_hippo,
                    discipline_brut="trot",
                    numero_reunion_brut=int(numero) if numero else None,
                    url_reunion_brut=url_reunion,
                    identifiant_source_reunion_brut=id_source,
                    pays_brut="France",
                    nombre_courses_reunion_brut=int(nb_courses) if nb_courses else None,
                    statut_reunion_brut=statut_str,
                    code_reunion_brut=str(num_hippo),
                    type_reunion_brut=type_reunion,
                    heure_reunion_brut=heure,
                    terrain_brut=terrain_lt,
                    extras={
                        "quinte": item.get("quinteEventuel", False),
                        "pick5": item.get("pick5", False),
                        "has_replay": item.get("hasReplay", False),
                        "heat": item.get("heat", False),
                        "corde_piste": corde_piste,
                        "federation": nom_fede,
                        "condition": condition_txt,
                        "nb_engages": nb_engages,
                        "nb_qualifies": nb_qualifies,
                    },
                )
                reunions.append(reunion)
            except (KeyError, TypeError, ValueError) as e:
                self.logger.warning("[Le Trot] Erreur parsing: %s | %s", e, str(item)[:200])

        self.logger.info("[Le Trot] %d réunions pour %s", len(reunions), jour.isoformat())
        return reunions

    def _extract_meeting_day_json(self, html_content: str) -> Optional[dict[str, Any]]:
        soup = BeautifulSoup(html_content, "html.parser")
        meeting_day = soup.find("meeting-day")

        # Attribut :program (binding Vue)
        if meeting_day:
            for attr_name in (":program", "program", ":initial-program"):
                raw = meeting_day.get(attr_name)
                if raw:
                    try:
                        return json.loads(html_module.unescape(raw))
                    except json.JSONDecodeError:
                        continue

        # Fallback regex
        pattern = r'<meeting-day\s[^>]*:program="([^"]*)"'
        match = re.search(pattern, html_content)
        if match:
            try:
                return json.loads(html_module.unescape(match.group(1)))
            except json.JSONDecodeError:
                pass

        return None


# ---------------------------------------------------------------------------
# GENY — HTML server-rendered avec .cartoucheReunion
# ---------------------------------------------------------------------------

class ParserGeny(BaseParser):
    """
    Endpoint : GET https://www.geny.com/reunions-courses-pmu/_d{YYYY-MM-DD}
    Type : HTML server-rendered
    Stratégie : HTML_CSS_SELECTORS

    Structure HTML :
      - <a name="reunionN"> ancre de chaque réunion
      - div.cartoucheReunion contient :
        - div.nomReunion : "jour : Hippodrome (RN)"
        - div.infoReunion : heure, terrain, NP
      - div.courseParis : blocs de courses individuelles
    """

    def fetch_reunions(self, jour: date) -> list[ReunionBrute]:
        if not HAS_BS4:
            self.logger.warning("[Geny] BeautifulSoup requis")
            return []

        url = self.src.build_url(jour)
        self.logger.info("[Geny] Récupération programme pour %s", jour.isoformat())

        html_content = self.http.get_html(url, self.src)
        if html_content is None:
            self.logger.warning("[Geny] Aucune donnée pour %s", jour.isoformat())
            return []
        return self._parse(html_content, jour, url)

    def _parse(self, html_content: str, jour: date, url_source: str) -> list[ReunionBrute]:
        reunions: list[ReunionBrute] = []
        ts = utc_now_iso()
        soup = BeautifulSoup(html_content, "html.parser")

        cartouches = soup.select(".cartoucheReunion")
        if cartouches:
            reunions = self._parse_cartouches(soup, cartouches, jour, url_source, ts)

        if not reunions:
            reunions = self._parse_course_links(soup, jour, url_source, ts)

        self.logger.info("[Geny] %d réunions pour %s", len(reunions), jour.isoformat())
        return reunions

    def _parse_cartouches(
        self, soup: BeautifulSoup, cartouches: list,
        jour: date, url_source: str, ts: str,
    ) -> list[ReunionBrute]:
        reunions: list[ReunionBrute] = []

        for cartouche in cartouches:
            try:
                nom_el = cartouche.select_one(".nomReunion")
                if not nom_el:
                    continue

                raw_text = nom_el.get_text(separator=" ", strip=True)
                hippodrome, numero, pays = self._parse_nom_reunion(raw_text)
                if not hippodrome:
                    continue

                anchor_name = ""
                prev = cartouche.find_previous_sibling(
                    "a", attrs={"name": re.compile(r"^reunion\d+$")}
                )
                if prev:
                    anchor_name = prev.get("name", "")

                heure = ""
                terrain = ""
                non_partants = ""
                info_el = cartouche.select_one(".infoReunion")
                if info_el:
                    info_text = info_el.get_text(separator=" ", strip=True)
                    hm = re.search(r"(\d{1,2}:\d{2})", info_text)
                    if hm:
                        heure = hm.group(1)
                    tm = re.search(r"Terrain\s*:\s*(\S+)", info_text)
                    if tm:
                        terrain = tm.group(1)
                    # Non-partants (ex: "511 - 701")
                    np_el = info_el.select_one(".nonPartant")
                    if np_el:
                        np_text = np_el.get_text(strip=True)
                        np_match = re.search(r":\s*(.+)$", np_text)
                        if np_match:
                            non_partants = np_match.group(1).strip()

                nb_courses = self._count_courses_after(cartouche)

                reunion = ReunionBrute(
                    source="geny",
                    url_source=url_source,
                    timestamp_collecte=ts,
                    date_reunion_brut=jour.isoformat(),
                    hippodrome_brut=hippodrome,
                    numero_reunion_brut=numero,
                    url_reunion_brut=f"{url_source}#{anchor_name}" if anchor_name else url_source,
                    identifiant_source_reunion_brut=anchor_name or f"geny-{normaliser_hippodrome(hippodrome)}",
                    pays_brut=pays,
                    nombre_courses_reunion_brut=nb_courses,
                    heure_reunion_brut=heure,
                    terrain_brut=terrain,
                    extras={
                        "non_partants": non_partants,
                    } if non_partants else {},
                )
                reunions.append(reunion)
            except Exception as e:
                self.logger.warning("[Geny] Erreur parsing cartouche: %s", e)

        return reunions

    @staticmethod
    def _parse_nom_reunion(raw_text: str) -> tuple[str, Optional[int], str]:
        """Parse 'jeudi : Chantilly (R1)' ou 'jeudi : Mons (Belgique) (R5)'."""
        text = re.sub(
            r"^[a-zéèêëàâäùûüôöîï]+\s*:\s*", "", raw_text, flags=re.IGNORECASE
        ).strip()

        numero = None
        rn_match = re.search(r"\(R(\d+)\)\s*$", text)
        if rn_match:
            numero = int(rn_match.group(1))
            text = text[:rn_match.start()].strip()

        pays = ""
        pays_match = re.search(r"[\(\[]\s*([^)\]]+)\s*[\)\]]$", text)
        if pays_match:
            pays = pays_match.group(1).strip()
            text = text[:pays_match.start()].strip()

        return text.strip(), numero, pays

    @staticmethod
    def _count_courses_after(cartouche: Any) -> Optional[int]:
        count = 0
        sib = cartouche.find_next_sibling()
        while sib:
            if sib.name == "a" and sib.get("name", "").startswith("reunion"):
                break
            classes = sib.get("class", [])
            if "cartoucheReunion" in classes:
                break
            if "courseParis" in classes:
                count += 1
            sib = sib.find_next_sibling()
        return count if count > 0 else None

    def _parse_course_links(
        self, soup: BeautifulSoup, jour: date, url_source: str, ts: str,
    ) -> list[ReunionBrute]:
        """Fallback : regroupe les liens /partants-pmu/ par hippodrome."""
        reunions: list[ReunionBrute] = []
        links = soup.find_all("a", href=re.compile(r"/partants-pmu/"))
        if not links:
            return []

        hippodromes: dict[str, list[str]] = {}
        date_str = jour.strftime("%Y-%m-%d")
        pat = re.compile(
            rf"/partants-pmu/{re.escape(date_str)}-([a-z0-9\-]+)-pmu-[^_]*_c(\d+)"
        )
        for link in links:
            m = pat.search(link.get("href", ""))
            if m:
                hippodromes.setdefault(m.group(1), []).append(m.group(2))

        for slug, ids in hippodromes.items():
            reunions.append(ReunionBrute(
                source="geny",
                url_source=url_source,
                timestamp_collecte=ts,
                date_reunion_brut=jour.isoformat(),
                hippodrome_brut=slug.replace("-", " ").title(),
                identifiant_source_reunion_brut=slug,
                nombre_courses_reunion_brut=len(ids),
            ))
        return reunions


# ===========================================================================
# REGISTRE DES PARSERS
# ===========================================================================

PARSER_REGISTRY: dict[str, type[BaseParser]] = {
    "pmu": ParserPMU,
    "letrot": ParserLeTrot,
    "geny": ParserGeny,
}


# ===========================================================================
# DÉDUPLICATION INTRA-SOURCE
# ===========================================================================

def deduplication_intra_source(
    reunions: list[ReunionBrute], logger: logging.Logger,
) -> list[ReunionBrute]:
    seen: set[str] = set()
    uniques: list[ReunionBrute] = []
    doublons = 0
    for r in reunions:
        key = (
            f"{r.source}|{r.date_reunion_brut}"
            f"|{normaliser_hippodrome(r.hippodrome_brut)}|{r.numero_reunion_brut}"
        )
        if key in seen:
            doublons += 1
            continue
        seen.add(key)
        uniques.append(r)
    if doublons:
        logger.info("Déduplication intra-source: %d doublons supprimés", doublons)
    return uniques


# ===========================================================================
# NORMALISATION BRUT -> NORMALISÉ + FEATURES DÉRIVÉES
# ===========================================================================

def normaliser_reunion(
    r: ReunionBrute, sources_config: dict[str, SourceEndpointConfig],
) -> ReunionNormalisee:
    """Transforme une ReunionBrute en ReunionNormalisee avec features dérivées."""
    hippo_norm = normaliser_hippodrome(r.hippodrome_brut)
    disc_norm = normaliser_discipline(r.discipline_brut)
    statut_norm = normaliser_statut(r.statut_reunion_brut)
    uid = generer_reunion_uid(r.date_reunion_brut, hippo_norm, r.source)
    cle = generer_cle_jour_hippo_numero(r.date_reunion_brut, hippo_norm, r.numero_reunion_brut)

    # Fallback discipline par pays si inconnu
    if disc_norm == Discipline.INCONNU:
        disc_norm = _deduire_discipline_par_pays(r.pays_brut, hippo_norm)

    # Enrichissement via HIPPODROMES_DB (région, pays)
    hippo_info = get_hippodrome_info(hippo_norm)
    region = r.region_brut
    pays = r.pays_brut
    if hippo_info:
        if not region:
            region = hippo_info.get("region", "")
        if not pays:
            pays = hippo_info.get("pays", "")

    # Normalisation du pays (title case unifié + nettoyage parasites Geny)
    if pays:
        import re as _re
        # Nettoyer suffixes parasites Geny
        pays = _re.sub(r"\s*[-–]\s*[Gg]enybet\b.*$", "", pays).strip()
        pays = _re.sub(r"\s*\([Gg]enybet\b.*$", "", pays).strip()
        if pays.lower() in ("genybet", "internet"):
            pays = ""
        pays_map = {
            "france": "France", "allemagne": "Allemagne", "belgique": "Belgique",
            "royaume-uni": "Royaume-Uni", "royaume uni": "Royaume-Uni",
            "grande-bretagne": "Royaume-Uni", "grande bretagne": "Royaume-Uni",
            "etats-unis": "États-Unis", "etats unis": "États-Unis", "usa": "États-Unis",
            "espagne": "Espagne", "italie": "Italie", "suisse": "Suisse",
            "chili": "Chili", "bresil": "Brésil", "argentine": "Argentine",
            "australie": "Australie", "suede": "Suède", "norvege": "Norvège",
            "finlande": "Finlande", "irlande": "Irlande", "autriche": "Autriche",
            "pays-bas": "Pays-Bas", "hongrie": "Hongrie", "republique tcheque": "République Tchèque",
            "emirats arabes unis": "Émirats Arabes Unis", "emirats": "Émirats Arabes Unis",
            "hong-kong": "Hong-Kong", "hong kong": "Hong-Kong",
            "singapour": "Singapour", "afrique du sud": "Afrique du Sud",
            "afrique-du-sud": "Afrique du Sud",
            "maroc": "Maroc", "tunisie": "Tunisie", "portugal": "Portugal",
            "perou": "Pérou", "uruguay": "Uruguay", "japon": "Japon",
            "danemark": "Danemark", "arabie saoudite": "Arabie Saoudite",
            "guadeloupe": "France", "guadaloupe": "France",  # DOM-TOM
            "nouvelle-zelande": "Nouvelle-Zélande",
        }
        if pays:
            pays_lower = pays.strip().lower()
            pays = pays_map.get(pays_lower, pays.strip().title())

    # Détection données incomplètes
    champs_critiques = [r.hippodrome_brut, r.date_reunion_brut]
    incomplet = not all(champs_critiques) or r.nombre_courses_reunion_brut is None

    src_cfg = sources_config.get(r.source)
    priorite_src = src_cfg.nom_source if src_cfg else r.source

    return ReunionNormalisee(
        # Identifiants
        reunion_uid=uid,
        cle_jour_hippodrome_numero=cle,
        # Normalisé
        source=r.source,
        date_reunion_iso=r.date_reunion_brut,
        hippodrome=r.hippodrome_brut,
        hippodrome_normalise=hippo_norm,
        discipline_normalisee=disc_norm.value,
        numero_reunion=r.numero_reunion_brut,
        url_reunion=r.url_reunion_brut,
        identifiant_source_reunion=r.identifiant_source_reunion_brut,
        pays=pays,
        region=region,
        nombre_courses_reunion=r.nombre_courses_reunion_brut,
        statut_reunion=statut_norm.value,
        # Bruts conservés
        libelle_reunion=r.libelle_reunion_brut,
        code_reunion=r.code_reunion_brut,
        type_reunion=r.type_reunion_brut,
        specialite=r.specialite_brut,
        heure_reunion=r.heure_reunion_brut,
        terrain=_nettoyer_terrain(r.terrain_brut),
        meteo=r.meteo_brut,
        # Météo structurée depuis extras PMU
        meteo_temperature=r.extras.get("meteo_temperature") if isinstance(r.extras, dict) else None,
        meteo_nebulosite=r.extras.get("meteo_nebulosite", "") if isinstance(r.extras, dict) else "",
        meteo_force_vent=r.extras.get("meteo_force_vent") if isinstance(r.extras, dict) else None,
        meteo_direction_vent=r.extras.get("meteo_direction_vent", "") if isinstance(r.extras, dict) else "",
        # Infos paris (PMU + Le Trot)
        has_quinte=(
            r.extras.get("has_quinte", False) or r.extras.get("quinte", False)
        ) if isinstance(r.extras, dict) else False,
        paris_evenements=r.extras.get("paris_evenements", []) if isinstance(r.extras, dict) else [],
        # Infos complémentaires
        corde_piste=r.extras.get("corde", "") if isinstance(r.extras, dict) else "",
        federation=_deduire_federation(disc_norm),
        condition=r.extras.get("condition", "") if isinstance(r.extras, dict) else "",
        non_partants=r.extras.get("non_partants", "") if isinstance(r.extras, dict) else "",
        nb_engages=r.extras.get("nb_partants") if isinstance(r.extras, dict) else None,
        has_replay=r.extras.get("has_replay", False) if isinstance(r.extras, dict) else False,
        # Features dérivées
        source_prioritaire=priorite_src,
        nb_sources_match=1,
        est_duplique_inter_source=False,
        indicateur_donnee_incomplete=incomplet,
        indicateur_reunion_fusionnee=False,
        date_collecte=utc_now().strftime("%Y-%m-%d"),
        timestamp_collecte=r.timestamp_collecte,
        url_source=r.url_source,
        # Provenance
        sources_multiples=[r.source],
        source_origine_principale=r.source,
        sources_secondaires=[],
        champs_confirmes_par_plusieurs_sources=[],
        champs_en_conflit={},
    )


# ===========================================================================
# FUSION INTER-SOURCES AVEC TRACKING CONFLITS
# ===========================================================================

# Champs comparables pour détecter confirmations et conflits
CHAMPS_COMPARABLES = [
    "hippodrome", "discipline_normalisee", "numero_reunion",
    "pays", "nombre_courses_reunion", "statut_reunion",
]


def fusion_inter_sources(
    reunions: list[ReunionNormalisee],
    sources_config: dict[str, SourceEndpointConfig],
    logger: logging.Logger,
) -> list[ReunionNormalisee]:
    """
    Fusionne les réunions identiques de sources différentes.
    Conserve la source de plus haute priorité comme base.
    Trace confirmations et conflits.
    """
    # Trier par priorité (plus basse = plus prioritaire)
    priorite_map = {
        code: cfg.priorite for code, cfg in sources_config.items()
    }
    reunions_triees = sorted(
        reunions,
        key=lambda r: priorite_map.get(r.source, 99),
    )

    index: dict[str, int] = {}
    result: list[ReunionNormalisee] = []
    doublons = 0

    for r in reunions_triees:
        cross_uid = generer_reunion_uid_cross_source(r.date_reunion_iso, r.hippodrome_normalise)

        if cross_uid not in index:
            # Première occurrence — source prioritaire
            r.cross_uid = cross_uid
            r.source_origine_principale = r.source
            r.source_prioritaire = r.source
            index[cross_uid] = len(result)
            result.append(r)
            continue

        # Fusion avec l'existant
        idx = index[cross_uid]
        existing = result[idx]
        doublons += 1

        # Ajouter la source
        if r.source not in existing.sources_multiples:
            existing.sources_multiples.append(r.source)
        if r.source not in existing.sources_secondaires and r.source != existing.source_origine_principale:
            existing.sources_secondaires.append(r.source)

        existing.nb_sources_match = len(existing.sources_multiples)
        existing.est_duplique_inter_source = True
        existing.indicateur_reunion_fusionnee = True

        # Comparer les champs pour confirmations et conflits
        for champ in CHAMPS_COMPARABLES:
            val_existing = getattr(existing, champ, None)
            val_new = getattr(r, champ, None)

            # Skip si une des valeurs est vide/None/inconnu
            if _is_empty(val_existing) and not _is_empty(val_new):
                # Enrichir le champ manquant
                setattr(existing, champ, val_new)
                continue
            if _is_empty(val_new) or _is_empty(val_existing):
                continue

            # Les deux ont une valeur
            if _values_match(val_existing, val_new, champ):
                if champ not in existing.champs_confirmes_par_plusieurs_sources:
                    existing.champs_confirmes_par_plusieurs_sources.append(champ)
            else:
                # Conflit détecté
                if champ not in existing.champs_en_conflit:
                    existing.champs_en_conflit[champ] = {
                        existing.source_origine_principale: str(val_existing)
                    }
                existing.champs_en_conflit[champ][r.source] = str(val_new)

        # Enrichir les champs bruts manquants
        if not existing.heure_reunion and r.heure_reunion:
            existing.heure_reunion = r.heure_reunion
        if not existing.terrain and r.terrain:
            existing.terrain = r.terrain
        if not existing.libelle_reunion and r.libelle_reunion:
            existing.libelle_reunion = r.libelle_reunion
        if not existing.type_reunion and r.type_reunion:
            existing.type_reunion = r.type_reunion
        if not existing.specialite and r.specialite:
            existing.specialite = r.specialite
        if not existing.meteo and r.meteo:
            existing.meteo = r.meteo
        if existing.meteo_temperature is None and r.meteo_temperature is not None:
            existing.meteo_temperature = r.meteo_temperature
            existing.meteo_nebulosite = r.meteo_nebulosite
            existing.meteo_force_vent = r.meteo_force_vent
            existing.meteo_direction_vent = r.meteo_direction_vent
        if not existing.has_quinte and r.has_quinte:
            existing.has_quinte = True
        if not existing.paris_evenements and r.paris_evenements:
            existing.paris_evenements = r.paris_evenements
        # Enrichir pick5 depuis Le Trot extras (via has_quinte propagation)
        if r.source == "letrot" and r.has_quinte and not existing.has_quinte:
            existing.has_quinte = True
        if not existing.region and r.region:
            existing.region = r.region
        if not existing.corde_piste and r.corde_piste:
            existing.corde_piste = r.corde_piste
        if not existing.federation and r.federation:
            existing.federation = r.federation
        if not existing.condition and r.condition:
            existing.condition = r.condition
        if not existing.non_partants and r.non_partants:
            existing.non_partants = r.non_partants
        if existing.nb_engages is None and r.nb_engages is not None:
            existing.nb_engages = r.nb_engages
        if not existing.has_replay and r.has_replay:
            existing.has_replay = True
        if not existing.code_reunion and r.code_reunion:
            existing.code_reunion = r.code_reunion

        # Marquer complet si enrichi
        if existing.indicateur_donnee_incomplete and existing.nombre_courses_reunion is not None:
            existing.indicateur_donnee_incomplete = False

        logger.debug(
            "Fusion %s: %s + %s (confirmés=%s, conflits=%s)",
            cross_uid, existing.source_origine_principale, r.source,
            existing.champs_confirmes_par_plusieurs_sources,
            list(existing.champs_en_conflit.keys()),
        )

    if doublons:
        logger.info("Fusion inter-sources: %d doublons fusionnés", doublons)
    return result


def _is_empty(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, str) and val.strip() in ("", "inconnu"):
        return True
    return False


def _values_match(val1: Any, val2: Any, champ: str) -> bool:
    """Compare deux valeurs en tenant compte du type de champ."""
    if champ == "hippodrome":
        return normaliser_hippodrome(str(val1)) == normaliser_hippodrome(str(val2))
    if champ in ("discipline_normalisee", "statut_reunion"):
        return normaliser_texte(str(val1)) == normaliser_texte(str(val2))
    if isinstance(val1, int) and isinstance(val2, int):
        return val1 == val2
    return str(val1).strip().lower() == str(val2).strip().lower()


# ===========================================================================
# CONSTRUCTION DES RÉFÉRENCES POUR 02_LISTE_COURSES.PY
# ===========================================================================

def build_reunion_references(
    normalisees: list[ReunionNormalisee],
    brutes: list[ReunionBrute],
) -> list[dict[str, Any]]:
    """
    Construit la table de références exploitable par 02_liste_courses.py.
    Agrège les URLs et IDs par source pour chaque réunion normalisée.
    """
    # Index des brutes par (date, hippo_norm, source)
    brutes_index: dict[str, ReunionBrute] = {}
    for b in brutes:
        key = f"{b.date_reunion_brut}|{normaliser_hippodrome(b.hippodrome_brut)}|{b.source}"
        brutes_index[key] = b

    refs: list[dict[str, Any]] = []
    for n in normalisees:
        ref: dict[str, Any] = {
            "reunion_uid": n.reunion_uid,
            "date_reunion_iso": n.date_reunion_iso,
            "hippodrome_normalise": n.hippodrome_normalise,
            "hippodrome": n.hippodrome,
            "discipline_normalisee": n.discipline_normalisee,
            "numero_reunion": n.numero_reunion,
            "pays": n.pays,
            "nombre_courses_reunion": n.nombre_courses_reunion,
            "sources": n.sources_multiples,
            "url_pmu": "",
            "url_letrot": "",
            "url_geny": "",
            "id_pmu": "",
            "id_letrot": "",
            "id_geny": "",
        }

        for src in n.sources_multiples:
            key = f"{n.date_reunion_iso}|{n.hippodrome_normalise}|{src}"
            b = brutes_index.get(key)
            if b:
                ref[f"url_{src}"] = b.url_reunion_brut
                ref[f"id_{src}"] = b.identifiant_source_reunion_brut

        refs.append(ref)

    return refs


# ===========================================================================
# SAUVEGARDE
# ===========================================================================

class Sauvegarder:
    def __init__(self, dossier: Path, logger: logging.Logger) -> None:
        self.dossier = dossier
        self.logger = logger
        self.dossier.mkdir(parents=True, exist_ok=True)

    def sauver_json(self, data: list[dict[str, Any]], nom: str) -> Path:
        path = self.dossier / nom
        tmp = path.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            tmp.rename(path)
            self.logger.info("JSON: %s (%d enreg.)", path, len(data))
        except Exception as e:
            self.logger.error("Erreur JSON %s: %s", path, e)
            if tmp.exists():
                tmp.unlink()
            raise
        return path

    def sauver_parquet(self, data: list[dict[str, Any]], nom: str) -> Optional[Path]:
        if not HAS_PARQUET:
            self.logger.warning("pyarrow absent, Parquet ignoré")
            return None
        path = self.dossier / nom
        tmp = path.with_suffix(".tmp.parquet")
        try:
            clean = []
            for row in data:
                cr = {}
                for k, v in row.items():
                    cr[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                clean.append(cr)
            table = pa.Table.from_pylist(clean)
            # Metadata pour indiquer les champs JSON sérialisés
            json_fields = [k for k, v in data[0].items() if isinstance(v, (list, dict))] if data else []
            if json_fields:
                existing_meta = table.schema.metadata or {}
                existing_meta[b"json_serialized_fields"] = json.dumps(json_fields).encode("utf-8")
                table = table.replace_schema_metadata(existing_meta)
            pq.write_table(table, tmp, compression="snappy")
            tmp.rename(path)
            self.logger.info("Parquet: %s (%d enreg.)", path, len(data))
        except Exception as e:
            self.logger.error("Erreur Parquet %s: %s", path, e)
            if tmp.exists():
                tmp.unlink()
            raise
        return path

    def sauver_csv(self, data: list[dict[str, Any]], nom: str) -> Path:
        path = self.dossier / nom
        tmp = path.with_suffix(".tmp.csv")
        try:
            if not data:
                path.write_text("", encoding="utf-8")
                return path
            fieldnames = list(data[0].keys())
            with open(tmp, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader()
                for row in data:
                    cr = {}
                    for k, v in row.items():
                        cr[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                    w.writerow(cr)
            tmp.rename(path)
            self.logger.info("CSV: %s (%d enreg.)", path, len(data))
        except Exception as e:
            self.logger.error("Erreur CSV %s: %s", path, e)
            if tmp.exists():
                tmp.unlink()
            raise
        return path


# ===========================================================================
# CHECKPOINT / REPRISE
# ===========================================================================

class CheckpointManager:
    def __init__(self, dossier: Path, logger: logging.Logger) -> None:
        self.fichier = dossier / ".checkpoint_calendrier.json"
        self.logger = logger
        self._data: dict[str, Any] = self._charger()

    def _charger(self) -> dict[str, Any]:
        if self.fichier.exists():
            try:
                with open(self.fichier, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning("Checkpoint corrompu, reset: %s", e)
        return {"jours_traites": {}}

    def _sauver(self) -> None:
        self.fichier.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.fichier.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)
        tmp.replace(self.fichier)

    def est_traite(self, jour: date, sources: list[str]) -> bool:
        key = jour.isoformat()
        if key not in self._data["jours_traites"]:
            return False
        return all(s in self._data["jours_traites"][key] for s in sources)

    def marquer_traite(self, jour: date, source: str) -> None:
        key = jour.isoformat()
        if key not in self._data["jours_traites"]:
            self._data["jours_traites"][key] = []
        if source not in self._data["jours_traites"][key]:
            self._data["jours_traites"][key].append(source)
        self._sauver()

    def reset(self) -> None:
        self._data = {"jours_traites": {}}
        self._sauver()


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

class PipelineCalendrier:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.logger = setup_logging(config)
        self.http = HttpClient(config, self.logger)
        self.sauvegarder = Sauvegarder(config.dossier_sortie, self.logger)
        self.checkpoint = CheckpointManager(config.dossier_sortie, self.logger)
        self.rapport = RapportQualite()
        self._parsers: dict[str, BaseParser] = {}
        self._init_parsers()

    _zero_counts: dict[str, int] = {}  # compteur health-check par source

    def _health_check_zero(self, source: str, jour: date) -> None:
        """Alerte si une source retourne 0 réunions de façon répétée."""
        self._zero_counts[source] = self._zero_counts.get(source, 0) + 1
        consecutive = self._zero_counts[source]
        if consecutive >= 5:
            self.logger.warning(
                "HEALTH-CHECK: [%s] 0 réunions pour %d jours consécutifs (dernier: %s) — endpoint peut-être mort ?",
                source, consecutive, jour.isoformat(),
            )
        # Reset si on a eu des réunions
        # (appelé seulement quand len==0, le reset est fait après)

    def _health_check_reset(self, source: str) -> None:
        """Reset le compteur quand une source retourne des données."""
        self._zero_counts[source] = 0

    def _init_parsers(self) -> None:
        for code, src_cfg in self.config.sources.items():
            if not src_cfg.active:
                self.logger.info("Source '%s' désactivée", code)
                continue
            parser_cls = PARSER_REGISTRY.get(code)
            if parser_cls is None:
                self.logger.warning("Pas de parser pour '%s'", code)
                continue
            self._parsers[code] = parser_cls(self.http, src_cfg, self.logger)
            self.logger.info("Parser initialisé: %s (%s, priorité=%d)",
                             src_cfg.nom_source, src_cfg.type_source.value, src_cfg.priorite)

    def executer(self) -> RapportQualite:
        self.rapport.timestamp_debut = utc_now_iso()
        self.rapport.date_debut = self.config.date_debut.isoformat()
        self.rapport.date_fin = self.config.date_fin.isoformat()
        self.rapport.sources_actives = list(self._parsers.keys())
        self.rapport.champs_disponibles_par_source = {
            code: list(self.config.sources[code].champs_disponibles)
            for code in self._parsers
        }
        t_start = time.monotonic()

        self.logger.info("=" * 70)
        self.logger.info("DÉBUT PIPELINE CALENDRIER RÉUNIONS")
        self.logger.info(
            "Plage: %s -> %s | Sources: %s | Mode reprise: %s",
            self.config.date_debut, self.config.date_fin,
            ", ".join(self.rapport.sources_actives), self.config.mode_reprise,
        )
        self.logger.info("=" * 70)

        toutes_brutes: list[ReunionBrute] = []
        jours = self._generer_plage_dates()

        # En mode reprise, recharger les données existantes pour ne pas les écraser
        if self.config.mode_reprise:
            toutes_brutes = self._charger_brutes_existantes()
            self.logger.info(
                "Mode reprise: %d brutes pré-chargées, reprise de la collecte...",
                len(toutes_brutes),
            )
            # Dedup inter-runs: index des (date, source, hippodrome) déjà collectées
            existing_keys: set[str] = set()
            for b in toutes_brutes:
                key = f"{b.date_reunion_brut}|{b.source}|{normaliser_hippodrome(b.hippodrome_brut)}"
                existing_keys.add(key)
            self.logger.info("Index dedup inter-runs: %d clés uniques", len(existing_keys))
        else:
            existing_keys = set()

        for i, jour in enumerate(jours):
            sources_actives = list(self._parsers.keys())

            if self.config.mode_reprise and self.checkpoint.est_traite(jour, sources_actives):
                self.logger.debug("Jour %s skip (reprise)", jour.isoformat())
                self.rapport.total_jours_ignores_reprise += 1
                continue

            self.rapport.total_jours_traites += 1
            self.logger.info("--- %s (%d/%d) ---", jour.isoformat(), i + 1, len(jours))

            for src_idx, (code, parser) in enumerate(self._parsers.items()):
                try:
                    reunions = parser.fetch_reunions(jour)
                    # Dedup inter-runs: filtrer les réunions déjà collectées
                    if existing_keys:
                        nouvelles = []
                        for r in reunions:
                            key = f"{r.date_reunion_brut}|{r.source}|{normaliser_hippodrome(r.hippodrome_brut)}"
                            if key not in existing_keys:
                                nouvelles.append(r)
                                existing_keys.add(key)
                        if len(nouvelles) < len(reunions):
                            self.logger.debug(
                                "[%s] %s: %d/%d nouvelles (dedup inter-runs)",
                                code, jour, len(nouvelles), len(reunions),
                            )
                        reunions = nouvelles
                    toutes_brutes.extend(reunions)
                    self.rapport.reunions_par_source[code] = (
                        self.rapport.reunions_par_source.get(code, 0) + len(reunions)
                    )
                    self.checkpoint.marquer_traite(jour, code)
                    # Health-check: alerte si 0 réunions un jour de semaine
                    if len(reunions) == 0 and jour.weekday() < 5:
                        self._health_check_zero(code, jour)
                    elif len(reunions) > 0:
                        self._health_check_reset(code)
                except requests.exceptions.RequestException as e:
                    self.rapport.erreurs_http += 1
                    self.rapport.erreurs_par_source[code] = (
                        self.rapport.erreurs_par_source.get(code, 0) + 1
                    )
                    self.logger.error("[%s] Erreur HTTP %s: %s", code, jour, e)
                except Exception as e:
                    self.rapport.erreurs_parsing += 1
                    self.rapport.erreurs_par_source[code] = (
                        self.rapport.erreurs_par_source.get(code, 0) + 1
                    )
                    self.logger.error("[%s] Erreur %s: %s", code, jour, e)
                # Rate-limiting inter-source
                if src_idx < len(self._parsers) - 1:
                    time.sleep(self.config.pause_inter_source)

            # Sauvegarde intermédiaire tous les 200 jours traités
            if self.rapport.total_jours_traites > 0 and self.rapport.total_jours_traites % 200 == 0:
                self._sauver_intermediaire(toutes_brutes)

            if i < len(jours) - 1:
                time.sleep(self.config.pause_inter_jour)

        # --- Post-traitement ---
        self.rapport.reunions_brutes_trouvees = len(toutes_brutes)
        self.logger.info("Brutes collectées: %d", len(toutes_brutes))

        toutes_brutes = deduplication_intra_source(toutes_brutes, self.logger)

        valides, invalides = self._valider(toutes_brutes)
        self.rapport.reunions_valides = len(valides)
        self.rapport.reunions_invalides = len(invalides)

        # Sauvegarde brutes
        brutes_dicts = [asdict(r) for r in valides]
        self._sauver_multi(brutes_dicts, "reunions_brut")

        # Normalisation
        normalisees = [normaliser_reunion(r, self.config.sources) for r in valides]

        # Fusion inter-sources
        nb_avant = len(normalisees)
        normalisees = fusion_inter_sources(normalisees, self.config.sources, self.logger)
        self.rapport.reunions_doublons = nb_avant - len(normalisees)
        self.rapport.reunions_fusionnees = sum(1 for r in normalisees if r.indicateur_reunion_fusionnee)
        self.rapport.conflits_detectes = sum(len(r.champs_en_conflit) for r in normalisees)
        self.rapport.reunions_donnees_incompletes = sum(
            1 for r in normalisees if r.indicateur_donnee_incomplete
        )
        self.rapport.reunions_normalisees_sauvees = len(normalisees)

        # Sauvegarde normalisées
        norm_dicts = [asdict(r) for r in normalisees]
        self._sauver_multi(norm_dicts, "reunions_normalisees")

        # Table de références pour 02_liste_courses.py
        refs = build_reunion_references(normalisees, valides)
        self._sauver_multi(refs, "reunions_references_02")

        # Cartographie des variables
        self.sauvegarder.sauver_json(
            [{"source": code, "mapping": mapping} for code, mapping in SOURCE_FIELD_MAPPINGS.items()],
            "cartographie_variables.json",
        )

        # Rapport
        self.rapport.duree_totale_secondes = round(time.monotonic() - t_start, 2)
        self.rapport.timestamp_fin = utc_now_iso()
        self.sauvegarder.sauver_json([asdict(self.rapport)], "rapport_qualite_reunions.json")
        self._generer_docs()
        self._afficher_rapport()

        self.logger.info("=" * 70)
        self.logger.info("FIN PIPELINE CALENDRIER RÉUNIONS")
        self.logger.info("=" * 70)
        return self.rapport

    def _sauver_intermediaire(self, toutes_brutes: list[ReunionBrute]) -> None:
        """Sauvegarde intermédiaire pour permettre la consultation en cours de collecte."""
        self.logger.info(
            ">>> Sauvegarde intermédiaire: %d brutes après %d jours <<<",
            len(toutes_brutes), self.rapport.total_jours_traites,
        )
        try:
            brutes_dicts = [asdict(r) for r in toutes_brutes]
            self._sauver_multi(brutes_dicts, "reunions_brut")

            normalisees = [normaliser_reunion(r, self.config.sources) for r in toutes_brutes]
            normalisees = fusion_inter_sources(normalisees, self.config.sources, self.logger)
            norm_dicts = [asdict(r) for r in normalisees]
            self._sauver_multi(norm_dicts, "reunions_normalisees")

            refs = build_reunion_references(normalisees, toutes_brutes)
            self._sauver_multi(refs, "reunions_references_02")

            self._generer_docs()

            self.logger.info(
                ">>> Intermédiaire OK: %d brutes, %d normalisées, %d références <<<",
                len(brutes_dicts), len(norm_dicts), len(refs),
            )
        except Exception as e:
            self.logger.error("Erreur sauvegarde intermédiaire: %s", e)

    def _generer_docs(self) -> None:
        """Génère automatiquement tous les fichiers de documentation dans docs/."""
        docs_dir = self.config.dossier_sortie.parent.parent / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        sauv = Sauvegarder(docs_dir, self.logger)

        # 1. Schema entrées
        sauv.sauver_json([{
            "script": "01_calendrier_reunions.py",
            "parametres_cli": {
                "--date-debut": {"type": "str (YYYY-MM-DD)", "defaut": "date du jour", "description": "Date de début"},
                "--date-fin": {"type": "str (YYYY-MM-DD)", "defaut": "date du jour", "description": "Date de fin"},
                "--sources": {"type": "str", "defaut": "pmu,letrot,geny", "description": "Sources séparées par virgules"},
                "--output": {"type": "str", "defaut": "output/01_calendrier_reunions", "description": "Dossier de sortie"},
                "--config": {"type": "str", "defaut": None, "description": "Fichier config YAML"},
                "--timeout": {"type": "int", "defaut": 30, "description": "Timeout HTTP (secondes)"},
                "--retry": {"type": "int", "defaut": 3, "description": "Retry max HTTP"},
                "--backoff": {"type": "float", "defaut": 1.0, "description": "Backoff factor"},
                "--pause": {"type": "float", "defaut": 0.5, "description": "Pause inter-jour (secondes)"},
                "--pause-source": {"type": "float", "defaut": 0.3, "description": "Pause inter-source (secondes)"},
                "--log-level": {"type": "str", "defaut": "INFO", "valeurs": ["DEBUG", "INFO", "WARNING", "ERROR"]},
                "--no-reprise": {"type": "flag", "description": "Désactive le mode reprise"},
                "--reset-checkpoint": {"type": "flag", "description": "Réinitialise le checkpoint"},
                "--no-csv": {"type": "flag", "description": "Désactive l'export CSV"},
                "--no-parquet": {"type": "flag", "description": "Désactive l'export Parquet"},
            },
        }], "01_SCHEMA_ENTREES.json")

        # 2. Schema sorties
        sauv.sauver_json([{
            "script": "01_calendrier_reunions.py",
            "dossier_sortie": str(self.config.dossier_sortie),
            "fichiers": {
                "reunions_brut.json/.parquet/.csv": {
                    "description": "Réunions brutes collectées (après dédup + validation)",
                    "champs": list(ReunionBrute.__dataclass_fields__.keys()),
                },
                "reunions_normalisees.json/.parquet/.csv": {
                    "description": "Réunions normalisées, fusionnées, avec features et provenance",
                    "champs": list(ReunionNormalisee.__dataclass_fields__.keys()),
                },
                "reunions_references_02.json/.parquet/.csv": {
                    "description": "Table de référence pour 02_liste_courses.py",
                    "champs": ["reunion_uid", "date_reunion_iso", "hippodrome_normalise", "hippodrome",
                               "discipline_normalisee", "numero_reunion", "pays", "nombre_courses_reunion",
                               "sources", "url_pmu", "url_letrot", "url_geny", "id_pmu", "id_letrot", "id_geny"],
                },
                "cartographie_variables.json": "Mapping champs source -> champs bruts internes",
                "rapport_qualite_reunions.json": "Rapport d'exécution avec métriques",
                ".checkpoint_calendrier.json": "État de reprise (jours traités par source)",
            },
            "formats": {"json": "toujours", "parquet": "si pyarrow", "csv": "par défaut"},
            "logs": {"console": "stdout", "fichier": "logs/01_calendrier_reunions_YYYY-MM-DD.log"},
        }], "02_SCHEMA_SORTIES.json")

        # 3. Endpoints
        endpoints = {}
        for code, cfg in self.config.sources.items():
            endpoints[code] = {
                "nom_source": cfg.nom_source,
                "type_source": cfg.type_source.value,
                "priorite": cfg.priorite,
                "url_base": cfg.url_base,
                "url_calendrier_jour_pattern": cfg.url_calendrier_jour_pattern,
                "methode_http": cfg.methode_http,
                "strategie_parsing": cfg.strategie_parsing.value,
                "timeout": cfg.timeout,
                "retry_max": cfg.retry_max,
                "retry_backoff": cfg.retry_backoff,
                "active": cfg.active,
                "champs_disponibles": list(cfg.champs_disponibles),
            }
        sauv.sauver_json([{"script": "01_calendrier_reunions.py", "sources": endpoints}], "03_ENDPOINTS.json")

        # 4. Variables
        sauv.sauver_json([{
            "script": "01_calendrier_reunions.py",
            "variables_brutes": {
                name: {"type": str(f.type), "default": str(f.default) if f.default is not f.default_factory else ""}
                for name, f in ReunionBrute.__dataclass_fields__.items()
            },
            "variables_normalisees": {
                name: {"type": str(f.type)}
                for name, f in ReunionNormalisee.__dataclass_fields__.items()
            },
            "mapping_source_vers_brut": SOURCE_FIELD_MAPPINGS,
            "features_derivees": [
                "reunion_uid", "cle_jour_hippodrome_numero", "source_prioritaire",
                "nb_sources_match", "est_duplique_inter_source", "indicateur_donnee_incomplete",
                "indicateur_reunion_fusionnee", "date_collecte",
            ],
            "provenance": [
                "sources_multiples", "source_origine_principale", "sources_secondaires",
                "champs_confirmes_par_plusieurs_sources", "champs_en_conflit",
            ],
            "champs_compares_pour_conflits": CHAMPS_COMPARABLES,
        }], "04_VARIABLES.json")

        # 5. Flux logique
        sauv.sauver_json([{
            "script": "01_calendrier_reunions.py",
            "etapes": [
                {"ordre": 1, "nom": "CLI / Configuration", "fonctions": ["parse_args()", "build_config()"]},
                {"ordre": 2, "nom": "Initialisation Pipeline", "fonctions": ["PipelineCalendrier.__init__()", "_init_parsers()"]},
                {"ordre": 3, "nom": "Génération plage de dates", "fonctions": ["_generer_plage_dates()"]},
                {"ordre": 4, "nom": "Boucle jour par jour", "sous_etapes": [
                    "Vérification checkpoint", "Collecte par source", "Gestion erreurs non bloquante",
                    "Marquage checkpoint", "Sauvegarde intermédiaire (tous les 200j)", "Pause inter-jour",
                ]},
                {"ordre": 5, "nom": "Déduplication intra-source", "fonctions": ["deduplication_intra_source()"]},
                {"ordre": 6, "nom": "Validation", "fonctions": ["valider_reunion_brute()"]},
                {"ordre": 7, "nom": "Sauvegarde brutes", "sortie": "reunions_brut.*"},
                {"ordre": 8, "nom": "Normalisation", "fonctions": ["normaliser_reunion()", "normaliser_hippodrome()", "normaliser_discipline()"]},
                {"ordre": 9, "nom": "Fusion inter-sources", "fonctions": ["fusion_inter_sources()"]},
                {"ordre": 10, "nom": "Sauvegarde normalisées", "sortie": "reunions_normalisees.*"},
                {"ordre": 11, "nom": "Références pour 02", "fonctions": ["build_reunion_references()"]},
                {"ordre": 12, "nom": "Cartographie variables", "sortie": "cartographie_variables.json"},
                {"ordre": 13, "nom": "Rapport qualité + docs", "sortie": "rapport_qualite_reunions.json + docs/"},
            ],
        }], "05_FLUX_LOGIQUE.json")

        # 6. Fonctions principales
        sauv.sauver_json([{
            "script": "01_calendrier_reunions.py",
            "classes": list(PARSER_REGISTRY.keys()) + [
                "SourceEndpointConfig", "PipelineConfig", "ReunionBrute", "ReunionNormalisee",
                "ReunionReference", "RapportQualite", "HttpClient", "BaseParser",
                "Sauvegarder", "CheckpointManager", "PipelineCalendrier",
            ],
            "parsers": {code: cls.__name__ for code, cls in PARSER_REGISTRY.items()},
            "fonctions_normalisation": ["normaliser_texte", "normaliser_hippodrome", "normaliser_discipline", "normaliser_statut"],
            "fonctions_uid": ["generer_reunion_uid", "generer_reunion_uid_cross_source", "generer_cle_jour_hippo_numero"],
            "fonctions_pipeline": ["deduplication_intra_source", "normaliser_reunion", "fusion_inter_sources", "build_reunion_references"],
            "enums": {
                "Discipline": [d.value for d in Discipline],
                "StatutReunion": [s.value for s in StatutReunion],
                "TypeSource": [t.value for t in TypeSource],
                "StrategieParsing": [sp.value for sp in StrategieParsing],
            },
        }], "06_FONCTIONS_PRINCIPALES.json")

        # 7. Edge cases
        sauv.sauver_json([{
            "script": "01_calendrier_reunions.py",
            "geres": [
                "Jour sans réunion → liste vide, continue",
                "HTTP 404/500/502/503/504 → retry backoff, non bloquant",
                "Page vide ou format inattendu → None, log warning",
                "Doublons intra-source → clé source|date|hippo_norm|numero",
                "Doublons inter-sources → fusion sha256(date|hippo_norm)",
                "Conflits de valeurs → tracés dans champs_en_conflit",
                "Noms hippodrome différents → normaliser_hippodrome()",
                "Disciplines différentes → DISCIPLINE_ALIASES + enrichissement",
                "Réunion annulée/reportée → normaliser_statut()",
                "Réunion à l'étranger → pays extrait Geny/PMU",
                "Interruption script → checkpoint atomique",
                "Checkpoint corrompu → reset auto",
                "Le Trot Vue absent → 3 attributs + fallback regex",
                "Geny HTML différent → fallback _parse_course_links()",
                "Rate limiting → pause + backoff + 429",
                "pyarrow absent → Parquet ignoré",
                "BeautifulSoup absent → parsers HTML liste vide",
                "Champs manquants → indicateur_donnee_incomplete + enrichissement",
            ],
            "a_surveiller": [
                "Changement structure HTML sources",
                "Hippodromes noms très similaires (Lyon-Parilly vs Lyon La Soie)",
                "Volume mémoire 10 ans (~75K brutes, ~300Mo RAM)",
                "Encodage caractères spéciaux",
            ],
            "dependances": {
                "obligatoires": ["requests>=2.31.0", "beautifulsoup4>=4.12.0"],
                "optionnelles": ["pyarrow>=14.0.0", "pyyaml>=6.0.1"],
            },
        }], "07_EDGE_CASES.json")

        self.logger.info("Documentation générée dans %s", docs_dir)

    def _generer_plage_dates(self) -> list[date]:
        jours: list[date] = []
        j = self.config.date_debut
        while j <= self.config.date_fin:
            jours.append(j)
            j += timedelta(days=1)
        return jours

    def _valider(
        self, reunions: list[ReunionBrute],
    ) -> tuple[list[ReunionBrute], list[tuple[ReunionBrute, list[ValidationError]]]]:
        valides: list[ReunionBrute] = []
        invalides: list[tuple[ReunionBrute, list[ValidationError]]] = []
        for r in reunions:
            errs = valider_reunion_brute(r)
            if errs:
                invalides.append((r, errs))
                self.logger.warning("Invalide [%s %s %s]: %s",
                                    r.source, r.date_reunion_brut, r.hippodrome_brut,
                                    "; ".join(str(e) for e in errs))
            else:
                valides.append(r)
        return valides, invalides

    def _charger_brutes_existantes(self) -> list[ReunionBrute]:
        """Recharge les données brutes existantes en mode reprise pour éviter de les perdre."""
        fichier_json = self.config.dossier_sortie / "reunions_brut.json"
        if not fichier_json.exists():
            self.logger.info("Aucun fichier brut existant à recharger")
            return []
        try:
            with open(fichier_json, "r", encoding="utf-8") as f:
                data = json.load(f)
            brutes = []
            for d in data:
                # Nettoyer les champs extras si nécessaire
                if "extras" not in d:
                    d["extras"] = {}
                if isinstance(d.get("extras"), str):
                    d["extras"] = {}
                brutes.append(ReunionBrute(**{
                    k: v for k, v in d.items()
                    if k in ReunionBrute.__dataclass_fields__
                }))
            self.logger.info(
                "Rechargé %d brutes existantes depuis %s",
                len(brutes), fichier_json.name,
            )
            return brutes
        except Exception as e:
            self.logger.error("Impossible de recharger les brutes existantes: %s", e)
            return []

    def _sauver_multi(self, data: list[dict[str, Any]], prefixe: str) -> None:
        if self.config.export_json:
            self.sauvegarder.sauver_json(data, f"{prefixe}.json")
        if self.config.export_parquet:
            self.sauvegarder.sauver_parquet(data, f"{prefixe}.parquet")
        if self.config.export_csv:
            self.sauvegarder.sauver_csv(data, f"{prefixe}.csv")

    def _afficher_rapport(self) -> None:
        r = self.rapport
        self.logger.info("-" * 50)
        self.logger.info("RAPPORT D'EXÉCUTION")
        self.logger.info("-" * 50)
        self.logger.info("Plage              : %s -> %s", r.date_debut, r.date_fin)
        self.logger.info("Sources            : %s", ", ".join(r.sources_actives))
        self.logger.info("Jours traités      : %d", r.total_jours_traites)
        self.logger.info("Jours skip reprise : %d", r.total_jours_ignores_reprise)
        self.logger.info("Brutes collectées  : %d", r.reunions_brutes_trouvees)
        self.logger.info("Valides            : %d", r.reunions_valides)
        self.logger.info("Invalides          : %d", r.reunions_invalides)
        self.logger.info("Doublons fusionnés : %d", r.reunions_doublons)
        self.logger.info("Réunions fusionnées: %d", r.reunions_fusionnees)
        self.logger.info("Conflits détectés  : %d", r.conflits_detectes)
        self.logger.info("Données incomplètes: %d", r.reunions_donnees_incompletes)
        self.logger.info("Sauvées            : %d", r.reunions_normalisees_sauvees)
        self.logger.info("Erreurs HTTP       : %d", r.erreurs_http)
        self.logger.info("Erreurs parsing    : %d", r.erreurs_parsing)
        for s, c in r.reunions_par_source.items():
            self.logger.info("  [%s] réunions  : %d", s, c)
        for s, c in r.erreurs_par_source.items():
            self.logger.info("  [%s] erreurs   : %d", s, c)
        self.logger.info("Durée              : %.2fs", r.duree_totale_secondes)
        self.logger.info("-" * 50)


# ===========================================================================
# CLI
# ===========================================================================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Collecte multi-sources du calendrier des réunions hippiques.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--date-debut", type=str, default=None)
    p.add_argument("--date-fin", type=str, default=None)
    p.add_argument("--config", type=str, default=None)
    p.add_argument("--output", type=str, default="output/01_calendrier_reunions")
    p.add_argument("--sources", type=str, default=None,
                    help="Sources séparées par virgules (ex: pmu,letrot,geny)")
    p.add_argument("--no-reprise", action="store_true")
    p.add_argument("--reset-checkpoint", action="store_true")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--retry", type=int, default=3)
    p.add_argument("--backoff", type=float, default=1.0)
    p.add_argument("--pause", type=float, default=0.5,
                    help="Pause en secondes entre les jours (défaut: 0.5)")
    p.add_argument("--pause-source", type=float, default=0.3,
                    help="Pause en secondes entre les sources (défaut: 0.3)")
    p.add_argument("--log-level", type=str, default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--no-csv", action="store_true")
    p.add_argument("--no-parquet", action="store_true")
    return p.parse_args(argv)


def build_config(args: argparse.Namespace) -> PipelineConfig:
    config = PipelineConfig.from_yaml(Path(args.config)) if args.config else PipelineConfig()

    if args.date_debut:
        config.date_debut = date.fromisoformat(args.date_debut)
    if args.date_fin:
        config.date_fin = date.fromisoformat(args.date_fin)
    elif args.date_debut and not args.date_fin:
        config.date_fin = config.date_debut

    config.dossier_sortie = Path(args.output)
    config.mode_reprise = not args.no_reprise
    config.http_timeout = args.timeout
    config.http_retry_max = args.retry
    config.http_retry_backoff = args.backoff
    config.pause_inter_jour = args.pause
    config.pause_inter_source = args.pause_source
    config.log_level = args.log_level
    config.export_csv = not args.no_csv
    config.export_parquet = not args.no_parquet

    if args.sources:
        actives = {s.strip().lower() for s in args.sources.split(",")}
        new_sources = {}
        for code, cfg in config.sources.items():
            new_sources[code] = SourceEndpointConfig(
                nom_source=cfg.nom_source,
                code_source=cfg.code_source,
                type_source=cfg.type_source,
                priorite=cfg.priorite,
                url_base=cfg.url_base,
                url_calendrier_jour_pattern=cfg.url_calendrier_jour_pattern,
                methode_http=cfg.methode_http,
                params_template=cfg.params_template,
                headers=cfg.headers,
                strategie_parsing=cfg.strategie_parsing,
                timeout=args.timeout,
                retry_max=args.retry,
                retry_backoff=args.backoff,
                active=code in actives,
                champs_disponibles=cfg.champs_disponibles,
            )
        config.sources = new_sources

    return config


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    config = build_config(args)

    if config.date_debut > config.date_fin:
        print(f"ERREUR: date_debut ({config.date_debut}) > date_fin ({config.date_fin})",
              file=sys.stderr)
        return 1

    pipeline = PipelineCalendrier(config)

    if args.reset_checkpoint:
        pipeline.checkpoint.reset()
        pipeline.logger.info("Checkpoint réinitialisé")

    try:
        rapport = pipeline.executer()
        return 0 if rapport.erreurs_http + rapport.erreurs_parsing == 0 else 2
    except KeyboardInterrupt:
        pipeline.logger.warning("Interruption clavier")
        return 130
    except Exception as e:
        pipeline.logger.critical("Erreur fatale: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
