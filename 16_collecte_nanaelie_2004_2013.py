#!/usr/bin/env python3
"""
16_collecte_nanaelie_2004_2013.py
=================================
Collecte des resultats de courses PMU via l'API gratuite Open PMU (nanaelie)
pour la periode 22/01/2004 - 18/02/2013 (avant le debut des donnees turfinfo).

Source :
  - Open PMU API : https://open-pmu-api.vercel.app/api/arrivees?date=DD/MM/YYYY
  - Pas d'authentification requise
  - Donnees disponibles depuis le 22/01/2004

Produit :
  - output/16_nanaelie/cache/{YYYY-MM-DD}.json  -- cache brut par jour
  - output/16_nanaelie/nanaelie_2004_2013.json
  - output/16_nanaelie/nanaelie_2004_2013.parquet
  - output/16_nanaelie/nanaelie_2004_2013.csv
  - .checkpoint_16.json

Usage :
    python3 16_collecte_nanaelie_2004_2013.py
    python3 16_collecte_nanaelie_2004_2013.py --pause 1.0
    python3 16_collecte_nanaelie_2004_2013.py --date-debut 2010-01-01 --date-fin 2010-12-31
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from utils.normalize import strip_accents
from utils.types import safe_int as _safe_int

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "16_nanaelie"
CACHE_DIR = OUTPUT_DIR / "cache"
CHECKPOINT_PATH = OUTPUT_DIR / ".checkpoint_16.json"

API_URL = "https://open-pmu-api.vercel.app/api/arrivees"

# Plage par defaut : 22/01/2004 -> 18/02/2013
DEFAULT_START = date(2004, 1, 22)
DEFAULT_END = date(2013, 2, 18)

DEFAULT_PAUSE = 0.5  # secondes entre chaque requete


# ===========================================================================
# DATACLASS
# ===========================================================================

@dataclass
class ResultatNanaelie:
    date_iso: str              # YYYY-MM-DD (normalise depuis DD/MM/YYYY)
    reunion_course: str        # "R1C1" etc.
    numero_reunion: int        # extrait de r/c
    numero_course: int         # extrait de r/c
    hippodrome: str            # champ lieu
    hippodrome_normalise: str  # minuscules, sans accents
    nom_prix: str              # champ prix
    discipline: str            # champ type : plat, attele, monte, etc.
    distance: int              # metres
    allocation: int            # montant
    nb_partants: int           # partants (peut etre str ou int)
    non_partants: list         # liste des numeros des non-partants
    arrivee_top5: list         # [1er, 2e, 3e, 4e, 5e] par numero
    conditions: str            # champ details
    source: str                # "nanaelie"
    cle_course: str            # "{date_iso}|{hippodrome_normalise}|R{n}|C{n}"


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# HTTP
# ===========================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "Mozilla/5.0 (nanaelie-collector)"})
    return session


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normaliser_hippodrome(lieu: str) -> str:
    """Normalise le nom d'hippodrome : minuscules, sans accents, espaces nettoyes."""
    if not lieu:
        return ""
    s = strip_accents(lieu.strip()).lower()
    # Remplacer les tirets et multi-espaces par un seul espace
    s = re.sub(r"[\s\-]+", " ", s)
    return s.strip()


def safe_int(val: Any, default: int = 0) -> int:
    """Convertit une valeur (str ou int) en int de maniere sure (default=0)."""
    return _safe_int(val, default)


def parse_rc(rc_str: str) -> tuple[int, int, str]:
    """
    Parse le champ r/c (ex: 'R1/C3', '1/3', 'R1C3') en (num_reunion, num_course, label).
    Retourne (0, 0, '') si le parsing echoue.
    """
    if not rc_str:
        return 0, 0, ""
    rc_str = rc_str.strip().upper()
    # Format "R1C3" ou "R1/C3"
    m = re.match(r"R?(\d+)\s*[/C]\s*C?(\d+)", rc_str)
    if m:
        nr, nc = int(m.group(1)), int(m.group(2))
        return nr, nc, f"R{nr}C{nc}"
    # Fallback : juste des chiffres separes par /
    m = re.match(r"(\d+)\s*/\s*(\d+)", rc_str)
    if m:
        nr, nc = int(m.group(1)), int(m.group(2))
        return nr, nc, f"R{nr}C{nc}"
    return 0, 0, rc_str


def parse_arrivee(arrivee_raw: Any) -> list:
    """
    Parse le champ arrivee (top 5).
    Peut etre une liste de numeros, une chaine separee par '-' ou ' ', etc.
    """
    if not arrivee_raw:
        return []
    if isinstance(arrivee_raw, list):
        return [safe_int(x) for x in arrivee_raw[:5]]
    if isinstance(arrivee_raw, str):
        # Nettoyer et separer
        parts = re.split(r"[\s\-/,]+", arrivee_raw.strip())
        nums = []
        for p in parts:
            n = safe_int(p, default=-1)
            if n > 0:
                nums.append(n)
        return nums[:5]
    return []


def parse_non_partants(np_raw: Any) -> list:
    """Parse le champ non_partants en liste de numeros."""
    if not np_raw:
        return []
    if isinstance(np_raw, list):
        return [safe_int(x) for x in np_raw if safe_int(x, -1) > 0]
    if isinstance(np_raw, str):
        parts = re.split(r"[\s\-/,]+", np_raw.strip())
        return [safe_int(p) for p in parts if safe_int(p, -1) > 0]
    return []


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
        return {"completed_days": []}

    def is_done(self, day_key: str) -> bool:
        return day_key in self._data.get("completed_days", [])

    def mark_done(self, day_key: str):
        self._data.setdefault("completed_days", []).append(day_key)

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False))
        tmp.replace(self.path)

    @property
    def count_done(self) -> int:
        return len(self._data.get("completed_days", []))


# ===========================================================================
# FETCH
# ===========================================================================

def fetch_day(
    session: requests.Session,
    dt: date,
    logger: logging.Logger,
) -> Optional[list]:
    """
    Recupere les resultats d'un jour via l'API nanaelie.
    Retourne la liste des courses (JSON parse) ou None en cas d'erreur.
    """
    date_str = dt.strftime("%d/%m/%Y")
    url = f"{API_URL}?date={date_str}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        # L'API retourne {"error": false, "message": [...courses...]}
        # ou directement une liste
        if isinstance(data, dict):
            if data.get("error"):
                return None
            msg = data.get("message")
            if isinstance(msg, list) and len(msg) > 0:
                return msg
            return None
        if isinstance(data, list) and len(data) > 0:
            # Peut être une liste de wrappers
            first = data[0]
            if isinstance(first, dict) and "message" in first:
                msg = first.get("message")
                return msg if isinstance(msg, list) else None
            return data
        return None
    except requests.exceptions.RequestException as e:
        logger.warning("HTTP error %s: %s", date_str, str(e)[:120])
        return None
    except json.JSONDecodeError as e:
        logger.warning("JSON error %s: %s", date_str, str(e)[:80])
        return None


# ===========================================================================
# PARSING COURSES
# ===========================================================================

def parse_courses_jour(
    day_data: list,
    date_iso: str,
) -> list[ResultatNanaelie]:
    """Parse toutes les courses d'un jour en ResultatNanaelie."""
    resultats = []
    for race in day_data:
        # Extraire les champs bruts
        lieu = race.get("lieu", "") or ""
        hippo_norm = normaliser_hippodrome(lieu)
        rc_raw = race.get("r/c", "") or race.get("rc", "") or ""
        num_reunion, num_course, rc_label = parse_rc(rc_raw)

        discipline = (race.get("type", "") or "").strip().lower()
        prix = race.get("prix", "") or ""
        distance = safe_int(race.get("distance", 0))
        montant = safe_int(race.get("montant", 0))
        partants = safe_int(race.get("partants", 0))
        np_raw = race.get("non_partants")
        arrivee_raw = race.get("arrivee")
        details = race.get("details", "") or ""

        # Date : utiliser la date de la requete, sauf si le champ date est present
        date_course = date_iso
        date_field = race.get("date", "")
        if date_field and "/" in str(date_field):
            try:
                dt_parsed = datetime.strptime(str(date_field).strip(), "%d/%m/%Y")
                date_course = dt_parsed.strftime("%Y-%m-%d")
            except ValueError:
                pass

        cle = f"{date_course}|{hippo_norm}|R{num_reunion}|C{num_course}"

        r = ResultatNanaelie(
            date_iso=date_course,
            reunion_course=rc_label,
            numero_reunion=num_reunion,
            numero_course=num_course,
            hippodrome=lieu.strip(),
            hippodrome_normalise=hippo_norm,
            nom_prix=prix.strip(),
            discipline=discipline,
            distance=distance,
            allocation=montant,
            nb_partants=partants,
            non_partants=parse_non_partants(np_raw),
            arrivee_top5=parse_arrivee(arrivee_raw),
            conditions=details.strip(),
            source="nanaelie",
            cle_course=cle,
        )
        resultats.append(r)

    return resultats


# ===========================================================================
# CACHE
# ===========================================================================

def load_cache(dt: date) -> Optional[list]:
    """Charge le cache JSON d'un jour s'il existe. Gère le format wrapper."""
    cache_file = CACHE_DIR / f"{dt:%Y-%m-%d}.json"
    if cache_file.exists() and cache_file.stat().st_size > 10:
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            # Extraire les courses du wrapper si nécessaire
            if isinstance(data, list) and len(data) > 0:
                first = data[0]
                if isinstance(first, dict) and "message" in first:
                    msg = first.get("message")
                    return msg if isinstance(msg, list) else None
                # Déjà une liste de courses
                if isinstance(first, dict) and ("lieu" in first or "prix" in first):
                    return data
            if isinstance(data, dict) and "message" in data:
                msg = data.get("message")
                return msg if isinstance(msg, list) else None
            return data
        except (json.JSONDecodeError, OSError):
            pass
    return None


def save_cache(dt: date, data: list):
    """Sauvegarde les donnees brutes d'un jour dans le cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{dt:%Y-%m-%d}.json"
    tmp = cache_file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(cache_file)


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
        if not HAS_PARQUET:
            logger.info("pyarrow non installe, export parquet ignore")
        return
    try:
        # Convertir les listes en chaines pour compatibilite parquet
        data_flat = []
        for row in data:
            r = dict(row)
            if isinstance(r.get("non_partants"), list):
                r["non_partants"] = json.dumps(r["non_partants"])
            if isinstance(r.get("arrivee_top5"), list):
                r["arrivee_top5"] = json.dumps(r["arrivee_top5"])
            data_flat.append(r)
        table = pa.Table.from_pylist(data_flat)
        pq.write_table(table, path)
        logger.info("Sauve: %s", path.name)
    except Exception as e:
        logger.warning("Parquet ignore: %s", e)


def sauver_csv(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # Convertir les listes en chaines
    data_flat = []
    for row in data:
        r = dict(row)
        if isinstance(r.get("non_partants"), list):
            r["non_partants"] = json.dumps(r["non_partants"])
        if isinstance(r.get("arrivee_top5"), list):
            r["arrivee_top5"] = json.dumps(r["arrivee_top5"])
        data_flat.append(r)

    fieldnames = list(data_flat[0].keys())
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data_flat)
    tmp.replace(path)
    logger.info("Sauve: %s (%d entrees)", path.name, len(data_flat))


def sauver_jsonl(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    tmp.replace(path)
    logger.info("Sauve: %s (%d entrees)", path.name, len(data))


# ===========================================================================
# MAIN
# ===========================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collecte des resultats nanaelie (Open PMU API) 2004-2013",
    )
    parser.add_argument(
        "--date-debut",
        default=DEFAULT_START.isoformat(),
        help=f"Date de debut YYYY-MM-DD (defaut: {DEFAULT_START})",
    )
    parser.add_argument(
        "--date-fin",
        default=DEFAULT_END.isoformat(),
        help=f"Date de fin YYYY-MM-DD (defaut: {DEFAULT_END})",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=DEFAULT_PAUSE,
        help=f"Pause entre requetes en secondes (defaut: {DEFAULT_PAUSE})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignorer le checkpoint et retelecharger tout",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("16_nanaelie")

    start_dt = date.fromisoformat(args.date_debut)
    end_dt = date.fromisoformat(args.date_fin)
    pause = args.pause

    total_days = (end_dt - start_dt).days + 1

    logger.info("=" * 60)
    logger.info("Collecte nanaelie : %s -> %s (%d jours)", start_dt, end_dt, total_days)
    logger.info("Pause entre requetes : %.2fs", pause)
    logger.info("Sortie : %s", OUTPUT_DIR.resolve())
    logger.info("=" * 60)

    # Creer les dossiers
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Checkpoint
    ckpt = CheckpointManager(CHECKPOINT_PATH)
    if not args.force:
        logger.info("Checkpoint : %d jours deja traites", ckpt.count_done)

    # Session HTTP
    session = create_session()

    # Compteurs
    all_resultats: list[ResultatNanaelie] = []
    nb_success = 0
    nb_no_data = 0
    nb_errors = 0
    nb_cached = 0
    nb_skipped_ckpt = 0

    current = start_dt
    day_num = 0

    while current <= end_dt:
        day_num += 1
        day_key = current.isoformat()

        # Verifier checkpoint
        if not args.force and ckpt.is_done(day_key):
            # Charger depuis le cache pour l'export final
            cached = load_cache(current)
            if cached:
                resultats = parse_courses_jour(cached, day_key)
                all_resultats.extend(resultats)
                nb_cached += 1
            nb_skipped_ckpt += 1
            current += timedelta(days=1)
            continue

        # Verifier le cache disque
        cached = load_cache(current)
        if cached is not None:
            resultats = parse_courses_jour(cached, day_key)
            all_resultats.extend(resultats)
            nb_cached += 1
            ckpt.mark_done(day_key)
            if nb_cached % 100 == 0:
                ckpt.save()
            current += timedelta(days=1)
            continue

        # Requete API
        day_data = fetch_day(session, current, logger)

        if day_data is None:
            nb_no_data += 1
        else:
            save_cache(current, day_data)
            resultats = parse_courses_jour(day_data, day_key)
            all_resultats.extend(resultats)
            nb_success += 1

        ckpt.mark_done(day_key)

        # Progress toutes les 100 requetes
        fetched = nb_success + nb_no_data + nb_errors
        if fetched > 0 and fetched % 100 == 0:
            logger.info(
                "Progression : jour %d/%d | %d ok, %d vides, %d erreurs, "
                "%d cache | %d courses collectees",
                day_num, total_days, nb_success, nb_no_data, nb_errors,
                nb_cached, len(all_resultats),
            )
            ckpt.save()

        # Pause entre requetes
        time.sleep(pause)
        current += timedelta(days=1)

    # Sauvegarder le checkpoint final
    ckpt.save()

    logger.info("-" * 60)
    logger.info("Collecte terminee")
    logger.info(
        "  Jours : %d total | %d ok | %d vides | %d erreurs | %d cache/ckpt",
        total_days, nb_success, nb_no_data, nb_errors, nb_cached + nb_skipped_ckpt,
    )
    logger.info("  Courses collectees : %d", len(all_resultats))

    if not all_resultats:
        logger.warning("Aucune course collectee, pas d'export")
        return

    # Convertir en dicts
    data_dicts = [asdict(r) for r in all_resultats]

    # Trier par date + cle_course
    data_dicts.sort(key=lambda x: (x["date_iso"], x["cle_course"]))

    # Export quadruple (JSON, JSONL, Parquet, CSV)
    base_name = "nanaelie_2004_2013"
    sauver_json(data_dicts, OUTPUT_DIR / f"{base_name}.json", logger)
    sauver_jsonl(data_dicts, OUTPUT_DIR / f"{base_name}.jsonl", logger)
    sauver_parquet(data_dicts, OUTPUT_DIR / f"{base_name}.parquet", logger)
    sauver_csv(data_dicts, OUTPUT_DIR / f"{base_name}.csv", logger)

    # Stats finales
    dates_uniques = sorted(set(r.date_iso for r in all_resultats))
    hippos_uniques = sorted(set(r.hippodrome_normalise for r in all_resultats if r.hippodrome_normalise))
    disciplines = {}
    for r in all_resultats:
        d = r.discipline or "inconnu"
        disciplines[d] = disciplines.get(d, 0) + 1

    logger.info("-" * 60)
    logger.info("Statistiques finales :")
    logger.info("  Courses : %d", len(all_resultats))
    logger.info("  Jours de course : %d", len(dates_uniques))
    if dates_uniques:
        logger.info("  Plage : %s -> %s", dates_uniques[0], dates_uniques[-1])
    logger.info("  Hippodromes : %d", len(hippos_uniques))
    for disc, count in sorted(disciplines.items(), key=lambda x: -x[1]):
        logger.info("    %s : %d courses", disc, count)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
