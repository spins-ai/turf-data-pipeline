#!/usr/bin/env python3
"""
Script 77 — Download ALL horse racing datasets from Kaggle
Source : kaggle.com/datasets (via Kaggle API ou URLs directes)
Collecte : tous les datasets horse racing disponibles sur Kaggle
           (Hong Kong, UK, Australie, US, etc.)
CRITIQUE pour : Training Data, Benchmark, Feature Discovery
PRE-REQUIS : pip install kaggle  +  ~/.kaggle/kaggle.json configure
"""

import argparse
import glob
import json
import os
import random
import subprocess
import sys
import time
import zipfile
from datetime import datetime

import requests

SCRIPT_NAME = "77_kaggle"
OUTPUT_DIR = os.path.join("output", SCRIPT_NAME)
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
DOWNLOAD_DIR = os.path.join(OUTPUT_DIR, "downloads")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, ".checkpoint.json")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import load_checkpoint, save_checkpoint, append_jsonl

log = setup_logging("77_kaggle")

# ============================================================
# Liste exhaustive des datasets Kaggle horse racing connus
# Format: "owner/dataset-slug"
# ============================================================
KAGGLE_DATASETS = [
    # Hong Kong Jockey Club
    "gdaley/hkracing",
    "lantanacamara/hong-kong-horse-racing",
    # UK / Ireland
    "lukeclarke123/uk-horse-racing-results",
    "hwaitt/horse-racing",
    "zygmunt/horse-racing-in-the-uk",
    # Australie
    "braquets/australian-horse-racing",
    "alanvourch/horse-racing",
    "tokyoracer/horse-racing-results-australia",
    # US
    "ionaskel/kentucky-derby-winners",
    "bogdanbaraban/horse-racing",
    "jcaliz/us-horse-racing",
    "hawerroth/horse-racing-dataset",
    # France
    "faressalah/french-horse-racing",
    "olivierbour/french-horse-racing-pmu",
    # General / Multi-pays
    "cprasad/horse-racing-results",
    "adamzakaria/horse-racing-data",
    "mattop/horse-racing-prediction",
    "drhoogstrate/horse-racing",
    "thedevastator/horse-racing-results",
    "sanjeetsinghnaik/horse-racing",
    "lemonkoala/horse-racing-data",
    # Betting / Odds
    "robjan/horse-racing-betting-data",
    "aldozeng/horse-racing-dataset",
    # Pedigree / Breeding
    "andrewgeorge/horse-pedigree",
    # Large compilations
    "paultimothymooney/horse-racing",
    "ailab2023/horse-racing-dataset",
]

# Mots-cles pour la recherche supplementaire
SEARCH_KEYWORDS = [
    "horse racing",
    "horse racing results",
    "horse racing betting",
    "horse racing prediction",
    "turf racing",
    "thoroughbred racing",
    "harness racing",
    "horse pedigree",
    "PMU courses",
]





def check_kaggle_cli():
    """Verifier si le CLI Kaggle est installe et configure."""
    try:
        result = subprocess.run(
            ["kaggle", "--version"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            log.info(f"  Kaggle CLI: {result.stdout.strip()}")
            return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    log.warning("  Kaggle CLI non disponible. Installation: pip install kaggle")
    log.warning("  Config: placer kaggle.json dans ~/.kaggle/")
    return False


def search_kaggle_datasets(keyword, page=1, max_results=20):
    """Rechercher des datasets via le CLI Kaggle."""
    try:
        result = subprocess.run(
            ["kaggle", "datasets", "list", "-s", keyword,
             "--sort-by", "relevance", "-p", str(page),
             "--max-size", "10737418240",  # 10GB max
             "--csv"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            log.warning(f"  Recherche echouee: {result.stderr.strip()}")
            return []

        datasets = []
        lines = result.stdout.strip().split("\n")
        if len(lines) < 2:
            return []

        headers = lines[0].split(",")
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) >= 2:
                ref = parts[0]
                datasets.append(ref)

        return datasets

    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        log.warning(f"  Erreur recherche: {e}")
        return []


def download_kaggle_dataset(dataset_ref, target_dir):
    """Telecharger un dataset Kaggle via le CLI."""
    ds_dir = os.path.join(target_dir, dataset_ref.replace("/", "_"))
    marker = os.path.join(ds_dir, ".download_complete")

    if os.path.exists(marker):
        log.info(f"    Deja telecharge: {dataset_ref}")
        return ds_dir

    os.makedirs(ds_dir, exist_ok=True)

    try:
        result = subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset_ref,
             "-p", ds_dir, "--unzip"],
            capture_output=True, text=True, timeout=600,
        )

        if result.returncode != 0:
            # Essayer sans --unzip
            result = subprocess.run(
                ["kaggle", "datasets", "download", "-d", dataset_ref,
                 "-p", ds_dir],
                capture_output=True, text=True, timeout=600,
            )

            if result.returncode != 0:
                log.warning(f"    Echec download {dataset_ref}: {result.stderr.strip()}")
                return None

            # Dezipper manuellement
            for zf in glob.glob(os.path.join(ds_dir, "*.zip")):
                try:
                    with zipfile.ZipFile(zf, "r") as z:
                        z.extractall(ds_dir)
                    os.remove(zf)
                except zipfile.BadZipFile:
                    log.warning(f"    ZIP corrompu: {zf}")

        # Marquer comme complet
        with open(marker, "w", encoding="utf-8") as f:
            f.write(datetime.now().isoformat())

        log.info(f"    OK: {dataset_ref}")
        return ds_dir

    except subprocess.TimeoutExpired:
        log.warning(f"    Timeout download {dataset_ref}")
        return None
    except Exception as e:
        log.warning(f"    Erreur download {dataset_ref}: {e}")
        return None


def download_via_url(dataset_ref, target_dir):
    """Fallback: telecharger via URL directe (sans CLI Kaggle)."""
    ds_dir = os.path.join(target_dir, dataset_ref.replace("/", "_"))
    marker = os.path.join(ds_dir, ".download_complete")

    if os.path.exists(marker):
        log.info(f"    Deja telecharge: {dataset_ref}")
        return ds_dir

    os.makedirs(ds_dir, exist_ok=True)

    url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_ref}"
    zip_path = os.path.join(ds_dir, "dataset.zip")

    try:
        # Necesssite un token dans ~/.kaggle/kaggle.json
        kaggle_json = os.path.expanduser("~/.kaggle/kaggle.json")
        headers = {}
        if os.path.exists(kaggle_json):
            with open(kaggle_json, encoding="utf-8") as f:
                creds = json.load(f)
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(creds.get("username", ""), creds.get("key", ""))
        else:
            auth = None
            log.warning(f"    Pas de credentials Kaggle, tentative sans auth...")

        resp = requests.get(url, auth=auth, stream=True, timeout=300)
        if resp.status_code != 200:
            log.warning(f"    HTTP {resp.status_code} pour {dataset_ref}")
            return None

        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        # Dezipper
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(ds_dir)
            os.remove(zip_path)
        except zipfile.BadZipFile:
            log.warning(f"    ZIP invalide pour {dataset_ref}")

        with open(marker, "w", encoding="utf-8") as f:
            f.write(datetime.now().isoformat())

        log.info(f"    OK (URL): {dataset_ref}")
        return ds_dir

    except Exception as e:
        log.warning(f"    Erreur URL download {dataset_ref}: {e}")
        return None


def inventory_dataset(ds_dir, dataset_ref):
    """Inventorier le contenu d'un dataset telecharge."""
    inventory = {
        "source": "kaggle",
        "dataset_ref": dataset_ref,
        "type": "dataset_inventory",
        "scraped_at": datetime.now().isoformat(),
        "files": [],
        "total_size_bytes": 0,
        "total_rows_estimate": 0,
    }

    if not ds_dir or not os.path.exists(ds_dir):
        return inventory

    for root, dirs, files in os.walk(ds_dir):
        for fname in files:
            if fname.startswith("."):
                continue
            fpath = os.path.join(root, fname)
            fsize = os.path.getsize(fpath)
            rel_path = os.path.relpath(fpath, ds_dir)

            file_info = {
                "path": rel_path,
                "size_bytes": fsize,
                "extension": os.path.splitext(fname)[1].lower(),
            }

            # Compter les lignes pour CSV/TSV
            if file_info["extension"] in (".csv", ".tsv", ".txt"):
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        line_count = sum(1 for _ in f)
                    file_info["line_count"] = line_count
                    inventory["total_rows_estimate"] += max(0, line_count - 1)
                except Exception as e:
                    log.debug(f"  Erreur comptage lignes {fpath}: {e}")

            # Lire les headers CSV
            if file_info["extension"] == ".csv":
                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        header = f.readline().strip()
                    file_info["columns"] = header.split(",")[:20]  # Max 20 colonnes
                except Exception as e:
                    log.debug(f"  Erreur lecture headers {fpath}: {e}")

            inventory["files"].append(file_info)
            inventory["total_size_bytes"] += fsize

    return inventory


def main():
    parser = argparse.ArgumentParser(description="Script 77 — Kaggle Horse Racing Datasets Downloader")
    parser.add_argument("--search", action="store_true", default=True,
                        help="Rechercher aussi des datasets supplementaires")
    parser.add_argument("--no-download", action="store_true", default=False,
                        help="Lister seulement, ne pas telecharger")
    parser.add_argument("--max-datasets", type=int, default=100,
                        help="Nombre max de datasets a telecharger")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Reprendre depuis le dernier checkpoint")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("SCRIPT 77 — Kaggle Horse Racing Datasets")
    log.info(f"  Datasets connus : {len(KAGGLE_DATASETS)}")
    log.info(f"  Search additionnel : {args.search}")
    log.info(f"  Download : {not args.no_download}")
    log.info("=" * 60)

    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    has_cli = check_kaggle_cli()
    output_file = os.path.join(OUTPUT_DIR, "kaggle_datasets.jsonl")

    # Construire la liste complete des datasets
    all_datasets = list(KAGGLE_DATASETS)

    # Recherche supplementaire via CLI
    if args.search and has_cli:
        log.info("--- Phase 1: Recherche datasets supplementaires ---")
        for keyword in SEARCH_KEYWORDS:
            log.info(f"  Recherche: '{keyword}'")
            found = search_kaggle_datasets(keyword)
            for ds in found:
                if ds not in all_datasets:
                    all_datasets.append(ds)
                    log.info(f"    + {ds}")
            time.sleep(2)

    log.info(f"  Total datasets a traiter: {len(all_datasets)}")

    # Filtrer par checkpoint
    downloaded = set(checkpoint.get("downloaded", []))
    failed = set(checkpoint.get("failed", []))
    total_records = checkpoint.get("total_records", 0)

    ds_count = 0
    ok_count = 0
    fail_count = 0

    for dataset_ref in all_datasets:
        if ds_count >= args.max_datasets:
            log.info(f"  Limite {args.max_datasets} atteinte")
            break

        if dataset_ref in downloaded:
            log.info(f"  Skip (deja fait): {dataset_ref}")
            continue

        log.info(f"  [{ds_count + 1}/{len(all_datasets)}] {dataset_ref}")

        if args.no_download:
            # Mode listing uniquement
            record = {
                "source": "kaggle",
                "dataset_ref": dataset_ref,
                "type": "dataset_listing",
                "url": f"https://www.kaggle.com/datasets/{dataset_ref}",
                "scraped_at": datetime.now().isoformat(),
            }
            append_jsonl(output_file, record)
            total_records += 1
        else:
            # Telecharger
            ds_dir = None
            if has_cli:
                ds_dir = download_kaggle_dataset(dataset_ref, DOWNLOAD_DIR)
            if not ds_dir:
                ds_dir = download_via_url(dataset_ref, DOWNLOAD_DIR)

            if ds_dir:
                # Inventorier
                inventory = inventory_dataset(ds_dir, dataset_ref)
                append_jsonl(output_file, inventory)
                total_records += 1
                downloaded.add(dataset_ref)
                ok_count += 1
            else:
                failed.add(dataset_ref)
                fail_count += 1
                append_jsonl(output_file, {
                    "source": "kaggle",
                    "dataset_ref": dataset_ref,
                    "type": "download_failed",
                    "scraped_at": datetime.now().isoformat(),
                })
                total_records += 1

        ds_count += 1

        if ds_count % 5 == 0:
            save_checkpoint(CHECKPOINT_FILE, {
                "downloaded": list(downloaded),
                "failed": list(failed),
                "total_records": total_records,
            })
            log.info(f"  Progression: {ds_count} traites, {ok_count} OK, {fail_count} echecs")

        # Pause entre les downloads
        time.sleep(random.uniform(2, 5))

    save_checkpoint(CHECKPOINT_FILE, {
        "downloaded": list(downloaded),
        "failed": list(failed),
        "total_records": total_records,
        "status": "done",
    })

    log.info("=" * 60)
    log.info(f"TERMINE: {ds_count} datasets traites")
    log.info(f"  OK: {ok_count} | Echecs: {fail_count}")
    log.info(f"  Records: {total_records} -> {output_file}")
    log.info(f"  Downloads: {DOWNLOAD_DIR}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
