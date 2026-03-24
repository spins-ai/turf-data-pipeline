#!/usr/bin/env python3
"""
15_download_external_datasets.py
Download freely available horse racing datasets from multiple sources.

Sources:
  1. Open PMU API (nanaelie) - French PMU results via REST API
  2. Betfair BSP CSV files - Daily Starting Price data (UK/IRE)
  3. Kaggle HKJC dataset - Hong Kong Jockey Club (requires kaggle auth)
  4. Kaggle JRA dataset - Japan Racing Association (requires kaggle auth)
  5. rpscrape (GitHub) - Racing Post scraper (clone + instructions)
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import json
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_OUTPUT = Path(__file__).resolve().parent / "../../output" / "15_external_datasets"

KAGGLE_DATASETS = {
    "kaggle_hkjc": "hrosebaby/horse-racing-dataset-for-experts-hong-kong",
    "kaggle_jra": "takamotoki/jra-horse-racing-dataset",
    "kaggle_pmu": "nanaelie/historical-pmu-horse-racing-dataset",
}

BETFAIR_BASE_URL = "https://promo.betfair.com/betfairsp/prices"
BETFAIR_REGIONS = ["ukwin", "ukplace", "irewin", "ireplace"]

OPEN_PMU_API_URL = "https://open-pmu-api.vercel.app/api/arrivees"

RPSCRAPE_REPO = "https://github.com/joenano/rpscrape.git"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def download_url(url: str, dest: Path, *, timeout: int = 30) -> bool:
    """Download a URL to a local file. Returns True on success."""
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            if len(data) < 50:
                return False
            dest.write_bytes(data)
        return True
    except (HTTPError, URLError, OSError):
        return False


def check_kaggle_cli() -> bool:
    """Return True if the kaggle CLI is available."""
    return shutil.which("kaggle") is not None


def print_header(title: str):
    sep = "=" * 60
    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)


def print_ok(msg: str):
    print(f"  [OK]  {msg}")


def print_skip(msg: str):
    print(f"  [SKIP] {msg}")


def print_err(msg: str):
    print(f"  [ERR] {msg}")


# ---------------------------------------------------------------------------
# 1. Open PMU API (nanaelie)
# ---------------------------------------------------------------------------

def download_open_pmu(start_date: str = None, end_date: str = None,
                      days_back: int = 30):
    """
    Download French PMU results from the Open PMU API.
    API: https://open-pmu-api.vercel.app/api/arrivees?date=DD/MM/YYYY
    Data available from 22/01/2004 to present.
    """
    print_header("Open PMU API (French PMU results)")
    out_dir = ensure_dir(BASE_OUTPUT / "open_pmu_api")

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()

    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=days_back)

    total_days = (end_dt - start_dt).days + 1
    print(f"  Fetching PMU results from {start_dt:%Y-%m-%d} to {end_dt:%Y-%m-%d} "
          f"({total_days} days)")

    success = 0
    errors = 0
    no_data = 0
    current = start_dt

    while current <= end_dt:
        date_str = current.strftime("%d/%m/%Y")
        file_name = f"pmu_{current:%Y%m%d}.json"
        dest = out_dir / file_name

        if dest.exists() and dest.stat().st_size > 10:
            current += timedelta(days=1)
            success += 1
            continue

        url = f"{OPEN_PMU_API_URL}?date={date_str}"
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=30) as resp:
                data = resp.read()
                parsed = json.loads(data)

                if not parsed or (isinstance(parsed, list) and len(parsed) == 0):
                    no_data += 1
                else:
                    dest.write_bytes(data)
                    success += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print_err(f"{date_str}: {e}")

        # Progress every 10 days
        day_num = (current - start_dt).days + 1
        if day_num % 10 == 0 or current == end_dt:
            print(f"  Progress: {day_num}/{total_days} days "
                  f"({success} ok, {no_data} empty, {errors} errors)")

        # Be polite to the API
        time.sleep(0.3)
        current += timedelta(days=1)

    print_ok(f"Open PMU API: {success} files saved to {out_dir}")
    if no_data:
        print(f"  ({no_data} days with no racing data)")
    if errors:
        print_err(f"{errors} download errors")


# ---------------------------------------------------------------------------
# 2. Betfair BSP CSV files
# ---------------------------------------------------------------------------

def download_betfair_bsp(start_date: str = None, end_date: str = None,
                         days_back: int = 30, regions: list = None):
    """
    Download Betfair Starting Price CSV files.
    URL pattern: https://promo.betfair.com/betfairsp/prices/dwbfprices{region}{DDMMYYYY}.csv
    Regions: ukwin, ukplace, irewin, ireplace
    Available from ~2007 to present.
    """
    print_header("Betfair BSP CSV files (UK & Ireland)")

    out_dir = ensure_dir(BASE_OUTPUT / "betfair_bsp")
    regions = regions or BETFAIR_REGIONS

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()

    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=days_back)

    total_days = (end_dt - start_dt).days + 1
    print(f"  Date range: {start_dt:%Y-%m-%d} to {end_dt:%Y-%m-%d} ({total_days} days)")
    print(f"  Regions: {', '.join(regions)}")
    print(f"  CSV columns: EVENT_ID, MENU_HINT, EVENT_NAME, EVENT_DT, "
          f"SELECTION_NAME, WIN_LOSE, BSP, PPWAP, MORNINGWAP, ...")

    success = 0
    skipped = 0
    missing = 0

    for region in regions:
        region_dir = ensure_dir(out_dir / region)
        current = start_dt

        while current <= end_dt:
            date_str = current.strftime("%d%m%Y")
            filename = f"dwbfprices{region}{date_str}.csv"
            dest = region_dir / filename

            if dest.exists() and dest.stat().st_size > 50:
                skipped += 1
                current += timedelta(days=1)
                continue

            url = f"{BETFAIR_BASE_URL}/{filename}"
            if download_url(url, dest):
                success += 1
            else:
                missing += 1
                if dest.exists():
                    dest.unlink()

            # Progress
            day_num = (current - start_dt).days + 1
            if day_num % 10 == 0:
                print(f"  [{region}] Progress: {day_num}/{total_days} days")

            time.sleep(0.2)
            current += timedelta(days=1)

    print_ok(f"Betfair BSP: {success} new files downloaded, "
             f"{skipped} already existed, {missing} not available")
    print(f"  Output: {out_dir}")


# ---------------------------------------------------------------------------
# 3 & 4 & 5. Kaggle datasets
# ---------------------------------------------------------------------------

def download_kaggle_dataset(name: str, dataset_slug: str):
    """
    Download a Kaggle dataset using the kaggle CLI.
    Requires: pip install kaggle + API token in ~/.kaggle/kaggle.json
    """
    print_header(f"Kaggle: {dataset_slug}")

    out_dir = ensure_dir(BASE_OUTPUT / name)

    if not check_kaggle_cli():
        print_err("kaggle CLI not found.")
        print()
        print("  To install and configure:")
        print("    1. pip install kaggle")
        print("    2. Go to https://www.kaggle.com/settings")
        print("    3. Click 'Create New Token' to download kaggle.json")
        print("    4. Place it at ~/.kaggle/kaggle.json")
        print("    5. chmod 600 ~/.kaggle/kaggle.json")
        print()
        print(f"  Then re-run this script, or manually download from:")
        print(f"    https://www.kaggle.com/datasets/{dataset_slug}")
        return

    # Check for API token
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if not kaggle_json.exists():
        print_err("~/.kaggle/kaggle.json not found.")
        print("  Get your API token from https://www.kaggle.com/settings")
        print(f"  Or download manually: https://www.kaggle.com/datasets/{dataset_slug}")
        return

    print(f"  Downloading to {out_dir} ...")
    try:
        result = subprocess.run(
            [
                "kaggle", "datasets", "download",
                "-d", dataset_slug,
                "-p", str(out_dir),
                "--unzip",
            ],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode == 0:
            files = list(out_dir.iterdir())
            print_ok(f"Downloaded {len(files)} file(s) to {out_dir}")
            for f in files[:10]:
                size_mb = f.stat().st_size / (1024 * 1024)
                print(f"    {f.name} ({size_mb:.1f} MB)")
            if len(files) > 10:
                print(f"    ... and {len(files) - 10} more")
        else:
            print_err(f"kaggle CLI error: {result.stderr.strip()}")
    except subprocess.TimeoutExpired:
        print_err("Download timed out (5 min limit)")
    except Exception as e:
        print_err(f"Error: {e}")


def download_all_kaggle():
    """Download all configured Kaggle datasets."""
    for name, slug in KAGGLE_DATASETS.items():
        download_kaggle_dataset(name, slug)


# ---------------------------------------------------------------------------
# 6. rpscrape (GitHub clone + usage guide)
# ---------------------------------------------------------------------------

def setup_rpscrape():
    """
    Clone rpscrape from GitHub and print usage instructions.
    rpscrape supports French racing (region code: fr).
    """
    print_header("rpscrape (Racing Post scraper)")

    out_dir = ensure_dir(BASE_OUTPUT / "rpscrape")
    repo_dir = out_dir / "rpscrape"

    if repo_dir.exists() and (repo_dir / ".git").exists():
        print(f"  Repository already cloned at {repo_dir}")
        print("  Pulling latest changes ...")
        try:
            subprocess.run(
                ["git", "-C", str(repo_dir), "pull"],
                capture_output=True, text=True, timeout=60,
            )
            print_ok("Updated to latest version")
        except Exception as e:
            print_err(f"git pull failed: {e}")
    else:
        print(f"  Cloning {RPSCRAPE_REPO} ...")
        try:
            result = subprocess.run(
                ["git", "clone", RPSCRAPE_REPO, str(repo_dir)],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0:
                print_ok(f"Cloned to {repo_dir}")
            else:
                print_err(f"git clone failed: {result.stderr.strip()}")
                return
        except Exception as e:
            print_err(f"Error: {e}")
            return

    # Print usage instructions
    print()
    print("  --- rpscrape usage guide ---")
    print()
    print("  Requirements: Python 3.13+, git")
    print()
    print("  Install dependencies:")
    print("    pip install curl_cffi jarowinkler lxml orjson "
          "python-dotenv tomli tqdm")
    print()
    print("  Usage examples:")
    print(f"    cd {repo_dir}/scripts")
    print()
    print("    # Scrape French flat results for a date")
    print("    python rpscrape.py -d 2024/01/15 -r fr")
    print()
    print("    # Scrape UK results")
    print("    python rpscrape.py -d 2024/01/15 -r gb")
    print()
    print("    # Scrape all races on a date")
    print("    python rpscrape.py -d 2024/01/15")
    print()
    print("    # Search for French courses")
    print("    python rpscrape.py --courses France")
    print()
    print("  Supported region codes:")
    print("    fr = France, gb = Great Britain, ire = Ireland")
    print()
    print("  Output is saved as CSV in the rpscrape/data/ directory.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download external horse racing datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Sources:
  pmu       Open PMU API (French results, free, no auth)
  betfair   Betfair BSP CSV files (UK/IRE, free, no auth)
  kaggle    All Kaggle datasets (HKJC, JRA, PMU - needs kaggle CLI)
  rpscrape  Clone rpscrape repo (Racing Post scraper)
  all       Download everything

Examples:
  python 15_download_external_datasets.py --source betfair --days 7
  python 15_download_external_datasets.py --source pmu --start 2024-01-01 --end 2024-01-31
  python 15_download_external_datasets.py --source all --days 30
        """,
    )
    parser.add_argument(
        "--source", "-s",
        choices=["pmu", "betfair", "kaggle", "rpscrape", "all"],
        default="all",
        help="Which dataset source to download (default: all)",
    )
    parser.add_argument(
        "--start",
        help="Start date YYYY-MM-DD (for pmu/betfair)",
    )
    parser.add_argument(
        "--end",
        help="End date YYYY-MM-DD (for pmu/betfair)",
    )
    parser.add_argument(
        "--days", "-d",
        type=int, default=30,
        help="Number of days back from today (default: 30, used if --start not set)",
    )
    parser.add_argument(
        "--betfair-regions",
        nargs="+",
        default=BETFAIR_REGIONS,
        help=f"Betfair regions to download (default: {' '.join(BETFAIR_REGIONS)})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"External Datasets Downloader")
    print(f"Output directory: {BASE_OUTPUT.resolve()}")
    print(f"Source: {args.source}")

    ensure_dir(BASE_OUTPUT)

    source = args.source

    if source in ("pmu", "all"):
        download_open_pmu(
            start_date=args.start,
            end_date=args.end,
            days_back=args.days,
        )

    if source in ("betfair", "all"):
        download_betfair_bsp(
            start_date=args.start,
            end_date=args.end,
            days_back=args.days,
            regions=args.betfair_regions,
        )

    if source in ("kaggle", "all"):
        download_all_kaggle()

    if source in ("rpscrape", "all"):
        setup_rpscrape()

    print()
    print("=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
