# Shared utilities package
#
# Modules:
#   utils.normalize     - normalize_name, strip_accents, normalize_date
#   utils.types         - safe_int, safe_float
#   utils.loaders       - load_json_or_jsonl, load_jsonl, load_json_safe
#   utils.logging_setup - setup_logging
#   utils.scraping      - smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint
#   utils.playwright    - launch_browser, navigate_with_retry, accept_cookies (Playwright helpers)
#   utils.html_parsing  - extract_embedded_json, extract_data_attributes
#   utils.output        - save_jsonl, sauver_json, sauver_csv, sauver_parquet

from utils.logging_setup import setup_logging
from utils.output import save_jsonl, sauver_json, sauver_csv, sauver_parquet
from utils.loaders import load_json_or_jsonl, load_jsonl, load_json_safe
from utils.normalize import normalize_name, strip_accents, normalize_date, normalize_name_for_matching
from utils.scraping import smart_pause, fetch_with_retry, append_jsonl, load_checkpoint, save_checkpoint
from utils.types import safe_int, safe_float
from utils.html_parsing import extract_embedded_json, extract_data_attributes
from utils.math import safe_mean, safe_rate, safe_stdev

__all__ = [
    # logging_setup
    "setup_logging",
    # loaders
    "load_json_or_jsonl",
    "load_jsonl",
    "load_json_safe",
    # normalize
    "normalize_name",
    "strip_accents",
    "normalize_date",
    "normalize_name_for_matching",
    # scraping
    "smart_pause",
    "fetch_with_retry",
    "append_jsonl",
    "load_checkpoint",
    "save_checkpoint",
    # types
    "safe_int",
    "safe_float",
    # output
    "save_jsonl",
    "sauver_json",
    "sauver_csv",
    "sauver_parquet",
    # html_parsing
    "extract_embedded_json",
    "extract_data_attributes",
    # math
    "safe_mean",
    "safe_rate",
    "safe_stdev",
]
