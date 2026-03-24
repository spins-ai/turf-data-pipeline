#!/usr/bin/env python3
"""
scripts/auto_learning_cycle.py — Pilier 19 : Cycle Auto-Apprenant
==================================================================
Skeleton for automatic model retraining cycle.

Steps:
  1. Check if new data is available (compare partants_master date vs last
     training date stored in models/last_training.json)
  2. If yes, trigger: re-compute features -> re-train models -> evaluate
     -> deploy if improved
  3. Log each cycle step with timestamps

This is a SKELETON — placeholder logic until real models exist.

RAM budget: < 1 GB (analysis only, no bulk data loading).

Usage:
    python scripts/auto_learning_cycle.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    DATA_MASTER_DIR,
    FEATURES_DIR,
    LABELS_DIR,
    LOGS_DIR,
    MODELS_DIR,
    PARTANTS_MASTER,
    QUALITY_DIR,
)
from utils.logging_setup import setup_logging  # noqa: E402

_TODAY = datetime.now().strftime("%Y%m%d")
logger = setup_logging(f"auto_learning_cycle_{_TODAY}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LAST_TRAINING_FILE = MODELS_DIR / "last_training.json"
CYCLE_LOG_FILE = LOGS_DIR / "learning_cycle_log.jsonl"
REPORT_PATH = QUALITY_DIR / "learning_cycle_report.md"

# Thresholds for model deployment
MIN_ACCURACY_IMPROVEMENT = 0.005   # 0.5% improvement required to deploy
MIN_SAMPLES_FOR_TRAINING = 10_000  # minimum records to trigger training


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def _log_cycle_step(step: str, status: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    """Log a cycle step to the JSONL cycle log and return the entry."""
    entry: dict[str, Any] = {
        "timestamp": _now_iso(),
        "step": step,
        "status": status,
    }
    if details:
        entry["details"] = details
    logger.info("Cycle step: %s — %s", step, status)
    # Append to log file
    CYCLE_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CYCLE_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return entry


def _get_last_training_date() -> str | None:
    """Read the last training date from models/last_training.json."""
    if not LAST_TRAINING_FILE.exists():
        return None
    try:
        with open(LAST_TRAINING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("last_training_date")
    except (json.JSONDecodeError, OSError):
        return None


def _get_partants_master_mtime() -> str | None:
    """Return the modification date of partants_master.jsonl as ISO string."""
    if not PARTANTS_MASTER.exists():
        return None
    mtime = os.path.getmtime(PARTANTS_MASTER)
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def _count_partants_lines() -> int:
    """Count lines in partants_master.jsonl (streaming, low RAM)."""
    if not PARTANTS_MASTER.exists():
        return 0
    count = 0
    with open(PARTANTS_MASTER, "r", encoding="utf-8") as f:
        for _ in f:
            count += 1
    return count


# ---------------------------------------------------------------------------
# Cycle steps (all skeleton / placeholder)
# ---------------------------------------------------------------------------
def step_check_new_data() -> tuple[bool, dict[str, Any]]:
    """Step 1: Check if new data is available since last training."""
    _log_cycle_step("check_new_data", "started")

    last_train = _get_last_training_date()
    master_mtime = _get_partants_master_mtime()
    n_records = _count_partants_lines()

    details = {
        "last_training_date": last_train,
        "partants_master_mtime": master_mtime,
        "partants_record_count": n_records,
    }

    if master_mtime is None:
        _log_cycle_step("check_new_data", "skipped_no_master", details)
        return False, details

    if n_records < MIN_SAMPLES_FOR_TRAINING:
        _log_cycle_step("check_new_data", "skipped_insufficient_data", details)
        return False, details

    # If never trained, or master is newer than last training
    if last_train is None or master_mtime > last_train:
        _log_cycle_step("check_new_data", "new_data_available", details)
        return True, details

    _log_cycle_step("check_new_data", "up_to_date", details)
    return False, details


def step_recompute_features() -> dict[str, Any]:
    """Step 2: Re-compute features (PLACEHOLDER)."""
    _log_cycle_step("recompute_features", "started")

    # PLACEHOLDER: In real implementation, this would call
    # master_feature_builder.py or equivalent
    features_exist = FEATURES_DIR.exists() and any(FEATURES_DIR.iterdir()) if FEATURES_DIR.exists() else False

    details = {
        "features_dir": str(FEATURES_DIR),
        "features_exist": features_exist,
        "action": "PLACEHOLDER — would run master_feature_builder.py",
    }

    _log_cycle_step("recompute_features", "completed_placeholder", details)
    return details


def step_retrain_models() -> dict[str, Any]:
    """Step 3: Re-train models (PLACEHOLDER)."""
    _log_cycle_step("retrain_models", "started")

    # PLACEHOLDER: In real implementation, this would:
    # 1. Load features_matrix + training_labels
    # 2. Train model (XGBoost, LightGBM, etc.)
    # 3. Save model to MODELS_DIR
    details = {
        "models_dir": str(MODELS_DIR),
        "action": "PLACEHOLDER — would train XGBoost/LightGBM models",
        "estimated_ram_mb": 800,
    }

    _log_cycle_step("retrain_models", "completed_placeholder", details)
    return details


def step_evaluate_model() -> tuple[bool, dict[str, Any]]:
    """Step 4: Evaluate model performance (PLACEHOLDER).

    Returns (should_deploy, details).
    """
    _log_cycle_step("evaluate_model", "started")

    # PLACEHOLDER: In real implementation, this would:
    # 1. Run model on validation set
    # 2. Compute metrics (accuracy, ROI, calibration)
    # 3. Compare with previous model
    current_accuracy = 0.0  # placeholder
    previous_accuracy = 0.0  # placeholder
    improvement = current_accuracy - previous_accuracy

    details = {
        "current_accuracy": current_accuracy,
        "previous_accuracy": previous_accuracy,
        "improvement": improvement,
        "min_required_improvement": MIN_ACCURACY_IMPROVEMENT,
        "action": "PLACEHOLDER — would compute validation metrics",
    }

    should_deploy = improvement >= MIN_ACCURACY_IMPROVEMENT

    status = "improved" if should_deploy else "not_improved"
    _log_cycle_step("evaluate_model", status, details)
    return should_deploy, details


def step_deploy_model(should_deploy: bool) -> dict[str, Any]:
    """Step 5: Deploy model if improved (PLACEHOLDER)."""
    _log_cycle_step("deploy_model", "started")

    if not should_deploy:
        details = {"deployed": False, "reason": "No improvement over previous model"}
        _log_cycle_step("deploy_model", "skipped", details)
        return details

    # PLACEHOLDER: In real implementation, this would:
    # 1. Copy model to production directory
    # 2. Update last_training.json
    # 3. Archive old model
    details = {
        "deployed": False,
        "action": "PLACEHOLDER — would copy model to production",
    }

    _log_cycle_step("deploy_model", "completed_placeholder", details)
    return details


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def _generate_report(steps_results: dict[str, Any], elapsed: float) -> None:
    """Write a markdown report of the learning cycle."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Pilier 19 — Cycle Auto-Apprenant",
        "",
        f"**Date**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Duree totale**: {elapsed:.1f}s",
        "",
        "## Resultats du cycle",
        "",
    ]

    for step_name, result in steps_results.items():
        lines.append(f"### {step_name}")
        lines.append("")
        if isinstance(result, dict):
            for k, v in result.items():
                lines.append(f"- **{k}**: {v}")
        elif isinstance(result, tuple):
            flag, details = result
            lines.append(f"- **Decision**: {flag}")
            if isinstance(details, dict):
                for k, v in details.items():
                    lines.append(f"- **{k}**: {v}")
        else:
            lines.append(f"- {result}")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("*NOTE: This is a SKELETON. Real model training will be added once")
    lines.append("ML models (XGBoost, LightGBM, neural nets) are implemented.*")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report written to %s", REPORT_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Run the auto-learning cycle."""
    t0 = time.time()
    logger.info("=== Pilier 19 : Cycle Auto-Apprenant ===")

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    results: dict[str, Any] = {}

    # Step 1: Check new data
    new_data_available, check_details = step_check_new_data()
    results["1_check_new_data"] = check_details

    if not new_data_available:
        logger.info("No new data or insufficient records. Cycle skipped.")
        results["cycle_outcome"] = "skipped"
        _generate_report(results, time.time() - t0)
        return 0

    # Step 2: Re-compute features
    results["2_recompute_features"] = step_recompute_features()

    # Step 3: Re-train models
    results["3_retrain_models"] = step_retrain_models()

    # Step 4: Evaluate
    should_deploy, eval_details = step_evaluate_model()
    results["4_evaluate_model"] = eval_details

    # Step 5: Deploy
    results["5_deploy_model"] = step_deploy_model(should_deploy)

    results["cycle_outcome"] = "completed"

    elapsed = time.time() - t0
    _generate_report(results, elapsed)
    logger.info("Cycle completed in %.1fs", elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
