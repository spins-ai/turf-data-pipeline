#!/bin/bash
# ============================================================
# run_full_pipeline.sh — Pipeline reproductible complet
# ============================================================
# Execute dans l'ordre:
# 1. Collecte (si --collect)
# 2. Feature builders (tous)
# 3. Audits qualite
# 4. Targets + splits
# 5. Consolidation Parquet
#
# Usage: bash scripts/run_full_pipeline.sh [--collect] [--builders-only]
# ============================================================

set -e
PYTHON="/c/Users/celia/AppData/Local/Programs/Python/Python312/python.exe"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "=========================================="
echo "TURF DATA PIPELINE — Full Run"
echo "Started: $(date)"
echo "=========================================="

# Parse args
DO_COLLECT=false
BUILDERS_ONLY=false
for arg in "$@"; do
    case $arg in
        --collect) DO_COLLECT=true ;;
        --builders-only) BUILDERS_ONLY=true ;;
    esac
done

# Helper: check RAM before heavy task
check_ram() {
    local free_gb=$(powershell -Command "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory / 1MB" 2>/dev/null | tr -d '\r')
    echo "  RAM libre: ${free_gb} GB"
    # If less than 7 GB free, abort (57 GB limit on 64 GB system)
    local threshold=7
    if (( $(echo "$free_gb < $threshold" | bc -l 2>/dev/null || echo 0) )); then
        echo "ERREUR: RAM insuffisante (${free_gb} GB libre, minimum ${threshold} GB)"
        exit 1
    fi
}

# Helper: run a single builder
run_builder() {
    local builder="$1"
    echo "--- Running builder: $builder ---"
    check_ram
    $PYTHON "feature_builders/$builder" 2>&1 | tail -5
    echo "  Done: $builder"
}

# ==========================================
# STEP 1: Collection (optional)
# ==========================================
if $DO_COLLECT; then
    echo ""
    echo "=== STEP 1: Collection ==="
    $PYTHON scripts/collection/01_calendrier_reunions.py
    $PYTHON scripts/collection/02_liste_courses.py
    $PYTHON scripts/collection/04_resultats.py
    echo "Collection done."
fi

# ==========================================
# STEP 2: Feature builders (1 at a time!)
# ==========================================
echo ""
echo "=== STEP 2: Feature Builders ==="

# List all builder .py files
BUILDERS=$(ls feature_builders/*.py | grep -v __init__ | grep -v __pycache__ | sort)
TOTAL=$(echo "$BUILDERS" | wc -l)
COUNT=0

for builder_path in $BUILDERS; do
    builder=$(basename "$builder_path")
    COUNT=$((COUNT + 1))
    echo "[$COUNT/$TOTAL] $builder"
    check_ram
    $PYTHON "$builder_path" 2>&1 | tail -3
    echo "  Done."
done

if $BUILDERS_ONLY; then
    echo "Builders-only mode: stopping here."
    exit 0
fi

# ==========================================
# STEP 3: Quality audits
# ==========================================
echo ""
echo "=== STEP 3: Quality Audits ==="
$PYTHON scripts/audit_fill_rates.py
$PYTHON scripts/audit_temporal_leakage.py
$PYTHON scripts/audit_dedup_features.py
$PYTHON scripts/audit_high_correlation.py
$PYTHON scripts/audit_schema_consistency.py
$PYTHON scripts/audit_outliers.py

# ==========================================
# STEP 4: Targets + Splits
# ==========================================
echo ""
echo "=== STEP 4: Targets + Splits ==="
$PYTHON scripts/prepare_targets.py
$PYTHON scripts/prepare_temporal_split.py

# ==========================================
# STEP 5: Tests
# ==========================================
echo ""
echo "=== STEP 5: Tests ==="
$PYTHON tests/test_fill_rate_regression.py
$PYTHON tests/test_builder_output_completeness.py
$PYTHON tests/test_temporal_ordering.py

# ==========================================
# STEP 6: Feature catalog
# ==========================================
echo ""
echo "=== STEP 6: Feature Catalog ==="
$PYTHON scripts/generate_feature_catalog_md.py

echo ""
echo "=========================================="
echo "PIPELINE COMPLETE"
echo "Finished: $(date)"
echo "=========================================="
