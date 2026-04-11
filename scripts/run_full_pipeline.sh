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
# STEP 6: New builders (performance + advanced + rapports + pagerank)
# ==========================================
echo ""
echo "=== STEP 6: Specialized Builders ==="
check_ram
echo "  Running new builders (C1-C8)..."
$PYTHON scripts/run_new_builders.py 2>&1 | tail -5
echo "  Running performance builders (C9-C11)..."
check_ram
$PYTHON scripts/run_perf_builders.py 2>&1 | tail -5
echo "  Running advanced builders (C12, C14, C15)..."
check_ram
$PYTHON scripts/run_advanced_builders.py 2>&1 | tail -5
echo "  Running rapports historiques builder (C7)..."
check_ram
$PYTHON scripts/run_rapports_builder.py 2>&1 | tail -5
echo "  Running PageRank builder (C13)..."
check_ram
$PYTHON scripts/run_pagerank_builder.py 2>&1 | tail -5

# ==========================================
# STEP 7: Consolidation + Integration
# ==========================================
echo ""
echo "=== STEP 7: Consolidation ==="
check_ram
$PYTHON scripts/consolidate_features.py 2>&1 | tail -5
echo "  Integrating new features..."
$PYTHON scripts/integrate_new_features.py 2>&1 | tail -5

# ==========================================
# STEP 8: Feature Selection
# ==========================================
echo ""
echo "=== STEP 8: Feature Selection ==="
check_ram
$PYTHON scripts/apply_feature_selection.py 2>&1 | tail -10

# ==========================================
# STEP 9: Validation
# ==========================================
echo ""
echo "=== STEP 9: Validation ==="
$PYTHON scripts/validate_pipeline_output.py

# ==========================================
# STEP 10: Feature catalog
# ==========================================
echo ""
echo "=== STEP 10: Feature Catalog ==="
$PYTHON scripts/generate_feature_catalog_md.py 2>&1 | tail -3 || echo "  (catalog generation skipped)"

# ==========================================
# STEP 11: Tests
# ==========================================
echo ""
echo "=== STEP 11: Tests ==="
$PYTHON -m pytest tests/ -v --tb=short || echo "  WARNING: Some tests failed"

echo ""
echo "=========================================="
echo "PIPELINE COMPLETE"
echo "Finished: $(date)"
echo "=========================================="
