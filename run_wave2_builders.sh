#!/bin/bash
# Wave 2: New high-value builders created in second session
# Run AFTER current wave completes (check with: ls D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/*/*.tmp | wc -l)
PYTHON="/c/Users/celia/AppData/Local/Programs/Python/Python312/python.exe"
CD="C:/Users/celia/turf-data-pipeline/.claude/worktrees/naughty-bardeen"
cd "$CD"

run_batch() {
    local pids=()
    for builder in "$@"; do
        echo "[$(date '+%H:%M:%S')] Launching: $builder"
        $PYTHON "feature_builders/${builder}_builder.py" > /dev/null 2>&1 &
        pids+=($!)
    done
    echo "[$(date '+%H:%M:%S')] Waiting for ${#pids[@]} processes..."
    for pid in "${pids[@]}"; do
        wait $pid
        echo "[$(date '+%H:%M:%S')] PID $pid finished (exit $?)"
    done
}

echo "=== Wave 2: Retry crashed + new high-value builders ==="
echo "[$(date '+%H:%M:%S')] First: retry 2 crashed builders"
run_batch mutual_info draw_bias
echo "[$(date '+%H:%M:%S')] Now: new high-value builders"
run_batch rapport_dividend earnings_velocity temporal_cyclical
run_batch pedigree_surface_interaction bayesian_shrinkage commentaire_deep_nlp
run_batch field_similarity network_centrality
run_batch weight_efficiency bounce_back
run_batch survival_hazard market_overreaction
run_batch hippo_draw_deep equipment_change
run_batch owner_pattern race_rhythm
run_batch race_zscore conditional_winprob
run_batch performance_stability age_performance_curve
run_batch field_strength optimal_conditions
run_batch value_detection target_leakfree

echo "=== WAVE 2 ALL DONE at $(date '+%H:%M:%S') ==="
