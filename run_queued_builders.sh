#!/bin/bash
# Launch builders in batches of 3, waiting for each batch to complete
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

echo "=== Phase 1: Failed/unlaunched builders from previous session ==="
run_batch master_ranking race_surface_speed horse_genealogy_deep
run_batch daily_form_snapshot multi_target_features log_transform

echo "=== Phase 2: New builders (batch A) ==="
run_batch travel_distance course_record annual_race_pattern
run_batch bet_liquidity age_distance_pref owner_investment
run_batch jockey_claim_weight musique_discipline_breakdown race_series
run_batch ground_going race_class_history rest_pattern
run_batch sibling_half_performance pace_position trainer_condition_pref
run_batch weight_distance_combo engagement_financial horse_sex_age_combo
run_batch smart_composite sequence_target

echo "=== Phase 3: Fixed old builders ==="
run_batch bayesian_rating class_consistency combo_triple
run_batch cross_features deep_learning_features elo_rating
run_batch market_inefficiency momentum polynomial_interaction
run_batch recency_bias sequence signal_features
run_batch speed_figure streak uncertainty
run_batch value_signal advanced_encoding ml_features
run_batch pace_profile pedigree_advanced derived_features

echo "=== ALL DONE at $(date '+%H:%M:%S') ==="
