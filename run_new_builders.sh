#!/bin/bash
# Launch NEW builders coded in this session (not in run_queued_builders.sh)
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

echo "=== New builders batch A ==="
run_batch field_homogeneity draw_bias prediction_calibration
run_batch pair_interaction_deep recent_trend_deep market_consensus
run_batch distance_change layoff_return trainer_horse_match
run_batch career_phase jockey_form_momentum race_conditions_encoded
run_batch feature_importance_proxy hippodrome_stats discipline_specialist
run_batch first_start_flags claiming_class race_pace_profile
run_batch stallion_stats mutual_info

echo "=== ALL NEW BUILDERS DONE at $(date '+%H:%M:%S') ==="
