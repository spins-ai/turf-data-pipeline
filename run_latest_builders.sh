#!/bin/bash
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

echo "=== Latest builders ==="
run_batch claiming_class race_pace_profile stallion_stats
run_batch mutual_info race_history_richness odds_transform
run_batch recency_weight categorical_hash rank_features
run_batch win_conditions target_encoding count_encoding

echo "=== DONE at $(date '+%H:%M:%S') ==="

echo "=== Extra latest batch ==="
run_batch basic_ratio_features binary_flags field_position_features
run_batch normalized_numeric
echo "=== EXTRA DONE at $(date '+%H:%M:%S') ==="
