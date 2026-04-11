#!/bin/bash
PYTHON="/c/Users/celia/AppData/Local/Programs/Python/Python312/python.exe"
cd "C:/Users/celia/turf-data-pipeline/.claude/worktrees/naughty-bardeen"

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

echo "=== Extra builders ==="
run_batch basic_ratio_features binary_flags field_position_features
run_batch normalized_numeric
echo "=== DONE at $(date '+%H:%M:%S') ==="
