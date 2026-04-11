#!/bin/bash
# Wave 3: Additional high-value builders (EWMA, class transitions, pace interactions)
# Run AFTER wave2 completes
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

echo "=== Wave 3: EWMA + Class + Pace builders ==="
run_batch recency_weighted class_transition
run_batch pace_position_interaction

echo "=== WAVE 3 ALL DONE at $(date '+%H:%M:%S') ==="
