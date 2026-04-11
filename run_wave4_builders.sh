#!/bin/bash
# Wave 4: Relative features + temporal gaps + discipline switch + opponent quality
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

echo "=== Wave 4: Relative + Temporal + Discipline + Opponents ==="
run_batch relative_to_field days_since
run_batch discipline_switch opponent_strength

echo "=== WAVE 4 ALL DONE at $(date '+%H:%M:%S') ==="
