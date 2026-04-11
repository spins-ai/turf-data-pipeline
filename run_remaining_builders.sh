#!/bin/bash
# Run remaining feature builders one at a time with RAM monitoring
# Kill if RAM usage exceeds 50GB used (i.e., free < 14GB on 64GB system)

BASEDIR="/c/Users/celia/turf-data-pipeline/.claude/worktrees/naughty-bardeen"
PYTHON="/c/Users/celia/AppData/Local/Programs/Python/Python312/python.exe"
LOGFILE="$BASEDIR/logs/builder_orchestrator.log"
TOTAL_KB=64636200
MAX_USED_KB=$((50 * 1024 * 1024))  # 50GB in KB

cd "$BASEDIR"

BUILDERS=(
  market_inefficiency_builder
  pattern_discovery_builder
  derived_features_builder
  sequence_builder
  graph_features_builder
  feature_improvements_builder
  rolling_advanced_builder
  interaction_advanced_builder
)

echo "=== Builder Orchestrator Start: $(date) ===" | tee -a "$LOGFILE"

for builder in "${BUILDERS[@]}"; do
  echo "" | tee -a "$LOGFILE"
  echo "=== Starting $builder: $(date) ===" | tee -a "$LOGFILE"

  "$PYTHON" "feature_builders/${builder}.py" > "logs/${builder}.log" 2>&1 &
  PID=$!
  echo "  PID: $PID" | tee -a "$LOGFILE"

  # Monitor: check every 15 seconds, max 10 minutes per builder
  CHECKS=0
  MAX_CHECKS=40  # 40 * 15s = 10 min
  KILLED=0

  while kill -0 $PID 2>/dev/null; do
    sleep 15
    CHECKS=$((CHECKS + 1))

    FREE_KB=$(powershell -command "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory" 2>/dev/null)
    USED_KB=$((TOTAL_KB - FREE_KB))
    FREE_GB=$((FREE_KB / 1024 / 1024))
    USED_GB=$((USED_KB / 1024 / 1024))

    if [ "$USED_GB" -ge 50 ]; then
      echo "  [$CHECKS] RAM LIMIT: ${USED_GB}GB used - KILLING $builder" | tee -a "$LOGFILE"
      kill -9 $PID 2>/dev/null
      wait $PID 2>/dev/null
      KILLED=1
      sleep 5
      break
    fi

    if [ $((CHECKS % 4)) -eq 0 ]; then
      echo "  [$CHECKS] ${builder}: running | Free: ${FREE_GB}GB | Used: ${USED_GB}GB" | tee -a "$LOGFILE"
    fi

    if [ "$CHECKS" -ge "$MAX_CHECKS" ]; then
      echo "  TIMEOUT (10min) - killing $builder" | tee -a "$LOGFILE"
      kill -9 $PID 2>/dev/null
      wait $PID 2>/dev/null
      KILLED=1
      break
    fi
  done

  if [ "$KILLED" -eq 0 ]; then
    wait $PID
    EXIT_CODE=$?
    echo "  $builder finished with exit code $EXIT_CODE at $(date)" | tee -a "$LOGFILE"
  fi

  # Check output
  OUTPUT_NAME="${builder//_builder/}"
  FOUND=0
  for suffix in "" "_features"; do
    d="$BASEDIR/output/${OUTPUT_NAME}${suffix}"
    if [ -d "$d" ]; then
      count=$(find "$d" -name "*.jsonl" 2>/dev/null | wc -l)
      if [ "$count" -gt 0 ]; then
        size=$(du -sh "$d" 2>/dev/null | cut -f1)
        echo "  OUTPUT OK: $d ($count files, $size)" | tee -a "$LOGFILE"
        FOUND=1
      fi
    fi
  done
  if [ "$FOUND" -eq 0 ]; then
    echo "  NO OUTPUT for $builder" | tee -a "$LOGFILE"
    tail -5 "logs/${builder}.log" 2>/dev/null | tee -a "$LOGFILE"
  fi

  # Brief pause between builders for memory cleanup
  sleep 5
done

echo "" | tee -a "$LOGFILE"
echo "=== Builder Orchestrator Complete: $(date) ===" | tee -a "$LOGFILE"
