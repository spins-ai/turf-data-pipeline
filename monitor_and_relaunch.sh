#!/bin/bash
# Monitor scripts and relaunch if crashed
LOG="/Users/quentinherve/models hybride/logs/monitor.log"
echo "$(date) | Monitor démarré" >> "$LOG"

while true; do
    cd "/Users/quentinherve/models hybride"
    
    # Check each script
    for script in 21_rapports_definitifs 27_citations_enjeux 28_combinaisons_marche 38_rapports_internet 14_pedigree_scraper 36_pedigree_query 37_rpscrape_racing_post; do
        running=$(ps aux | grep "${script}.py" | grep -v grep | wc -l | tr -d ' ')
        if [ "$running" = "0" ]; then
            echo "$(date) | ⚠️ ${script} PAS EN COURS - Relance..." >> "$LOG"
            python3 "${script}.py" >> "logs/${script}_auto.log" 2>&1 &
            echo "$(date) | ✅ ${script} relancé PID $!" >> "$LOG"
        fi
    done
    
    # Log RAM usage
    ram=$(ps aux | grep python3 | grep -v grep | awk '{sum+=$4} END {printf "%.1f", sum}')
    echo "$(date) | RAM Python: ${ram}% | Scripts actifs: $(ps aux | grep -E '(rapports|citations|combinaisons|pedigree|racing).*\.py' | grep -v grep | wc -l | tr -d ' ')" >> "$LOG"
    
    # Wait 10 minutes
    sleep 600
done
