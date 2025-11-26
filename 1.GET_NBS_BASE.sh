#! /bin/bash

################################################
################################################
##                                            ##
##  This script works for Jose Manco only!    ##
##                                            ##
##  Creator: Jose Manco                       ##
##  Reviewer: Jose Manco                      ##
##  Last Modified: 2025-11-07                 ##
##                                            ##
################################################
################################################

################################################
################################################
##                                            ##
##      DO NOT MODIFY BELOW THIS LINE!!!      ##
##                                            ##
################################################
################################################

# Log configuration
LOGFILE="/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Logs/1.GET_NBS_BASE.log"
DEST_DIR="/Users/josemanco/CVAS/CVAS_BEYOND_DATA/User_Base/NBS_BASE"

if [ $# -eq 0 ]; then
    # Default mode: download for yesterday
    yday=$(date -v-1d +"%Y%m%d")
    DEST_FILE="${DEST_DIR}/${yday}_NBS_Base.csv"
    
    # Download the file
    if scp -i /Users/josemanco/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/Users/josemanco/.ssh/known_hosts -q -O omadmin@10.26.82.53:/opt/postgres/lvas_reports/NBS_Base.csv "$DEST_FILE" 2>>"$LOGFILE"; then
        # Count lines in the downloaded file
        if [ -f "$DEST_FILE" ]; then
            LINE_COUNT=$(wc -l < "$DEST_FILE" | xargs)
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: Downloaded ${yday}_NBS_Base.csv - ${LINE_COUNT} lines" > "$LOGFILE"
        else
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: File not found after download" > "$LOGFILE"
            exit 1
        fi
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Download failed for date ${yday}" > "$LOGFILE"
        exit 1
    fi
else
    echo "Usage: $0 "
    exit 1
fi

sleep 2

python /Users/josemanco/CVAS/CVAS_BEYOND_DATA/Scripts/aggregate_user_base.py >> $LOGFILE


