#!/bin/bash

################################################
################################################
##                                            ##
##  This script works for Jose Manco only!    ##
##                                            ##
##  Creator: Jose Manco                       ##
##  Reviewer: Jose Manco                      ##
##  Last Modified: 2025-12-15                 ##
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

# Get script directory and set paths relative to it
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOGFILE="${SCRIPT_DIR}/Logs/1.GET_NBS_BASE.log"
DEST_DIR="${SCRIPT_DIR}/User_Base/NBS_BASE"

# Source log rotation utility
source "${SCRIPT_DIR}/Scripts/utils/log_rotation.sh"

# Ensure log directory exists
mkdir -p "${SCRIPT_DIR}/Logs"

# Rotate log to keep only last 15 days
rotate_log "$LOGFILE"

# Date handling (cross-platform)
if [[ "$OSTYPE" == "darwin"* ]]; then
    yday=$(date -v-1d +"%Y%m%d")
else
    yday=$(date -d "yesterday" +"%Y%m%d")
fi

DEST_FILE="${DEST_DIR}/${yday}_NBS_Base.csv"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting NBS Base download for date: ${yday}" >> "$LOGFILE"

# Download the file
if scp -i /Users/josemanco/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/Users/josemanco/.ssh/known_hosts -q -O omadmin@10.26.82.53:/opt/postgres/lvas_reports/NBS_Base.csv "$DEST_FILE" 2>>"$LOGFILE"; then
    # Count lines in the downloaded file
    if [ -f "$DEST_FILE" ]; then
        LINE_COUNT=$(wc -l < "$DEST_FILE" | xargs)
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: Downloaded ${yday}_NBS_Base.csv - ${LINE_COUNT} lines" >> "$LOGFILE"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: File not found after download" >> "$LOGFILE"
        exit 1
    fi
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Download failed for date ${yday}" >> "$LOGFILE"
    exit 1
fi

# Validate that the file was actually downloaded and has content
if [ ! -f "$DEST_FILE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Downloaded file not found: $DEST_FILE" >> "$LOGFILE"
    exit 1
fi

FILE_SIZE=$(stat -f%z "$DEST_FILE" 2>/dev/null || stat -c%s "$DEST_FILE" 2>/dev/null)
if [ "$FILE_SIZE" -lt 100 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Downloaded file is too small (${FILE_SIZE} bytes), likely empty or corrupt" >> "$LOGFILE"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] File validation passed: ${FILE_SIZE} bytes" >> "$LOGFILE"

sleep 2

# Run aggregation script
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting user base aggregation..." >> "$LOGFILE"
if /opt/anaconda3/bin/python "${SCRIPT_DIR}/Scripts/aggregate_user_base.py" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: User base aggregation completed" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: User base aggregation failed with exit code $?" >> "$LOGFILE"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ALL TASKS COMPLETED SUCCESSFULLY" >> "$LOGFILE"
