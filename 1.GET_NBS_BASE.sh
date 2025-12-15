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

# ============================================================================
# START OF RUN - Day Separator
# ============================================================================
echo "" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 1: GET NBS BASE - START" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Target Date: ${yday}" >> "$LOGFILE"
echo "" >> "$LOGFILE"

# ============================================================================
# STEP 1: Download NBS Base File
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 1: DOWNLOADING NBS BASE FILE                       │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Remote Server: omadmin@10.26.82.53" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Remote Path: /opt/postgres/lvas_reports/NBS_Base.csv" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Local File: ${yday}_NBS_Base.csv" >> "$LOGFILE"

# Download the file
if scp -i /Users/josemanco/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/Users/josemanco/.ssh/known_hosts -q -O omadmin@10.26.82.53:/opt/postgres/lvas_reports/NBS_Base.csv "$DEST_FILE" 2>>"$LOGFILE"; then
    # Count lines in the downloaded file
    if [ -f "$DEST_FILE" ]; then
        LINE_COUNT=$(wc -l < "$DEST_FILE" | xargs)
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Download successful: ${LINE_COUNT} lines" >> "$LOGFILE"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: File not found after download" >> "$LOGFILE"
        exit 1
    fi
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Download failed" >> "$LOGFILE"
    exit 1
fi

# ============================================================================
# STEP 2: Validate Downloaded File
# ============================================================================
echo "" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 2: VALIDATING DOWNLOADED FILE                      │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

if [ ! -f "$DEST_FILE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: File does not exist: $DEST_FILE" >> "$LOGFILE"
    exit 1
fi

FILE_SIZE=$(stat -f%z "$DEST_FILE" 2>/dev/null || stat -c%s "$DEST_FILE" 2>/dev/null)
if [ "$FILE_SIZE" -lt 100 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: File too small (${FILE_SIZE} bytes)" >> "$LOGFILE"
    exit 1
fi

FILE_SIZE_KB=$((FILE_SIZE / 1024))
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ File size validation passed: ${FILE_SIZE_KB} KB" >> "$LOGFILE"

sleep 2

# ============================================================================
# STEP 3: Aggregate User Base Data
# ============================================================================
echo "" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 3: AGGREGATING USER BASE DATA                      │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

if /opt/anaconda3/bin/python "${SCRIPT_DIR}/Scripts/01_aggregate_user_base.py" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ User base aggregation completed successfully" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Aggregation failed with exit code $?" >> "$LOGFILE"
    exit 1
fi

# ============================================================================
# END OF RUN - Summary
# ============================================================================
echo "" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ ✓ ALL TASKS COMPLETED SUCCESSFULLY                      │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 1: GET NBS BASE - END" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "" >> "$LOGFILE"
