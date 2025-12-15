#!/bin/bash

# Configuration
# Get script directory and use relative paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPTS_DIR="${SCRIPT_DIR}/Scripts"
LOGFILE="${SCRIPT_DIR}/Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log"

# Source log rotation utility
source "${SCRIPT_DIR}/Scripts/utils/log_rotation.sh"

# Ensure log directory exists
mkdir -p "${SCRIPT_DIR}/Logs"

# Rotate log to keep only last 15 days
rotate_log "$LOGFILE"

# Date to process (default to yesterday if not provided)
if [ -z "$1" ]; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        yday=$(date -v-1d +%Y-%m-%d)
    else
        yday=$(date -d "yesterday" +%Y-%m-%d)
    fi
else
    yday="$1"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Processing date: ${yday}" >> "$LOGFILE"

# Validate that required daily data files exist
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Validating daily data files..." >> "$LOGFILE"

REQUIRED_FILES=(
    "${SCRIPT_DIR}/Daily_Data/act_atlas_day.csv"
    "${SCRIPT_DIR}/Daily_Data/reno_atlas_day.csv"
    "${SCRIPT_DIR}/Daily_Data/dct_atlas_day.csv"
)

MISSING_FILES=()
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        MISSING_FILES+=("$(basename "$file")")
    fi
done

if [ ${#MISSING_FILES[@]} -gt 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Missing required data files: ${MISSING_FILES[*]}" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Please run 2.FETCH_DAILY_DATA.sh first" >> "$LOGFILE"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] All required data files present" >> "$LOGFILE"

# 1. Process Daily Data
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting 03_process_daily.py for date: ${yday}..." >> "$LOGFILE"
if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/03_process_daily.py" "${yday}" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: 03_process_daily.py completed" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 03_process_daily.py failed with exit code $?" >> "$LOGFILE"
    exit 1
fi

# Validate that parquet data was created
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Validating parquet data..." >> "$LOGFILE"
if [ ! -d "${SCRIPT_DIR}/Parquet_Data/transactions/act" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Parquet data directory not found" >> "$LOGFILE"
    exit 1
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Parquet data validated" >> "$LOGFILE"

# 2. Build Subscription View
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting 04_build_subscription_view.py..." >> "$LOGFILE"
if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/04_build_subscription_view.py" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: 04_build_subscription_view.py completed" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 04_build_subscription_view.py failed with exit code $?" >> "$LOGFILE"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ALL PROCESSING COMPLETED SUCCESSFULLY" >> "$LOGFILE"
