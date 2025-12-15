#!/bin/bash

# Configuration
# Get script directory and use relative paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPTS_DIR="${SCRIPT_DIR}/Scripts"
LOGFILE="${SCRIPT_DIR}/Logs/2.FETCH_DAILY_DATA.log"

# Source log rotation utility
source "${SCRIPT_DIR}/Scripts/utils/log_rotation.sh"

# Ensure log directory exists
mkdir -p "${SCRIPT_DIR}/Logs"

# Rotate log to keep only last 15 days
rotate_log "$LOGFILE"

# Cleanup old fetch_* logs (legacy cleanup)
cleanup_fetch_logs "${SCRIPT_DIR}/Logs"

# Date to fetch (default to yesterday if not provided)
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
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting daily data fetch for date: ${yday}" >> "$LOGFILE"

# Fetch all transaction types
TYPES=("act" "reno" "dct" "ppd" "cnr" "rfnd")
FAILED_TYPES=()

for type in "${TYPES[@]}"; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Fetching ${type}..." >> "$LOGFILE"
    if "${SCRIPTS_DIR}/02_fetch_remote_nova_data.sh" "${type}" "${yday}" >> "$LOGFILE" 2>&1; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: ${type} fetch completed" >> "$LOGFILE"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: ${type} fetch failed" >> "$LOGFILE"
        FAILED_TYPES+=("${type}")
    fi
done

if [ ${#FAILED_TYPES[@]} -gt 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] COMPLETED WITH ERRORS - Failed types: ${FAILED_TYPES[*]}" >> "$LOGFILE"
    exit 1
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ALL FETCHES COMPLETED SUCCESSFULLY" >> "$LOGFILE"
fi
