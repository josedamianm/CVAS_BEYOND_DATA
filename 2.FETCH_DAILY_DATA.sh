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

# ============================================================================
# START OF RUN - Day Separator
# ============================================================================
echo "" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 2: FETCH DAILY DATA - START" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Target Date: ${yday}" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Transaction Types: ACT, RENO, DCT, PPD, CNR, RFND" >> "$LOGFILE"
echo "" >> "$LOGFILE"

# Fetch all transaction types
TYPES=("act" "reno" "dct" "ppd" "cnr" "rfnd")
FAILED_TYPES=()
SUCCESS_COUNT=0

for type in "${TYPES[@]}"; do
    TYPE_UPPER=$(echo "${type}" | tr '[:lower:]' '[:upper:]')

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ FETCHING: ${TYPE_UPPER} TRANSACTIONS" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

    # Execute fetch script and capture output to temp file to clean it
    TEMP_LOG=$(mktemp)
    if "${SCRIPTS_DIR}/02_fetch_remote_nova_data.sh" "${type}" "${yday}" > "$TEMP_LOG" 2>&1; then
        # Filter out SSH warnings and unnecessary messages
        grep -v "Authorized users only" "$TEMP_LOG" | \
        grep -v "All activity may be monitored" | \
        grep -v "^$" | \
        sed 's/^/['"$(date '+%Y-%m-%d %H:%M:%S')"'] /' >> "$LOGFILE"

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ ${TYPE_UPPER} fetch completed successfully" >> "$LOGFILE"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        cat "$TEMP_LOG" | sed 's/^/['"$(date '+%Y-%m-%d %H:%M:%S')"'] /' >> "$LOGFILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: ${TYPE_UPPER} fetch failed" >> "$LOGFILE"
        FAILED_TYPES+=("${type}")
    fi
    rm -f "$TEMP_LOG"
    echo "" >> "$LOGFILE"
done

# ============================================================================
# END OF RUN - Summary
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
if [ ${#FAILED_TYPES[@]} -gt 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ ✗ COMPLETED WITH ERRORS                                 │" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Successful: ${SUCCESS_COUNT}/6 transaction types" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed: ${FAILED_TYPES[*]}" >> "$LOGFILE"
    echo "================================================================================" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 2: FETCH DAILY DATA - END (WITH ERRORS)" >> "$LOGFILE"
    echo "================================================================================" >> "$LOGFILE"
    echo "" >> "$LOGFILE"
    exit 1
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ ✓ ALL FETCHES COMPLETED SUCCESSFULLY                    │" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] All 6 transaction types fetched successfully" >> "$LOGFILE"
    echo "================================================================================" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 2: FETCH DAILY DATA - END (SUCCESS)" >> "$LOGFILE"
    echo "================================================================================" >> "$LOGFILE"
    echo "" >> "$LOGFILE"
fi
