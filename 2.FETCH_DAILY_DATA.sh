#!/bin/bash

# ==============================================================================
# 2.FETCH_DAILY_DATA.sh
# Orchestrates the fetching of all daily data types.
# ==============================================================================

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FETCH_SCRIPT="${SCRIPT_DIR}/Scripts/02_fetch_remote_nova_data.sh"
LOG_FILE="${SCRIPT_DIR}/Logs/2.FETCH_DAILY_DATA.log"

# Source log rotation utility
source "${SCRIPT_DIR}/Scripts/utils/log_rotation.sh"

# Rotate log to keep only last 7 days
rotate_log "$LOG_FILE"

# Start logging
mkdir -p "${SCRIPT_DIR}/Logs"
exec > >(tee -a ${LOG_FILE}) 2>&1

echo "[$(date)] ========================================"
echo "[$(date)] STARTING DAILY DATA FETCH"
echo "[$(date)] ========================================"

# Make sure fetch script is executable
chmod +x "$FETCH_SCRIPT"

# Run fetch for all types
echo "[$(date)] Fetching all transaction types..."
"$FETCH_SCRIPT" "all"

if [ $? -ne 0 ]; then
    echo "[$(date)] ❌ ERROR: Failed to fetch data."
    exit 1
else
    echo "[$(date)] ✓ SUCCESS: All data fetched."
fi

echo "[$(date)] ========================================"
echo "[$(date)] DAILY DATA FETCH COMPLETED"
echo "[$(date)] ========================================"
