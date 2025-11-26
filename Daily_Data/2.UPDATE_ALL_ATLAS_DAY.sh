#!/bin/bash

# ==============================================================================
# Master Update Script
# Orchestrates the fetching of all daily data types using the unified fetch script.
# ==============================================================================

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
FETCH_SCRIPT="${PROJECT_ROOT}/Scripts/fetch_remote_data.sh"
LOG_FILE="${PROJECT_ROOT}/Logs/run_all_scripts.log"

# Transaction types to process
TYPES=(
    "act"
    "cnr"
    "dct"
    "ppd"
    "reno"
    "rfnd"
)

# Start logging
mkdir -p "${PROJECT_ROOT}/Logs"
exec > >(tee -a ${LOG_FILE}) 2>&1

echo "[$(date)] ========================================"
echo "[$(date)] STARTING DAILY DATA UPDATE"
echo "[$(date)] ========================================"

# Make sure fetch script is executable
chmod +x "$FETCH_SCRIPT"

# Run fetch for each type
for TYPE in "${TYPES[@]}"; do
    echo "[$(date)] Processing TYPE: ${TYPE}..."
    
    # Call the unified script
    "$FETCH_SCRIPT" "$TYPE"
    
    # Check status
    if [ $? -ne 0 ]; then
        echo "[$(date)] ❌ ERROR: Failed to fetch ${TYPE} data."
        # We continue to the next type even if one fails
    else
        echo "[$(date)] ✓ SUCCESS: ${TYPE} data updated."
    fi
    echo ""
done

echo "[$(date)] ========================================"
echo "[$(date)] DAILY DATA UPDATE COMPLETED"
echo "[$(date)] ========================================"
