#!/bin/bash

# Configuration
# Get script directory and use relative paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPTS_DIR="${SCRIPT_DIR}/Scripts"
LOGFILE="${SCRIPT_DIR}/Logs/process_daily.log"

# Ensure log directory exists
mkdir -p "${SCRIPT_DIR}/Logs"

# Date to process (default to yesterday if not provided)
if [ -z "$1" ]; then
    yday=$(date -v-1d +%Y-%m-%d)
else
    yday="$1"
fi

echo "Processing date: ${yday}" >> "$LOGFILE"

# 1. Process Daily Data
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting 03_process_daily.py for date: ${yday}..." >> "$LOGFILE"
if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/03_process_daily.py" "${yday}" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: 03_process_daily.py completed" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 03_process_daily.py failed with exit code $?" >> "$LOGFILE"
    exit 1
fi

# 2. Build Subscription View
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting 04_build_subscription_view.py..." >> "$LOGFILE"
if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/04_build_subscription_view.py" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: 04_build_subscription_view.py completed" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 04_build_subscription_view.py failed with exit code $?" >> "$LOGFILE"
    exit 1
fi
