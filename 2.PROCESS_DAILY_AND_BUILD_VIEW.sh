#! /bin/bash

################################################
################################################
##                                            ##
##  This script works for Jose Manco only!    ##
##                                            ##
##  Creator: Jose Manco                       ##
##  Reviewer: Jose Manco                      ##
##  Last Modified: 2025-11-14                 ##
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
LOGFILE="/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Logs/2.PROCESS_DAILY_AND_BUILD_VIEW.log"
SCRIPTS_DIR="/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Scripts"

# Calculate yesterday's date in YYYY-MM-DD format
yday=$(date -v-1d +"%Y-%m-%d")

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" > "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting daily processing for date: ${yday}" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" >> "$LOGFILE"

# Run 02_process_daily.py
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 1: Running 02_process_daily.py" >> "$LOGFILE"
if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/02_process_daily.py" "${yday}" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: 02_process_daily.py completed" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 02_process_daily.py failed with exit code $?" >> "$LOGFILE"
    exit 1
fi

echo "" >> "$LOGFILE"

# Run 05_build_subscription_view.py
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Step 2: Running 05_build_subscription_view.py" >> "$LOGFILE"
if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/05_build_subscription_view.py" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: 05_build_subscription_view.py completed" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 05_build_subscription_view.py failed with exit code $?" >> "$LOGFILE"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] All processing completed successfully" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========================================" >> "$LOGFILE"

exit 0
