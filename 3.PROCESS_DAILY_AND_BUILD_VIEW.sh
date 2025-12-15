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

# ============================================================================
# START OF RUN - Day Separator
# ============================================================================
echo "" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 3: PROCESS DAILY AND BUILD VIEW - START" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Target Date: ${yday}" >> "$LOGFILE"
echo "" >> "$LOGFILE"

# ============================================================================
# STEP 1: Validate Daily Data Files
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 1: VALIDATING DAILY DATA FILES                     │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

REQUIRED_FILES=(
    "${SCRIPT_DIR}/Daily_Data/act_atlas_day.csv"
    "${SCRIPT_DIR}/Daily_Data/reno_atlas_day.csv"
    "${SCRIPT_DIR}/Daily_Data/dct_atlas_day.csv"
    "${SCRIPT_DIR}/Daily_Data/cnr_atlas_day.csv"
    "${SCRIPT_DIR}/Daily_Data/ppd_atlas_day.csv"
    "${SCRIPT_DIR}/Daily_Data/rfnd_atlas_day.csv"
)

MISSING_FILES=()
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        MISSING_FILES+=("$(basename "$file")")
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ Missing: $(basename "$file")" >> "$LOGFILE"
    else
        FILE_SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)
        FILE_SIZE_KB=$((FILE_SIZE / 1024))
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Found: $(basename "$file") (${FILE_SIZE_KB} KB)" >> "$LOGFILE"
    fi
done

if [ ${#MISSING_FILES[@]} -gt 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Missing ${#MISSING_FILES[@]} required file(s)" >> "$LOGFILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Please run 2.FETCH_DAILY_DATA.sh first" >> "$LOGFILE"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ All 6 data files validated successfully" >> "$LOGFILE"
echo "" >> "$LOGFILE"

# ============================================================================
# STEP 2: Process Daily Data to Parquet
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 2: PROCESSING DAILY DATA (CSV → PARQUET)           │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/03_process_daily.py" "${yday}" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Daily data processing completed successfully" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Daily data processing failed (exit code: $?)" >> "$LOGFILE"
    exit 1
fi

# Validate parquet data was created
if [ ! -d "${SCRIPT_DIR}/Parquet_Data/transactions/act" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Parquet data directory not found" >> "$LOGFILE"
    exit 1
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Parquet data structure validated" >> "$LOGFILE"
echo "" >> "$LOGFILE"

# ============================================================================
# STEP 3: Build Subscription View
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 3: BUILDING SUBSCRIPTION VIEW                      │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/04_build_subscription_view.py" >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Subscription view build completed successfully" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Subscription view build failed (exit code: $?)" >> "$LOGFILE"
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
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 3: PROCESS DAILY AND BUILD VIEW - END (SUCCESS)" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "" >> "$LOGFILE"
