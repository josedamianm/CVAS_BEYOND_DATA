#!/bin/bash

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPTS_DIR="${SCRIPT_DIR}/Scripts"
LOGFILE="${SCRIPT_DIR}/Logs/4.BUILD_TRANSACTION_COUNTERS.log"

# Source log rotation utility
source "${SCRIPT_DIR}/Scripts/utils/log_rotation.sh"

# Ensure log directory exists
mkdir -p "${SCRIPT_DIR}/Logs"

# Rotate log to keep only last 15 days
rotate_log "$LOGFILE"

# Parse arguments
FORCE_FLAG=""
START_DATE=""
END_DATE=""
TARGET_DATE=""
BACKFILL_FLAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE_FLAG="--force"
            shift
            ;;
        --backfill)
            BACKFILL_FLAG="--backfill"
            shift
            ;;
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        *)
            TARGET_DATE="$1"
            shift
            ;;
    esac
done

# ============================================================================
# START OF RUN
# ============================================================================
echo "" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 4: BUILD TRANSACTION COUNTERS - START" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"

if [ -n "$BACKFILL_FLAG" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Mode: Backfill (process all missing dates)" >> "$LOGFILE"
elif [ -n "$START_DATE" ] && [ -n "$END_DATE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Date Range: ${START_DATE} to ${END_DATE}" >> "$LOGFILE"
elif [ -n "$TARGET_DATE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Target Date: ${TARGET_DATE}" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Mode: Daily (process yesterday)" >> "$LOGFILE"
fi

if [ -n "$FORCE_FLAG" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Force recompute: enabled" >> "$LOGFILE"
fi
echo "" >> "$LOGFILE"

# ============================================================================
# STEP 1: Validate Parquet Data Exists
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 1: VALIDATING PARQUET DATA                         │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

PARQUET_DIRS=("act" "reno" "dct" "cnr" "ppd" "rfnd")
MISSING_DIRS=()

for dir in "${PARQUET_DIRS[@]}"; do
    if [ -d "${SCRIPT_DIR}/Parquet_Data/transactions/${dir}" ]; then
        PARQUET_COUNT=$(find "${SCRIPT_DIR}/Parquet_Data/transactions/${dir}" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ ${dir}: ${PARQUET_COUNT} parquet files" >> "$LOGFILE"
    else
        MISSING_DIRS+=("$dir")
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ Missing: ${dir}" >> "$LOGFILE"
    fi
done

if [ ${#MISSING_DIRS[@]} -gt 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️  Warning: Some transaction types missing" >> "$LOGFILE"
fi

# Validate MASTERCPC.csv
if [ ! -f "${SCRIPT_DIR}/MASTERCPC.csv" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: MASTERCPC.csv not found" >> "$LOGFILE"
    exit 1
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ MASTERCPC.csv found" >> "$LOGFILE"
echo "" >> "$LOGFILE"

# ============================================================================
# STEP 2: Build Transaction Counters
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 2: BUILDING TRANSACTION COUNTERS                   │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

# Build command arguments
CMD_ARGS=""
if [ -n "$BACKFILL_FLAG" ]; then
    CMD_ARGS="--backfill"
elif [ -n "$START_DATE" ] && [ -n "$END_DATE" ]; then
    CMD_ARGS="--start-date ${START_DATE} --end-date ${END_DATE}"
elif [ -n "$TARGET_DATE" ]; then
    CMD_ARGS="${TARGET_DATE}"
fi

if [ -n "$FORCE_FLAG" ]; then
    CMD_ARGS="${CMD_ARGS} ${FORCE_FLAG}"
fi

if /opt/anaconda3/bin/python "${SCRIPTS_DIR}/05_build_counters.py" ${CMD_ARGS} >> "$LOGFILE" 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Transaction counters built successfully" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Counter build failed (exit code: $?)" >> "$LOGFILE"
    exit 1
fi
echo "" >> "$LOGFILE"

# ============================================================================
# STEP 3: Validate Output Files
# ============================================================================
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ┌─────────────────────────────────────────────────────────┐" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] │ STEP 3: VALIDATING OUTPUT FILES                         │" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] └─────────────────────────────────────────────────────────┘" >> "$LOGFILE"

COUNTERS_CPC="${SCRIPT_DIR}/Counters/Counters_CPC.parquet"
COUNTERS_SERVICE="${SCRIPT_DIR}/Counters/Counters_Service.csv"

if [ -f "$COUNTERS_CPC" ]; then
    FILE_SIZE=$(stat -f%z "$COUNTERS_CPC" 2>/dev/null || stat -c%s "$COUNTERS_CPC" 2>/dev/null)
    FILE_SIZE_KB=$((FILE_SIZE / 1024))
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Counters_CPC.parquet (${FILE_SIZE_KB} KB)" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Counters_CPC.parquet not created" >> "$LOGFILE"
    exit 1
fi

if [ -f "$COUNTERS_SERVICE" ]; then
    FILE_SIZE=$(stat -f%z "$COUNTERS_SERVICE" 2>/dev/null || stat -c%s "$COUNTERS_SERVICE" 2>/dev/null)
    FILE_SIZE_KB=$((FILE_SIZE / 1024))
    LINE_COUNT=$(wc -l < "$COUNTERS_SERVICE" | tr -d ' ')
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Counters_Service.csv (${FILE_SIZE_KB} KB, ${LINE_COUNT} lines)" >> "$LOGFILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Counters_Service.csv not created" >> "$LOGFILE"
    exit 1
fi

echo "" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SCRIPT 4: BUILD TRANSACTION COUNTERS - COMPLETE" >> "$LOGFILE"
echo "================================================================================" >> "$LOGFILE"

exit 0
