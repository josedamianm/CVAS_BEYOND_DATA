#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="${SCRIPT_DIR}/Scripts"
LOGFILE="${SCRIPT_DIR}/Logs/5.BACKFILL_MISSING_DATES.log"

source "${SCRIPTS_DIR}/utils/log_rotation.sh"
rotate_log "${LOGFILE}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOGFILE}"
}

log "SCRIPT 5: BACKFILL MISSING DATES - START"
log "============================================"

DRY_RUN=""
SOURCE_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --source-path)
            SOURCE_PATH="--source-path $2"
            shift 2
            ;;
        *)
            log "Unknown option: $1"
            log "Usage: $0 [--dry-run] [--source-path PATH]"
            exit 1
            ;;
    esac
done

if [ -n "${DRY_RUN}" ]; then
    log "Running in DRY RUN mode - no changes will be made"
fi

log "Executing backfill script..."
/opt/anaconda3/bin/python "${SCRIPTS_DIR}/05_backfill_missing_dates.py" ${DRY_RUN} ${SOURCE_PATH} 2>&1 | tee -a "${LOGFILE}"

if [ ${PIPESTATUS[0]} -eq 0 ]; then
    log "✓ Backfill completed successfully"
    log "SCRIPT 5: BACKFILL MISSING DATES - END"
    exit 0
else
    log "✗ Backfill failed"
    log "SCRIPT 5: BACKFILL MISSING DATES - END"
    exit 1
fi
