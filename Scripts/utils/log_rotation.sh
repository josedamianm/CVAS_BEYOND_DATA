#!/bin/bash

# ==============================================================================
# Log Rotation Helper
# Keeps only the last 15 days of log entries in a log file
# Usage: rotate_log <logfile>
# ==============================================================================

rotate_log() {
    local LOGFILE="$1"
    local DAYS_TO_KEEP=15
    
    # Only rotate if log file exists and is not empty
    if [ ! -f "$LOGFILE" ] || [ ! -s "$LOGFILE" ]; then
        return 0
    fi
    
    # Calculate cutoff date (15 days ago)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        CUTOFF_DATE=$(date -v-${DAYS_TO_KEEP}d +%Y-%m-%d)
    else
        # Linux
        CUTOFF_DATE=$(date -d "${DAYS_TO_KEEP} days ago" +%Y-%m-%d)
    fi
    
    # Create temporary file with only recent entries
    # Assumes log lines start with [YYYY-MM-DD HH:MM:SS]
    grep -E "^\[20[0-9]{2}-[0-9]{2}-[0-9]{2}" "$LOGFILE" | \
    awk -v cutoff="$CUTOFF_DATE" '
    {
        # Extract date from log line [YYYY-MM-DD HH:MM:SS]
        if (match($0, /\[([0-9]{4}-[0-9]{2}-[0-9]{2})/)) {
            date_str = substr($0, RSTART+1, 10)
            if (date_str >= cutoff) {
                print $0
            }
        }
    }' > "${LOGFILE}.tmp"
    
    # Replace old log with rotated log if temp file has content
    if [ -s "${LOGFILE}.tmp" ]; then
        mv "${LOGFILE}.tmp" "$LOGFILE"
    else
        # If no recent entries, just truncate the log
        : > "$LOGFILE"
        rm -f "${LOGFILE}.tmp"
    fi
}

# ==============================================================================
# Cleanup old fetch_* logs (keeps last 15 days)
# Usage: cleanup_fetch_logs <log_dir>
# ==============================================================================

cleanup_fetch_logs() {
    local LOG_DIR="$1"
    local DAYS_TO_KEEP=15
    
    if [ ! -d "$LOG_DIR" ]; then
        return 0
    fi
    
    # Find and delete fetch_* logs older than 15 days
    find "$LOG_DIR" -name "fetch_*.log" -type f -mtime +${DAYS_TO_KEEP} -delete 2>/dev/null
}
