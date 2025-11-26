#!/bin/bash

# ==============================================================================
# Log Rotation Helper
# Keeps only the last 7 days of log entries in a log file
# Usage: rotate_log <logfile>
# ==============================================================================

rotate_log() {
    local LOGFILE="$1"
    local DAYS_TO_KEEP=7
    
    # Only rotate if log file exists and is not empty
    if [ ! -f "$LOGFILE" ] || [ ! -s "$LOGFILE" ]; then
        return 0
    fi
    
    # Calculate cutoff date (7 days ago)
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
        match($0, /\[([0-9]{4}-[0-9]{2}-[0-9]{2})/, date_match)
        if (date_match[1] >= cutoff) {
            print $0
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
