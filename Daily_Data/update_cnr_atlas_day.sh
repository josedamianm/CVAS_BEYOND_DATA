#!/bin/bash

# Parse input parameters
START_DATE=""
END_DATE=""
OUTPUT_FOLDER=""
MULTI_DAY_MODE=false

if [ $# -eq 3 ]; then
    START_DATE="$1"
    END_DATE="$2"
    OUTPUT_FOLDER="$3"
    MULTI_DAY_MODE=true

    # Validate date format (YYYY-MM-DD)
    if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "Error: Dates must be in YYYY-MM-DD format"
        echo "Usage: $0 [start_date end_date output_folder]"
        echo "Example: $0 2024-01-01 2024-01-31 /path/to/output"
        exit 1
    fi

    echo "Running in MULTI-DAY mode:"
    echo "  Start Date: $START_DATE"
    echo "  End Date: $END_DATE"
    echo "  Output Folder: $OUTPUT_FOLDER"
elif [ $# -eq 0 ]; then
    echo "Running in DEFAULT mode (yesterday only with SSH transfer)"
    # Set dates for yesterday in default mode
    START_DATE=$(date -d "yesterday" +%Y-%m-%d)
    END_DATE=$(date -d "yesterday" +%Y-%m-%d)
else
    echo "Error: Invalid number of parameters"
    echo "Usage: $0                                    # Default: yesterday only with SSH transfer"
    echo "       $0 <start_date> <end_date> <folder>  # Multi-day: date range to local folder"
    echo "Example: $0 2024-01-01 2024-01-31 /BEYOND_DATA_OLD/Custom_Output/"
    exit 1
fi

# Variables
REMOTE_USER="omadmin"
REMOTE_HOST="10.26.82.53"
REMOTE_PSQL="/usr/local/pgsql/bin/psql"
REMOTE_DB="postgres"
SQL_SCRIPT_PATH="/var/omadmin_reports/create_cnr_atlas_day.sql"
CSV_FILE_PATH="/var/omadmin_reports/cnr_atlas_day.csv"
LOCAL_DIR="/BEYOND_DATA_OLD/Daily_Data/"
LOG_FILE="/BEYOND_DATA_OLD/LOGS/cnr_atlas_day_script.log"

# Start logging
exec > >(tee -a ${LOG_FILE}) 2>&1
echo "[$(date)] Script execution started."

# Create and run SQL script on remote server
echo "[$(date)] Connecting to remote server to create and execute SQL script..."

# Single SSH session with SQL script using bash variables
ssh ${REMOTE_USER}@${REMOTE_HOST} << EOF
set -e  # Exit immediately if any command fails
echo "[$(date)] Connected to remote server."

# Create the SQL script with date range (using bash variable substitution)
cat > ${SQL_SCRIPT_PATH} << 'SQL'
DO \$\$
DECLARE
    start_date DATE := '${START_DATE}';
    end_date DATE := '${END_DATE}';
BEGIN
    DROP TABLE IF EXISTS cnr_atlas_day;

    CREATE TABLE cnr_atlas_day AS
    SELECT
        timestamp as cancel_date,
        cast(sbn_id as numeric),
        msisdn as tmuserid,
        cpc,
        mode
    FROM
        delaydcttlog_v1_0_0_fact
    WHERE
        timestamp >= start_date
        AND timestamp < (end_date + interval '1 day')::DATE;
END \$\$;
SQL

echo "[$(date)] SQL script created at ${SQL_SCRIPT_PATH}."

# Run the SQL script to create the table
echo "[$(date)] Executing the SQL script..."
${REMOTE_PSQL} -U postgres -d ${REMOTE_DB} -f ${SQL_SCRIPT_PATH}
echo "[$(date)] Table cnr_atlas_day created successfully."

# Export the table to a CSV file
echo "[$(date)] Exporting table to CSV file at ${CSV_FILE_PATH}..."
${REMOTE_PSQL} -U postgres -d ${REMOTE_DB} -c "COPY cnr_atlas_day TO '${CSV_FILE_PATH}' WITH CSV HEADER;"
echo "[$(date)] CSV file created at ${CSV_FILE_PATH}."
EOF

# Check if SSH command was successful
if [ $? -ne 0 ]; then
    echo "[$(date)] Error occurred during SSH or SQL execution." >&2
    exit 1
fi

# Handle output based on mode
if [ "$MULTI_DAY_MODE" = true ]; then
    # Multi-day mode: Move to specified output folder with date range in filename
    OUTPUT_FILE="${OUTPUT_FOLDER}/cnr_atlas_day_${START_DATE}_to_${END_DATE}.csv"
    echo "[$(date)] Moving CSV file to Nova server Path: ${OUTPUT_FILE}..."
    ssh ${REMOTE_USER}@${REMOTE_HOST} "mkdir -p ${OUTPUT_FOLDER} && sudo mv ${CSV_FILE_PATH} ${OUTPUT_FILE}"
    echo "[$(date)] CANCELATIONS CSV file has been successfully saved to ${OUTPUT_FILE}."
else
    # Default mode: Copy to local directory
    echo "[$(date)] Copying CSV file from remote server to local directory..."
    scp -O ${REMOTE_USER}@${REMOTE_HOST}:${CSV_FILE_PATH} ${LOCAL_DIR}

    if [ $? -ne 0 ]; then
        echo "[$(date)] Error occurred during SCP." >&2
        exit 1
    fi

    echo "[$(date)] CANCELATIONS CSV file has been successfully downloaded to ${LOCAL_DIR}."
fi

echo "[$(date)] CANCELATIONS Script execution completed."
