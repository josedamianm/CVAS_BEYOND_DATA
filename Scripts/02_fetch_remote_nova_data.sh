#!/bin/bash

# ==============================================================================
# Unified Remote Data Fetching Script
# Fetches daily data from remote PostgreSQL server for various transaction types.
#
# Usage:
#   ./02_fetch_remote_nova_data.sh <type> [start_date] [end_date]
#
# Parameters:
#   type: act, reno, dct, ppd, cnr, rfnd, all
#   start_date (optional): YYYY-MM-DD (default: yesterday)
#   end_date (optional): YYYY-MM-DD (default: yesterday)
# ==============================================================================

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Remote Server Config
REMOTE_USER="omadmin"
REMOTE_HOST="10.26.82.53"
REMOTE_PSQL="/usr/local/pgsql/bin/psql"
REMOTE_DB="postgres"

# Local Paths
LOCAL_DATA_DIR="${PROJECT_ROOT}/Daily_Data"
LOG_DIR="${PROJECT_ROOT}/Logs"
mkdir -p "$LOG_DIR" "$LOCAL_DATA_DIR"

# ------------------------------------------------------------------------------
# Input Parsing & Validation
# ------------------------------------------------------------------------------

TRANS_TYPE=$1
START_DATE=$2
END_DATE=$3

if [ -z "$TRANS_TYPE" ]; then
    echo "Error: Transaction type is required."
    echo "Usage: $0 <type> [start_date] [end_date]"
    echo "Types: act, reno, dct, ppd, cnr, rfnd, all"
    exit 1
fi

# Validate Transaction Type
if [ "$TRANS_TYPE" == "all" ]; then
    TYPES=("act" "reno" "dct" "ppd" "cnr" "rfnd")
else
    case "$TRANS_TYPE" in
        act|reno|dct|ppd|cnr|rfnd) TYPES=("$TRANS_TYPE") ;;
        *) echo "Error: Invalid transaction type '$TRANS_TYPE'. Must be one of: act, reno, dct, ppd, cnr, rfnd, all"; exit 1 ;;
    esac
fi

# Date Handling (Cross-platform)
if [ -z "$START_DATE" ]; then
    # Default to yesterday
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        START_DATE=$(date -v-1d +%Y-%m-%d)
        END_DATE=$(date -v-1d +%Y-%m-%d)
    else
        # Linux
        START_DATE=$(date -d "yesterday" +%Y-%m-%d)
        END_DATE=$(date -d "yesterday" +%Y-%m-%d)
    fi
    echo "Running in DEFAULT mode (yesterday: $START_DATE)"
else
    # Validate input dates
    if [[ ! "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "Error: Start date must be YYYY-MM-DD"
        exit 1
    fi
    if [ -z "$END_DATE" ]; then
        END_DATE="$START_DATE"
    fi
    echo "Running in CUSTOM DATE mode: $START_DATE to $END_DATE"
fi

# Loop through types (if 'all' was selected, this runs for each; otherwise just once)
for CURRENT_TYPE in "${TYPES[@]}"; do

    echo "[$(date)] Starting fetch for TYPE: $CURRENT_TYPE"

    # ------------------------------------------------------------------------------
    # SQL Generation
    # ------------------------------------------------------------------------------

    SQL_SCRIPT_PATH="/var/omadmin_reports/create_${CURRENT_TYPE}_atlas_day.sql"
    CSV_FILE_PATH="/var/omadmin_reports/${CURRENT_TYPE}_atlas_day.csv"

    # Define SQL based on type
    case "$CURRENT_TYPE" in
        act)
            SQL_QUERY="
            SELECT
                ft.msisdn as tmuserid,
                split_part(split_part(ft.generic_act_info,'chrg_refid=',2),';',1) as msisdn,
                split_part(split_part(ft.generic_act_info,'cpc=',2),';',1) as cpc,
                ft.trans_type_id,
                ft.channel_id,
                chnl.channel_desc as channel_act,
                ft.tlog_ts as trans_date,
                request_ts as act_date,
                next_charging_date as reno_date,
                split_part(split_part(ft.generic_act_info,'campaign=',2),';',1) as camp_name,
                split_part(split_part(ft.generic_act_info,'TEFProvider=',2),';',1) as TEF_PROV,
                split_part(split_part(ft.generic_act_info,'TEFmedium=',2),';',1) as campana_medium,
                split_part(split_part(ft.generic_act_info,'TEFcampaign=',2),';',1) as campana_id,
                ft.subscription_id,
                ft.charged_amount as rev
            FROM telefonicaes_sub_mgr_fact AS ft
            LEFT JOIN channel_dim as chnl ON ft.channel_id = chnl.channel_id
            WHERE ft.tlog_ts >= start_date
              AND ft.tlog_ts < (end_date + interval '1 day')::DATE
              AND ft.trans_type_id = 1;"
            ;;
        reno)
            SQL_QUERY="
            SELECT
                ft.msisdn as tmuserid,
                split_part(split_part(ft.generic_act_info,'chrg_refid=',2),';',1) as msisdn,
                split_part(split_part(ft.generic_act_info,'cpc=',2),';',1) as cpc,
                ft.trans_type_id,
                ft.channel_id,
                chnl.channel_desc as channel_act,
                ft.tlog_ts as trans_date,
                request_ts as act_date,
                next_charging_date as reno_date,
                split_part(split_part(ft.generic_act_info,'campaign=',2),';',1) as camp_name,
                split_part(split_part(ft.generic_act_info,'TEFProvider=',2),';',1) as TEF_PROV,
                split_part(split_part(ft.generic_act_info,'TEFmedium=',2),';',1) as campana_medium,
                split_part(split_part(ft.generic_act_info,'TEFcampaign=',2),';',1) as campana_id,
                ft.subscription_id,
                ft.charged_amount as rev
            FROM telefonicaes_sub_mgr_fact AS ft
            LEFT JOIN channel_dim as chnl ON ft.channel_id = chnl.channel_id
            WHERE ft.tlog_ts >= start_date
              AND ft.tlog_ts < (end_date + interval '1 day')::DATE
              AND ft.trans_type_id = 2;"
            ;;
        dct)
            SQL_QUERY="
            SELECT
                ft.msisdn as tmuserid,
                split_part(split_part(ft.generic_act_info,'chrg_refid=',2),';',1) as msisdn,
                split_part(split_part(ft.generic_act_info,'cpc=',2),';',1) as cpc,
                ft.trans_type_id,
                chnl.channel_desc as channel_dct,
                ft.tlog_ts as trans_date,
                request_ts as act_date,
                next_charging_date as reno_date,
                split_part(split_part(ft.generic_act_info,'campaign=',2),';',1) as camp_name,
                split_part(split_part(ft.generic_act_info,'TEFProvider=',2),';',1) as TEF_PROV,
                split_part(split_part(ft.generic_act_info,'TEFmedium=',2),';',1) as campana_medium,
                split_part(split_part(ft.generic_act_info,'TEFcampaign=',2),';',1) as campana_id,
                ft.subscription_id
            FROM telefonicaes_sub_mgr_fact AS ft
            LEFT JOIN channel_dim as chnl ON ft.channel_id = chnl.channel_id
            WHERE ft.tlog_ts >= start_date
              AND ft.tlog_ts < (end_date + interval '1 day')::DATE
              AND ft.trans_type_id = 3;"
            ;;
        ppd)
            SQL_QUERY="
            SELECT
                ft.msisdn as tmuserid,
                split_part(split_part(ft.generic_act_info,'chrg_refid=',2),';',1) as msisdn,
                split_part(split_part(ft.generic_act_info,'cpc=',2),';',1) as cpc,
                ft.trans_type_id,
                ft.channel_id,
                ft.tlog_ts as trans_date,
                request_ts as act_date,
                next_charging_date as reno_date,
                split_part(split_part(ft.generic_act_info,'campaign=',2),';',1) as camp_name,
                split_part(split_part(ft.generic_act_info,'TEFProvider=',2),';',1) as TEF_PROV,
                split_part(split_part(ft.generic_act_info,'TEFmedium=',2),';',1) as campana_medium,
                split_part(split_part(ft.generic_act_info,'TEFcampaign=',2),';',1) as campana_id,
                ft.subscription_id,
                ft.charged_amount as rev
            FROM telefonicaes_sub_mgr_fact AS ft
            LEFT JOIN channel_dim as chnl ON ft.channel_id = chnl.channel_id
            WHERE ft.tlog_ts >= start_date
              AND ft.tlog_ts < (end_date + interval '1 day')::DATE
              AND ft.trans_type_id = 4;"
            ;;
        cnr)
            SQL_QUERY="
            SELECT
                timestamp as cancel_date,
                cast(sbn_id as numeric),
                msisdn as tmuserid,
                cpc,
                mode
            FROM delaydcttlog_v1_0_0_fact
            WHERE timestamp >= start_date
              AND timestamp < (end_date + interval '1 day')::DATE;"
            ;;
        rfnd)
            SQL_QUERY="
            SELECT
                msisdn as tmuserid,
                cpc,
                timestamp::DATE as refnd_date,
                SUM(amount) as rfnd_amount,
                count(split_part(split_part(info, 'sbnId=', 2), ',', 1)) as rfnd_cnt,
                split_part(split_part(info, 'sbnId=', 2), ',', 1) AS sbnid,
                info ilike '%Automatic Refund%' as instant_rfnd
            FROM refund_v1_0_0_fact
            WHERE timestamp >= start_date
              AND timestamp < (end_date + interval '1 day')::DATE
            GROUP BY 6,1,2,3,7;"
            ;;
    esac

    # ------------------------------------------------------------------------------
    # Remote Execution
    # ------------------------------------------------------------------------------

    echo "[$(date)] Connecting to ${REMOTE_HOST}..."

    ssh ${REMOTE_USER}@${REMOTE_HOST} << EOF
    set -e
    echo "[REMOTE] Connected."

    # Create SQL Script
    cat > ${SQL_SCRIPT_PATH} << 'SQL'
    DO \$\$
    DECLARE
        start_date DATE := '${START_DATE}';
        end_date DATE := '${END_DATE}';
    BEGIN
        DROP TABLE IF EXISTS ${CURRENT_TYPE}_atlas_day;
        CREATE TABLE ${CURRENT_TYPE}_atlas_day AS
        ${SQL_QUERY}
    END \$\$;
    SQL

    echo "[REMOTE] Executing SQL..."
    ${REMOTE_PSQL} -U postgres -d ${REMOTE_DB} -f ${SQL_SCRIPT_PATH}

    echo "[REMOTE] Exporting to CSV..."
    ${REMOTE_PSQL} -U postgres -d ${REMOTE_DB} -c "COPY ${CURRENT_TYPE}_atlas_day TO '${CSV_FILE_PATH}' WITH CSV HEADER;"
EOF

    if [ $? -ne 0 ]; then
        echo "[$(date)] ERROR: Remote execution failed."
        exit 1
    fi

    # ------------------------------------------------------------------------------
    # File Transfer
    # ------------------------------------------------------------------------------

    echo "[$(date)] Downloading CSV to ${LOCAL_DATA_DIR}..."
    scp -O ${REMOTE_USER}@${REMOTE_HOST}:${CSV_FILE_PATH} ${LOCAL_DATA_DIR}

    if [ $? -eq 0 ]; then
        echo "[$(date)] SUCCESS: ${CURRENT_TYPE} data fetched."
    else
        echo "[$(date)] ERROR: Download failed."
        exit 1
    fi

done
