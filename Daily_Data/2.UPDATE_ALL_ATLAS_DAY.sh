#!/bin/bash

# Directory containing the scripts
SCRIPT_DIR="/BEYOND_DATA_OLD/Scripts_Bash"

# Array of script names
SCRIPTS=(
    "update_act_atlas_day.sh"
    "update_cnr_atlas_day.sh"
    "update_dct_atlas_day.sh"
    "update_ppd_atlas_day.sh"
    "update_reno_atlas_day.sh"
    "update_rfnd_atlas_day.sh"
    "update_subs_base.sh"
    "update_vp_subs.sh"
)

# Log file
LOG_FILE="/BEYOND_DATA_OLD/LOGS/run_all_scripts.log"

# Start logging
exec > >(tee -a ${LOG_FILE}) 2>&1
echo "[$(date)] Starting execution of all scripts."

# Change to the script directory
cd ${SCRIPT_DIR} || { echo "[$(date)] Failed to change directory to ${SCRIPT_DIR}." >&2; exit 1; }

# Run each script
for SCRIPT in "${SCRIPTS[@]}"; do
    echo "[$(date)] Running ${SCRIPT}..."
    ./${SCRIPT}
    
    # Check if the script was successful
    if [ $? -ne 0 ]; then
        echo "[$(date)] Error occurred while running ${SCRIPT}." >&2
        exit 1
    else
        echo "[$(date)] ${SCRIPT} completed successfully."
    fi
done

echo "[$(date)] All scripts executed successfully."
