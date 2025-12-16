# CVAS Beyond Data Pipeline

## Table of Contents
- [Project Description](#project-description)
- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Directory Structure](#directory-structure)
- [Pipeline Workflow](#pipeline-workflow)
- [Installation & Setup](#installation--setup)
- [Scheduled Automation](#scheduled-automation)
- [Manual Execution](#manual-execution)
- [Monitoring & Logs](#monitoring--logs)
- [Data Schema](#data-schema)
- [Troubleshooting](#troubleshooting)

---

## Project Description

**CVAS Beyond Data** is a production-grade ETL (Extract, Transform, Load) pipeline designed for telecommunications subscription data processing and analytics. The system automates the daily collection, processing, and aggregation of CVAS (Content Value Added Services) transaction data from a remote PostgreSQL database.

### Purpose
Process and analyze telecommunication subscription lifecycle data by:
- Extracting 6 transaction types from remote PostgreSQL servers
- Transforming CSV data into optimized Parquet columnar format
- Building comprehensive subscription views for business analytics
- Aggregating user base information from daily snapshots

### Key Features
- **Automated Daily Execution**: Runs via macOS launchd scheduler (3 scheduled jobs)
- **Sequential Pipeline**: 3-stage orchestration ensuring data consistency
- **Columnar Storage**: Parquet format with Hive partitioning for efficient querying
- **User Base Tracking**: Processes 1100+ daily snapshots of subscriber data
- **Remote Data Fetching**: Secure SSH/SCP connection to production database
- **Transaction Processing**: Handles 6 transaction types (ACT, RENO, DCT, CNR, RFND, PPD)
- **Log Management**: 15-day rotation with detailed execution tracking

---

## Architecture Overview

### Three-Stage Pipeline

```
┌────────────────────────────────────────────────────────────────┐
│ STAGE 1: USER BASE COLLECTION (8:05 AM)                        │
│ Script: 1.GET_NBS_BASE.sh                                      │
├────────────────────────────────────────────────────────────────┤
│ 1. Download NBS_Base.csv from remote server via SCP           │
│ 2. Validate downloaded file integrity                         │
│ 3. Execute: Scripts/01_aggregate_user_base.py                 │
│    → Process 1100+ CSV files in User_Base/NBS_BASE/           │
│    → Generate: user_base_by_service.csv                       │
│    → Generate: user_base_by_category.csv                      │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STAGE 2: TRANSACTION DATA FETCH (8:25 AM)                      │
│ Script: 2.FETCH_DAILY_DATA.sh                                  │
├────────────────────────────────────────────────────────────────┤
│ For each transaction type (6 types):                          │
│ → Execute: Scripts/02_fetch_remote_nova_data.sh <type> <date> │
│    → Connect to PostgreSQL via SSH tunnel                     │
│    → Query transaction data for specified date                │
│    → Save to Daily_Data/<date>/<TYPE>.csv                     │
│                                                                │
│ Transaction Types:                                            │
│ • ACT  - Activations (new subscriptions + upgrades)          │
│ • RENO - Renewals (subscription renewals)                    │
│ • DCT  - Deactivations (service cancellations)               │
│ • CNR  - Cancellations (user-initiated)                      │
│ • RFND - Refunds (payment refunds)                           │
│ • PPD  - Prepaid (prepaid transactions)                      │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STAGE 3: PROCESSING & AGGREGATION (11:30 AM)                   │
│ Script: 3.PROCESS_DAILY_AND_BUILD_VIEW.sh                      │
├────────────────────────────────────────────────────────────────┤
│ Step 1: Validate all 6 CSV files exist                        │
│                                                                │
│ Step 2: Execute Scripts/03_process_daily.py <date>            │
│    → Convert CSV to Parquet format                            │
│    → Apply Hive partitioning (year_month=YYYY-MM)             │
│    → Save to Parquet_Data/transactions/<type>/                │
│                                                                │
│ Step 3: Execute Scripts/04_build_subscription_view.py         │
│    → Load all Parquet transaction files                       │
│    → Execute 241-line DuckDB SQL query                        │
│    → Build comprehensive subscription lifecycle view          │
│    → Save to Parquet_Data/aggregated/subscriptions.parquet    │
└────────────────────────────────────────────────────────────────┘
```

### Data Flow Diagram

```
Remote PostgreSQL ──SSH──> Daily_Data (CSV) ──Python──> Parquet_Data (Columnar)
                                                                  │
NBS Server ──SCP──> User_Base/NBS_BASE ──Python──> user_base_by_*.csv
                                                                  │
                                                                  ↓
                                                    DuckDB Aggregation
                                                                  ↓
                                              subscriptions.parquet (Final View)
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.x | Data processing and transformation |
| **Data Processing** | Polars | High-performance DataFrame operations |
| **Database** | DuckDB | In-process analytical queries |
| **Storage Format** | Parquet + Hive Partitioning | Columnar storage for efficient analytics |
| **Data Transfer** | PyArrow, Pandas | Serialization and compatibility |
| **Orchestration** | Bash Shell Scripts | Pipeline coordination |
| **Scheduling** | macOS launchd | Automated daily execution |
| **Remote Access** | SSH/SCP | Secure data retrieval |

---

## Directory Structure

```
CVAS_BEYOND_DATA/                         # Project root
│
├── 1.GET_NBS_BASE.sh                     # Stage 1 orchestrator
├── 2.FETCH_DAILY_DATA.sh                 # Stage 2 orchestrator
├── 3.PROCESS_DAILY_AND_BUILD_VIEW.sh     # Stage 3 orchestrator
├── MASTERCPC.csv                         # Reference: Service/CPC mapping table
├── requirements.txt                      # Python dependencies
├── README.md                             # This file
│
├── Scripts/                              # Core processing scripts
│   ├── 01_aggregate_user_base.py         # [ACTIVE] User base aggregation
│   ├── 02_fetch_remote_nova_data.sh      # [ACTIVE] Remote data fetcher
│   ├── 03_process_daily.py               # [ACTIVE] CSV to Parquet converter
│   ├── 04_build_subscription_view.py     # [ACTIVE] Subscription aggregator
│   ├── 00_convert_historical.py          # [HISTORICAL] One-time conversion
│   │
│   ├── utils/                            # Utility scripts
│   │   └── log_rotation.sh               # 15-day log retention manager
│   │
│   └── others/                           # Testing & validation scripts
│       ├── check_transactions_parquet_data.py
│       ├── check_subscriptions_parquet_data.py
│       ├── check_users.py
│       └── extract_music_subscriptions.py
│
├── sql/                                  # SQL queries
│   └── build_subscription_view.sql       # 241-line DuckDB aggregation query
│
├── Daily_Data/                           # [GIT-IGNORED] Temporary CSV staging
│   └── YYYY-MM-DD/                       # Daily folders
│       ├── ACT.csv
│       ├── RENO.csv
│       ├── DCT.csv
│       ├── CNR.csv
│       ├── RFND.csv
│       └── PPD.csv
│
├── Parquet_Data/                         # [GIT-IGNORED] Columnar storage
│   ├── transactions/                     # Partitioned by transaction type
│   │   ├── act/year_month=YYYY-MM/*.parquet
│   │   ├── reno/year_month=YYYY-MM/*.parquet
│   │   ├── dct/year_month=YYYY-MM/*.parquet
│   │   ├── cnr/year_month=YYYY-MM/*.parquet
│   │   ├── ppd/year_month=YYYY-MM/*.parquet
│   │   └── rfnd/year_month=__HIVE_DEFAULT_PARTITION__/*.parquet
│   │
│   └── aggregated/                       # Final processed data
│       └── subscriptions.parquet         # Comprehensive subscription view
│
├── User_Base/                            # User base data
│   ├── NBS_BASE/                         # [GIT-IGNORED] 1100+ daily snapshots
│   ├── user_base_by_service.csv          # [GIT-IGNORED] Aggregated by service
│   └── user_base_by_category.csv         # [GIT-IGNORED] Aggregated by category
│
└── Logs/                                 # [GIT-IGNORED] Execution logs
    ├── 1.GET_NBS_BASE.log
    ├── 2.FETCH_DAILY_DATA.log
    └── 3.PROCESS_DAILY_AND_BUILD_VIEW.log
```

### File Descriptions

#### Orchestration Scripts (Root Level)
| File | Purpose | Runs At | Dependencies |
|------|---------|---------|--------------|
| `1.GET_NBS_BASE.sh` | Downloads and aggregates user base | 8:05 AM | None |
| `2.FETCH_DAILY_DATA.sh` | Fetches 6 transaction types | 8:25 AM | Script 1 must complete |
| `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` | Processes and builds views | 11:30 AM | Script 2 must complete |

#### Active Pipeline Scripts (Scripts/)
| Script | Called By | Purpose |
|--------|-----------|---------|
| `01_aggregate_user_base.py` | Script 1 (line 113) | Aggregates User_Base/NBS_BASE/*.csv files |
| `02_fetch_remote_nova_data.sh` | Script 2 (line 57) | Connects to PostgreSQL, fetches transactions |
| `03_process_daily.py` | Script 3 (line 83) | Converts CSV → Parquet with partitioning |
| `04_build_subscription_view.py` | Script 3 (line 105) | Builds final subscription view in DuckDB |

#### Utility Scripts
| Script | Purpose |
|--------|---------|
| `utils/log_rotation.sh` | Deletes logs older than 15 days |

#### Testing & Validation Scripts (Scripts/others/)
| Script | Purpose |
|--------|---------|
| `check_transactions_parquet_data.py` | Validates transaction Parquet integrity |
| `check_subscriptions_parquet_data.py` | Validates subscription Parquet integrity |
| `check_users.py` | Validates user data quality |
| `extract_music_subscriptions.py` | Extracts music-specific subscriptions |

---

## Pipeline Workflow

### Stage 1: User Base Collection (1.GET_NBS_BASE.sh)

```bash
START
  ↓
Log rotation (delete logs > 15 days)
  ↓
Download NBS_Base.csv from remote server
  ↓
Validate file exists and has content
  ↓
Execute: Scripts/01_aggregate_user_base.py
  ├─ Read all CSV files in User_Base/NBS_BASE/
  ├─ Aggregate by service type
  ├─ Aggregate by category
  ├─ Output: user_base_by_service.csv
  └─ Output: user_base_by_category.csv
  ↓
Log completion timestamp
  ↓
END
```

### Stage 2: Transaction Data Fetch (2.FETCH_DAILY_DATA.sh)

```bash
START
  ↓
Log rotation (delete logs > 15 days)
  ↓
Set DATE (defaults to yesterday)
  ↓
Create directory: Daily_Data/YYYY-MM-DD/
  ↓
FOR EACH transaction type (ACT, RENO, DCT, PPD, CNR, RFND):
  ├─ Execute: Scripts/02_fetch_remote_nova_data.sh <TYPE> <DATE>
  │   ├─ SSH to remote PostgreSQL server
  │   ├─ Execute SQL query for transaction type
  │   ├─ Save result to Daily_Data/YYYY-MM-DD/<TYPE>.csv
  │   └─ Log row count and status
  └─ Continue to next type
  ↓
Validate all 6 CSV files exist
  ↓
Log completion timestamp
  ↓
END
```

### Stage 3: Processing & Aggregation (3.PROCESS_DAILY_AND_BUILD_VIEW.sh)

```bash
START
  ↓
Log rotation (delete logs > 15 days)
  ↓
Set DATE (defaults to yesterday)
  ↓
Validate 6 CSV files exist in Daily_Data/YYYY-MM-DD/
  ↓
Execute: Scripts/03_process_daily.py <DATE>
  ├─ Read each CSV file
  ├─ Convert to Parquet format
  ├─ Apply Hive partitioning (year_month=YYYY-MM)
  ├─ Save to Parquet_Data/transactions/<type>/
  └─ Log processing statistics
  ↓
Execute: Scripts/04_build_subscription_view.py
  ├─ Load all Parquet transaction files
  ├─ Execute sql/build_subscription_view.sql (241 lines)
  ├─ Build comprehensive subscription lifecycle view
  │   ├─ Join transactions with user base
  │   ├─ Calculate subscription metrics
  │   ├─ Aggregate revenue data
  │   └─ Compute lifecycle statistics
  └─ Save to Parquet_Data/aggregated/subscriptions.parquet
  ↓
Log completion timestamp
  ↓
END
```

---

## Installation & Setup

### Prerequisites

- macOS (for launchd scheduling)
- Python 3.x
- SSH access to remote PostgreSQL server
- Sufficient disk space for data storage

### Installation Steps

1. **Clone/Navigate to Project Directory**
   ```bash
   cd /Users/josemanco/CVAS/CVAS_BEYOND_DATA
   ```

2. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify SSH Access**
   ```bash
   ssh user@remote-server
   ```

4. **Create Required Directories**
   ```bash
   mkdir -p Daily_Data Parquet_Data/transactions Parquet_Data/aggregated User_Base/NBS_BASE Logs
   ```

5. **Set Script Permissions**
   ```bash
   chmod +x *.sh
   chmod +x Scripts/*.sh
   chmod +x Scripts/utils/*.sh
   ```

---

## Scheduled Automation

### Launchd Configuration

The pipeline runs automatically via macOS launchd with 3 scheduled jobs:

| Job ID | Script | Schedule | Purpose |
|--------|--------|----------|---------|
| `com.josemanco.nbs_base` | `1.GET_NBS_BASE.sh` | 8:05 AM daily | User base collection |
| `com.josemanco.fetch_daily` | `2.FETCH_DAILY_DATA.sh` | 8:25 AM daily | Transaction data fetch |
| `com.josemanco.process_daily` | `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` | 11:30 AM daily | Processing & aggregation |

### Modify Schedule

Replace `<job>` with: `nbs_base`, `fetch_daily`, or `process_daily`

1. **Edit the plist file:**
   ```bash
   nvim /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist
   ```

2. **Change hour (0-23) and minute (0-59):**
   ```xml
   <key>Hour</key>
   <integer>8</integer>     <!-- Change this -->
   <key>Minute</key>
   <integer>5</integer>     <!-- Change this -->
   ```

3. **Reload the job:**
   ```bash
   launchctl unload /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist
   launchctl load /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist
   ```

### Useful Launchd Commands

```bash
# Check if job is loaded
launchctl list | grep com.josemanco.<job>

# View job details and next run time
launchctl print gui/$(id -u)/com.josemanco.<job>

# View last run status
launchctl print gui/$(id -u)/com.josemanco.<job> | grep -E "last exit|state"

# Manually trigger job (same environment as scheduled run)
launchctl start com.josemanco.<job>

# Unload (stop) job
launchctl unload /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist

# Load (start) job
launchctl load /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist

# Check all CVAS jobs
launchctl list | grep josemanco
```

---

## Manual Execution

### Run Individual Scripts

```bash
# Stage 1: User Base Collection
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/1.GET_NBS_BASE.sh

# Stage 2: Transaction Data Fetch
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/2.FETCH_DAILY_DATA.sh

# Stage 3: Processing & Aggregation
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/3.PROCESS_DAILY_AND_BUILD_VIEW.sh
```

### Run Specific Date

```bash
# Fetch data for specific date
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/2.FETCH_DAILY_DATA.sh 2024-01-15

# Process specific date
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/3.PROCESS_DAILY_AND_BUILD_VIEW.sh 2024-01-15
```

### Run Individual Components

```bash
# Aggregate user base only
/opt/anaconda3/bin/python Scripts/01_aggregate_user_base.py

# Fetch single transaction type
bash Scripts/02_fetch_remote_nova_data.sh ACT 2024-01-15

# Process daily data
/opt/anaconda3/bin/python Scripts/03_process_daily.py 2024-01-15

# Build subscription view
/opt/anaconda3/bin/python Scripts/04_build_subscription_view.py
```

---

## Monitoring & Logs

### Log Files Location

```
Logs/
├── 1.GET_NBS_BASE.log                    # Stage 1 execution log
├── 2.FETCH_DAILY_DATA.log                # Stage 2 execution log
└── 3.PROCESS_DAILY_AND_BUILD_VIEW.log    # Stage 3 execution log
```

### View Logs

```bash
# View full log
cat Logs/1.GET_NBS_BASE.log

# View last 50 lines
tail -n 50 Logs/2.FETCH_DAILY_DATA.log

# Real-time monitoring
tail -f Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log

# Search for errors
grep -i error Logs/*.log

# Check today's execution
grep "$(date +%Y-%m-%d)" Logs/*.log
```

### Log Rotation

- **Retention Period:** 15 days
- **Managed By:** `Scripts/utils/log_rotation.sh`
- **Executed:** At the start of each orchestration script
- **Command:** `find Logs/ -name "*.log" -mtime +15 -delete`

---

## Data Schema

### Transaction Types

| Code | Name | Description | Has Revenue | Volume |
|------|------|-------------|-------------|--------|
| **ACT** | Activations | New subscriptions + upgrades | ✅ Yes | High |
| **RENO** | Renewals | Subscription renewals | ✅ Yes | Highest |
| **DCT** | Deactivations | Service cancellations | ❌ No | Medium |
| **CNR** | Cancellations | User-initiated cancellations | ❌ No | Low |
| **RFND** | Refunds | Payment refunds | ✅ Yes (negative) | Low |
| **PPD** | Prepaid | Prepaid transactions | ✅ Yes | Medium |

### Key Data Files

| File | Format | Size | Purpose |
|------|--------|------|---------|
| `MASTERCPC.csv` | CSV | ~500 KB | Service/CPC mapping reference |
| `user_base_by_service.csv` | CSV | ~2 MB | User base aggregated by service |
| `user_base_by_category.csv` | CSV | ~1 MB | User base aggregated by category |
| `subscriptions.parquet` | Parquet | Varies | Final comprehensive subscription view |

### Parquet Partitioning Strategy

- **Method:** Hive-style partitioning
- **Partition Key:** `year_month=YYYY-MM`
- **Structure:** `Parquet_Data/transactions/<type>/year_month=YYYY-MM/*.parquet`
- **Benefits:**
  - Faster query performance (partition pruning)
  - Organized data by time period
  - Efficient storage management
  - Easy data lifecycle management

---

## Troubleshooting

### Common Issues

| Issue | Possible Cause | Solution |
|-------|---------------|----------|
| **"command not found"** | launchd PATH issues | Use absolute paths (e.g., `/opt/anaconda3/bin/python`) |
| **Job not running** | Job not loaded in launchd | `launchctl list \| grep <job>` |
| **Permission denied** | Script not executable | `chmod +x <script-path>` |
| **SSH connection failed** | SSH keys not configured | Set up SSH key authentication |
| **Missing CSV files** | Stage 2 incomplete | Check `2.FETCH_DAILY_DATA.log` for errors |
| **Parquet write failed** | Disk space insufficient | Check disk space: `df -h` |
| **Works manually but fails in launchd** | Environment differences | Add absolute paths in scripts |

### Validation Commands

```bash
# Check pipeline status
launchctl list | grep josemanco

# Verify data exists
ls -lh Daily_Data/$(date +%Y-%m-%d)/
ls -lh Parquet_Data/transactions/act/

# Count records in Parquet
python -c "import polars as pl; print(pl.read_parquet('Parquet_Data/aggregated/subscriptions.parquet').shape)"

# Verify all 6 transaction types
for type in ACT RENO DCT PPD CNR RFND; do
    echo -n "$type: "
    wc -l "Daily_Data/$(date -v-1d +%Y-%m-%d)/$type.csv"
done

# Check disk usage
du -sh Parquet_Data/
du -sh User_Base/NBS_BASE/
```

### Emergency Recovery

```bash
# Stop all jobs
launchctl unload /Users/josemanco/Library/LaunchAgents/com.josemanco.*.plist

# Clear problematic data
rm -rf Daily_Data/$(date +%Y-%m-%d)/

# Rerun specific stage
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/2.FETCH_DAILY_DATA.sh $(date +%Y-%m-%d)

# Restart all jobs
launchctl load /Users/josemanco/Library/LaunchAgents/com.josemanco.*.plist
```

### Debug Mode

Enable verbose logging by editing scripts:

```bash
# At the top of any .sh script, add:
set -x  # Enable debug mode
set -e  # Exit on error
```

---

## Project Maintenance

### Regular Tasks

| Task | Frequency | Command |
|------|-----------|---------|
| **Check logs** | Daily | `tail -100 Logs/*.log` |
| **Monitor disk space** | Weekly | `du -sh Parquet_Data/ User_Base/` |
| **Verify data quality** | Weekly | Run validation scripts in `Scripts/others/` |
| **Review job status** | Daily | `launchctl list \| grep josemanco` |
| **Archive old data** | Monthly | Move old Parquet partitions to archive |

### Data Retention Policy

- **Daily CSV Files:** Deleted after Parquet conversion (manual cleanup)
- **Logs:** 15-day retention (automatic)
- **Parquet Data:** Indefinite (manual archive when needed)
- **User Base Snapshots:** All snapshots retained (1100+ files)

---

## Notes for LLMs

This project follows these conventions:

1. **Sequential Execution Required:** Scripts 1 → 2 → 3 must run in order
2. **All 6 Transaction Types Required:** Pipeline fails if any CSV is missing
3. **Absolute Paths in Automation:** launchd requires full paths (e.g., `/opt/anaconda3/bin/python`)
4. **Hive Partitioning:** All Parquet files use `year_month=YYYY-MM` partitioning
5. **No Manual Directory Changes:** Structure is fixed, scripts use relative paths from project root
6. **15-Day Log Retention:** Logs auto-delete after 15 days
7. **User Base Aggregation:** Processes 1100+ daily snapshots into 2 summary CSV files
8. **DuckDB for Analytics:** Final subscription view built with 241-line SQL query
9. **Columnar Storage:** All analytics data stored as Parquet for performance
10. **macOS Specific:** Uses launchd (macOS scheduling system), not cron

### Key Directories
- **Source of Truth:** `Parquet_Data/aggregated/subscriptions.parquet`
- **Reference Data:** `MASTERCPC.csv` (service/CPC mapping)
- **Active Scripts:** `Scripts/01_*.py`, `Scripts/02_*.sh`, `Scripts/03_*.py`, `Scripts/04_*.py`
- **Temporary Data:** `Daily_Data/` (CSV files, can be deleted after processing)

---

**Project Maintained By:** Jose Manco  
**Project Path:** `/Users/josemanco/CVAS/CVAS_BEYOND_DATA`  
**Last Updated:** 2024
