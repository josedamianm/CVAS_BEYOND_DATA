# CVAS Beyond Data

A production-grade ETL pipeline for telecommunications subscription data processing and analytics. This system automates the daily collection, processing, and aggregation of CVAS (Content Value Added Services) transaction data.

## Overview

This pipeline processes multiple transaction types (activations, renewals, deactivations, cancellations, refunds, and prepaid transactions) and builds comprehensive subscription views for analytics. The system runs automated daily jobs using macOS launchd for scheduled execution.

## Project Structure

```
CVAS_BEYOND_DATA/
├── 1.GET_NBS_BASE.sh              # Script 1: Downloads NBS user base data (runs 8:05 AM)
├── 2.FETCH_DAILY_DATA.sh          # Script 2: Fetches daily transaction data from Nova (runs 8:25 AM)
├── 3.PROCESS_DAILY_AND_BUILD_VIEW.sh  # Script 3: Processes daily data (runs 11:30 AM)
├── README.md                      # Comprehensive project documentation
├── requirements.txt               # Python dependencies
│
├── Scripts/                        # Python and shell processing scripts
│   ├── 00_convert_historical.py   # Historical data conversion (interactive)
│   ├── 01_aggregate_user_base.py  # User base aggregation script
│   ├── 02_fetch_remote_nova_data.sh  # Unified remote data fetching script
│   ├── 03_process_daily.py        # Daily incremental processing
│   ├── 04_build_subscription_view.py  # Subscription aggregation
│   ├── others/                    # Unused/standalone scripts
│   │   ├── check_transactions_parquet_data.py  # Transaction data validation
│   │   ├── check_subscriptions_parquet_data.py # Subscription data validation
│   │   ├── check_users.py         # Interactive subscription query tool
│   │   └── extract_music_subscriptions.py  # Music subscription extraction
│   └── utils/
│       └── log_rotation.sh        # Log rotation utility (15-day retention)
│
├── sql/                            # SQL queries
│   └── build_subscription_view.sql  # Subscription aggregation query (230+ lines)
│
├── Daily_Data/                     # Daily CSV transaction files (git-ignored)
├── Parquet_Data/                   # Columnar storage (git-ignored)
│   ├── transactions/              # Partitioned by transaction type and year_month
│   └── aggregated/                # Aggregated subscription views
│
├── User_Base/                      # User base data
│   ├── NBS_BASE/                  # Daily NBS snapshots (git-ignored)
│   ├── user_base_by_service.csv   # Aggregated by service (git-ignored)
│   └── user_base_by_category.csv  # Aggregated by category (git-ignored)
│
└── Logs/                          # Execution logs (git-ignored)
```

## Transaction Types

The pipeline processes six transaction types:

- **ACT** (Activations) - New subscriptions
- **RENO** (Renewals) - Subscription renewals  
- **DCT** (Deactivations) - Service cancellations
- **CNR** (Cancellations) - User-initiated cancellations
- **RFND** (Refunds) - Payment refunds
- **PPD** (Prepaid) - Prepaid transactions

## Technology Stack

- **Python 3.x** - Core processing language
- **Polars** - High-performance DataFrame library for CSV processing
- **DuckDB** - In-process SQL OLAP database for analytics
- **PyArrow/Parquet** - Columnar storage format with Hive partitioning
- **macOS launchd** - Job scheduling and automation

## Setup

### Prerequisites

```bash
# Install required Python packages
pip install -r requirements.txt
```

### Configuration

The scripts now use relative paths based on the project root. Key configuration:

1. **SSH Access**: Configure SSH keys for remote data retrieval (Scripts 1 & 2)
2. **Python Environment**: Ensure Python path is correct in shell scripts (default: `/opt/anaconda3/bin/python`)
3. **Remote Server**: Update `REMOTE_USER` and `REMOTE_HOST` in `Scripts/02_fetch_remote_nova_data.sh` if needed

### Directory Structure

Ensure all data directories exist:

```bash
mkdir -p Daily_Data Parquet_Data/transactions Parquet_Data/aggregated User_Base/NBS_BASE Logs
```

## Usage

### Manual Execution

Run scripts directly for testing:

```bash
# Script 1: Download NBS user base
bash 1.GET_NBS_BASE.sh

# Script 2: Fetch daily transaction data from Nova
bash 2.FETCH_DAILY_DATA.sh

# Script 3: Process daily data and build views
bash 3.PROCESS_DAILY_AND_BUILD_VIEW.sh

# Or run individual transaction types:
bash Scripts/02_fetch_remote_nova_data.sh act    # Fetch activations only
bash Scripts/02_fetch_remote_nova_data.sh all    # Fetch all types
```

### Automated Scheduling

The system uses macOS launchd for automated daily execution:

- **Script 1** runs at 8:05 AM daily (NBS user base)
- **Script 2** runs at 8:25 AM daily (Nova transaction data)
- **Script 3** runs at 11:30 AM daily (Processing and aggregation)

Each script depends on the previous one completing successfully. See `How2Manage_Launchd_Jobs.txt` for detailed scheduling management instructions.

### Processing Workflow

1. **NBS User Base Collection** (8:05 AM)
   - Downloads NBS user base from remote server
   - Aggregates user base data by service and category

2. **Transaction Data Fetch** (8:25 AM)
   - Connects to Nova PostgreSQL server via SSH
   - Executes SQL queries for all transaction types (ACT, RENO, DCT, CNR, RFND, PPD)
   - Downloads CSV files to `Daily_Data/`

3. **Data Processing & Aggregation** (11:30 AM)
   - Converts daily CSV files to Parquet format
   - Partitions data by transaction type and year_month
   - Builds aggregated subscription views with lifecycle tracking

## Data Pipeline

```
Daily CSV Files → Parquet Storage (Hive Partitioned) → Aggregated Views
```

### Key Features

- **Incremental Processing**: Daily append-only operations
- **Hive Partitioning**: Efficient querying by year_month
- **Edge Case Handling**: Missing activations, CPC upgrades, multiple CPCs per subscription
- **Comprehensive Logging**: Timestamped execution logs with error tracking
- **Data Validation**: Built-in validation and count verification

## Subscription View

The aggregated subscription view (`Scripts/04_build_subscription_view.py`) provides:

- Complete subscription lifecycle tracking
- CPC (Content Provider Code) change history
- Revenue aggregation
- Campaign and channel attribution
- Handling of missing activation records
- Upgrade detection and tracking

## Monitoring

Check execution logs:

```bash
# View latest execution logs
cat Logs/1.GET_NBS_BASE.log
cat Logs/2.FETCH_DAILY_DATA.log
cat Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log

# Or view real-time logs
tail -f Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log
```

### Log Rotation

All orchestration scripts automatically rotate their logs to keep only the last 7 days of entries. This prevents log files from growing indefinitely:

- Logs are rotated at the start of each script execution
- Only entries from the last 7 days are retained
- Older entries are automatically removed
- No manual cleanup required

## Data Validation & Query Tools

### Validation Scripts

The project includes comprehensive validation scripts to ensure data quality and monitor pipeline performance:

#### 1. Transaction Data Validation
```bash
python Scripts/check_transactions_parquet_data.py
```

Validates transaction parquet data with:
- **Daily Data Completeness**: Checks for yesterday's transactions across all types (ACT, RENO, DCT, CNR, RFND, PPD)
- **Monthly Summary**: Aggregates transaction counts and revenue by month and type
- **Data Validation**: Verifies row counts, date ranges, schema integrity, and data quality
- **Query Performance**: Tests query execution times for optimization monitoring

#### 2. Subscription Data Validation
```bash
python Scripts/check_subscriptions_parquet_data.py
```

Validates subscription aggregated data with:
- **Daily Data Completeness**: Checks for recent activations, renewals, deactivations, and cancellations
- **Monthly Summary**: Aggregates subscription metrics by month (activations, renewals, revenue, etc.)
- **Data Validation**: Verifies subscription counts, date ranges, schema, and data quality
- **Query Performance**: Tests aggregation query performance

### Interactive Query Tool

#### Subscription Query Tool
```bash
python Scripts/check_users.py
```

Interactive tool for querying subscription data with three query modes:

**Query Options:**
1. **By Subscription ID** - Query a single subscription
2. **By User ID (tmuserid)** - Query all subscriptions for a user (may return multiple)
3. **By MSISDN** - Query all subscriptions for a phone number (may return multiple)

**Output Sections:**
- **Section 1: Summary Per Subscription** - Detailed information for each subscription including:
  - Basic information (ID, user, MSISDN, status, lifetime)
  - CPC information (list, count, upgrades)
  - Activation details (dates, campaign, channel, revenue)
  - Renewal information (count, revenue, dates)
  - Termination details (deactivation, cancellation)
  - Financial summary (activation, renewal, total revenue, refunds)

- **Section 2: Aggregated Summary** - Overall statistics across all matching subscriptions:
  - Overall statistics (total subscriptions, revenue, renewals, etc.)
  - CPC breakdown by first_cpc
  - Status breakdown (Active, Deactivated, Cancelled)
  - Subscription timeline
  - Key insights with actionable information

- **Section 3: Complete Raw Data Output** - All columns from the parquet file in table format

- **Section 4: Detailed Column-by-Column Breakdown** - Each field organized by category for easy reading

**Features:**
- Interactive menu-driven interface
- Supports multiple queries in a single session
- Comprehensive output with 4 detailed sections
- Works from any directory (uses absolute paths)

**Example Usage:**
```bash
$ python Scripts/check_users.py

====================================================================================================
SUBSCRIPTION QUERY TOOL
====================================================================================================

Select query type:
  1. Query by Subscription ID (single subscription)
  2. Query by User ID (tmuserid - may return multiple subscriptions)
  3. Query by MSISDN (phone number - may return multiple subscriptions)
  0. Exit

Enter your choice (0-3): 1
Enter Subscription ID: 10151796

# ... detailed output follows ...
```

## Data Structure & Analysis

### NBS_BASE User Data Repository

The `User_Base/NBS_BASE/` directory contains daily NBS (Non-Billable Services) base data files spanning from November 2022 to present. This repository contains over 1,000 daily CSV snapshots tracking content provider services and billing information.

#### File Naming Convention

Files follow the standardized pattern: `YYYYMMDD_NBS_Base.csv`

- Example: `20221114_NBS_Base.csv` represents data from November 14, 2022
- Files are consistently named and can be sorted chronologically
- Daily files provide consistent snapshots over approximately 3 years (Nov 2022 - Present)

#### CSV Schema

Each daily NBS_Base CSV file contains the following columns:

| Column | Description |
|--------|-------------|
| `cpc` | Content Provider Code (numeric identifier) |
| `content_provider` | Provider name (e.g., "2dayuk") |
| `service_name` | Name of the service being tracked |
| `last_billed_amount` | Most recent billing amount (decimal) |
| `tme_category` | Category classification (e.g., "Light") |
| `channel_desc` | Channel description (e.g., "ACTIVE", "CCC-CR") |
| `count` | Count/quantity metric |

#### Data Analysis Commands

**Examining Data Structure:**
```bash
# View first few rows of a specific date
head -10 User_Base/NBS_BASE/20221114_NBS_Base.csv

# Count total records in a file
wc -l User_Base/NBS_BASE/20221114_NBS_Base.csv

# Check unique content providers
cut -d',' -f2 User_Base/NBS_BASE/20221114_NBS_Base.csv | sort -u

# Check date range of available data
ls -1 User_Base/NBS_BASE/ | head -1  # earliest date
ls -1 User_Base/NBS_BASE/ | tail -1  # latest date
```

**File Operations:**
```bash
# Count total files
ls -1 User_Base/NBS_BASE/ | wc -l

# Find files in a date range
ls User_Base/NBS_BASE/ | grep "202211[12]"  # November 2022

# Compare two dates (requires diff)
diff User_Base/NBS_BASE/20221114_NBS_Base.csv User_Base/NBS_BASE/20221115_NBS_Base.csv
```

#### Time Series Analysis

Data is organized chronologically, making it suitable for:
- **Daily trend analysis** - Track changes day-over-day
- **Provider activity tracking** - Monitor provider behavior over time
- **Billing amount changes** - Analyze price fluctuations
- **Service availability monitoring** - Track service status changes

#### Common Analysis Patterns

When analyzing NBS_BASE data:
1. Files can be processed sequentially by date
2. CSV format allows for easy parsing with standard tools (Python pandas, R, Excel)
3. Each file represents a complete daily snapshot
4. The `cpc` field serves as a consistent identifier for providers
5. Data continuity maintained across approximately 1,000+ daily files

#### Aggregated Outputs

The pipeline produces two aggregated views from NBS_BASE data:
- **`user_base_by_service.csv`** - User base aggregated by service
- **`user_base_by_category.csv`** - User base aggregated by category

These are generated by `01_aggregate_user_base.py` during Script 1 execution.

---

## Scheduled Job Management (Launchd)

### Overview

The CVAS pipeline runs automatically via macOS Launchd scheduled jobs:

| Script | Run Time | Launchd Job | Dependencies |
|--------|----------|-------------|--------------|
| **Script 1:** `1.GET_NBS_BASE.sh` | 8:05 AM | `com.josemanco.nbs_base` | None |
| **Script 2:** `2.FETCH_DAILY_DATA.sh` | 8:25 AM | `com.josemanco.fetch_daily` | Script 1 must complete first |
| **Script 3:** `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` | 11:30 AM | `com.josemanco.process_daily` | Script 2 must complete first |

### Change Scheduled Time

Replace `<job>` with: `nbs_base`, `fetch_daily`, or `process_daily`

1. **Edit the plist:**
   ```bash
   nvim /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist
   ```

2. **Change Hour (0-23) and Minute (0-59):**
   ```xml
   <key>Hour</key>
   <integer>8</integer>     <!-- Change this -->
   <key>Minute</key>
   <integer>5</integer>     <!-- Change this -->
   ```

3. **Reload the job:**
   ```bash
   launchctl unload /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist && \
   launchctl load /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist
   ```

### Test Manually (Without Changing Schedule)

**Method 1: Run script directly**
```bash
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/1.GET_NBS_BASE.sh
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/2.FETCH_DAILY_DATA.sh
bash /Users/josemanco/CVAS/CVAS_BEYOND_DATA/3.PROCESS_DAILY_AND_BUILD_VIEW.sh
```

**Method 2: Trigger via launchctl (same environment as scheduled run)**
```bash
launchctl start com.josemanco.nbs_base
launchctl start com.josemanco.fetch_daily
launchctl start com.josemanco.process_daily
```

**Check logs:**
```bash
cat /Users/josemanco/CVAS/CVAS_BEYOND_DATA/Logs/1.GET_NBS_BASE.log
cat /Users/josemanco/CVAS/CVAS_BEYOND_DATA/Logs/2.FETCH_DAILY_DATA.log
cat /Users/josemanco/CVAS/CVAS_BEYOND_DATA/Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log
```

### Useful Commands

Replace `<job>` with: `nbs_base`, `fetch_daily`, or `process_daily`

```bash
# Check if job is loaded
launchctl list | grep com.josemanco.<job>

# View job details and next run time
launchctl print gui/$(id -u)/com.josemanco.<job>

# View last run status
launchctl print gui/$(id -u)/com.josemanco.<job> | grep -E "last exit|state"

# Unload (stop) job
launchctl unload /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist

# Load (start) job
launchctl load /Users/josemanco/Library/LaunchAgents/com.josemanco.<job>.plist

# View real-time log
tail -f /Users/josemanco/CVAS/CVAS_BEYOND_DATA/Logs/<log-file>.log

# Check all jobs
launchctl list | grep josemanco

# Reload all jobs after changes
launchctl unload /Users/josemanco/Library/LaunchAgents/com.josemanco.*.plist && \
launchctl load /Users/josemanco/Library/LaunchAgents/com.josemanco.*.plist
```

### Common Issues

| Issue | Solution |
|-------|----------|
| "command not found" | Use absolute paths in scripts (e.g., `/opt/anaconda3/bin/python`) |
| Job not running | Check: `launchctl list \| grep <job>` |
| Permission denied | `chmod +x <script-path>` |
| Works manually but fails in launchd | Use absolute paths, check PATH in plist |

---

## Scripts Usage Audit

### Active Pipeline Scripts

These scripts are **actively used** in the daily data pipeline orchestration:

#### Core Pipeline Scripts

| Script | Used By | Purpose |
|--------|---------|---------|
| **01_aggregate_user_base.py** | `1.GET_NBS_BASE.sh` (line 113) | Aggregates user base data from multiple CSV files |
| **02_fetch_remote_nova_data.sh** | `2.FETCH_DAILY_DATA.sh` (line 57) | Fetches transaction data from remote PostgreSQL server |
| **03_process_daily.py** | `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` (line 83) | Converts daily CSV files to Parquet format |
| **04_build_subscription_view.py** | `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` (line 105) | Builds comprehensive subscription view in DuckDB |

#### Utility Scripts

| Script | Used By | Purpose |
|--------|---------|---------|
| **utils/log_rotation.sh** | All 3 orchestration scripts | Log rotation utility (15-day retention) |

### Unused/Standalone Scripts (in Scripts/others/)

These scripts are **NOT referenced** in the main orchestration pipeline and have been organized into the `Scripts/others/` subfolder:

#### Testing/Validation Scripts

| Script | Type | Likely Purpose |
|--------|------|----------------|
| **check_subscriptions_parquet_data.py** | Validation | Manual testing tool to verify subscription Parquet data integrity |
| **check_transactions_parquet_data.py** | Validation | Manual testing tool to verify transaction Parquet data integrity |
| **check_users.py** | Validation | Manual testing tool to verify user data |

#### Analysis/Extract Scripts

| Script | Type | Likely Purpose |
|--------|------|----------------|
| **extract_music_subscriptions.py** | Extract/Analysis | Specialized script to extract music-specific subscription data |

#### Historical/One-Time Scripts

| Script | Type | Likely Purpose |
|--------|------|----------------|
| **00_convert_historical.py** | Historical | One-time conversion script for historical data migration (kept in Scripts/ root) |

### Usage Summary

| Category | Count | Scripts |
|----------|-------|---------|
| **Active Pipeline** | 4 | 01_aggregate_user_base.py, 02_fetch_remote_nova_data.sh, 03_process_daily.py, 04_build_subscription_view.py |
| **Active Utils** | 1 | utils/log_rotation.sh |
| **Unused (in others/)** | 4 | check_subscriptions_parquet_data.py, check_transactions_parquet_data.py, check_users.py, extract_music_subscriptions.py |
| **Historical (kept)** | 1 | 00_convert_historical.py |
| **TOTAL** | 10 | - |

### Pipeline Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ SCRIPT 1: 1.GET_NBS_BASE.sh                                          │
├─────────────────────────────────────────────────────────────────────┤
│ 1. Download NBS_Base.csv from remote server (SCP)                   │
│ 2. Validate file                                                     │
│ 3. Run: 01_aggregate_user_base.py  ← ACTIVE SCRIPT                  │
│    → Processes User_Base/NBS_BASE/*.csv files                        │
│    → Outputs aggregated user base data                               │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ SCRIPT 2: 2.FETCH_DAILY_DATA.sh                                      │
├─────────────────────────────────────────────────────────────────────┤
│ For each transaction type (ACT, RENO, DCT, PPD, CNR, RFND):         │
│ → Run: 02_fetch_remote_nova_data.sh <type> <date>  ← ACTIVE SCRIPT  │
│    → Connects to PostgreSQL server via SSH                           │
│    → Fetches transaction data                                        │
│    → Saves to Nova_Data/YYYY-MM-DD/<type>.csv                        │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ SCRIPT 3: 3.PROCESS_DAILY_AND_BUILD_VIEW.sh                          │
├─────────────────────────────────────────────────────────────────────┤
│ 1. Validate 6 CSV files exist                                        │
│ 2. Run: 03_process_daily.py <date>  ← ACTIVE SCRIPT                 │
│    → Converts CSV files to Parquet format                            │
│    → Saves to Parquet_Data/transactions/<type>/                      │
│ 3. Run: 04_build_subscription_view.py  ← ACTIVE SCRIPT               │
│    → Loads all Parquet files                                         │
│    → Builds comprehensive subscription view                          │
│    → Saves to Parquet_Data/subscriptions/subscription_data.parquet   │
└─────────────────────────────────────────────────────────────────────┘
```

### Scripts Folder Organization

```
Scripts/
├── 00_convert_historical.py       [Historical - kept in root]
├── 01_aggregate_user_base.py      [Active - Pipeline]
├── 02_fetch_remote_nova_data.sh   [Active - Pipeline]
├── 03_process_daily.py            [Active - Pipeline]
├── 04_build_subscription_view.py  [Active - Pipeline]
├── others/                        [Unused/standalone scripts]
│   ├── check_subscriptions_parquet_data.py
│   ├── check_transactions_parquet_data.py
│   ├── check_users.py
│   └── extract_music_subscriptions.py
└── utils/
    └── log_rotation.sh            [Active - Utility]
```

**Key Points:**
- ✅ **Active pipeline scripts** remain in `Scripts/` root for easy access
- ✅ **Unused validation/analysis scripts** moved to `Scripts/others/`
- ✅ **Historical script** (`00_convert_historical.py`) kept in root per requirement
- ✅ **Utils folder** contains shared utilities
- ✅ **No impact on pipeline** - All orchestration scripts continue to work

**Scripts in `others/` folder:**
- Available when needed for debugging/analysis
- Not called by any orchestration scripts
- Can be used manually as needed

**Historical script:**
- `00_convert_historical.py` kept in Scripts/ root
- Available for future historical data conversions if needed

### Verification Commands

```bash
# Verify Scripts folder structure
ls -la Scripts/
ls -la Scripts/others/
ls -la Scripts/utils/

# Find all script references in orchestration scripts
grep -E "\.py|\.sh" *.sh | grep -v "^#"

# Check if any Python scripts import others
grep -r "^import\|^from" Scripts/*.py | grep "Scripts/"

# List all executable scripts
find Scripts/ -type f -perm +111
```

### Change Log

**Last Updated:** December 15, 2025
**Status:** Reorganization and renaming complete ✅

- **2025-12-15:** Renamed scripts for better pipeline sequence clarity
  - `aggregate_user_base.py` → `01_aggregate_user_base.py` (now runs before 02_fetch_remote_nova_data.sh)
  - `01_convert_historical.py` → `00_convert_historical.py` (not part of regular pipeline)
- **2025-12-15:** Moved 4 unused scripts to `Scripts/others/` folder
  - check_subscriptions_parquet_data.py
  - check_transactions_parquet_data.py
  - check_users.py
  - extract_music_subscriptions.py

---

## Notes

- Scripts run sequentially: Script 1 → Script 2 → Script 3
- All scripts use relative paths for portability
- Data files are excluded from Git (see `.gitignore`)
- SSH key authentication required for remote data access (Scripts 1 & 2)
- Cross-platform date handling (supports both macOS and Linux)
- Historical data conversion (`00_convert_historical.py`) is interactive and prompts for data path
- Validation scripts should be run regularly to ensure data quality
- Query tool provides instant access to subscription details for troubleshooting

## License

Internal use only - Proprietary

## Author

Jose Manco
