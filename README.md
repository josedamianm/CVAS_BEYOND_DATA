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
├── How2Manage_Launchd_Jobs.txt    # Documentation for managing scheduled jobs
├── requirements.txt               # Python dependencies
│
├── Scripts/                        # Python and shell processing scripts
│   ├── 01_convert_historical.py   # Historical data conversion (interactive)
│   ├── 02_fetch_remote_nova_data.sh  # Unified remote data fetching script
│   ├── 03_process_daily.py        # Daily incremental processing
│   ├── 04_build_subscription_view.py  # Subscription aggregation
│   ├── aggregate_user_base.py     # User base aggregation script
│   ├── check_transactions_parquet_data.py  # Transaction data validation & performance testing
│   ├── check_subscriptions_parquet_data.py # Subscription data validation & performance testing
│   ├── check_users.py             # Interactive subscription query tool
│   └── extract_music_subscriptions.py  # Music subscription extraction
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

## Notes

- Scripts run sequentially: Script 1 → Script 2 → Script 3
- All scripts use relative paths for portability
- Data files are excluded from Git (see `.gitignore`)
- SSH key authentication required for remote data access (Scripts 1 & 2)
- Cross-platform date handling (supports both macOS and Linux)
- Historical data conversion (`01_convert_historical.py`) is interactive and prompts for data path
- Validation scripts should be run regularly to ensure data quality
- Query tool provides instant access to subscription details for troubleshooting

## License

Internal use only - Proprietary

## Author

Jose Manco
