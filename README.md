# CVAS Beyond Data

A production-grade ETL pipeline for telecommunications subscription data processing and analytics. This system automates the daily collection, processing, and aggregation of CVAS (Content Value Added Services) transaction data.

## Overview

This pipeline processes multiple transaction types (activations, renewals, deactivations, cancellations, refunds, and prepaid transactions) and builds comprehensive subscription views for analytics. The system runs automated daily jobs using macOS launchd for scheduled execution.

## Project Structure

```
CVAS_BEYOND_DATA/
├── 1.GET_NBS_BASE.sh              # Script 1: Downloads NBS user base data (runs 8:05 AM)
├── 2.PROCESS_DAILY_AND_BUILD_VIEW.sh  # Script 2: Processes daily data (runs 11:30 AM)
├── How2Manage_Launchd_Jobs.txt    # Documentation for managing scheduled jobs
│
├── Scripts/                        # Python processing scripts
│   ├── 01_convert_historical.py   # Initial historical data conversion
│   ├── 02_process_daily.py        # Daily incremental processing
│   ├── 03_validate_data.py        # Data quality validation
│   ├── 04_test_queries.py         # Query testing utilities
│   ├── 05_build_subscription_view.py  # Subscription aggregation
│   ├── check_Counts.py            # Data count verification
│   └── test_install.py            # Environment testing
│
├── Daily_Data/                     # Daily CSV transaction files (git-ignored)
├── Historical_Data/                # Historical CSV data (git-ignored)
├── Parquet_Data/                   # Columnar storage (git-ignored)
│   ├── transactions/              # Partitioned by transaction type and year_month
│   └── aggregated/                # Aggregated subscription views
│
├── User_Base/                      # User base data
│   ├── NBS_BASE/                  # Daily NBS snapshots (git-ignored)
│   └── aggregate_user_base.py     # User base aggregation script
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
pip install polars duckdb pyarrow
```

### Configuration

The scripts use absolute paths that need to be updated for your environment:

1. Update paths in shell scripts (`1.GET_NBS_BASE.sh`, `2.PROCESS_DAILY_AND_BUILD_VIEW.sh`)
2. Update paths in Python scripts (look for `/Users/josemanco/...` references)
3. Configure SSH access for remote data retrieval (Script 1)

### Directory Structure

Ensure all data directories exist:

```bash
mkdir -p Daily_Data Historical_Data Parquet_Data/transactions Parquet_Data/aggregated User_Base/NBS_BASE Logs
```

## Usage

### Manual Execution

Run scripts directly for testing:

```bash
# Script 1: Download NBS user base
bash 1.GET_NBS_BASE.sh

# Script 2: Process daily data and build views
bash 2.PROCESS_DAILY_AND_BUILD_VIEW.sh
```

### Automated Scheduling

The system uses macOS launchd for automated daily execution:

- **Script 1** runs at 8:05 AM daily
- **Script 2** runs at 11:30 AM daily (depends on Script 1 completion)

See `How2Manage_Launchd_Jobs.txt` for detailed scheduling management instructions.

### Processing Workflow

1. **Data Collection** (8:05 AM)
   - Downloads NBS user base from remote server
   - Aggregates user base data

2. **Data Processing** (11:30 AM)
   - Converts daily CSV files to Parquet format
   - Partitions data by transaction type and year_month
   - Builds aggregated subscription views

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

The aggregated subscription view (`05_build_subscription_view.py`) provides:

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
cat Logs/2.PROCESS_DAILY_AND_BUILD_VIEW.log
```

## Notes

- Script 2 depends on Script 1 completing successfully
- Data files are excluded from Git (see `.gitignore`)
- SSH key authentication required for remote data access
- Designed for macOS environment (uses `date -v-1d` syntax)

## License

Internal use only - Proprietary

## Author

Jose Manco
