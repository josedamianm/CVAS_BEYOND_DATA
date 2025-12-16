# CVAS Beyond Data - Project Rules & Context

## Project Overview

**CVAS Beyond Data** is a production-grade ETL pipeline for telecommunications subscription data processing and analytics. This system automates the daily collection, processing, and aggregation of CVAS (Content Value Added Services) transaction data from a remote PostgreSQL database.

**Purpose:** Extract, Transform, Load (ETL) pipeline that processes 6 transaction types (ACT, RENO, DCT, CNR, RFND, PPD) and builds comprehensive subscription lifecycle views for analytics.

**Technology Stack:**
- Python 3.x (Polars, DuckDB, PyArrow, Pandas)
- Bash scripting for orchestration
- Parquet columnar storage with Hive partitioning
- macOS launchd for scheduling
- SSH/SCP for remote data retrieval

---

## Critical Architecture Rules

### 1. Data Flow Pipeline (Sequential Execution)

**Daily Automated Workflow:**
```
[8:05 AM] Script 1: 1.GET_NBS_BASE.sh
    ↓ Downloads NBS user base from remote server
    ↓ Runs: Scripts/01_aggregate_user_base.py
    ↓
[8:25 AM] Script 2: 2.FETCH_DAILY_DATA.sh
    ↓ Fetches 6 transaction types via Scripts/02_fetch_remote_nova_data.sh
    ↓ Outputs: Daily_Data/*.csv (6 files)
    ↓
[11:30 AM] Script 3: 3.PROCESS_DAILY_AND_BUILD_VIEW.sh
    ↓ Runs: Scripts/03_process_daily.py (CSV → Parquet)
    ↓ Runs: Scripts/04_build_subscription_view.py (Aggregation)
    ↓ Outputs: Parquet_Data/aggregated/subscriptions.parquet
```

**RULE:** Scripts MUST run sequentially. Each depends on the previous completing successfully.

### 2. Transaction Types (6 Types)

| Code | Name | Description | Has Revenue |
|------|------|-------------|-------------|
| ACT | Activations | New subscriptions + upgrades | Yes |
| RENO | Renewals | Subscription renewals | Yes |
| DCT | Deactivations | Service cancellations | No |
| CNR | Cancellations | User-initiated cancellations | No |
| RFND | Refunds | Payment refunds | Yes (negative) |
| PPD | Prepaid | Prepaid transactions | Yes |

**RULE:** All 6 types must be processed together. Missing types will cause pipeline failure.

### 3. Directory Structure (CRITICAL - Do Not Modify)

```
CVAS_BEYOND_DATA/
├── 1.GET_NBS_BASE.sh              # Orchestration: NBS user base
├── 2.FETCH_DAILY_DATA.sh          # Orchestration: Transaction fetch
├── 3.PROCESS_DAILY_AND_BUILD_VIEW.sh  # Orchestration: Processing
├── MASTERCPC.csv                  # Reference: Service/CPC mapping
├── requirements.txt               # Dependencies
│
├── Scripts/                       # Processing logic
│   ├── 00_convert_historical.py  # One-time historical conversion
│   ├── 01_aggregate_user_base.py # User base aggregation
│   ├── 02_fetch_remote_nova_data.sh  # Remote data fetcher
│   ├── 03_process_daily.py       # Daily CSV→Parquet processor
│   ├── 04_build_subscription_view.py  # Subscription aggregator
│   ├── others/                   # Validation/analysis tools
│   │   ├── check_transactions_parquet_data.py
│   │   ├── check_subscriptions_parquet_data.py
│   │   ├── check_users.py
│   │   └── extract_music_subscriptions.py
│   └── utils/
│       └── log_rotation.sh       # 15-day log retention
│
├── sql/
│   └── build_subscription_view.sql  # 241-line DuckDB query
│
├── Daily_Data/                   # Temporary CSV staging (git-ignored)
├── Parquet_Data/                 # Columnar storage (git-ignored)
│   ├── transactions/             # Partitioned by type/year_month
│   │   ├── act/year_month=YYYY-MM/*.parquet
│   │   ├── reno/year_month=YYYY-MM/*.parquet
│   │   ├── dct/year_month=YYYY-MM/*.parquet
│   │   ├── cnr/year_month=YYYY-MM/*.parquet
│   │   ├── ppd/year_month=YYYY-MM/*.parquet
│   │   └── rfnd/year_month=__HIVE_DEFAULT_PARTITION__/*.parquet
│   └── aggregated/
│       └── subscriptions.parquet
│
├── User_Base/                    # NBS user base data
│   ├── NBS_BASE/                # 1123+ daily snapshots (git-ignored)
│   ├── user_base_by_service.csv # Aggregated by service (git-ignored)
│   └── user_base_by_category.csv # Aggregated by category (git-ignored)
│
└── Logs/                         # Execution logs (git-ignored)
```

**RULE:** Never modify directory structure. Scripts use relative paths based on project root.

### 4. Parquet Data Schema (CRITICAL)

#### Transaction Tables (ACT/RENO/PPD - 15 columns):
```
tmuserid: string          # User ID
msisdn: string            # Phone number
cpc: int64                # Content Provider Code (service ID)
trans_type_id: int64      # 1=activation, 2=renewal
channel_id: int64
channel_act: string
trans_date: timestamp
act_date: timestamp
reno_date: timestamp
camp_name: string
tef_prov: int64
campana_medium: string
campana_id: string
subscription_id: int64    # PRIMARY KEY
rev: double               # Revenue
```

#### DCT (13 columns - no rev):
```
Same as above minus rev, plus:
channel_dct: string       # Deactivation channel
```

#### CNR (5 columns):
```
cancel_date: timestamp
sbn_id: int64            # subscription_id
tmuserid: string
cpc: int64
mode: string
```

#### RFND (7 columns):
```
tmuserid: string
cpc: int64
refnd_date: timestamp
rfnd_amount: double
rfnd_cnt: int64
sbnid: int64             # subscription_id
instant_rfnd: string
```

#### Aggregated Subscriptions (32 columns):
```
subscription_id: int64              # PRIMARY KEY
tmuserid, msisdn: string
cpc_list: list<int64>              # All CPCs (ordered)
cpc_count: int64
first_cpc, current_cpc: int64
has_upgraded: bool
upgrade_date, upgraded_to_cpc: timestamp, int64
activation_date, activation_trans_date: timestamp
missing_act_record: bool           # True if started with RENO
activation_campaign, activation_channel: string
activation_revenue: double
activation_month: string           # YYYY-MM
renewal_count: int64
renewal_revenue: double
last_renewal_date, first_renewal_date: timestamp
last_activity_date: timestamp
deactivation_date, deactivation_mode: timestamp, string
cancellation_date, cancellation_mode: timestamp, string
refund_count: int64
total_refunded: double
last_refund_date: timestamp
total_revenue, total_revenue_with_upgrade: double
subscription_status: string        # ACTIVE/DEACTIVATED/CANCELLED
lifetime_days: int64
end_date: timestamp
```

**RULE:** Schema enforcement is CRITICAL. All scripts enforce strict schemas via Polars.

### 5. Hive Partitioning Strategy

**Partitioning:** `year_month=YYYY-MM/` for all transaction types (except RFND)

**Purpose:** Enables partition pruning in DuckDB for efficient time-based queries

**Implementation:**
```python
# Add partition column
df = df.with_columns(pl.lit(year_month).alias('year_month'))

# Write with Hive partitioning
df.write_parquet(
    path,
    use_pyarrow=True,
    pyarrow_options={'partition_cols': ['year_month']}
)
```

**RULE:** Always maintain Hive partitioning. Queries depend on this structure.

### 6. Remote Data Access Configuration

**Remote Server:**
- Host: `10.26.82.53`
- User: `omadmin`
- Database: `postgres`
- Table: `telefonicaes_sub_mgr_fact`
- Auth: SSH key (`~/.ssh/id_ed25519`)

**RULE:** SSH key authentication required. Never commit credentials.

### 7. Edge Cases Handled by Pipeline

#### A. Missing Activation Records
**Problem:** Some subscriptions start with RENO (renewal) without prior ACT (activation)

**Solution:** Use first transaction (ACT or RENO) as activation, flag with `missing_act_record = true`

**Location:** `sql/build_subscription_view.sql:69-95`

#### B. CPC Upgrades
**Problem:** Subscriptions can change services (CPC codes) mid-lifecycle

**Solution:** Track all CPCs chronologically in `cpc_list`, detect upgrades via `trans_type_id = 1` in ACT table

**Location:** `sql/build_subscription_view.sql:50-108`

#### C. Subscription Status Logic
```sql
subscription_status = CASE
    WHEN deactivation_date IS NOT NULL THEN 'DEACTIVATED'
    WHEN cancellation_date IS NOT NULL THEN 'CANCELLED'
    ELSE 'ACTIVE'
END
```

**RULE:** Status determination follows strict hierarchy: DCT > CNR > ACTIVE

---

## Development Rules

### 1. Path Management

**ALWAYS use relative paths from project root:**
```bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"  # For Scripts/ subdirectory
```

**EXCEPTION:** Python path in shell scripts uses absolute path:
```bash
/opt/anaconda3/bin/python
```

**RULE:** Never hardcode absolute paths except for Python interpreter.

### 2. Python Dependencies

**Required packages (requirements.txt):**
```
polars==1.34.0      # High-performance DataFrames
pyarrow==19.0.0     # Parquet I/O
duckdb==1.2.1       # OLAP analytics
pandas==2.2.3       # Legacy support
```

**RULE:** Pin exact versions. Polars/PyArrow compatibility is critical.

### 3. Log Rotation

**All orchestration scripts MUST:**
1. Source `Scripts/utils/log_rotation.sh`
2. Call `rotate_log "$LOGFILE"` at start
3. Keep only last 15 days of logs

**RULE:** Never disable log rotation. Prevents disk space issues.

### 4. Error Handling

**All scripts MUST:**
- Exit with non-zero code on failure
- Log errors with timestamp: `[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: ...`
- Validate inputs before processing
- Check file existence before operations

**Example:**
```bash
if [ ! -f "$FILE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: File not found: $FILE" >> "$LOGFILE"
    exit 1
fi
```

### 5. Date Handling (Cross-Platform)

**ALWAYS support both macOS and Linux:**
```bash
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    yday=$(date -v-1d +%Y-%m-%d)
else
    # Linux
    yday=$(date -d "yesterday" +%Y-%m-%d)
fi
```

**RULE:** Test date operations on both platforms.

### 6. SQL Query Management

**Complex queries belong in `sql/` directory:**
- `sql/build_subscription_view.sql` (241 lines)

**Python scripts load SQL via:**
```python
sql_file = project_root / 'sql' / 'build_subscription_view.sql'
query = sql_file.read_text()
query = query.replace('{parquet_path}', str(parquet_path))
```

**RULE:** Never embed 200+ line SQL in Python. Use external files.

---

## Operational Rules

### 1. Manual Testing

**Test individual scripts:**
```bash
# From project root
bash 1.GET_NBS_BASE.sh
bash 2.FETCH_DAILY_DATA.sh
bash 3.PROCESS_DAILY_AND_BUILD_VIEW.sh

# Test specific transaction type
bash Scripts/02_fetch_remote_nova_data.sh act 2025-12-15
```

**RULE:** Always test manually before modifying launchd jobs.

### 2. Launchd Job Management

**Jobs:**
- `com.josemanco.nbs_base` (8:05 AM)
- `com.josemanco.fetch_daily` (8:25 AM)
- `com.josemanco.process_daily` (11:30 AM)

**Reload after changes:**
```bash
launchctl unload ~/Library/LaunchAgents/com.josemanco.<job>.plist
launchctl load ~/Library/LaunchAgents/com.josemanco.<job>.plist
```

**RULE:** Always reload jobs after plist modifications.

### 3. Data Validation

**Run validation scripts regularly:**
```bash
python Scripts/others/check_transactions_parquet_data.py
python Scripts/others/check_subscriptions_parquet_data.py
```

**RULE:** Validate data after major changes or pipeline failures.

### 4. Query Tool Usage

**Interactive subscription queries:**
```bash
python Scripts/others/check_users.py
```

**Query modes:**
1. By Subscription ID (single)
2. By User ID (tmuserid - multiple)
3. By MSISDN (phone number - multiple)

**RULE:** Use query tool for troubleshooting subscription issues.

### 5. Historical Data Conversion

**One-time conversion (interactive):**
```bash
python Scripts/00_convert_historical.py [path_to_historical_data]
```

**RULE:** Only run for initial setup or backfilling historical data.

---

## Data Governance Rules

### 1. User Base Aggregation

**Category Mapping:**
- Education + Images → `Edu_Ima`
- News + Sports → `News_Sport`

**Exclusions:**
- Services containing "nubico" (case-insensitive)
- Services containing "challenge arena" (case-insensitive)

**Location:** `Scripts/01_aggregate_user_base.py:27-53`

**RULE:** Never modify category mappings without business approval.

### 2. NBS_BASE Data

**File naming:** `YYYYMMDD_NBS_Base.csv`

**Schema:**
```
cpc, content_provider, service_name, last_billed_amount,
tme_category, channel_desc, count
```

**Coverage:** 1123+ daily files (Nov 2022 - Present)

**RULE:** NBS_BASE files are immutable. Never modify historical snapshots.

### 3. MASTERCPC Reference

**Purpose:** Maps services to CPC codes, pricing, and categories

**Schema:**
```
Service Name, TME Category, Free_CPC, Free_Period,
Upgrade_CPC, CHG_Period, CHG_Price, CPCs
```

**RULE:** MASTERCPC.csv is reference data. Update only when services change.

### 4. Git Ignore Rules

**NEVER commit:**
- `Daily_Data/` (temporary staging)
- `Parquet_Data/` (large binary files)
- `User_Base/NBS_BASE/` (1123+ files)
- `User_Base/*.csv` (aggregated outputs)
- `Logs/` (execution logs)

**RULE:** Only commit code, configs, and documentation.

---

## Performance Rules

### 1. Parquet Optimization

**Compression:** SNAPPY (balance between speed and size)

**Partitioning:** By `year_month` for time-based queries

**RULE:** Never use GZIP compression (too slow). Always use SNAPPY.

### 2. DuckDB Query Optimization

**Leverage Hive partitioning:**
```sql
-- Good: Uses partition pruning
SELECT * FROM 'Parquet_Data/transactions/act/**/*.parquet'
WHERE year_month = '2025-01'

-- Bad: Scans all partitions
SELECT * FROM 'Parquet_Data/transactions/act/**/*.parquet'
WHERE trans_date >= '2025-01-01'
```

**RULE:** Always filter on `year_month` partition column when possible.

### 3. Polars vs Pandas

**Use Polars for:**
- CSV reading (10x faster)
- Schema enforcement
- Large dataset operations

**Use Pandas for:**
- DuckDB result conversion (`.fetchdf()`)
- Legacy compatibility

**RULE:** Prefer Polars for all new data processing code.

---

## Security Rules

### 1. SSH Key Management

**Key location:** `~/.ssh/id_ed25519`

**Usage:**
```bash
scp -i ~/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new \
    omadmin@10.26.82.53:/path/to/file local_file
```

**RULE:** Never commit SSH keys. Never use password authentication.

### 2. Remote Server Access

**Allowed operations:**
- Read from `telefonicaes_sub_mgr_fact` table
- Write to `/var/omadmin_reports/` (temporary)
- Execute psql queries

**RULE:** Never modify remote database. Read-only access only.

### 3. Sensitive Data

**PII fields:**
- `tmuserid` (user ID)
- `msisdn` (phone number)

**RULE:** Never log PII. Never export PII to unsecured locations.

---

## Troubleshooting Guide

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "command not found" in launchd | Relative paths | Use absolute paths in plist |
| Missing daily data | Remote server down | Check SSH connectivity |
| Parquet schema mismatch | Schema drift | Verify CSV columns match expected schema |
| DuckDB out of memory | Large query | Add partition filters |
| Log files too large | Rotation disabled | Check `log_rotation.sh` is sourced |

### Debug Commands

```bash
# Check last run status
launchctl print gui/$(id -u)/com.josemanco.process_daily | grep -E "last exit|state"

# View real-time logs
tail -f Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log

# Validate Parquet schema
python3 -c "import pyarrow.parquet as pq; print(pq.read_schema('Parquet_Data/aggregated/subscriptions.parquet'))"

# Check partition structure
ls -R Parquet_Data/transactions/act/ | head -20

# Test remote connectivity
ssh -i ~/.ssh/id_ed25519 omadmin@10.26.82.53 "echo 'Connection OK'"
```

---

## File Modification Guidelines

### When Modifying Scripts

**Before editing:**
1. Read entire script to understand context
2. Check dependencies (what calls this script?)
3. Review error handling patterns
4. Test manually before committing

**After editing:**
1. Test with sample data
2. Verify logs show expected output
3. Check downstream dependencies still work
4. Update README if behavior changes

### When Modifying SQL

**sql/build_subscription_view.sql:**
- 241 lines of complex CTEs
- Handles missing activations, upgrades, multiple CPCs
- Test with `Scripts/04_build_subscription_view.py`

**RULE:** Never modify SQL without understanding full query logic.

### When Adding New Transaction Types

**Required changes:**
1. Add to `file_types` dict in `03_process_daily.py`
2. Add schema to `schemas` dict
3. Add SQL case in `02_fetch_remote_nova_data.sh`
4. Update `build_subscription_view.sql` if needed
5. Update README transaction types section

**RULE:** All 6 transaction types must remain synchronized.

---

## Quick Reference

### Key Files by Purpose

| Purpose | File | Lines |
|---------|------|-------|
| NBS user base download | `1.GET_NBS_BASE.sh` | 131 |
| Transaction data fetch | `2.FETCH_DAILY_DATA.sh` | 98 |
| Daily processing | `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` | 123 |
| User base aggregation | `Scripts/01_aggregate_user_base.py` | 227 |
| Remote data fetcher | `Scripts/02_fetch_remote_nova_data.sh` | 271 |
| CSV→Parquet processor | `Scripts/03_process_daily.py` | 281 |
| Subscription aggregator | `Scripts/04_build_subscription_view.py` | 191 |
| Subscription SQL | `sql/build_subscription_view.sql` | 241 |

### Critical Paths

```
Project Root: /Users/josemanco/CVAS/CVAS_BEYOND_DATA
Python: /opt/anaconda3/bin/python
SSH Key: ~/.ssh/id_ed25519
Remote: omadmin@10.26.82.53
```

### Dependencies

```
polars==1.34.0
pyarrow==19.0.0
duckdb==1.2.1
pandas==2.2.3
```

---

## Project Metadata

**Author:** Jose Manco  
**License:** Internal use only - Proprietary  
**Last Updated:** December 15, 2025  
**Python Version:** 3.x  
**Platform:** macOS (with Linux compatibility)  
**Data Volume:** 1123+ days of historical data  
**Transaction Types:** 6 (ACT, RENO, DCT, CNR, RFND, PPD)  
**Partitions:** 24 months per type (2024-01 to 2025-12)

---

## Important Notes

1. **Sequential Execution:** Scripts MUST run in order (1→2→3)
2. **Schema Enforcement:** All data processing enforces strict schemas
3. **Hive Partitioning:** Critical for query performance
4. **Edge Cases:** Pipeline handles missing activations and CPC upgrades
5. **Log Rotation:** Automatic 15-day retention prevents disk issues
6. **Cross-Platform:** Supports both macOS and Linux date handling
7. **SSH Authentication:** Required for remote data access
8. **Data Validation:** Regular validation prevents data quality issues
9. **Git Ignore:** Never commit data files (only code/configs)
10. **Production System:** Changes require careful testing

---

**END OF PROJECT RULES**

# CVAS Beyond Data - Unified Project Documentation

> **Purpose:** This document serves as the single source of truth for both human developers and AI assistants (LLMs). It combines project overview, architecture, operational procedures, and development rules into one comprehensive reference.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Data Flow](#architecture--data-flow)
3. [Data Structures & Schemas](#data-structures--schemas)
4. [Directory Structure](#directory-structure)
5. [Technology Stack](#technology-stack)
6. [Setup & Configuration](#setup--configuration)
7. [Daily Operations](#daily-operations)
8. [Development Guidelines](#development-guidelines)
9. [Data Governance](#data-governance)
10. [Troubleshooting](#troubleshooting)
11. [Quick Reference](#quick-reference)

---

## Project Overview

**CVAS Beyond Data** is a production-grade ETL pipeline for telecommunications subscription data processing and analytics. It automates the daily collection, processing, and aggregation of CVAS (Content Value Added Services) transaction data from a remote PostgreSQL database (Nova system).

### What It Does

**Extract:** Downloads transaction data from remote PostgreSQL server (`omadmin@10.26.82.53`)  
**Transform:** Converts CSV files to Parquet format with Hive partitioning and schema enforcement  
**Load:** Builds comprehensive subscription lifecycle views combining all transaction types

### Key Capabilities

- **Processes 6 transaction types:** ACT (Activations), RENO (Renewals), DCT (Deactivations), CNR (Cancellations), RFND (Refunds), PPD (Prepaid)
- **Handles edge cases:** Missing activation records, CPC upgrades, multiple services per subscription
- **Tracks complete lifecycle:** From activation through renewals to deactivation/cancellation
- **Maintains 1123+ days of history:** November 2022 to present
- **Automated daily execution:** Via macOS launchd at 8:05 AM, 8:25 AM, and 11:30 AM

### Business Context

- **CPC (Content Provider Code):** Unique identifier for each service/subscription type
- **Subscription Lifecycle:** Activation → Renewals → Deactivation/Cancellation
- **Revenue Tracking:** Aggregates activation, renewal, and upgrade revenue
- **User Base:** Daily snapshots of active subscribers by service and category

---

## Architecture & Data Flow

### Sequential Pipeline (CRITICAL: Must Run in Order)

```
┌─────────────────────────────────────────────────────────────────┐
│ SCRIPT 1: 1.GET_NBS_BASE.sh (8:05 AM)                           │
├─────────────────────────────────────────────────────────────────┤
│ 1. Download NBS_Base.csv from remote server via SCP             │
│ 2. Validate file size and integrity                             │
│ 3. Run: Scripts/01_aggregate_user_base.py                       │
│    → Processes 1123+ daily NBS_BASE/*.csv files                 │
│    → Outputs: user_base_by_service.csv, user_base_by_category.csv│
│    → Applies category mapping (Edu_Ima, News_Sport)             │
│    → Excludes: Nubico, Challenge Arena services                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ SCRIPT 2: 2.FETCH_DAILY_DATA.sh (8:25 AM)                       │
├─────────────────────────────────────────────────────────────────┤
│ For each transaction type (ACT, RENO, DCT, PPD, CNR, RFND):     │
│ → Calls: Scripts/02_fetch_remote_nova_data.sh <type> <date>     │
│    1. SSH to omadmin@10.26.82.53                                │
│    2. Generate SQL script on remote server                      │
│    3. Execute: /usr/local/pgsql/bin/psql                        │
│    4. Query: telefonicaes_sub_mgr_fact table                    │
│    5. Export to CSV on remote: /var/omadmin_reports/            │
│    6. SCP back to local: Daily_Data/<type>_atlas_day.csv        │
│    7. Cleanup remote files                                      │
│ → Outputs: 6 CSV files in Daily_Data/                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ SCRIPT 3: 3.PROCESS_DAILY_AND_BUILD_VIEW.sh (11:30 AM)          │
├─────────────────────────────────────────────────────────────────┤
│ STEP 1: Validate 6 CSV files exist in Daily_Data/               │
│                                                                  │
│ STEP 2: Run Scripts/03_process_daily.py <date>                  │
│    → Reads: Daily_Data/<type>_atlas_day.csv (6 files)           │
│    → Enforces strict schemas via Polars                         │
│    → Adds year_month partition column (YYYY-MM)                 │
│    → Writes: Parquet_Data/transactions/<type>/year_month=YYYY-MM/│
│    → Uses: Hive partitioning, SNAPPY compression                │
│                                                                  │
│ STEP 3: Run Scripts/04_build_subscription_view.py               │
│    → Loads: sql/build_subscription_view.sql (241 lines)         │
│    → Reads: All Parquet_Data/transactions/**/*.parquet          │
│    → Executes: Complex DuckDB query with 10+ CTEs               │
│    → Handles: Missing activations, CPC upgrades, status logic   │
│    → Writes: Parquet_Data/aggregated/subscriptions.parquet      │
│    → Outputs: Statistics (status, revenue, campaigns, etc.)     │
└─────────────────────────────────────────────────────────────────┘
```

### Critical Dependencies

**Script 1 → Script 2:** NBS user base must be downloaded before transaction data  
**Script 2 → Script 3:** All 6 CSV files must exist before processing  
**Script 3 Steps:** CSV validation → Parquet conversion → Subscription aggregation

**⚠️ RULE:** Scripts MUST run sequentially. Each depends on the previous completing successfully.

### Edge Cases Handled

#### 1. Missing Activation Records (~10-20% of subscriptions)

**Problem:** Some subscriptions appear in RENO table without prior ACT record

**Solution:**
```sql
-- Use first transaction (ACT or RENO) as activation
first_transaction AS (
    SELECT subscription_id, ...
    FROM all_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date ASC) = 1
)

-- Flag missing ACT records
missing_act_record = (actual_act_trans_date IS NULL)
```

**Location:** `sql/build_subscription_view.sql:69-95`

#### 2. CPC Upgrades (Service Changes)

**Problem:** Subscriptions can change services (CPC codes) mid-lifecycle

**Example:** User starts with CPC 43551 (Movistar Juegos), upgrades to CPC 46109 (premium tier)

**Solution:**
```sql
-- Track all CPCs chronologically
cpc_list AS (
    SELECT subscription_id,
           LIST(cpc ORDER BY first_seen) as cpc_list,  -- [43551, 46109]
           COUNT(DISTINCT cpc) as cpc_count            -- 2
    FROM cpc_with_order
    GROUP BY subscription_id
)

-- Detect upgrades (trans_type_id = 1 in ACT table)
cpc_changes AS (
    SELECT subscription_id, cpc as new_cpc, trans_date as upgrade_date
    FROM all_transactions
    WHERE transaction_type = 'ACT' AND trans_type_id = 1
)
```

**Location:** `sql/build_subscription_view.sql:50-108`

#### 3. Subscription Status Hierarchy

```sql
subscription_status = CASE
    WHEN deactivation_date IS NOT NULL THEN 'DEACTIVATED'  -- System-initiated
    WHEN cancellation_date IS NOT NULL THEN 'CANCELLED'    -- User-initiated
    ELSE 'ACTIVE'
END

end_date = COALESCE(deactivation_date, cancellation_date, last_activity_date)
lifetime_days = DATEDIFF('day', activation_date, end_date)
```

**Priority:** DCT (Deactivation) > CNR (Cancellation) > ACTIVE

**Location:** `sql/build_subscription_view.sql:151-241`

---

## Data Structures & Schemas

### Transaction Types (6 Types)

| Code | Name | Description | Revenue | Partitions | Schema Columns |
|------|------|-------------|---------|------------|----------------|
| **ACT** | Activations | New subscriptions + upgrades | Yes | 24 months | 15 |
| **RENO** | Renewals | Subscription renewals | Yes | 24 months | 15 |
| **DCT** | Deactivations | Service cancellations | No | 24 months | 13 |
| **CNR** | Cancellations | User-initiated cancellations | No | 24 months | 5 |
| **RFND** | Refunds | Payment refunds | Yes (negative) | 1 (default) | 7 |
| **PPD** | Prepaid | Prepaid transactions | Yes | 23 months | 15 |

### Parquet Schemas (Enforced by Polars)

#### ACT / RENO / PPD (15 columns)
```python
{
    'tmuserid': pl.Utf8,          # User ID (phone number)
    'msisdn': pl.Utf8,            # Mobile Station ISDN (actual phone)
    'cpc': pl.Int64,              # Content Provider Code (service ID)
    'trans_type_id': pl.Int64,    # 1=activation, 2=renewal
    'channel_id': pl.Int64,       # Channel identifier
    'channel_act': pl.Utf8,       # Channel description (UPGRADE, WEB, etc.)
    'trans_date': pl.Utf8,        # Transaction timestamp
    'act_date': pl.Utf8,          # Activation date
    'reno_date': pl.Utf8,         # Next renewal date
    'camp_name': pl.Utf8,         # Campaign name
    'tef_prov': pl.Int64,         # Telefonica provider code
    'campana_medium': pl.Utf8,    # Campaign medium
    'campana_id': pl.Utf8,        # Campaign ID
    'subscription_id': pl.Int64,  # PRIMARY KEY
    'rev': pl.Float64             # Revenue amount
}
```

**Sample Data:**
```csv
tmuserid,msisdn,cpc,trans_type_id,channel_id,channel_act,trans_date,act_date,reno_date,camp_name,tef_prov,campana_medium,campana_id,subscription_id,rev
8343825210089100002,34644264856,46109,1,3,UPGRADE,2025-12-15 01:01:20,2025-12-08 01:01:10,2026-01-14 01:01:20,bd47cf4,007,campana,bd47cf4,12656974,5.36
```

#### DCT (13 columns - no revenue)
```python
{
    'tmuserid': pl.Utf8,
    'msisdn': pl.Utf8,
    'cpc': pl.Int64,
    'trans_type_id': pl.Int64,
    'channel_dct': pl.Utf8,       # Deactivation channel (UPGRADE, USER_REQUEST)
    'trans_date': pl.Utf8,
    'act_date': pl.Utf8,
    'reno_date': pl.Utf8,
    'camp_name': pl.Utf8,
    'tef_prov': pl.Int64,
    'campana_medium': pl.Utf8,
    'campana_id': pl.Utf8,
    'subscription_id': pl.Int64
}
```

#### CNR (5 columns)
```python
{
    'cancel_date': pl.Utf8,       # Cancellation timestamp
    'sbn_id': pl.Int64,          # Subscription ID
    'tmuserid': pl.Utf8,         # User ID
    'cpc': pl.Int64,             # Service code
    'mode': pl.Utf8              # Cancellation mode
}
```

#### RFND (7 columns)
```python
{
    'tmuserid': pl.Utf8,
    'cpc': pl.Int64,
    'refnd_date': pl.Utf8,       # Refund date
    'rfnd_amount': pl.Float64,   # Refund amount
    'rfnd_cnt': pl.Int64,        # Refund count
    'sbnid': pl.Int64,           # Subscription ID
    'instant_rfnd': pl.Utf8      # Instant refund flag
}
```

### Aggregated Subscriptions (32 columns)

**Location:** `Parquet_Data/aggregated/subscriptions.parquet`

```python
{
    # Identity
    'subscription_id': int64,              # PRIMARY KEY
    'tmuserid': string,                    # User identifier
    'msisdn': string,                      # Phone number
    
    # CPC Tracking
    'cpc_list': list<int64>,              # All CPCs used (ordered by first appearance)
    'cpc_count': int64,                    # Number of distinct CPCs
    'first_cpc': int64,                    # Initial service code
    'current_cpc': int64,                  # Current service code
    
    # Upgrade Detection
    'has_upgraded': bool,                  # True if CPC changed
    'upgrade_date': timestamp,             # When upgrade occurred
    'upgraded_to_cpc': int64,             # Target CPC after upgrade
    
    # Activation
    'activation_date': timestamp,          # Subscription start date
    'activation_trans_date': timestamp,    # First transaction date
    'missing_act_record': bool,            # True if started with RENO
    'activation_campaign': string,         # Initial campaign
    'activation_channel': string,          # Activation channel
    'activation_revenue': double,          # Initial revenue
    'activation_month': string,            # YYYY-MM format
    
    # Renewals
    'renewal_count': int64,                # Number of renewals
    'renewal_revenue': double,             # Total renewal revenue
    'last_renewal_date': timestamp,        # Most recent renewal
    'first_renewal_date': timestamp,       # First renewal
    
    # Activity
    'last_activity_date': timestamp,       # Last transaction
    
    # Termination
    'deactivation_date': timestamp,        # Service end date (system)
    'deactivation_mode': string,           # How it ended
    'cancellation_date': timestamp,        # User cancellation date
    'cancellation_mode': string,           # Cancellation method
    
    # Refunds
    'refund_count': int64,                 # Number of refunds
    'total_refunded': double,              # Total refund amount
    'last_refund_date': timestamp,         # Most recent refund
    
    # Financial Summary
    'total_revenue': double,               # Sum of all revenue
    'total_revenue_with_upgrade': double,  # Including upgrade revenue
    
    # Status
    'subscription_status': string,         # ACTIVE/DEACTIVATED/CANCELLED
    'lifetime_days': int64,                # Subscription duration
    'end_date': timestamp                  # Effective end date
}
```

### NBS_BASE User Data

**Location:** `User_Base/NBS_BASE/YYYYMMDD_NBS_Base.csv` (1123+ files)

**Schema:**
```csv
cpc,content_provider,service_name,last_billed_amount,tme_category,channel_desc,count
```

**Sample:**
```csv
10194,TelefonicaES,News Service,3.0,News,ACTIVE,1250
```

**Aggregated Outputs:**
- `user_base_by_service.csv`: `date|service_name|tme_category|User_Base`
- `user_base_by_category.csv`: `date|tme_category|User_Base`

### MASTERCPC Reference

**Location:** `MASTERCPC.csv`

**Schema:**
```csv
Service Name,TME Category,Free_CPC,Free_Period,Upgrade_CPC,CHG_Period,CHG_Price,CPCs
```

**Purpose:** Maps services to CPC codes, pricing, and categories

---

## Directory Structure

```
CVAS_BEYOND_DATA/
├── 1.GET_NBS_BASE.sh              # Orchestration: NBS user base (131 lines)
├── 2.FETCH_DAILY_DATA.sh          # Orchestration: Transaction fetch (98 lines)
├── 3.PROCESS_DAILY_AND_BUILD_VIEW.sh  # Orchestration: Processing (123 lines)
├── MASTERCPC.csv                  # Reference: Service/CPC mapping
├── requirements.txt               # Python dependencies (4 packages)
├── README.md                      # Project documentation (639 lines)
│
├── Scripts/                       # Processing logic
│   ├── 00_convert_historical.py  # One-time historical conversion (273 lines)
│   ├── 01_aggregate_user_base.py # User base aggregation (227 lines)
│   ├── 02_fetch_remote_nova_data.sh  # Remote data fetcher (271 lines)
│   ├── 03_process_daily.py       # Daily CSV→Parquet processor (281 lines)
│   ├── 04_build_subscription_view.py  # Subscription aggregator (191 lines)
│   ├── others/                   # Validation/analysis tools (not in pipeline)
│   │   ├── check_transactions_parquet_data.py  # Transaction validation
│   │   ├── check_subscriptions_parquet_data.py # Subscription validation
│   │   ├── check_users.py        # Interactive query tool
│   │   └── extract_music_subscriptions.py  # Music subscription extraction
│   └── utils/
│       └── log_rotation.sh       # 15-day log retention
│
├── sql/
│   └── build_subscription_view.sql  # 241-line DuckDB query with 10+ CTEs
│
├── Daily_Data/                   # Temporary CSV staging (git-ignored)
│   ├── act_atlas_day.csv
│   ├── reno_atlas_day.csv
│   ├── dct_atlas_day.csv
│   ├── cnr_atlas_day.csv
│   ├── ppd_atlas_day.csv
│   └── rfnd_atlas_day.csv
│
├── Parquet_Data/                 # Columnar storage (git-ignored)
│   ├── transactions/             # Hive partitioned by type/year_month
│   │   ├── act/
│   │   │   ├── year_month=2024-01/*.parquet
│   │   │   ├── year_month=2024-02/*.parquet
│   │   │   └── ... (24 months: 2024-01 to 2025-12)
│   │   ├── reno/                 # Same structure (24 months)
│   │   ├── dct/                  # Same structure (24 months)
│   │   ├── cnr/                  # Same structure (24 months)
│   │   ├── ppd/                  # 23 months (2024-01 to 2025-11)
│   │   └── rfnd/
│   │       └── year_month=__HIVE_DEFAULT_PARTITION__/*.parquet
│   └── aggregated/
│       └── subscriptions.parquet # Final aggregated view
│
├── User_Base/                    # NBS user base data
│   ├── NBS_BASE/                # 1123+ daily snapshots (git-ignored)
│   │   ├── 20221114_NBS_Base.csv
│   │   ├── 20221115_NBS_Base.csv
│   │   └── ... (Nov 2022 - Present)
│   ├── user_base_by_service.csv # Aggregated by service (git-ignored)
│   └── user_base_by_category.csv # Aggregated by category (git-ignored)
│
└── Logs/                         # Execution logs (git-ignored)
    ├── 1.GET_NBS_BASE.log
    ├── 2.FETCH_DAILY_DATA.log
    └── 3.PROCESS_DAILY_AND_BUILD_VIEW.log
```

**⚠️ CRITICAL:** Never modify directory structure. Scripts use relative paths based on project root.

**Git Ignore Rules:** Never commit `Daily_Data/`, `Parquet_Data/`, `User_Base/NBS_BASE/`, `User_Base/*.csv`, or `Logs/`

---

## Technology Stack

### Core Technologies

| Technology | Version | Purpose | Key Features |
|------------|---------|---------|--------------|
| **Python** | 3.x | Core processing | Polars, DuckDB, PyArrow |
| **Polars** | 1.34.0 | DataFrame operations | 10x faster than Pandas, strict schemas |
| **DuckDB** | 1.2.1 | OLAP analytics | In-process SQL, Hive partitioning support |
| **PyArrow** | 19.0.0 | Parquet I/O | Columnar format, SNAPPY compression |
| **Pandas** | 2.2.3 | Legacy support | DuckDB result conversion |
| **Bash** | - | Orchestration | SSH/SCP, remote execution |
| **macOS launchd** | - | Scheduling | Automated daily jobs |

### Why These Technologies?

**Polars over Pandas:**
- 10x faster CSV reading
- Strict schema enforcement
- Better memory efficiency
- Native Parquet support

**DuckDB over PostgreSQL:**
- In-process (no server)
- Excellent Parquet integration
- Hive partitioning support
- Fast analytical queries

**Parquet over CSV:**
- Columnar storage (faster queries)
- Built-in compression (SNAPPY)
- Schema preservation
- Partition pruning support

---

## Setup & Configuration

### Prerequisites

```bash
# Install Python dependencies
pip install -r requirements.txt

# Verify installation
python3 -c "import polars, duckdb, pyarrow; print('All packages installed')"
```

### SSH Configuration

**Required for Scripts 1 & 2:**

```bash
# Generate SSH key (if not exists)
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519

# Copy public key to remote server
ssh-copy-id -i ~/.ssh/id_ed25519.pub omadmin@10.26.82.53

# Test connection
ssh -i ~/.ssh/id_ed25519 omadmin@10.26.82.53 "echo 'Connection OK'"
```

**⚠️ RULE:** Never commit SSH keys. Never use password authentication.

### Directory Setup

```bash
# Create required directories
mkdir -p Daily_Data Parquet_Data/transactions Parquet_Data/aggregated User_Base/NBS_BASE Logs

# Verify structure
ls -la
```

### Configuration Files

**Update if needed:**

1. **Python Path** (in shell scripts):
   ```bash
   # Default: /opt/anaconda3/bin/python
   # Update in: 1.GET_NBS_BASE.sh, 3.PROCESS_DAILY_AND_BUILD_VIEW.sh
   ```

2. **Remote Server** (in `Scripts/02_fetch_remote_nova_data.sh`):
   ```bash
   REMOTE_USER="omadmin"
   REMOTE_HOST="10.26.82.53"
   REMOTE_PSQL="/usr/local/pgsql/bin/psql"
   REMOTE_DB="postgres"
   ```

### Launchd Scheduling (macOS)

**Job Files:** `~/Library/LaunchAgents/com.josemanco.<job>.plist`

**Jobs:**
- `com.josemanco.nbs_base` → 8:05 AM
- `com.josemanco.fetch_daily` → 8:25 AM
- `com.josemanco.process_daily` → 11:30 AM

**Change Schedule:**
```bash
# Edit plist
nvim ~/Library/LaunchAgents/com.josemanco.nbs_base.plist

# Change time
<key>Hour</key>
<integer>8</integer>     <!-- 0-23 -->
<key>Minute</key>
<integer>5</integer>     <!-- 0-59 -->

# Reload job
launchctl unload ~/Library/LaunchAgents/com.josemanco.nbs_base.plist
launchctl load ~/Library/LaunchAgents/com.josemanco.nbs_base.plist
```

**Useful Commands:**
```bash
# Check if job is loaded
launchctl list | grep com.josemanco

# View job details and next run time
launchctl print gui/$(id -u)/com.josemanco.nbs_base

# Trigger manually (same environment as scheduled run)
launchctl start com.josemanco.nbs_base

# View logs
cat Logs/1.GET_NBS_BASE.log
tail -f Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log
```

---

## Daily Operations

### Manual Execution

**Run all scripts:**
```bash
# From project root
bash 1.GET_NBS_BASE.sh
bash 2.FETCH_DAILY_DATA.sh
bash 3.PROCESS_DAILY_AND_BUILD_VIEW.sh
```

**Run specific transaction type:**
```bash
bash Scripts/02_fetch_remote_nova_data.sh act 2025-12-15
bash Scripts/02_fetch_remote_nova_data.sh all 2025-12-15
```

**Run with custom date:**
```bash
bash 2.FETCH_DAILY_DATA.sh 2025-12-15
bash 3.PROCESS_DAILY_AND_BUILD_VIEW.sh 2025-12-15
```

### Data Validation

**Check transaction data:**
```bash
python Scripts/others/check_transactions_parquet_data.py
```

**Output:**
- Daily data completeness (yesterday's transactions)
- Monthly summary (counts and revenue by type)
- Data validation (row counts, date ranges, schema)
- Query performance metrics

**Check subscription data:**
```bash
python Scripts/others/check_subscriptions_parquet_data.py
```

**Output:**
- Recent activations, renewals, deactivations, cancellations
- Monthly subscription metrics
- Data quality validation
- Aggregation query performance

### Interactive Query Tool

**Query subscriptions:**
```bash
python Scripts/others/check_users.py
```

**Query Modes:**
1. By Subscription ID (single subscription)
2. By User ID (tmuserid - may return multiple)
3. By MSISDN (phone number - may return multiple)

**Output Sections:**
1. **Summary Per Subscription:** Detailed info for each subscription
2. **Aggregated Summary:** Overall statistics across all matches
3. **Complete Raw Data:** All columns in table format
4. **Column-by-Column Breakdown:** Each field organized by category

### Historical Data Conversion

**One-time conversion (interactive):**
```bash
python Scripts/00_convert_historical.py [path_to_historical_data]
```

**Purpose:** Convert historical CSV files to Parquet format with Hive partitioning

**⚠️ RULE:** Only run for initial setup or backfilling historical data.

### Monitoring

**Check execution logs:**
```bash
# View latest logs
cat Logs/1.GET_NBS_BASE.log
cat Logs/2.FETCH_DAILY_DATA.log
cat Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log

# Real-time monitoring
tail -f Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log

# Check for errors
grep "ERROR" Logs/*.log
```

**Log Format:**
```
[2025-12-15 08:05:23] ✓ Download successful: 125000 lines
[2025-12-15 08:05:24] ✓ File size validation passed: 4523 KB
```

**Log Rotation:**
- Automatic 15-day retention
- Rotates at start of each script execution
- No manual cleanup required

---

## Development Guidelines

### Path Management

**ALWAYS use relative paths:**
```bash
# In root scripts
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# In Scripts/ subdirectory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
```

**EXCEPTION:** Python interpreter uses absolute path:
```bash
/opt/anaconda3/bin/python
```

**⚠️ RULE:** Never hardcode absolute paths except for Python interpreter.

### Schema Enforcement

**All CSV processing MUST enforce schemas:**
```python
# Define schema
schema = {
    'tmuserid': pl.Utf8,
    'cpc': pl.Int64,
    'rev': pl.Float64,
    # ... all columns
}

# Read with strict schema
df = pl.read_csv(
    csv_file,
    schema=schema,
    null_values=['', 'NULL', 'null'],
    ignore_errors=False  # Fail on schema mismatch
)
```

**⚠️ RULE:** Schema enforcement is CRITICAL. Never disable or relax schemas.

### Hive Partitioning

**Always partition by year_month:**
```python
# Add partition column
year_month = date_str[:7]  # '2025-12-15' → '2025-12'
df = df.with_columns(pl.lit(year_month).alias('year_month'))

# Write with Hive partitioning
df.write_parquet(
    output_path,
    use_pyarrow=True,
    pyarrow_options={
        'partition_cols': ['year_month'],
        'existing_data_behavior': 'overwrite_or_ignore',
        'compression': 'snappy'
    }
)
```

**Result:** Directory structure `year_month=2025-12/` enables partition pruning

**⚠️ RULE:** Always maintain Hive partitioning. Queries depend on this structure.

### Error Handling

**All scripts MUST:**
```bash
# Exit with non-zero code on failure
if [ ! -f "$FILE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: File not found: $FILE" >> "$LOGFILE"
    exit 1
fi

# Validate inputs
if [ -z "$TRANS_TYPE" ]; then
    echo "Error: Transaction type is required."
    exit 1
fi

# Check command success
if ! command_here; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: Command failed" >> "$LOGFILE"
    exit 1
fi
```

### Cross-Platform Date Handling

**Support both macOS and Linux:**
```bash
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    yday=$(date -v-1d +%Y-%m-%d)
else
    # Linux
    yday=$(date -d "yesterday" +%Y-%m-%d)
fi
```

**⚠️ RULE:** Always test date operations on both platforms.

### SQL Query Management

**Complex queries (200+ lines) belong in `sql/` directory:**
```python
# Load SQL from file
sql_file = project_root / 'sql' / 'build_subscription_view.sql'
query = sql_file.read_text()

# Replace placeholders
query = query.replace('{parquet_path}', str(parquet_path))

# Execute
con.execute(query)
```

**⚠️ RULE:** Never embed 200+ line SQL in Python. Use external files.

### Performance Optimization

**DuckDB Query Optimization:**
```sql
-- ✅ GOOD: Uses partition pruning
SELECT * FROM 'Parquet_Data/transactions/act/**/*.parquet'
WHERE year_month = '2025-01'

-- ❌ BAD: Scans all partitions
SELECT * FROM 'Parquet_Data/transactions/act/**/*.parquet'
WHERE trans_date >= '2025-01-01'
```

**Polars vs Pandas:**
- Use Polars for: CSV reading, schema enforcement, large datasets
- Use Pandas for: DuckDB result conversion (`.fetchdf()`), legacy compatibility

**Compression:**
- Always use SNAPPY (balance between speed and size)
- Never use GZIP (too slow for daily operations)

### Adding New Transaction Types

**Required changes:**
1. Add to `file_types` dict in `Scripts/03_process_daily.py`
2. Add schema to `schemas` dict
3. Add SQL case in `Scripts/02_fetch_remote_nova_data.sh`
4. Update `sql/build_subscription_view.sql` if needed
5. Update this documentation

**⚠️ RULE:** All 6 transaction types must remain synchronized.

---

## Data Governance

### Category Mapping

**Applied by `Scripts/01_aggregate_user_base.py`:**
```python
category_mapping = {
    'education': 'Edu_Ima',
    'images': 'Edu_Ima',
    'news': 'News_Sport',
    'sports': 'News_Sport'
}
```

**Service Exclusions:**
- Services containing "nubico" (case-insensitive)
- Services containing "challenge arena" (case-insensitive)

**⚠️ RULE:** Never modify category mappings without business approval.

### Data Immutability

**Immutable Data:**
- `User_Base/NBS_BASE/*.csv` - Historical snapshots (never modify)
- `MASTERCPC.csv` - Reference data (update only when services change)
- `Parquet_Data/transactions/**/*.parquet` - Historical transactions (append-only)

**Mutable Data:**
- `Daily_Data/*.csv` - Temporary staging (overwritten daily)
- `Parquet_Data/aggregated/subscriptions.parquet` - Rebuilt daily

### PII Handling

**PII Fields:**
- `tmuserid` (user ID)
- `msisdn` (phone number)

**⚠️ RULES:**
- Never log PII
- Never export PII to unsecured locations
- Never commit PII to Git

### Remote Server Access

**Allowed Operations:**
- Read from `telefonicaes_sub_mgr_fact` table
- Write to `/var/omadmin_reports/` (temporary files only)
- Execute psql queries

**⚠️ RULE:** Never modify remote database. Read-only access only.

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "command not found" in launchd | Relative paths in plist | Use absolute paths in plist files |
| Missing daily data | Remote server down | Check SSH: `ssh omadmin@10.26.82.53` |
| Parquet schema mismatch | CSV columns changed | Verify CSV columns match expected schema |
| DuckDB out of memory | Large query without filters | Add `WHERE year_month = 'YYYY-MM'` filter |
| Log files too large | Rotation disabled | Check `log_rotation.sh` is sourced |
| Script 3 fails | Missing CSV files | Run Script 2 first, check Daily_Data/ |
| Subscription count mismatch | Missing activations | Check `missing_act_record` flag in output |

### Debug Commands

**Check launchd status:**
```bash
# Check if job is loaded
launchctl list | grep com.josemanco

# View last run status
launchctl print gui/$(id -u)/com.josemanco.process_daily | grep -E "last exit|state"

# Check next run time
launchctl print gui/$(id -u)/com.josemanco.nbs_base | grep "NextRunTime"
```

**Validate data:**
```bash
# Check Parquet schema
python3 -c "import pyarrow.parquet as pq; print(pq.read_schema('Parquet_Data/aggregated/subscriptions.parquet'))"

# Check partition structure
ls -R Parquet_Data/transactions/act/ | head -20

# Count records in Parquet
python3 -c "import duckdb; print(duckdb.execute(\"SELECT COUNT(*) FROM 'Parquet_Data/aggregated/subscriptions.parquet'\").fetchone())"

# Check CSV files
ls -lh Daily_Data/
head -5 Daily_Data/act_atlas_day.csv
```

**Test connectivity:**
```bash
# Test SSH
ssh -i ~/.ssh/id_ed25519 omadmin@10.26.82.53 "echo 'Connection OK'"

# Test remote psql
ssh -i ~/.ssh/id_ed25519 omadmin@10.26.82.53 "/usr/local/pgsql/bin/psql --version"
```

**Check logs:**
```bash
# View errors
grep "ERROR" Logs/*.log

# View warnings
grep "WARNING" Logs/*.log

# Check last run
tail -50 Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log
```

### Recovery Procedures

**If Script 1 fails:**
```bash
# Check remote server connectivity
ssh omadmin@10.26.82.53

# Manually download NBS_Base.csv
scp -i ~/.ssh/id_ed25519 omadmin@10.26.82.53:/opt/postgres/lvas_reports/NBS_Base.csv User_Base/NBS_BASE/$(date +%Y%m%d)_NBS_Base.csv

# Run aggregation manually
python Scripts/01_aggregate_user_base.py
```

**If Script 2 fails:**
```bash
# Check which transaction types failed
grep "ERROR" Logs/2.FETCH_DAILY_DATA.log

# Retry specific type
bash Scripts/02_fetch_remote_nova_data.sh act 2025-12-15

# Verify CSV files
ls -lh Daily_Data/
```

**If Script 3 fails:**
```bash
# Check which step failed
grep "ERROR" Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log

# Validate CSV files exist
ls -lh Daily_Data/*.csv

# Run processing manually
python Scripts/03_process_daily.py 2025-12-15

# Run aggregation manually
python Scripts/04_build_subscription_view.py
```

---

## Quick Reference

### Key Files by Purpose

| Purpose | File | Lines | Language |
|---------|------|-------|----------|
| NBS user base download | `1.GET_NBS_BASE.sh` | 131 | Bash |
| Transaction data fetch | `2.FETCH_DAILY_DATA.sh` | 98 | Bash |
| Daily processing | `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` | 123 | Bash |
| User base aggregation | `Scripts/01_aggregate_user_base.py` | 227 | Python |
| Remote data fetcher | `Scripts/02_fetch_remote_nova_data.sh` | 271 | Bash |
| CSV→Parquet processor | `Scripts/03_process_daily.py` | 281 | Python |
| Subscription aggregator | `Scripts/04_build_subscription_view.py` | 191 | Python |
| Subscription SQL | `sql/build_subscription_view.sql` | 241 | SQL |
| Log rotation | `Scripts/utils/log_rotation.sh` | - | Bash |

### Critical Paths

```
Project Root:  /Users/josemanco/CVAS/CVAS_BEYOND_DATA
Python:        /opt/anaconda3/bin/python
SSH Key:       ~/.ssh/id_ed25519
Remote Host:   omadmin@10.26.82.53
Remote DB:     postgres
Remote Table:  telefonicaes_sub_mgr_fact
```

### Dependencies

```
polars==1.34.0      # High-performance DataFrames
pyarrow==19.0.0     # Parquet I/O
duckdb==1.2.1       # OLAP analytics
pandas==2.2.3       # Legacy support
```

### Transaction Type Quick Reference

| Code | Name | trans_type_id | Has Revenue | Partition Count |
|------|------|---------------|-------------|-----------------|
| ACT | Activations | 1 | Yes | 24 |
| RENO | Renewals | 2 | Yes | 24 |
| DCT | Deactivations | 3 | No | 24 |
| CNR | Cancellations | - | No | 24 |
| RFND | Refunds | - | Yes | 1 |
| PPD | Prepaid | - | Yes | 23 |

### Useful One-Liners

```bash
# Count total subscriptions
duckdb -c "SELECT COUNT(*) FROM 'Parquet_Data/aggregated/subscriptions.parquet'"

# Check active subscriptions
duckdb -c "SELECT subscription_status, COUNT(*) FROM 'Parquet_Data/aggregated/subscriptions.parquet' GROUP BY subscription_status"

# Find subscriptions with upgrades
duckdb -c "SELECT COUNT(*) FROM 'Parquet_Data/aggregated/subscriptions.parquet' WHERE has_upgraded = true"

# Check missing activations
duckdb -c "SELECT COUNT(*) FROM 'Parquet_Data/aggregated/subscriptions.parquet' WHERE missing_act_record = true"

# View recent activations
duckdb -c "SELECT * FROM 'Parquet_Data/aggregated/subscriptions.parquet' WHERE activation_month = '2025-12' LIMIT 10"

# Check partition sizes
du -sh Parquet_Data/transactions/*/year_month=2025-12/
```

---

## Project Metadata

**Author:** Jose Manco  
**License:** Internal use only - Proprietary  
**Last Updated:** December 15, 2025  
**Python Version:** 3.x  
**Platform:** macOS (with Linux compatibility)  
**Data Coverage:** November 2022 - Present (1123+ days)  
**Transaction Types:** 6 (ACT, RENO, DCT, CNR, RFND, PPD)  
**Partitions:** 24 months per type (2024-01 to 2025-12)  
**Total Files:** 1123+ NBS_BASE snapshots + 24 months × 6 types = 1267+ files

---

## Troubleshooting Guide

### Issue 1: Row Count Mismatch in Historical Data Conversion

**Symptom:** `00_convert_historical.py` fails with "Row count mismatch!" error

**Root Cause:** `pyarrow.parquet.write_to_dataset()` appends data to existing partitions instead of overwriting, causing duplicate rows when script runs multiple times

**Solution:**
1. Script now removes existing Parquet data before writing using `shutil.rmtree(output_path)`
2. Verification step compares row counts between source DataFrame and written Parquet files
3. If mismatch occurs, manually clean up: `rm -rf Parquet_Data/transactions/<type>/*`

**Prevention:** Always check if output directory exists before running conversion script

### Issue 2: Orchestration Script Not Running (LaunchAgent)

**Symptom:** `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` doesn't run on schedule, while other scripts do

**Root Cause:** Incorrect path in LaunchAgent plist file (`~/Library/LaunchAgents/com.josemanco.process_daily.plist`)

**Diagnosis Steps:**
```bash
# Check LaunchAgent status
launchctl list | grep josemanco

# View LaunchAgent configuration
cat ~/Library/LaunchAgents/com.josemanco.process_daily.plist

# Check logs for errors
cat Logs/3.PROCESS_DAILY_AND_BUILD_VIEW.log
```

**Solution:**
1. Correct the `ProgramArguments` path in plist file
2. Add proper environment variables and interpreter (`/bin/bash`)
3. Reload LaunchAgent:
   ```bash
   launchctl unload ~/Library/LaunchAgents/com.josemanco.process_daily.plist
   launchctl load ~/Library/LaunchAgents/com.josemanco.process_daily.plist
   ```
4. Verify with: `launchctl list | grep process_daily` (exit code should be 0)

**Prevention:** Always verify LaunchAgent paths match actual script names after renaming

### Issue 3: Missing Refund Data in Validation Reports

**Symptom:** `check_aggregated_parquet_data.py` shows "Empty DataFrame" for refunds section

**Root Cause:** Date parsing format mismatch in `00_convert_historical.py` - script used `%Y-%m-%d %H:%M:%S` format for `refnd_date` column, but CSV files contained `YYYY-MM-DD` format only

**Diagnosis Steps:**
```bash
# Check refund Parquet files
duckdb -c "SELECT COUNT(*), COUNT(refnd_date) FROM 'Parquet_Data/transactions/rfnd/**/*.parquet'"

# Check original CSV
head -20 Daily_Data/rfnd_atlas.csv
```

**Solution:**
1. Modified date parsing logic to handle both formats:
   - If date string contains space: parse as `%Y-%m-%d %H:%M:%S`
   - Otherwise: parse as `%Y-%m-%d`
2. Remove existing refund data: `rm -rf Parquet_Data/transactions/rfnd`
3. Rerun conversion: `python Scripts/00_convert_historical.py`
4. Rebuild subscription view: `python Scripts/04_build_subscription_view.py`

**Prevention:** Always inspect source CSV date formats before defining parsing logic

### Issue 4: Incorrect Activation Date Validation

**Symptom:** `check_aggregated_parquet_data.py` reports many "missing days" for activation data

**Root Cause:** Validation script checked completeness from earliest `activation_date` (2006-06-18), but transaction data only started from 2024-01-01

**Solution:**
1. Modified validation logic to determine `transactions_start_date` from earliest `last_renewal_date`
2. For activation checks, validation now starts from the later of:
   - Earliest activation date
   - Transaction start date (2024-01-01)
3. This prevents false positives for historical activation records without corresponding transaction data

**Prevention:** Always align validation date ranges with actual data availability periods

### General Debugging Commands

```bash
# Check LaunchAgent schedules
launchctl list | grep josemanco

# View all logs
tail -f Logs/*.log

# Check Parquet data integrity
python Scripts/others/check_aggregated_parquet_data.py

# Verify partition counts
find Parquet_Data/transactions -type d -name "year_month=*" | wc -l

# Check for duplicate rows
duckdb -c "SELECT msisdn, COUNT(*) as cnt FROM 'Parquet_Data/aggregated/subscriptions.parquet' GROUP BY msisdn HAVING cnt > 1"
```

---

## Important Reminders

1. **Sequential Execution:** Scripts MUST run in order (1→2→3)
2. **Schema Enforcement:** All data processing enforces strict schemas via Polars
3. **Hive Partitioning:** Critical for query performance - never disable
4. **Edge Cases:** Pipeline handles missing activations (~10-20%) and CPC upgrades
5. **Log Rotation:** Automatic 15-day retention prevents disk issues
6. **Cross-Platform:** Supports both macOS and Linux date handling
7. **SSH Authentication:** Required for remote data access - never commit keys
8. **Data Validation:** Run validation scripts regularly to ensure quality
9. **Git Ignore:** Never commit data files (Daily_Data, Parquet_Data, Logs)
10. **Production System:** Changes require careful testing before deployment

---

**END OF UNIFIED DOCUMENTATION**

*This document serves as the single source of truth for both human developers and AI assistants working with the CVAS Beyond Data project.*

**Author:** Jose Manco
**License:** Internal use only - Proprietary
**Last Updated:** December 16, 2025
**Python Version:** 3.x
**Platform:** macOS (with Linux compatibility)
**Data Coverage:** November 2022 - Present (1123+ days)
**Transaction Types:** 6 (ACT, RENO, DCT, CNR, RFND, PPD)
**Partitions:** 24 months per type (2024-01 to 2025-12)
**Total Files:** 1123+ NBS_BASE snapshots + 24 months × 6 types = 1267+ files
