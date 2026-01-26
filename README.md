# CVAS Beyond Data - Telecommunications ETL Pipeline

> **Last Updated**: 2026-02-15
>
> **AI Agents**: Read `CLAUDE.md` for complete context, rules, and session history

---

## 📋 Table of Contents
- [Project Overview](#project-overview)
- [ETL Data Flow](#etl-data-flow)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Directory Structure](#directory-structure)
- [Pipeline Stages](#pipeline-stages)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Data Schemas](#data-schemas)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## 🎯 Project Overview

**CVAS Beyond Data** is a production-grade ETL pipeline for telecommunications subscription analytics. It processes millions of transaction records daily, transforming raw PostgreSQL data into optimized Parquet format for business intelligence and reporting.

### Key Capabilities
- **Automated Daily Processing**: Scheduled execution via macOS launchd (8:05 AM - 9:30 AM)
- **6 Transaction Types**: ACT (activations), RENO (renewals), DCT (deactivations), CNR (cancellations), RFND (refunds), PPD (prepaid)
- **High-Performance Storage**: Hive-partitioned Parquet with SNAPPY compression
- **Subscription Lifecycle Tracking**: DuckDB-powered aggregation for complete user journeys
- **Business Metrics**: Daily CPC-level and service-level transaction counters

### Performance Metrics
- **Daily Processing Time**: ~1.5 hours (full pipeline)
- **Historical Data**: 1,123+ user base snapshots
- **Transaction Volume**: Millions of records/month
- **Data Format**: Parquet (columnar, compressed)

---

## 🔄 ETL Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CVAS BEYOND DATA ETL PIPELINE                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 0: Configuration Setup (Manual/As-Needed)                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  0.GET_MASTERCPC_CSV.py                                                      │
│  ├─ Reads: Master CPCs Excel files                                          │
│  ├─ Processes: CPC metadata (service names, categories, prices, periods)    │
│  ├─ Special Logic: Sets cpc_period=99999 for PPD transactions               │
│  └─ Outputs: MASTERCPC.csv (CPC→Service mapping)                            │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: Extract User Base (8:05 AM) - Duration: ~5 min                     │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  1.GET_NBS_BASE.sh → Scripts/01_aggregate_user_base.py                      │
│  ├─ Extracts: NBS_Base.csv from Nova PostgreSQL (via SCP)                   │
│  ├─ Transforms: Aggregates by service, category, and CPC                    │
│  │   • Excludes: nubico, challenge arena, movistar apple music, juegos onmo │
│  │   • Maps: education/images → Edu_Ima, news/sports → News_Sport          │
│  └─ Loads:                                                                   │
│      • User_Base/YYYYMMDD_NBS_Base.csv (raw snapshot)                       │
│      • User_Base/user_base_by_service.csv (service-level aggregation)       │
│      • User_Base/user_base_by_category.csv (category-level aggregation)     │
│      • User_Base/user_base_by_cpc.csv (CPC-level aggregation)               │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: Extract Transactions (8:25 AM) - Duration: ~10 min                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  2.FETCH_DAILY_DATA.sh → Scripts/02_fetch_remote_nova_data.sh               │
│  ├─ Extracts: Yesterday's transactions from Nova PostgreSQL                 │
│  │   For each type (ACT, RENO, DCT, CNR, RFND, PPD):                        │
│  │   • Connects via SSH to remote server (10.26.82.53)                      │
│  │   • Executes SQL query to create temp table                              │
│  │   • Exports to CSV on remote server                                      │
│  │   • Downloads via SCP to local Daily_Data/                               │
│  └─ Loads:                                                                   │
│      • Daily_Data/act_atlas_day.csv                                          │
│      • Daily_Data/reno_atlas_day.csv                                         │
│      • Daily_Data/dct_atlas_day.csv                                          │
│      • Daily_Data/cnr_atlas_day.csv                                          │
│      • Daily_Data/rfnd_atlas_day.csv                                         │
│      • Daily_Data/ppd_atlas_day.csv                                          │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: Transform & Load (8:30 AM) - Duration: ~45 min                     │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  3.PROCESS_DAILY_AND_BUILD_VIEW.sh                                          │
│  ├─ Step 3A: Scripts/03_process_daily.py                                    │
│  │   ├─ Reads: Daily CSV files from Stage 2                                 │
│  │   ├─ Transforms:                                                          │
│  │   │   • Applies strict Polars schemas (type enforcement)                 │
│  │   │   • Parses date columns to Datetime                                  │
│  │   │   • Adds year_month partition column (YYYY-MM)                       │
│  │   │   • Deduplicates:                                                     │
│  │   │     - ACT/RENO/DCT/PPD: by (subscription_id, trans_date, trans_type_id) │
│  │   │     - CNR: by (sbn_id, cancel_date)                                  │
│  │   │     - RFND: by (sbnid, refnd_date)                                   │
│  │   └─ Loads: Parquet_Data/transactions/{type}/year_month=YYYY-MM/*.parquet │
│  │                                                                            │
│  └─ Step 3B: Scripts/04_build_subscription_view.py                          │
│      ├─ Reads: All transaction Parquet files                                │
│      ├─ Transforms (DuckDB SQL):                                             │
│      │   • Unions ACT + RENO transactions                                   │
│      │   • Tracks CPC changes/upgrades per subscription                     │
│      │   • Identifies first transaction (activation proxy)                  │
│      │   • Aggregates renewals, deactivations, cancellations, refunds       │
│      │   • Calculates subscription status and lifetime                      │
│      │   • Excludes upgrade deactivations (channel_dct != 'UPGRADE')        │
│      └─ Loads: Parquet_Data/aggregated/subscriptions.parquet                │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: Build Counters (9:30 AM) - Duration: ~15 min [INDEPENDENT]         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  4.BUILD_TRANSACTION_COUNTERS.sh → Scripts/05_build_counters.py             │
│  ├─ Reads:                                                                   │
│  │   • Parquet_Data/transactions/{type}/**/*.parquet (all transaction types)│
│  │   • MASTERCPC.csv (CPC metadata)                                         │
│  ├─ Transforms:                                                              │
│  │   • Computes daily counts by CPC:                                        │
│  │     - act_count (excludes channel_act='UPGRADE')                         │
│  │     - act_free (rev=0, non-upgrade)                                      │
│  │     - act_pay (rev>0, non-upgrade)                                       │
│  │     - upg_count (channel_act='UPGRADE')                                  │
│  │     - reno_count                                                          │
│  │     - dct_count (excludes channel_dct='UPGRADE')                         │
│  │     - upg_dct_count (channel_dct='UPGRADE')                              │
│  │     - cnr_count                                                           │
│  │     - ppd_count                                                           │
│  │     - rfnd_count (SUM of rfnd_cnt column, NOT row count!)                │
│  │     - rfnd_amount, rev                                                    │
│  │   • Merges with historical counters (idempotent)                         │
│  │   • Joins with MASTERCPC.csv for service metadata                        │
│  │   • Filters out: nubico, challenge arena, movistar apple music, juegos onmo │
│  └─ Loads:                                                                   │
│      • Counters/Counters_CPC.parquet (historical CPC-level counters)        │
│      • Counters/Counters_Service.csv (CPC-level with service metadata)      │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ MAINTENANCE: Historical Data Regeneration (As-Needed)                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Scripts/00_convert_historical.py                                           │
│  ├─ Purpose: One-time or refresh conversion of historical CSV to Parquet    │
│  ├─ Reads: Historical_Data/{act,reno,dct,cnr,rfnd,ppd}_atlas.csv           │
│  ├─ Transforms: Same as Stage 3A (schemas, partitioning, deduplication)     │
│  └─ Loads: Parquet_Data/transactions/{type}/year_month=*/*.parquet          │
│                                                                               │
│  When to run:                                                                │
│  • After historical CSV files are updated                                   │
│  • When Parquet data becomes stale                                          │
│  • Before backfilling counters                                              │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏗️ Architecture

### Pipeline Dependencies

```
Stage 0 (Manual) ──┐
                   ├──> Stage 1 ──> Stage 2 ──> Stage 3 ──> Stage 4
                   │                                           ↑
                   └───────────────────────────────────────────┘
                       (MASTERCPC.csv required for Stage 4)

CRITICAL: Stages 1→2→3 MUST run sequentially
Stage 4 is independent but requires Stage 3 completion
```

### Data Storage Architecture

```
Parquet_Data/
├── transactions/              # Hive-partitioned transaction data
│   ├── act/
│   │   ├── year_month=2024-01/
│   │   ├── year_month=2024-02/
│   │   └── ...
│   ├── reno/year_month=*/
│   ├── dct/year_month=*/
│   ├── cnr/year_month=*/
│   ├── rfnd/year_month=*/
│   └── ppd/year_month=*/
└── aggregated/
    └── subscriptions.parquet  # Subscription lifecycle view
```

---

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Data Processing** | Python 3.x + Polars | High-performance DataFrame operations |
| **SQL Aggregation** | DuckDB | In-process OLAP for subscription views |
| **Storage Format** | Parquet (SNAPPY) | Columnar compression, Hive partitioning |
| **Data Source** | PostgreSQL | Remote Nova server (10.26.82.53) |
| **Orchestration** | Bash Scripts | Pipeline stage coordination |
| **Scheduler** | macOS launchd | Automated daily execution |
| **Data Transfer** | SSH/SCP | Secure remote data fetching |

---

## 📁 Directory Structure

```
CVAS_BEYOND_DATA/
├── README.md                            # This file (human-readable overview)
├── CLAUDE.md                            # AI agent context (rules, history, schemas)
├── MASTERCPC.csv                        # CPC→Service mapping (generated by Stage 0)
├── requirements.txt                     # Python dependencies
│
├── 0.GET_MASTERCPC_CSV.py               # Stage 0: Generate CPC metadata
├── 1.GET_NBS_BASE.sh                    # Stage 1: Fetch user base
├── 2.FETCH_DAILY_DATA.sh                # Stage 2: Fetch transactions
├── 3.PROCESS_DAILY_AND_BUILD_VIEW.sh    # Stage 3: Transform & load
├── 4.BUILD_TRANSACTION_COUNTERS.sh      # Stage 4: Build counters
│
├── Scripts/
│   ├── 00_convert_historical.py         # Maintenance: Historical CSV→Parquet
│   ├── 01_aggregate_user_base.py        # Stage 1: User base aggregation
│   ├── 02_fetch_remote_nova_data.sh     # Stage 2: Remote data fetching
│   ├── 03_process_daily.py              # Stage 3A: Daily CSV→Parquet
│   ├── 04_build_subscription_view.py    # Stage 3B: Subscription lifecycle
│   ├── 05_build_counters.py             # Stage 4: Counter generation
│   └── utils/
│       ├── counter_utils.py             # Counter helper functions
│       └── log_rotation.sh              # Log management (15-day retention)
│
├── sql/
│   └── build_subscription_view.sql      # DuckDB aggregation query
│
├── Daily_Data/                          # Daily CSV files (gitignored)
│   ├── act_atlas_day.csv
│   ├── reno_atlas_day.csv
│   ├── dct_atlas_day.csv
│   ├── cnr_atlas_day.csv
│   ├── rfnd_atlas_day.csv
│   └── ppd_atlas_day.csv
│
├── Parquet_Data/                        # Parquet storage (gitignored)
│   ├── transactions/
│   │   ├── act/year_month=*/
│   │   ├── reno/year_month=*/
│   │   ├── dct/year_month=*/
│   │   ├── cnr/year_month=*/
│   │   ├── rfnd/year_month=*/
│   │   └── ppd/year_month=*/
│   └── aggregated/
│       └── subscriptions.parquet
│
├── User_Base/                           # User base snapshots (gitignored)
│   ├── NBS_BASE/
│   │   └── YYYYMMDD_NBS_Base.csv
│   ├── user_base_by_service.csv
│   ├── user_base_by_category.csv
│   └── user_base_by_cpc.csv
│
├── Counters/                            # Counter outputs (gitignored)
│   ├── Counters_CPC.parquet             # Historical CPC-level counters
│   └── Counters_Service.csv             # CPC-level with service metadata
│
└── Logs/                                # Pipeline logs (gitignored)
    ├── 1_get_nbs_base_YYYYMMDD.log
    ├── 2_fetch_daily_data_YYYYMMDD.log
    ├── 3_process_daily_YYYYMMDD.log
    └── 4_build_counters_YYYYMMDD.log
```

---

## 🚀 Pipeline Stages

### Stage 0: Generate CPC Metadata (Manual)
**Script**: `0.GET_MASTERCPC_CSV.py`
**Trigger**: Manual execution when CPC master files are updated
**Purpose**: Generate `MASTERCPC.csv` from Excel master files
**Output**: `MASTERCPC.csv` (cpc, service_name, tme_category, cpc_period, cpc_price)
**Special Logic**: Sets `cpc_period=99999` for PPD transactions

### Stage 1: Extract User Base (8:05 AM)
**Script**: `1.GET_NBS_BASE.sh` → `Scripts/01_aggregate_user_base.py`
**Duration**: ~5 minutes
**Purpose**: Fetch and aggregate daily user base snapshot
**Outputs**:
- `User_Base/NBS_BASE/YYYYMMDD_NBS_Base.csv` (raw snapshot)
- `User_Base/user_base_by_service.csv` (service-level aggregation)
- `User_Base/user_base_by_category.csv` (category-level aggregation)
- `User_Base/user_base_by_cpc.csv` (CPC-level aggregation)

### Stage 2: Extract Transactions (8:25 AM)
**Script**: `2.FETCH_DAILY_DATA.sh` → `Scripts/02_fetch_remote_nova_data.sh`  
**Duration**: ~10 minutes  
**Purpose**: Fetch yesterday's transactions for all 6 types  
**Outputs**: `Daily_Data/{act,reno,dct,cnr,rfnd,ppd}_atlas_day.csv`

### Stage 3: Transform & Load (8:30 AM)
**Script**: `3.PROCESS_DAILY_AND_BUILD_VIEW.sh`  
**Duration**: ~45 minutes  
**Sub-stages**:
- **3A**: `Scripts/03_process_daily.py` - Convert CSVs to Parquet with deduplication
- **3B**: `Scripts/04_build_subscription_view.py` - Build subscription lifecycle view

**Outputs**:
- `Parquet_Data/transactions/{type}/year_month=YYYY-MM/*.parquet`
- `Parquet_Data/aggregated/subscriptions.parquet`

### Stage 4: Build Counters (9:30 AM) [INDEPENDENT]
**Script**: `4.BUILD_TRANSACTION_COUNTERS.sh` → `Scripts/05_build_counters.py`  
**Duration**: ~15 minutes  
**Purpose**: Generate daily transaction counters by CPC  
**Outputs**:
- `Counters/Counters_CPC.parquet` (historical CPC-level counters)
- `Counters/Counters_Service.csv` (CPC-level with service metadata)

**Modes**:
- Daily: Process yesterday's date
- Backfill: Process date range
- Force: Overwrite existing data

---

## ⚙️ Installation & Setup

### Prerequisites
```bash
# Python 3.x with required packages
pip install -r requirements.txt

# Or install manually:
pip install polars duckdb pandas python-dateutil pyarrow
```

### Configuration Steps

1. **Update PostgreSQL Connection**
   Edit `Scripts/02_fetch_remote_nova_data.sh`:
   ```bash
   REMOTE_USER="omadmin"
   REMOTE_HOST="10.26.82.53"
   REMOTE_DB="nova"
   ```

2. **Verify Python Path**
   All scripts use: `/opt/anaconda3/bin/python`  
   Update if your Python installation differs.

3. **Generate MASTERCPC.csv**
   ```bash
   /opt/anaconda3/bin/python 0.GET_MASTERCPC_CSV.py
   ```

4. **Set Up launchd (Optional - for automation)**
   ```bash
   # Copy plist files to LaunchAgents
   cp launchd/*.plist ~/Library/LaunchAgents/
   
   # Load jobs
   launchctl load ~/Library/LaunchAgents/com.cvas.stage1.plist
   launchctl load ~/Library/LaunchAgents/com.cvas.stage2.plist
   launchctl load ~/Library/LaunchAgents/com.cvas.stage3.plist
   launchctl load ~/Library/LaunchAgents/com.cvas.stage4.plist
   ```

---

## 💻 Usage

### Manual Execution

#### Run Full Pipeline
```bash
cd /Users/josemanco/CVAS/CVAS_BEYOND_DATA

# Sequential execution (Stages 1-4)
./1.GET_NBS_BASE.sh && \
./2.FETCH_DAILY_DATA.sh && \
./3.PROCESS_DAILY_AND_BUILD_VIEW.sh && \
./4.BUILD_TRANSACTION_COUNTERS.sh
```

#### Run Individual Stages
```bash
# Stage 1: User Base
./1.GET_NBS_BASE.sh

# Stage 2: Fetch Transactions
./2.FETCH_DAILY_DATA.sh

# Stage 3: Transform & Load
./3.PROCESS_DAILY_AND_BUILD_VIEW.sh

# Stage 4: Build Counters (with options)
./4.BUILD_TRANSACTION_COUNTERS.sh                    # Daily mode
./4.BUILD_TRANSACTION_COUNTERS.sh --force            # Force overwrite
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill         # Backfill all dates
./4.BUILD_TRANSACTION_COUNTERS.sh 2025-01-15         # Specific date
./4.BUILD_TRANSACTION_COUNTERS.sh --start-date 2025-01-01 --end-date 2025-01-31
```

### Maintenance Tasks

#### Regenerate Historical Parquet Data
```bash
# When historical CSV files are updated
/opt/anaconda3/bin/python Scripts/00_convert_historical.py

# Then rebuild counters
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force
```

#### Generate CPC Metadata
```bash
# When master CPC Excel files are updated
/opt/anaconda3/bin/python 0.GET_MASTERCPC_CSV.py
```

---

## 📊 Monitoring

### Log Files
```
Logs/
├── 1_get_nbs_base_YYYYMMDD.log
├── 2_fetch_daily_data_YYYYMMDD.log
├── 3_process_daily_YYYYMMDD.log
└── 4_build_counters_YYYYMMDD.log
```

**Log Retention**: 15 days (automatic rotation via `Scripts/utils/log_rotation.sh`)

### Check Pipeline Status
```bash
# View latest logs
tail -f Logs/4_build_counters_$(date +%Y%m%d).log

# Check launchd job status
launchctl list | grep com.cvas

# Verify output files
ls -lh Counters/
ls -lh Parquet_Data/transactions/act/year_month=2025-02/
```

---

## 📋 Data Schemas

### Transaction Types

| Type | Columns | Key Fields | Notes |
|------|---------|------------|-------|
| **ACT** | 15 | subscription_id, cpc, trans_date, channel_act, rev | Activations (excludes UPGRADE) |
| **RENO** | 15 | subscription_id, cpc, trans_date, channel_act, rev | Renewals |
| **DCT** | 13 | subscription_id, cpc, trans_date, channel_dct | Deactivations (excludes UPGRADE) |
| **CNR** | 5 | sbn_id, cpc, cancel_date, mode | Cancellations |
| **RFND** | 7 | sbnid, cpc, refnd_date, rfnd_cnt, rfnd_amount | Refunds (sum rfnd_cnt!) |
| **PPD** | 15 | subscription_id, cpc, trans_date, rev | Prepaid (cpc_period=99999) |

### Output Files

#### Counters_CPC.parquet (13 columns)
```
date, cpc, act_count, act_free, act_pay, upg_count, reno_count, 
dct_count, upg_dct_count, cnr_count, ppd_count, rfnd_count, 
rfnd_amount, rev, last_updated
```

#### Counters_Service.csv (18 columns)
```
date, service_name, tme_category, cpc, cpc_period, cpc_price, 
act_count, act_free, act_pay, upg_count, reno_count, dct_count, 
upg_dct_count, cnr_count, ppd_count, rfnd_count, rfnd_amount, rev
```

#### MASTERCPC.csv (5 columns)
```
cpc, service_name, tme_category, cpc_period, cpc_price
```

---

## 🔧 Troubleshooting

### Issue: Pipeline Stage Fails

**Symptoms**: Stage exits with error code

**Checklist**:
1. Check logs in `Logs/` directory
2. Verify previous stage completed successfully
3. Ensure data files exist in expected locations
4. Verify Python path: `/opt/anaconda3/bin/python`
5. Check disk space and permissions

### Issue: Counter Mismatches

**Symptoms**: Counters don't match manual CSV counts

**Common Causes**:
1. **Refund undercounting**: Ensure `rfnd_cnt` column is summed, not row count
2. **Stale Parquet data**: Run `Scripts/00_convert_historical.py`
3. **Upgrade inclusion**: Verify `channel_act != 'UPGRADE'` and `channel_dct != 'UPGRADE'` filters

**Solution**:
```bash
# Regenerate Parquet files
/opt/anaconda3/bin/python Scripts/00_convert_historical.py

# Rebuild counters
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force
```

### Issue: Missing MASTERCPC.csv

**Symptoms**: Stage 4 fails with "MASTERCPC.csv not found"

**Solution**:
```bash
/opt/anaconda3/bin/python 0.GET_MASTERCPC_CSV.py
```

### Issue: SSH/SCP Connection Failures

**Symptoms**: Stage 2 cannot connect to remote server

**Checklist**:
1. Verify SSH keys are configured
2. Test manual connection: `ssh omadmin@10.26.82.53`
3. Check network connectivity
4. Verify remote server is accessible

---

## 🤝 Contributing

This project is maintained with AI assistance (Abacus AI Desktop, Claude Sonnet 4.5).

**For AI Agents**: Read `CLAUDE.md` for complete context, critical rules, session history, and development guidelines.

**For Humans**: Follow existing code patterns, maintain schema consistency, and update documentation when making changes.

---

## 📄 License

Jose Manco Only

---

**Project Root**: `/Users/josemanco/CVAS/CVAS_BEYOND_DATA`  
**Python Environment**: `/opt/anaconda3/bin/python`  
**Remote Server**: `omadmin@10.26.82.53` (Nova PostgreSQL)
