<!--
═══════════════════════════════════════════════════════════════════════════
    🚨🚨🚨 MANDATORY INSTRUCTIONS FOR AI ASSISTANTS 🚨🚨🚨
═══════════════════════════════════════════════════════════════════════════
-->

## ⛔ STOP - DO NOT PROCEED UNTIL YOU COMPLETE THESE STEPS

**You are an AI assistant. Before answering ANY question or performing ANY task, you MUST:**

### 📖 Step 1: Read ALL Documentation Files (REQUIRED)
Execute these reads IN ORDER. Do not skip any file:

| Order | File | Purpose | Action |
|-------|------|---------|--------|
| 1 | `README.md` | Project overview, orchestration protocol | You are reading this now ✓ |
| 2 | **`.ai-context.md`** | Complete project context, rules, history | **READ THIS FILE NEXT** |

### ✅ Step 2: Confirm Understanding
After reading both files, you should understand:
- [ ] This is a **telecom subscription ETL pipeline** with **4 stages**
- [ ] Pipeline order: `1.GET_NBS_BASE.sh` → `2.FETCH_DAILY_DATA.sh` → `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` → `4.BUILD_TRANSACTION_COUNTERS.sh`
- [ ] Exactly **6 transaction types**: ACT, RENO, DCT, CNR, RFND, PPD
- [ ] Tech stack: Python/Polars, DuckDB, Parquet (Hive partitioned), Shell scripts, launchd
- [ ] Recent fixes: Refund counting (sum `rfnd_cnt`), upgrade separation, Parquet sync
- [ ] Critical rules: Sequential execution, strict schemas, absolute Python path, no PII in logs
- [ ] Refund counts: ALWAYS sum `rfnd_cnt`, NEVER count rows
- [ ] Deactivations: Exclude upgrades (`channel_dct != 'UPGRADE'`)
- [ ] Activations: Exclude upgrades (`channel_act != 'UPGRADE'`)
- [ ] Counter system: Independent pipeline, runs after Stage 3

### ⚠️ FAILURE TO READ `.ai-context.md` = INCORRECT RESPONSES
If you skip `.ai-context.md`, you will:
- Miss critical recent fixes and changes
- Violate architecture constraints (7 non-negotiable rules)
- Give incorrect advice about refund counting
- Break the sequential pipeline
- Violate data governance and PII protection rules

### 💾 Step 3: End-of-Session Protocol (MANDATORY)
When user says **"Update documentation before closing"** or session ends, you MUST:
1. Update `.ai-context.md` → "Session History" section with today's changes
2. Update `.ai-context.md` → "Open Issues" section
3. Update `.ai-context.md` → "Last Updated" date at top
4. Update `README.md` → "Last Updated" date below (line after this section)
5. Save both files

**DO NOT skip this step. Documentation synchronization is critical for session continuity.**

---

# CVAS Beyond Data

> **Last Updated**: 2025-01-19

---

## 🤖 AI AGENT ORCHESTRATION (DETAILED PROTOCOL)

**This section is for AI agents only. Human users can skip to "Project Description" below.**

### 📖 START OF SESSION CHECKLIST
- [ ] Read `README.md` (this file) - mandatory AI instructions at top
- [ ] Read `.ai-context.md` - complete project context
- [ ] Check `.ai-context.md` → "Session History" for recent changes
- [ ] Check `.ai-context.md` → "Open Issues" for pending work
- [ ] Confirm understanding of:
  - 4-stage sequential pipeline (NEVER break order)
  - 6 transaction types (NEVER change count)
  - Refund counting: sum `rfnd_cnt` column (NOT row count)
  - Deactivation/Activation counting: exclude upgrades
  - Absolute Python path: `/opt/anaconda3/bin/python`
  - No PII in logs (SECURITY)
- [ ] Ready to proceed with user's request

### 💾 END OF SESSION CHECKLIST
When user says **"Update documentation before closing"** or session ends:
- [ ] Update `.ai-context.md` → "Session History" section:
  - Add new entry with date (YYYY-MM-DD)
  - Summarize changes made this session
  - List files modified
  - Note any new issues discovered
- [ ] Update `.ai-context.md` → "Open Issues" section:
  - Add new issues discovered
  - Mark resolved issues as completed
  - Update status of in-progress issues
- [ ] Update `.ai-context.md` → "Last Updated" date (line 3)
- [ ] Update `README.md` → "Last Updated" date (line 52)
- [ ] Save both files
- [ ] Confirm to user: "Documentation updated and synchronized"

**CRITICAL**: Do NOT skip end-of-session updates. Session continuity depends on accurate history.

---

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

**CVAS Beyond Data** is a production-grade ETL (Extract, Transform, Load) pipeline designed for telecommunications subscription data processing and analytics. It automates the daily collection of 6 transaction types (ACT, RENO, DCT, CNR, RFND, PPD) from remote PostgreSQL servers, transforms them into optimized Parquet columnar format, and builds comprehensive lifecycle views for business analytics.

### Key Features
- **Automated Daily Execution**: Runs via macOS launchd scheduler (3 sequential jobs + 1 independent counter job).
- **6 Transaction Types**: ACT (activations), RENO (renewals), DCT (deactivations), CNR (cancellations), RFND (refunds), PPD (prepaid).
- **Parquet Storage**: Hive-partitioned Parquet files for efficient querying.
- **DuckDB Aggregation**: High-performance SQL aggregation for subscription lifecycle views.
- **Transaction Counters**: Service-level and CPC-level daily aggregates.

### Performance
- **Daily Processing**: ~1.5 hours for full pipeline (8:05 AM - 9:30 AM)
- **Historical Data**: 1123+ user base snapshots
- **Transaction Volume**: Millions of records per month
- **Counter Generation**: Service and CPC-level daily aggregates

---

## Architecture Overview

### 4-Stage Sequential Pipeline

```
1.GET_NBS_BASE.sh (8:05 AM)
    ↓ Fetches user base snapshot from Nova
2.FETCH_DAILY_DATA.sh (8:25 AM)
    ↓ Fetches 6 transaction types (ACT, RENO, DCT, CNR, RFND, PPD)
3.PROCESS_DAILY_AND_BUILD_VIEW.sh (8:30 AM)
    ↓ Converts CSVs to Parquet, builds subscription view
4.BUILD_TRANSACTION_COUNTERS.sh (9:30 AM)
    ↓ Generates service and CPC-level counters (INDEPENDENT)
```

**CRITICAL**: Stages 1-3 MUST run sequentially. Stage 4 is independent but requires Stage 3 completion.

---

## Technology Stack

- **Python 3.x**: Data processing and transformation
- **Polars**: High-performance DataFrame library
- **DuckDB**: In-process SQL OLAP database for aggregation
- **PostgreSQL**: Remote Nova server (data source)
- **Parquet**: Columnar storage format with SNAPPY compression
- **Shell Scripts**: Bash for orchestration
- **launchd**: macOS scheduler for automated execution

---

## Directory Structure

```
CVAS_BEYOND_DATA/
├── README.md                            # This file (orchestrator + GitHub description)
├── .ai-context.md                       # All project context (read this for details)
├── 1.GET_NBS_BASE.sh                    # Stage 1: Fetch user base
├── 2.FETCH_DAILY_DATA.sh                # Stage 2: Fetch transactions
├── 3.PROCESS_DAILY_AND_BUILD_VIEW.sh    # Stage 3: Process & aggregate
├── 4.BUILD_TRANSACTION_COUNTERS.sh      # Stage 4: Build counters
├── Scripts/
│   ├── 01_aggregate_user_base.py        # User base aggregation
│   ├── 02_fetch_remote_nova_data.sh     # Remote data fetching
│   ├── 03_process_daily.py              # Daily CSV to Parquet
│   ├── 04_build_subscription_view.py    # Subscription lifecycle view
│   ├── 05_build_counters.py             # Counter generation
│   ├── 00_convert_historical.py         # Historical CSV to Parquet
│   └── utils/
│       ├── counter_utils.py             # Counter utilities
│       └── log_rotation.sh              # Log management
├── sql/
│   └── build_subscription_view.sql      # DuckDB aggregation query
├── MASTERCPC.csv                        # Service and CPC metadata
├── Daily_Data/                          # Daily CSV files (gitignored)
├── Parquet_Data/                        # Parquet storage (gitignored)
│   └── transactions/
│       ├── act/year_month=*/
│       ├── reno/year_month=*/
│       ├── dct/year_month=*/
│       ├── cnr/year_month=*/
│       ├── rfnd/year_month=*/
│       └── ppd/year_month=*/
├── User_Base/                           # User base snapshots (gitignored)
├── Counters/                            # Counter outputs (gitignored)
│   ├── Counters_CPC.parquet
│   └── Counters_Service.csv
└── Logs/                                # Pipeline logs (gitignored)
```

---

## Pipeline Workflow

### Stage 1: Get User Base (8:05 AM)
- **Script**: `1.GET_NBS_BASE.sh` → `Scripts/01_aggregate_user_base.py`
- **Purpose**: Fetch current user base snapshot from Nova PostgreSQL server
- **Output**: `User_Base/NBS_BASE_YYYYMMDD.csv`
- **Duration**: ~5 minutes

### Stage 2: Fetch Daily Data (8:25 AM)
- **Script**: `2.FETCH_DAILY_DATA.sh` → `Scripts/02_fetch_remote_nova_data.sh`
- **Purpose**: Fetch yesterday's transaction data for all 6 types
- **Output**: `Daily_Data/{act,reno,dct,cnr,rfnd,ppd}_atlas_day.csv`
- **Duration**: ~10 minutes

### Stage 3: Process & Build View (8:30 AM)
- **Script**: `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` → `Scripts/03_process_daily.py` + `Scripts/04_build_subscription_view.py`
- **Purpose**: Convert CSVs to Parquet, build subscription lifecycle view
- **Output**: `Parquet_Data/transactions/{type}/year_month=YYYY-MM/*.parquet`
- **Duration**: ~45 minutes

### Stage 4: Build Counters (9:30 AM)
- **Script**: `4.BUILD_TRANSACTION_COUNTERS.sh` → `Scripts/05_build_counters.py`
- **Purpose**: Generate service and CPC-level transaction counters
- **Output**: `Counters/Counters_Service.csv`, `Counters/Counters_CPC.parquet`
- **Duration**: ~15 minutes
- **Note**: Independent pipeline, can run after Stage 3

---

## Installation & Setup

### Prerequisites
```bash
# Python 3.x with required packages
pip install polars duckdb pandas python-dateutil
```

### Configuration
1. Update PostgreSQL connection details in `Scripts/02_fetch_remote_nova_data.sh`
2. Verify Python path in all scripts: `/opt/anaconda3/bin/python`
3. Ensure `MASTERCPC.csv` is present in project root

---

## Scheduled Automation

### launchd Configuration
The pipeline runs automatically via macOS launchd:

```xml
<!-- ~/Library/LaunchAgents/com.cvas.stage1.plist -->
<plist>
  <dict>
    <key>Label</key>
    <string>com.cvas.stage1</string>
    <key>ProgramArguments</key>
    <array>
      <string>/Users/josemanco/CVAS/CVAS_BEYOND_DATA/1.GET_NBS_BASE.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
      <key>Hour</key>
      <integer>8</integer>
      <key>Minute</key>
      <integer>5</integer>
    </dict>
  </dict>
</plist>
```

### Load launchd Jobs
```bash
launchctl load ~/Library/LaunchAgents/com.cvas.stage1.plist
launchctl load ~/Library/LaunchAgents/com.cvas.stage2.plist
launchctl load ~/Library/LaunchAgents/com.cvas.stage3.plist
launchctl load ~/Library/LaunchAgents/com.cvas.stage4.plist
```

---

## Manual Execution

### Run Full Pipeline
```bash
cd /Users/josemanco/CVAS/CVAS_BEYOND_DATA
./1.GET_NBS_BASE.sh && \
./2.FETCH_DAILY_DATA.sh && \
./3.PROCESS_DAILY_AND_BUILD_VIEW.sh && \
./4.BUILD_TRANSACTION_COUNTERS.sh
```

### Run Individual Stages
```bash
# Stage 1: User Base
./1.GET_NBS_BASE.sh

# Stage 2: Fetch Data
./2.FETCH_DAILY_DATA.sh

# Stage 3: Process & Build View
./3.PROCESS_DAILY_AND_BUILD_VIEW.sh

# Stage 4: Build Counters
./4.BUILD_TRANSACTION_COUNTERS.sh

# Stage 4 with backfill
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force
```

### Regenerate Historical Data
```bash
# Convert historical CSVs to Parquet
/opt/anaconda3/bin/python Scripts/00_convert_historical.py

# Rebuild all counters
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force
```

---

## Monitoring & Logs

### Log Files
```
Logs/
├── 1_get_nbs_base_YYYYMMDD.log
├── 2_fetch_daily_data_YYYYMMDD.log
├── 3_process_daily_YYYYMMDD.log
└── 4_build_counters_YYYYMMDD.log
```

### Log Rotation
- **Retention**: 15 days
- **Script**: `Scripts/utils/log_rotation.sh`
- **Execution**: Runs automatically at end of each stage

### Check Pipeline Status
```bash
# View latest logs
tail -f Logs/4_build_counters_$(date +%Y%m%d).log

# Check for errors
grep -i error Logs/*.log
```

---

## Data Schema

### Transaction Types

#### ACT/RENO/PPD (15 columns):
```
tmuserid, msisdn, cpc, trans_type_id, channel_id, channel_act, trans_date,
act_date, reno_date, camp_name, tef_prov, campana_medium, campana_id,
subscription_id, rev
```

#### DCT (13 columns):
```
tmuserid, msisdn, cpc, trans_type_id, channel_id, channel_dct, trans_date,
act_date, reno_date, camp_name, tef_prov, campana_medium, campana_id,
subscription_id
```

#### CNR (5 columns):
```
cancel_date, sbn_id, tmuserid, cpc, mode
```

#### RFND (7 columns):
```
tmuserid, cpc, refnd_date, rfnd_amount, rfnd_cnt, sbnid, instant_rfnd
```

### Counter Schemas

#### Counters_CPC.parquet (13 columns):
```
date, cpc, act_count, act_free, act_pay, upg_count, reno_count, dct_count,
upg_dct_count, cnr_count, ppd_count, rfnd_count, rfnd_amount, rev, last_updated
```

#### Counters_Service.csv (21 columns):
```
date, service_name, tme_category, cpcs, Free_CPC, Free_Period, Upgrade_CPC,
CHG_Period, CHG_Price, act_count, act_free, act_pay, upg_count, reno_count,
dct_count, upg_dct_count, cnr_count, ppd_count, rfnd_count, rfnd_amount, rev
```

---

## Troubleshooting

### Issue: Counters don't match manual counts
**Solution**:
1. Verify `rfnd_cnt` is being summed (not row count)
2. Check parquet files are up to date with source CSVs
3. Run `Scripts/00_convert_historical.py` to regenerate parquet files
4. Rebuild counters: `./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force`

### Issue: Pipeline stage fails
**Solution**:
1. Check previous stage completed successfully
2. Review logs in `Logs/` directory
3. Verify data files exist in expected locations
4. Ensure Python path is `/opt/anaconda3/bin/python`

### Issue: Missing data in parquet files
**Solution**:
1. Re-run `Scripts/00_convert_historical.py`
2. Verify source CSVs in `/Users/josemanco/Dropbox/BEYOND_DATA_OLD_backup/`

See `.ai-context.md` for detailed troubleshooting.

---

## 🤝 Contributing

This project uses AI-assisted development with automatic documentation updates.

**For AI agents**: Follow the orchestration protocol at the top of this file.

---

## 📄 License

[Add your license here]

---

**For detailed technical documentation, architecture constraints, and session history, see `.ai-context.md`**
