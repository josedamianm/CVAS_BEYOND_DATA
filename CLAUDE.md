# 🚨 AI CONTEXT - READ THIS FILE FIRST

> **Last Updated**: 2026-02-15
> **Project**: CVAS Beyond Data - Telecommunications ETL Pipeline
> **Purpose**: Complete AI agent context, rules, schemas, and session history

---

## ⛔ MANDATORY FIRST ACTIONS

**Before responding to ANY user request:**

1. **Read this entire file** (CLAUDE.md) - Contains critical rules and complete context
2. **Read `README.md`** - Contains project overview and human-readable documentation
3. **Then respond** to the user's request

**DO NOT skip these steps. DO NOT just acknowledge. EXECUTE them.**

---

## 📋 Table of Contents

1. [System Information](#system-information)
2. [Project Overview](#project-overview)
3. [Critical Architecture Constraints](#critical-architecture-constraints)
4. [Data Schemas](#data-schemas)
5. [File Locations & Structure](#file-locations--structure)
6. [Business Logic Rules](#business-logic-rules)
7. [Quick Start Commands](#quick-start-commands)
8. [Troubleshooting Guide](#troubleshooting-guide)
9. [Session History](#session-history)
10. [Open Issues & TODOs](#open-issues--todos)
11. [Key Concepts & Domain Knowledge](#key-concepts--domain-knowledge)
12. [Validation & Metrics](#validation--metrics)

---

## 🖥️ System Information

- **Last Updated**: 2026-02-15
- **Primary Agent**: Abacus AI Desktop (Claude Sonnet 4.5)
- **Project Root**: `/Users/josemanco/CVAS/CVAS_BEYOND_DATA`
- **Python Environment**: `/opt/anaconda3/bin/python` (absolute path required for launchd)
- **Remote Server**: `omadmin@10.26.82.53` (Nova PostgreSQL)
- **Scheduler**: macOS launchd (8:05 AM - 9:30 AM daily)

---

## 🎯 Project Overview

### What This Pipeline Does

**CVAS Beyond Data** is a production ETL pipeline that:
1. Extracts subscription transaction data from remote PostgreSQL (Nova)
2. Transforms CSV data into optimized Parquet format with Hive partitioning
3. Loads data into analytical storage for business intelligence
4. Generates daily transaction counters by CPC and service

### Pipeline Architecture (4 Stages + 1 Setup)

```
Stage 0 (Manual): Generate MASTERCPC.csv from Excel master files
    ↓
Stage 1 (8:05 AM): Fetch user base snapshot from Nova → User_Base/
    ↓
Stage 2 (8:25 AM): Fetch 6 transaction types from Nova → Daily_Data/
    ↓
Stage 3 (8:30 AM): Convert CSVs to Parquet + Build subscription view → Parquet_Data/
    ↓
Stage 4 (9:30 AM): Build transaction counters → Counters/
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Processing | Python 3.x + Polars | High-performance DataFrame operations |
| SQL Aggregation | DuckDB | In-process OLAP for subscription views |
| Storage Format | Parquet (SNAPPY) | Columnar compression, Hive partitioning |
| Data Source | PostgreSQL | Remote Nova server |
| Orchestration | Bash Scripts | Pipeline stage coordination |
| Scheduler | macOS launchd | Automated daily execution |

---

## 🚨 CRITICAL ARCHITECTURE CONSTRAINTS (NON-NEGOTIABLE)

### 1. Sequential Pipeline Dependency (NEVER BREAK)

**RULE**: Stages 1→2→3 MUST execute in strict order. Each depends on the previous completing successfully.

```
1.GET_NBS_BASE.sh → 2.FETCH_DAILY_DATA.sh → 3.PROCESS_DAILY_AND_BUILD_VIEW.sh → 4.BUILD_TRANSACTION_COUNTERS.sh
```

**Why**:
- Stage 2 needs yesterday's date (determined by Stage 1 timing)
- Stage 3 needs all 6 transaction CSV files from Stage 2
- Stage 4 needs Parquet transaction data from Stage 3

**DO NOT**:
- ❌ Make scripts independent
- ❌ Add parallel execution for Stages 1-3
- ❌ Remove dependency validation
- ❌ Change execution order

**DO**:
- ✅ Validate previous stage completed before starting
- ✅ Log dependencies clearly
- ✅ Exit with error if prerequisites missing

### 2. Six Transaction Types (NEVER CHANGE COUNT)

**RULE**: Exactly 6 transaction types must be processed. Adding/removing types breaks the entire pipeline.

```
ACT, RENO, DCT, CNR, RFND, PPD
```

**Why**: DuckDB aggregation query (`sql/build_subscription_view.sql`) expects all 6. Missing types cause SQL failures.

### 3. Directory Structure (IMMUTABLE)

**RULE**: Never modify the directory structure. Scripts use relative paths from project root.

**Critical Paths**:
- `Daily_Data/` - Daily CSV files
- `Parquet_Data/transactions/{type}/year_month=*/` - Hive-partitioned Parquet
- `User_Base/` - User base snapshots
- `Counters/` - Counter outputs
- `Logs/` - Pipeline logs

### 4. Strict Schema Enforcement (NON-NEGOTIABLE)

**RULE**: All Parquet files must follow exact Polars schemas defined in `Scripts/03_process_daily.py`. Schema violations cause aggregation failures.

**Schema Locations**:
- ACT/RENO/PPD: 15 columns (with `rev`)
- DCT: 13 columns (no `rev`, has `channel_dct`)
- CNR: 5 columns
- RFND: 7 columns (includes `rfnd_cnt`)

### 5. Hive Partitioning (REQUIRED FOR PERFORMANCE)

**RULE**: All transaction Parquet files MUST use Hive partitioning by `year_month=YYYY-MM`.

**Format**: `Parquet_Data/transactions/{type}/year_month=2025-02/*.parquet`

**Why**: Enables partition pruning for efficient date-range queries.

### 6. Absolute Python Path (LAUNCHD REQUIREMENT)

**RULE**: All Python scripts called from launchd MUST use absolute path: `/opt/anaconda3/bin/python`

**Why**: launchd doesn't inherit shell environment variables. Relative paths fail.

### 7. No PII in Logs (SECURITY)

**RULE**: NEVER log `tmuserid` or `msisdn` in automated processes. Only in manual debugging.

**Why**: Data privacy compliance.

### 8. Refund Count Logic (CRITICAL)

**RULE**: Refund counts MUST sum the `rfnd_cnt` column, NOT count rows.

**Why**: `rfnd_atlas.csv` contains aggregated data where one row can represent multiple refunds.

**Correct**: `pl.col('rfnd_cnt').sum()`  
**Incorrect**: `len(rfnd_df)` or `count(rows)`

### 9. Upgrade Exclusion (CRITICAL)

**RULE**: 
- Activations: Exclude `channel_act = 'UPGRADE'`
- Deactivations: Exclude `channel_dct = 'UPGRADE'`

**Why**: Upgrades are tracked separately in `upg_count` and `upg_dct_count`. Including them in regular counts inflates metrics.

---

## 📊 Data Schemas

### Transaction Types (Polars Schemas)

#### ACT (Activations) - 15 columns
```python
{
    'tmuserid': pl.Utf8,           # User ID (PII - don't log)
    'msisdn': pl.Utf8,             # Phone number (PII - don't log)
    'cpc': pl.Int64,               # Content Provider Code
    'trans_type_id': pl.Int64,     # Transaction type (0=new, 1=upgrade)
    'channel_id': pl.Int64,        # Channel ID
    'channel_act': pl.Utf8,        # Channel name (e.g., 'UPGRADE', 'WEB', 'SMS')
    'trans_date': pl.Datetime,     # Transaction date
    'act_date': pl.Datetime,       # Activation date
    'reno_date': pl.Datetime,      # Renewal date
    'camp_name': pl.Utf8,          # Campaign name
    'tef_prov': pl.Int64,          # Telefonica provider
    'campana_medium': pl.Utf8,     # Campaign medium
    'campana_id': pl.Utf8,         # Campaign ID
    'subscription_id': pl.Int64,   # PRIMARY KEY
    'rev': pl.Float64              # Revenue
}
```

#### RENO (Renewals) - 15 columns
Same schema as ACT.

#### DCT (Deactivations) - 13 columns
```python
{
    'tmuserid': pl.Utf8,
    'msisdn': pl.Utf8,
    'cpc': pl.Int64,
    'trans_type_id': pl.Int64,
    'channel_dct': pl.Utf8,        # Deactivation channel (note: channel_dct, not channel_act)
    'trans_date': pl.Datetime,
    'act_date': pl.Datetime,
    'reno_date': pl.Datetime,
    'camp_name': pl.Utf8,
    'tef_prov': pl.Int64,
    'campana_medium': pl.Utf8,
    'campana_id': pl.Utf8,
    'subscription_id': pl.Int64    # PRIMARY KEY
    # NO 'rev' column
}
```

#### CNR (Cancellations) - 5 columns
```python
{
    'cancel_date': pl.Datetime,
    'sbn_id': pl.Int64,            # subscription_id
    'tmuserid': pl.Utf8,
    'cpc': pl.Int64,
    'mode': pl.Utf8                # Cancellation mode
}
```

#### RFND (Refunds) - 7 columns
```python
{
    'tmuserid': pl.Utf8,
    'cpc': pl.Int64,
    'refnd_date': pl.Datetime,
    'rfnd_amount': pl.Float64,
    'rfnd_cnt': pl.Int64,          # CRITICAL: Sum this, don't count rows!
    'sbnid': pl.Int64,             # subscription_id
    'instant_rfnd': pl.Utf8        # Instant refund flag
}
```

#### PPD (Prepaid) - 15 columns
```python
{
    'tmuserid': pl.Utf8,
    'msisdn': pl.Utf8,
    'cpc': pl.Int64,
    'trans_type_id': pl.Int64,
    'channel_id': pl.Int64,
    'trans_date': pl.Datetime,
    'act_date': pl.Datetime,
    'reno_date': pl.Datetime,
    'camp_name': pl.Utf8,
    'tef_prov': pl.Int64,
    'campana_medium': pl.Utf8,
    'campana_id': pl.Utf8,
    'subscription_id': pl.Int64,
    'rev': pl.Float64
    # NO 'channel_act' column
}
```

### Configuration Files

#### MASTERCPC.csv - 5 columns
```csv
cpc,service_name,tme_category,cpc_period,cpc_price
893,La Gruta del Sexo,Light,7,2.99
928,Gofresh logos,Images,30,2.0
3109,MoviMessenger,Free Time,30,
```

**Special Rules**:
- PPD transactions: `cpc_period = 99999` (set by `0.GET_MASTERCPC_CSV.py`)
- No quotes in CSV output (`quote_style='never'`)

### Output Schemas

#### Counters_CPC.parquet - 13 columns
```python
{
    'date': pl.Date,
    'cpc': pl.Int64,
    'act_count': pl.Int64,         # Non-upgrade activations (channel_act != 'UPGRADE')
    'act_free': pl.Int64,          # Free non-upgrade activations (rev=0, channel_act != 'UPGRADE')
    'act_pay': pl.Int64,           # Paid non-upgrade activations (rev>0, channel_act != 'UPGRADE')
    'upg_count': pl.Int64,         # Upgrade activations (channel_act == 'UPGRADE')
    'reno_count': pl.Int64,        # Renewals
    'dct_count': pl.Int64,         # Non-upgrade deactivations (channel_dct != 'UPGRADE')
    'upg_dct_count': pl.Int64,     # Upgrade deactivations (channel_dct == 'UPGRADE')
    'cnr_count': pl.Int64,         # Cancellations
    'ppd_count': pl.Int64,         # Prepaid transactions
    'rfnd_count': pl.Int64,        # Refund count (SUM of rfnd_cnt column)
    'rfnd_amount': pl.Float64,     # Total refund amount
    'rev': pl.Float64,             # Total revenue
    'last_updated': pl.Datetime    # Timestamp of last update
}
```

#### Counters_Service.csv - 18 columns
```csv
date,service_name,tme_category,cpc,cpc_period,cpc_price,act_count,act_free,act_pay,upg_count,reno_count,dct_count,upg_dct_count,cnr_count,ppd_count,rfnd_count,rfnd_amount,rev
```

**Note**: This is CPC-level data with service metadata joined, NOT aggregated by service.

#### User Base Outputs

**user_base_by_service.csv**:
```csv
date|service_name|tme_category|User_Base
```

**user_base_by_category.csv**:
```csv
date|tme_category|User_Base
```

---

## 📁 File Locations & Structure

### Root Scripts

| Script | Purpose | Trigger |
|--------|---------|---------|
| `0.GET_MASTERCPC_CSV.py` | Generate MASTERCPC.csv from Excel | Manual (when master files update) |
| `1.GET_NBS_BASE.sh` | Fetch user base snapshot | launchd (8:05 AM) |
| `2.FETCH_DAILY_DATA.sh` | Fetch 6 transaction types | launchd (8:25 AM) |
| `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` | Transform & load | launchd (8:30 AM) |
| `4.BUILD_TRANSACTION_COUNTERS.sh` | Build counters | launchd (9:30 AM) |

### Processing Scripts

| Script | Called By | Purpose |
|--------|-----------|---------|
| `Scripts/00_convert_historical.py` | Manual | Convert historical CSVs to Parquet |
| `Scripts/01_aggregate_user_base.py` | Stage 1 | Aggregate user base by service/category |
| `Scripts/02_fetch_remote_nova_data.sh` | Stage 2 | Fetch data from remote PostgreSQL |
| `Scripts/03_process_daily.py` | Stage 3 | Convert daily CSVs to Parquet |
| `Scripts/04_build_subscription_view.py` | Stage 3 | Build subscription lifecycle view |
| `Scripts/05_build_counters.py` | Stage 4 | Generate transaction counters |

### Utility Scripts

| Script | Purpose |
|--------|---------|
| `Scripts/utils/counter_utils.py` | Counter helper functions |
| `Scripts/utils/log_rotation.sh` | Log management (15-day retention) |

### SQL Files

| File | Purpose |
|------|---------|
| `sql/build_subscription_view.sql` | DuckDB query for subscription lifecycle aggregation |

### Data Directories

| Directory | Contents | Gitignored |
|-----------|----------|------------|
| `Daily_Data/` | Daily CSV files from Stage 2 | Yes |
| `Parquet_Data/transactions/` | Hive-partitioned Parquet files | Yes |
| `Parquet_Data/aggregated/` | Subscription lifecycle view | Yes |
| `User_Base/` | User base snapshots and aggregations | Yes |
| `Counters/` | Transaction counter outputs | Yes |
| `Logs/` | Pipeline execution logs | Yes |

---

## 🔧 Business Logic Rules

### Counter Definitions

#### act_count (Activations)
- **Formula**: `COUNT(ACT) WHERE channel_act != 'UPGRADE'`
- **Excludes**: Upgrade activations
- **Includes**: New subscriptions only

#### act_free (Free Activations)
- **Formula**: `COUNT(ACT) WHERE channel_act != 'UPGRADE' AND rev = 0`
- **Purpose**: Track free trial activations

#### act_pay (Paid Activations)
- **Formula**: `COUNT(ACT) WHERE channel_act != 'UPGRADE' AND rev > 0`
- **Purpose**: Track paid activations

#### upg_count (Upgrade Activations)
- **Formula**: `COUNT(ACT) WHERE channel_act = 'UPGRADE'`
- **Purpose**: Track service upgrades

#### reno_count (Renewals)
- **Formula**: `COUNT(RENO)`
- **Purpose**: Track subscription renewals

#### dct_count (Deactivations)
- **Formula**: `COUNT(DCT) WHERE channel_dct != 'UPGRADE'`
- **Excludes**: Upgrade deactivations
- **Purpose**: Track true deactivations

#### upg_dct_count (Upgrade Deactivations)
- **Formula**: `COUNT(DCT) WHERE channel_dct = 'UPGRADE'`
- **Purpose**: Track deactivations due to upgrades

#### cnr_count (Cancellations)
- **Formula**: `COUNT(CNR)`
- **Purpose**: Track user-initiated cancellations

#### ppd_count (Prepaid)
- **Formula**: `COUNT(PPD)`
- **Purpose**: Track prepaid transactions

#### rfnd_count (Refunds)
- **Formula**: `SUM(rfnd_cnt)` **NOT** `COUNT(RFND)`
- **CRITICAL**: Must sum the `rfnd_cnt` column, not count rows
- **Why**: One row can represent multiple refunds

#### rfnd_amount (Refund Amount)
- **Formula**: `SUM(rfnd_amount)`
- **Purpose**: Total refund amount in euros

#### rev (Revenue)
- **Formula**: `SUM(rev) FROM ACT, RENO, PPD`
- **Purpose**: Total revenue from activations, renewals, and prepaid

### Transaction Filtering

#### User Exclusion List (Users_No_Limits.csv)
- **File**: `Users_No_Limits.csv` (project root)
- **Format**: CSV with columns `msisdn` and `tmuserid`
- **Purpose**: Exclude specific users (e.g., test accounts, internal users) from all counter calculations
- **Setup**:
  1. Create initial CSV with one MSISDN per line (no header)
  2. Run `python Scripts/enrich_users_no_limits.py` to add `tmuserid` column
  3. Script scans ACT/RENO/DCT/PPD transactions to find corresponding TMUSERIDs
  4. Creates backup (`.csv.bak`) and writes enriched CSV with both columns
- **Implementation**:
  - `05_build_counters.py` loads both columns at startup
  - Filters transactions in `load_transactions_for_date()`:
    - **ACT, RENO, DCT, PPD**: Filter by `msisdn` column
    - **CNR, RFND**: Filter by `tmuserid` column
  - Filtering occurs before any aggregation to ensure excluded users don't affect any counters

### Service Categories

#### Category Mapping (in `Scripts/01_aggregate_user_base.py`)

| Original Category | Mapped Category |
|-------------------|-----------------|
| education, images | Edu_Ima |
| news, sports | News_Sport |
| games, ugames | Games & Ugames |
| kids | KIDS |
| light | Light |
| music | Music |
| free time | Free Time |
| beauty & health | Beauty & Health |

#### Excluded Services

**Never include in aggregations**:
- nubico
- challenge arena
- movistar apple music

**Where excluded**:
- `Scripts/01_aggregate_user_base.py` (user base aggregation)
- `Scripts/05_build_counters.py` (counter aggregation)

### Critical Formulas

#### Net Activations
```
net_activations = act_count - dct_count
```

#### Churn Rate
```
churn_rate = dct_count / (user_base_start + act_count)
```

#### ARPU (Average Revenue Per User)
```
arpu = rev / user_base_avg
```

---

## 🚀 Quick Start Commands

### Daily Pipeline Execution (Automated)

```bash
# Automated via launchd (8:05 AM - 9:30 AM)
# No manual intervention required
```

### Manual Execution

```bash
# Full pipeline
cd /Users/josemanco/CVAS/CVAS_BEYOND_DATA
./1.GET_NBS_BASE.sh && \
./2.FETCH_DAILY_DATA.sh && \
./3.PROCESS_DAILY_AND_BUILD_VIEW.sh && \
./4.BUILD_TRANSACTION_COUNTERS.sh

# Individual stages
./1.GET_NBS_BASE.sh
./2.FETCH_DAILY_DATA.sh
./3.PROCESS_DAILY_AND_BUILD_VIEW.sh
./4.BUILD_TRANSACTION_COUNTERS.sh

# Stage 4 options
./4.BUILD_TRANSACTION_COUNTERS.sh                    # Daily mode
./4.BUILD_TRANSACTION_COUNTERS.sh --force            # Force overwrite
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill         # Backfill all dates
./4.BUILD_TRANSACTION_COUNTERS.sh 2025-01-15         # Specific date
./4.BUILD_TRANSACTION_COUNTERS.sh --start-date 2025-01-01 --end-date 2025-01-31
```

### Maintenance Commands

```bash
# Regenerate historical Parquet data
/opt/anaconda3/bin/python Scripts/00_convert_historical.py

# Rebuild all counters
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force

# Generate MASTERCPC.csv
/opt/anaconda3/bin/python 0.GET_MASTERCPC_CSV.py
```

### Validation Queries (Polars)

```python
import polars as pl

# Check refund counts for Beauty & Health (Dec 2025)
counters = pl.read_csv('Counters/Counters_Service.csv')
beauty_health = counters.filter(
    (pl.col('tme_category').str.contains('(?i)beauty')) &
    (pl.col('date').str.starts_with('2025-12'))
)
summary = beauty_health.select([
    pl.col('rfnd_count').sum(),  # Should be 7,001
    pl.col('rfnd_amount').sum()  # Should be €15,033.93
])
print(summary)

# Check CPC-level counters
cpc_counters = pl.read_parquet('Counters/Counters_CPC.parquet')
print(cpc_counters.filter(pl.col('date') == '2025-12-01'))
```

---

## 🐛 Troubleshooting Guide

### Issue: Counters Don't Match Manual Counts

**Symptoms**: `rfnd_count` or `rfnd_amount` differs from manual CSV aggregation

**Checklist**:
1. ✅ Is `rfnd_cnt` being summed (not row count)?
2. ✅ Are Parquet files up to date with source CSVs?
3. ✅ Has `Scripts/00_convert_historical.py` been run recently?
4. ✅ Are deactivations excluding upgrades (`channel_dct != 'UPGRADE'`)?
5. ✅ Are activations excluding upgrades (`channel_act != 'UPGRADE'`)?

**Solution**:
```bash
# Regenerate Parquet files
/opt/anaconda3/bin/python Scripts/00_convert_historical.py

# Rebuild counters
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force
```

### Issue: Missing Data in Parquet Files

**Symptoms**: Parquet row count < CSV row count

**Root Cause**: Historical conversion script not run after CSV updates

**Solution**:
```bash
/opt/anaconda3/bin/python Scripts/00_convert_historical.py
```

### Issue: Pipeline Stage Fails

**Symptoms**: Stage 2, 3, or 4 exits with error

**Checklist**:
1. ✅ Did previous stage complete successfully?
2. ✅ Check logs in `Logs/` directory
3. ✅ Verify data files exist in expected locations
4. ✅ Check Python path is `/opt/anaconda3/bin/python`
5. ✅ Verify disk space and permissions

**Common Errors**:
- **"File not found"**: Previous stage didn't complete
- **"Schema mismatch"**: CSV format changed, update schema in `Scripts/03_process_daily.py`
- **"Permission denied"**: Check file permissions and ownership

### Issue: SSH/SCP Connection Failures

**Symptoms**: Stage 2 cannot connect to remote server

**Checklist**:
1. ✅ Verify SSH keys are configured
2. ✅ Test manual connection: `ssh omadmin@10.26.82.53`
3. ✅ Check network connectivity
4. ✅ Verify remote server is accessible

### Issue: MASTERCPC.csv Missing or Outdated

**Symptoms**: Stage 4 fails with "MASTERCPC.csv not found" or counters have many unmapped CPCs

**Solution**:
```bash
/opt/anaconda3/bin/python 0.GET_MASTERCPC_CSV.py
```

---

## 📝 Session History

> **🤖 AI ASSISTANT**: When user says **"update docs"**, add a new session entry using this template:
>
> ```markdown
> ### Session: YYYY-MM-DD - [Brief Title]
> **Changes Made**:
> - [Change 1]
> - [Change 2]
>
> **Files Modified**:
> - `filename.py` - [brief description]
>
> **Validation**:
> - [Tests run or verifications]
> ```
> Then update "Last Updated" dates in both `CLAUDE.md` and `README.md`, and reply: **"✅ Documentation updated"**

### Session: 2026-02-15 - MASTERCPC Service Name Corrections
**Changes Made**:
- Fixed typo in service name: "Movistar Juegos – EA 0,99e" → "Movistar Juegos EA 099e"
- Removed leading space from "Movistar Musica" service name: " Movistar Musica" → "Movistar Musica"

**Files Modified**:
- `MASTERCPC.csv` - Corrected service name typos

**Action Required**:
- Run `./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force` to regenerate counters with corrected names

**Validation**:
- Confirmed MASTERCPC.csv only affects Stage 4 (counter generation) ✅

### Session: 2026-02-14 - Complete Documentation Rewrite
**Changes Made**:
- Completely rewrote `README.md` with ETL flow diagram for human understanding
- Completely rewrote `CLAUDE.md` with comprehensive AI agent context
- Added missing `0.GET_MASTERCPC_CSV.py` script to documentation
- Corrected directory structure (removed non-existent `.ai-context.md`)
- Fixed `Counters_Service.csv` schema (18 columns, not 19 or 21)
- Added visual ETL data flow diagram with all stages
- Clarified that `Counters_Service.csv` is CPC-level with service metadata, not aggregated by service
- Added comprehensive troubleshooting guide
- Organized all critical rules, schemas, and business logic

**Files Modified**:
- `README.md` - Complete rewrite for human understanding with ETL flow diagram
- `CLAUDE.md` - Complete rewrite for AI agents with full context

**Validation**:
- Verified all file paths exist
- Verified all schemas match actual code
- Verified all scripts are documented

### Session: 2026-01-21 - Added CPC-Level User Base Aggregation
**Changes Made**:
- Added `user_base_by_cpc.csv` output to Stage 1 (01_aggregate_user_base.py)
- Implemented CPC-level aggregation alongside existing service and category aggregations
- Created validation script to verify service totals match sum of CPC user bases
- Validated "Movistar Musica" service: all dates match ✅

**Files Modified**:
- `Scripts/01_aggregate_user_base.py` - Added CPC aggregation logic and output
- `Scripts/validate_user_base.py` - Created validation script

**Validation**:
- Movistar Musica (10 CPCs): Service totals = CPC sums for all dates ✅
- Output file: `User_Base/user_base_by_cpc.csv` (3.3 MB, 100K+ records) ✅

### Session: 2026-02-14 - Documentation Reconciliation & Schema Logic Verification
**Changes Made**:
- Conducted comprehensive audit of all scripts and folder structures
- Verified `Counters_Service.csv` schema matches actual output (18 columns, CPC-level)
- Confirmed `Scripts/00_convert_historical.py` role as setup/maintenance tool
- Confirmed aggregation logic in `Scripts/01_aggregate_user_base.py` (Service/Category level, not CPC)
- Updated `CLAUDE.md` schemas to reflect ground truth

**Files Modified**:
- `CLAUDE.md` - Updated "Last Updated" and schemas
- `README.md` - Updated "Last Updated"

**Validation**:
- Verified `Counters_Service.csv` header matches schema
- Verified `MASTERCPC.csv` structure matches code expectations

### Session: 2026-01-18 - Refund Count Fix
**Changes Made**:
- `rfnd_count` was undercounting by ~90% for Beauty & Health
- Parquet files were outdated (missing 436 rows for Dec 2025)
- Changed `rfnd_count` logic to sum `rfnd_cnt` column
- Regenerated all Parquet files from source CSVs
- Rebuilt all counters with `--backfill --force`

**Files Modified**:
- `Scripts/05_build_counters.py` - Changed aggregation logic in `compute_daily_cpc_counts()`
- `Scripts/utils/counter_utils.py` - Ensured `rfnd_cnt` column is loaded

**Validation**:
- Beauty & Health Dec 2025: 7,001 refunds, €15,033.93 ✅
- All categories match manual counts ✅

### Session: 2025-01-20 - Counter Generation Restructure & MASTERCPC Consolidation
**Changes Made**:
- Restructured `Counters_Service.csv` to one row per CPC (instead of aggregating by service)
- Simplified `MASTERCPC.csv` format to: `cpc, service_name, tme_category, cpc_period, cpc_price`
- Updated `0.GET_MASTERCPC_CSV.py` to set `cpc_period=99999` for PPD (Pay Per Download) transactions
- Changed CSV output to `quote_style='never'` (removed all quotes)
- Consolidated `MASTER_CPC_MATRIX.xlsx` with `Master CPCs 15-January-2026.xlsx`:
  - Created `MASTER_CPC_MATRIX_CONSOLIDATED.xlsx` with 661 CPCs
  - 404 CPCs updated with January 2026 prices
  - 3 new CPCs from January 2026
  - 254 CPCs kept from original master

**Files Modified**:
- `Scripts/05_build_counters.py` - Rewrote `aggregate_by_service()` to join metadata without aggregation
- `Scripts/utils/counter_utils.py` - Simplified `load_mastercpc()`, changed `write_atomic_csv()` quote_style
- `0.GET_MASTERCPC_CSV.py` - Set PPD period to 99999

**New Files Created**:
- `/Users/josemanco/Downloads/MASTER_CPC_MATRIX_CONSOLIDATED.xlsx` - Consolidated CPC master file

**Validation**:
- `Counters_Service.csv` now shows one row per date/CPC combination
- PPD transactions identifiable by `cpc_period=99999`
- All string fields unquoted in CSV output

### Session: 2025-01-28 - Documentation Simplification (2 files)
**Changes Made**:
- Consolidated `.ai-context.md` into `CLAUDE.md`
- Simplified to 2-file structure: `README.md` (human/GitHub) + `CLAUDE.md` (AI context)
- Added mandatory first action instructions to `CLAUDE.md`
- Deleted `.ai-context.md`

**Files Modified**:
- `CLAUDE.md` - Created with full AI context
- `README.md` - Updated to reference `CLAUDE.md`
- `.ai-context.md` - Deleted

**Validation**:
- 2-file structure verified ✅

### Session: 2025-01-19 - Documentation Consolidation
**Changes Made**:
- Consolidated CONTEXT.md, RULES.md, WARP.md into single `.ai-context.md`
- Created new README.md with mandatory AI instructions
- Deleted old documentation files (CONTEXT.md, RULES.md, WARP.md)
- Implemented 2-file structure: README.md (orchestrator) + .ai-context.md (complete context)

**Files Modified**:
- Created `.ai-context.md` - Complete project context
- Rewrote `README.md` - Mandatory AI instructions + GitHub overview
- Deleted `CONTEXT.md`, `RULES.md`, `WARP.md`

**Validation**:
- Final structure verified: 2 files only
- All critical content preserved
- AI orchestration protocol implemented

---

## 🔧 Open Issues & TODOs

> **🤖 AI ASSISTANT**: Update this section when new issues are discovered or resolved.

**No pending issues at this time.**

### Completed Items

#### 1. Add User Base by CPC Output ✅ (2026-01-21)
- Modified `Scripts/01_aggregate_user_base.py` to generate CPC-level aggregation
- Added output to `User_Base/user_base_by_cpc.csv` (3.3 MB, 100K+ records)
- Format: `date|cpc|User_Base`
- Validated: Service totals match sum of CPC user bases ✅

---

## 🧠 Key Concepts & Domain Knowledge

### Transaction Types Explained

| Type | Full Name | Description | Key Fields |
|------|-----------|-------------|------------|
| **ACT** | Activation | New subscription or upgrade | `channel_act`, `rev`, `trans_type_id` |
| **RENO** | Renewal | Subscription renewal | `channel_act`, `rev` |
| **DCT** | Deactivation | Subscription termination | `channel_dct` |
| **CNR** | Cancellation | User-initiated cancellation | `mode` |
| **RFND** | Refund | Refund issued to user | `rfnd_cnt`, `rfnd_amount`, `instant_rfnd` |
| **PPD** | Prepaid | Pay-per-download transaction | `rev` |

### Edge Cases & Pitfalls

#### 1. Missing Activation Records
**Issue**: Some subscriptions have RENO records but no ACT record.

**Handling**: `sql/build_subscription_view.sql` uses the first transaction (ACT or RENO) as the activation proxy. Sets `missing_act_record = true` flag.

#### 2. Upgrade Transactions
**Issue**: Upgrades create both ACT and DCT records with `channel_act='UPGRADE'` and `channel_dct='UPGRADE'`.

**Handling**: 
- Exclude from regular `act_count` and `dct_count`
- Track separately in `upg_count` and `upg_dct_count`

#### 3. Refund Aggregation
**Issue**: `rfnd_atlas.csv` contains pre-aggregated data where one row can represent multiple refunds.

**Handling**: Always sum `rfnd_cnt` column, never count rows.

#### 4. PPD Identification
**Issue**: PPD transactions need to be identifiable in counters.

**Handling**: Set `cpc_period=99999` in `MASTERCPC.csv` for PPD CPCs.

#### 5. Service Exclusions
**Issue**: Some services should never appear in reports (test services, deprecated services).

**Handling**: Exclude `nubico`, `challenge arena`, `movistar apple music` in aggregation scripts.

### Data Governance Rules

1. **PII Protection**: Never log `tmuserid` or `msisdn` in automated processes
2. **Schema Immutability**: Never change Parquet schemas without updating all downstream processes
3. **Partition Consistency**: Always use `year_month=YYYY-MM` format for Hive partitioning
4. **Deduplication**: Always deduplicate before writing to Parquet
5. **Idempotency**: Stage 4 can be re-run for the same date without data corruption

---

## 📈 Validation & Metrics

### December 2025 Counter Validation

#### Beauty & Health Category
| Metric | Value | Status |
|--------|-------|--------|
| Activations | 1,390 | ✅ |
| Renewals | 121,973 | ✅ |
| Deactivations | 1,815 | ✅ (excludes upgrades) |
| Refunds | **7,001** | ✅ (fixed from 737) |
| Refund Amount | **€15,033.93** | ✅ (fixed from €8,959.41) |
| Revenue | €218,243.27 | ✅ |

#### All Categories (Dec 2025)
| Category | Refunds | Amount |
|----------|---------|--------|
| Beauty & Health | 7,001 | €15,033.93 |
| Education & Images | 4,764 | €8,022.24 |
| Free Time | 17,716 | €45,693.89 |
| Games & Ugames | 4,848 | €12,852.35 |
| KIDS | 1,462 | €3,646.90 |
| Light | 2,790 | €8,220.66 |
| Music | 3,777 | €31,113.72 |
| News & Sport | 1,752 | €3,282.92 |

---

## 🎓 End of Session Command

When user says **"update docs"**:

1. Add new session entry to **Session History** section
2. Update **"Last Updated"** date in both `CLAUDE.md` and `README.md`
3. Update **Open Issues & TODOs** if applicable
4. Reply: **"✅ Documentation updated"**

---

**For human-readable project documentation and GitHub overview, see `README.md`**
