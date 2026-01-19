# WARP.md - AI Assistant Guide for CVAS Beyond Data

> **📘 Quick Start for AI Assistants using Warp**
> 
> This document serves as your entry point to understanding and working with the CVAS Beyond Data project. Before answering ANY question or making ANY modification, you MUST follow the documentation reading sequence below.

---

## 🚀 MANDATORY: Documentation Reading Sequence

You MUST read these files IN ORDER before proceeding with any task:

### 1️⃣ **README.md** - Project Documentation (Read First)
**Purpose:** Complete project overview, architecture, and operational guide

**Command to read:**
```bash
Read README.md
```

**What you'll learn:**
- 🏗️ **Architecture Overview:** 4-stage sequential pipeline (User Base → Fetch → Process → Counters)
- 📊 **Transaction Types:** 6 types (ACT, RENO, DCT, CNR, RFND, PPD) - NEVER change this count
- 🛠️ **Technology Stack:** Python/Polars, DuckDB, Parquet with Hive partitioning, Shell scripts, launchd
- 📂 **Directory Structure:** Immutable structure with orchestration scripts at root
- 🔄 **Pipeline Workflow:** Detailed execution flow for each stage
- 📋 **Data Schemas:** Strict schemas for all transaction types
- 🔍 **Troubleshooting:** Common issues and solutions
- ⏰ **Scheduling:** Automated execution via macOS launchd (8:05 AM - 9:30 AM)

**Key Takeaways:**
- Pipeline MUST run sequentially: `1.GET_NBS_BASE.sh` → `2.FETCH_DAILY_DATA.sh` → `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` → `4.BUILD_TRANSACTION_COUNTERS.sh`
- Stage 4 (counters) is independent and runs after Stage 3
- All data stored in Parquet format with Hive partitioning (`year_month=YYYY-MM`)
- Scripts use absolute Python path: `/opt/anaconda3/bin/python` (launchd requirement)

---

### 2️⃣ **CONTEXT.md** - Session Memory & Recent Changes (Read Second)
**Purpose:** Recent fixes, data characteristics, and session history

**Command to read:**
```bash
Read CONTEXT.md
```

**What you'll learn:**
- 🔧 **Recent Fixes (Jan 2026):**
  - Refund count logic fixed: Sum `rfnd_cnt` column instead of counting rows
  - Parquet data regeneration: 436 missing rows for Dec 2025 added
  - Deactivation count logic: Exclude upgrade deactivations
- 📁 **Critical File Locations:** Where source data, scripts, and outputs are stored
- 📊 **Known Data Characteristics:** Schema details for refunds, deactivations, service categories
- ✅ **Validation Results:** December 2025 metrics (7,001 refunds, €15,033.93 for Beauty & Health)
- 🔄 **Workflow for Updates:** Steps when source CSVs update or daily pipeline runs
- 📝 **Session Notes:** Historical record of changes and fixes

**Key Takeaways:**
- Refund counts: ALWAYS sum `rfnd_cnt`, NEVER count rows
- Parquet files must be in sync with source CSVs (run `00_convert_historical.py` after CSV updates)
- Upgrade deactivations tracked separately: `upg_dct_count` vs `dct_count`
- Historical data location: `/Users/josemanco/Dropbox/BEYOND_DATA_OLD_backup/`

---

### 3️⃣ **RULES.md** - Non-Negotiable Constraints (Read Third)
**Purpose:** Critical architecture constraints and development rules

**Command to read:**
```bash
Read RULES.md
```

**What you'll learn:**
- 🚨 **Critical Architecture Constraints:**
  1. Sequential pipeline dependency (NEVER break 1→2→3→4 order)
  2. Six transaction types (NEVER add/remove types)
  3. Directory structure (IMMUTABLE)
  4. Strict schema enforcement (NON-NEGOTIABLE)
  5. Counter system (INDEPENDENT pipeline)
  6. Hive partitioning (REQUIRED for performance)
  7. Absolute Python path (LAUNCHD requirement)
- 🛠️ **Development Rules:**
  - Path management (relative paths from project root)
  - Cross-platform date handling (macOS + Linux)
  - Log rotation (15-day retention, MANDATORY)
  - Error handling patterns (non-zero exit codes)
  - SQL query management (complex SQL in `sql/` directory)
  - Python dependencies (PINNED versions)
- 🔒 **Data Governance:**
  - User base category mapping (BUSINESS logic - no changes without approval)
  - NBS_BASE immutability (1123+ historical snapshots)
  - PII protection (SECURITY - no logging `tmuserid` or `msisdn` in automated processes)
  - Git ignore enforcement (NEVER commit data files)
- 🐛 **Edge Cases & Pitfalls:**
  - Missing activation records (some subscriptions start with RENO)
  - CPC upgrades (subscriptions can change services)
  - Subscription status hierarchy (DCT > CNR > ACTIVE)
  - RFND partitioning (`__HIVE_DEFAULT_PARTITION__` for NULL dates)
  - Launchd environment differences (minimal PATH)
  - Parquet compression (always use SNAPPY)
- ⚡ **Performance Rules:**
  - DuckDB query optimization (filter on `year_month` partition)
  - Polars vs Pandas (prefer Polars for new code)

**Key Takeaways:**
- NEVER break sequential execution order (except Stage 4 which is independent)
- NEVER change the 6 transaction types without updating ALL components
- NEVER modify directory structure or remove Hive partitioning
- ALWAYS use absolute Python path: `/opt/anaconda3/bin/python` in shell scripts
- ALWAYS call `rotate_log()` at script start
- ALWAYS sum `rfnd_cnt` column, NEVER count rows
- PII protection: No logging in pipeline scripts, allowed only in manual query scripts

---

## ✅ Verification Checklist

After reading all 3 files, confirm you understand:

- [ ] This is a **telecom subscription ETL pipeline** with **4 sequential stages**
- [ ] Pipeline order: `1.GET_NBS_BASE.sh` → `2.FETCH_DAILY_DATA.sh` → `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` → `4.BUILD_TRANSACTION_COUNTERS.sh`
- [ ] Stage 4 (counters) is **independent** and runs after Stage 3 completes
- [ ] Exactly **6 transaction types**: ACT, RENO, DCT, CNR, RFND, PPD
- [ ] Tech stack: **Python/Polars, DuckDB, Parquet (Hive partitioned), Shell scripts, launchd**
- [ ] Recent fixes: **Refund counting** (sum `rfnd_cnt`), **upgrade separation**, **Parquet sync**
- [ ] Critical rules: **Sequential execution**, **strict schemas**, **absolute Python path**, **no PII in logs**
- [ ] Counter system: **Nubico filtering**, **idempotent updates**, **upgrade separation**

---

## 🎯 Common Tasks Quick Reference

### Task: Understanding the Project
1. Read `README.md` - Get complete overview
2. Read `CONTEXT.md` - Learn recent changes
3. Read `RULES.md` - Understand constraints
4. Review `sql/build_subscription_view.sql` - See DuckDB aggregation logic

### Task: Debugging Counter Issues
1. Read `CONTEXT.md` - Check recent fixes (refund counting, upgrade separation)
2. Read `RULES.md` - Review counter system rules and schemas
3. Examine `Scripts/05_build_counters.py` - Counter calculation logic
4. Check `Scripts/utils/counter_utils.py` - Utility functions
5. Validate with: `python Scripts/others/check_transactions_parquet_data.py`

### Task: Modifying Pipeline Scripts
1. Read `RULES.md` - Review modification checklist
2. Check dependencies: `grep -r "script_name" .`
3. Test manually before committing
4. Update documentation if behavior changes
5. Run validation scripts in `Scripts/others/`

### Task: Adding New Transaction Type (⚠️ Complex)
1. **STOP:** Review `RULES.md` section on transaction types
2. Update `02_fetch_remote_nova_data.sh` - Add SQL query
3. Update `03_process_daily.py` - Add schema definition
4. Update `sql/build_subscription_view.sql` - Add JOIN/aggregation
5. Update `04_build_subscription_view.py` - Verify paths
6. Update `README.md` - Document new type
7. Test end-to-end with real data

### Task: Investigating Data Issues
1. Check logs: `tail -100 Logs/*.log`
2. Query by MSISDN: `python Scripts/others/query_msisdn_from_tx.py <msisdn>`
3. Query by TMUSERID: `python Scripts/others/query_tmuserid_from_tx.py <tmuserid>`
4. Check Parquet data: `python Scripts/others/check_transactions_parquet_data.py`
5. Validate aggregated data: `python Scripts/others/check_aggregated_parquet_data.py`

### Task: Regenerating Parquet Data
```bash
# When source CSVs update
cd /Users/josemanco/CVAS/CVAS_BEYOND_DATA
/opt/anaconda3/bin/python Scripts/00_convert_historical.py

# Rebuild counters
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force
```

---

## 📂 Critical Files Map

### Orchestration Scripts (Root Level)
```
1.GET_NBS_BASE.sh                    # Stage 1: User base collection (8:05 AM)
2.FETCH_DAILY_DATA.sh                # Stage 2: Fetch transactions (8:25 AM)
3.PROCESS_DAILY_AND_BUILD_VIEW.sh   # Stage 3: Process & aggregate (8:30 AM)
4.BUILD_TRANSACTION_COUNTERS.sh     # Stage 4: Build counters (9:30 AM, independent)
```

### Active Pipeline Scripts (Scripts/)
```
01_aggregate_user_base.py            # Aggregates 1100+ user base snapshots
02_fetch_remote_nova_data.sh         # Fetches data from PostgreSQL via SSH
03_process_daily.py                  # Converts CSV to Parquet with Hive partitioning
04_build_subscription_view.py        # Builds subscription view with DuckDB
05_build_counters.py                 # Builds transaction counters (CPC & Service)
```

### Utility Scripts (Scripts/utils/)
```
log_rotation.sh                      # 15-day log retention
counter_utils.py                     # Counter system utilities (MASTERCPC parsing, atomic writes)
```

### Query & Validation Scripts (Scripts/others/)
```
query_msisdn_from_tx.py             # Query subscription lifecycle by MSISDN
query_tmuserid_from_tx.py           # Query subscription lifecycle by TMUSERID
check_transactions_parquet_data.py  # Validate transaction Parquet files
check_aggregated_parquet_data.py    # Validate aggregated subscription data
check_users.py                       # Validate user data quality
extract_music_subscriptions.py      # Extract music subscriptions
calculate_lt_ltv.py                 # Calculate lifetime metrics
```

### SQL Queries
```
sql/build_subscription_view.sql     # 241-line DuckDB aggregation query
```

### Configuration & Documentation
```
requirements.txt                     # Python dependencies (pinned versions)
MASTERCPC.csv                       # Service/CPC mapping reference
README.md                           # Complete project documentation
CONTEXT.md                          # Session memory & recent changes
RULES.md                            # Architecture constraints & development rules
WARP.md                             # This file - AI assistant guide
```

### Data Directories (Git-Ignored)
```
Daily_Data/                         # Temporary CSV files (staging)
Parquet_Data/                       # Columnar storage (Hive partitioned)
  ├── transactions/                 # Transaction data by type
  │   ├── act/year_month=YYYY-MM/
  │   ├── reno/year_month=YYYY-MM/
  │   ├── dct/year_month=YYYY-MM/
  │   ├── cnr/year_month=YYYY-MM/
  │   ├── rfnd/year_month=YYYY-MM/
  │   └── ppd/year_month=YYYY-MM/
  └── aggregated/
      └── subscriptions.parquet     # Final comprehensive view
Counters/                           # Transaction counters
  ├── Counters_CPC.parquet          # CPC-level counters (15 columns)
  └── Counters_Service.csv          # Service-level counters (21 columns)
User_Base/                          # User base data
  ├── NBS_BASE/                     # 1100+ daily snapshots
  ├── user_base_by_service.csv      # Aggregated by service
  └── user_base_by_category.csv     # Aggregated by category
Logs/                               # Execution logs (15-day retention)
```

---

## 🔄 Session Management

### Starting a Session
When user sends: `"Read @./README.md"` or `"Start session"`

**You MUST:**
1. Read `README.md`
2. Read `CONTEXT.md`
3. Read `RULES.md`
4. Confirm: *"I've read all documentation. Ready to assist with CVAS Beyond Data pipeline."*

### Ending a Session
When user sends: `"End session"` or `"Close session"`

**You MUST:**
1. **Update `CONTEXT.md`:**
   - Add new section under `## 📝 Session Notes` with format:
     ```markdown
     ### Session: [DATE] - [Brief Title]
     **Changes Made:**
     - [List each change/fix/addition]
     
     **Files Modified:**
     - [List files with brief description]
     
     **Validation:**
     - [Any tests run or validations performed]
     ```
   - Update `**Last Updated:**` date at top

2. **Update `RULES.md` (if applicable):**
   - Add new rules/constraints discovered during session
   - Update compliance status table if needed
   - Update `**Last Updated:**` date at top

3. **Update `README.md` (if applicable):**
   - Update if features, schemas, or behavior changed
   - Update `**Last Updated:**` date at top

4. **Update `WARP.md` (if applicable):**
   - Update if AI assistant workflows or instructions changed
   - Update `**Last Updated:**` date at top

5. **Confirm to user:**
   *"Session documented. Updated: [list files updated]. Summary: [1-2 sentence summary]"*

---

## ⚠️ CRITICAL REMINDERS

### Before Making ANY Changes
1. ✅ Read ALL 3 documentation files (`README.md`, `CONTEXT.md`, `RULES.md`)
2. ✅ Understand the sequential pipeline dependency
3. ✅ Check if change affects downstream processes
4. ✅ Verify schemas remain consistent
5. ✅ Test with sample data before committing

### Never Do This (❌)
- ❌ Break sequential execution order (1→2→3→4, with 4 independent)
- ❌ Add/remove transaction types without updating ALL components
- ❌ Modify directory structure
- ❌ Change column names or data types in schemas
- ❌ Remove Hive partitioning
- ❌ Use relative Python paths in shell scripts
- ❌ Log PII (`tmuserid`, `msisdn`) in automated pipeline scripts
- ❌ Commit data files (`Daily_Data/`, `Parquet_Data/`, `Counters/`, `Logs/`)
- ❌ Count refund rows instead of summing `rfnd_cnt` column
- ❌ Include upgrades in `act_count` (they go in `upg_count`)
- ❌ Include Nubico services in counter aggregations

### Always Do This (✅)
- ✅ Use absolute Python path: `/opt/anaconda3/bin/python` in shell scripts
- ✅ Call `rotate_log()` at start of orchestration scripts
- ✅ Validate inputs before processing
- ✅ Exit with non-zero code on errors
- ✅ Log errors with timestamps
- ✅ Use Hive partitioning for all transaction Parquet files
- ✅ Filter on `year_month` partition in DuckDB queries
- ✅ Sum `rfnd_cnt` column for refund counts
- ✅ Exclude upgrades from `act_count` (filter `channel_act != 'UPGRADE'`)
- ✅ Filter out Nubico services in counter aggregations
- ✅ Round monetary values to 2 decimals
- ✅ Test with `launchctl start` if modifying orchestration scripts
- ✅ Run validation scripts after changes

---

## 🛠️ Development Workflow

### 1. Understanding Existing Code
```bash
# Read documentation
cat README.md CONTEXT.md RULES.md

# Explore structure
ls -lh Scripts/
cat sql/build_subscription_view.sql

# Check recent changes
git log --oneline -10
tail -50 Logs/*.log
```

### 2. Making Changes
```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes (follow RULES.md)
# Test manually
bash 1.GET_NBS_BASE.sh
bash 2.FETCH_DAILY_DATA.sh
bash 3.PROCESS_DAILY_AND_BUILD_VIEW.sh
bash 4.BUILD_TRANSACTION_COUNTERS.sh

# Validate
python Scripts/others/check_transactions_parquet_data.py
python Scripts/others/check_aggregated_parquet_data.py

# Test with launchctl (if modified orchestration)
launchctl start com.josemanco.nbs_base
tail -f Logs/1.GET_NBS_BASE.log
```

### 3. Committing Changes
```bash
# Review changes
git status
git diff

# Commit (with co-author line)
git add <files>
git commit -m "Brief description

Detailed explanation of changes.

Co-Authored-By: Warp <agent@warp.dev>"

# Push
git push origin feature/your-feature-name
```

### 4. Updating Documentation
- Update `README.md` if functionality changed
- Update `CONTEXT.md` session notes
- Update `RULES.md` if new constraints added
- Update `WARP.md` if AI workflows changed

---

## 📊 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 1: USER BASE COLLECTION (8:05 AM)                         │
│ 1.GET_NBS_BASE.sh → Scripts/01_aggregate_user_base.py          │
├─────────────────────────────────────────────────────────────────┤
│ Input:  User_Base/NBS_BASE/*.csv (1100+ snapshots)             │
│ Output: user_base_by_service.csv, user_base_by_category.csv    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 2: TRANSACTION DATA FETCH (8:25 AM)                       │
│ 2.FETCH_DAILY_DATA.sh → Scripts/02_fetch_remote_nova_data.sh   │
├─────────────────────────────────────────────────────────────────┤
│ Input:  Remote PostgreSQL (10.26.82.53)                        │
│ Output: Daily_Data/YYYY-MM-DD/{ACT,RENO,DCT,CNR,RFND,PPD}.csv  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 3: PROCESSING & AGGREGATION (8:30 AM)                     │
│ 3.PROCESS_DAILY_AND_BUILD_VIEW.sh                              │
│   → Scripts/03_process_daily.py (CSV to Parquet)               │
│   → Scripts/04_build_subscription_view.py (DuckDB aggregation) │
├─────────────────────────────────────────────────────────────────┤
│ Input:  Daily_Data/YYYY-MM-DD/*.csv                            │
│ Output: Parquet_Data/transactions/*/year_month=YYYY-MM/        │
│         Parquet_Data/aggregated/subscriptions.parquet           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 4: TRANSACTION COUNTERS (9:30 AM, INDEPENDENT)            │
│ 4.BUILD_TRANSACTION_COUNTERS.sh → Scripts/05_build_counters.py │
├─────────────────────────────────────────────────────────────────┤
│ Input:  Parquet_Data/transactions/*/year_month=YYYY-MM/        │
│         MASTERCPC.csv                                           │
│ Output: Counters/Counters_CPC.parquet (15 columns)             │
│         Counters/Counters_Service.csv (21 columns)              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🎓 Learning Resources

### Understanding the Pipeline
1. **Start here:** `README.md` - Architecture Overview section
2. **Data flow:** `README.md` - Pipeline Workflow section
3. **Schemas:** `README.md` - Data Schema section
4. **Recent changes:** `CONTEXT.md` - Recent Fixes & Changes section

### Understanding Transaction Types
- **ACT (Activations):** New subscriptions + upgrades (has revenue)
- **RENO (Renewals):** Subscription renewals (has revenue)
- **DCT (Deactivations):** Service cancellations (no revenue)
- **CNR (Cancellations):** User-initiated cancellations (no revenue)
- **RFND (Refunds):** Payment refunds (negative revenue)
- **PPD (Prepaid):** Pay-per-download one-time purchases (has revenue)

### Understanding Counter System
- **Purpose:** Aggregate transaction counts by CPC and Service
- **Independence:** Runs separately from main pipeline (Stage 4)
- **Modes:** Daily (default), Backfill (`--backfill`), Force (`--force`)
- **Key features:** Nubico filtering, upgrade separation, idempotent updates
- **Schemas:** 15 columns (CPC), 21 columns (Service)

### Understanding Hive Partitioning
- **Format:** `year_month=YYYY-MM` (e.g., `year_month=2025-01`)
- **Location:** `Parquet_Data/transactions/<type>/year_month=YYYY-MM/`
- **Benefits:** 100x faster queries via partition pruning in DuckDB
- **Implementation:** Always add `year_month` column before writing Parquet

### Understanding Edge Cases
1. **Missing ACT records:** Some subscriptions start with RENO
   - Solution: Treat first transaction as activation, flag with `missing_act_record`
2. **CPC upgrades:** Subscriptions can change services
   - Solution: Track all CPCs in `cpc_list`, detect via `trans_type_id = 1`
3. **Status hierarchy:** DCT > CNR > ACTIVE
   - Solution: Check deactivation first, then cancellation, default to active
4. **RFND partitioning:** NULL dates cause issues
   - Solution: Use `__HIVE_DEFAULT_PARTITION__` for NULL `year_month`

---

## 🔗 External Resources

### Source Data Locations
- **Historical Data:** `/Users/josemanco/Dropbox/BEYOND_DATA_OLD_backup/Historical_Data/`
- **Daily Data:** `/Users/josemanco/Dropbox/BEYOND_DATA_OLD_backup/Daily_Data/`
- **Remote Server:** `10.26.82.53` (PostgreSQL via SSH)

### Launchd Jobs
```bash
# Check all CVAS jobs
launchctl list | grep josemanco

# View job details
launchctl print gui/$(id -u)/com.josemanco.nbs_base
launchctl print gui/$(id -u)/com.josemanco.fetch_daily
launchctl print gui/$(id -u)/com.josemanco.process_daily
launchctl print gui/$(id -u)/com.josemanco.build_counters

# Manually trigger job
launchctl start com.josemanco.nbs_base
```

### Python Environment
- **Python Path:** `/opt/anaconda3/bin/python`
- **Dependencies:** `requirements.txt` (pinned versions)
  - polars==1.34.0
  - pyarrow==19.0.0
  - duckdb==1.2.1
  - pandas==2.2.3

---

## 📝 Maintenance Checklist

### Daily
- [ ] Check logs: `tail -100 Logs/*.log`
- [ ] Verify job execution: `launchctl list | grep josemanco`
- [ ] Monitor disk space: `du -sh Parquet_Data/ User_Base/ Counters/`

### Weekly
- [ ] Run validation scripts: `python Scripts/others/check_*.py`
- [ ] Review data quality
- [ ] Check for errors in logs: `grep -i error Logs/*.log`

### Monthly
- [ ] Archive old Parquet partitions if needed
- [ ] Review data retention policy
- [ ] Update documentation if needed

### After CSV Updates
- [ ] Regenerate Parquet: `python Scripts/00_convert_historical.py`
- [ ] Rebuild counters: `./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force`
- [ ] Validate key metrics

---

## 🆘 Getting Help

### For Code Issues
1. Read `RULES.md` - Edge Cases & Common Pitfalls section
2. Check `CONTEXT.md` - Known Data Characteristics section
3. Review `README.md` - Troubleshooting section
4. Check logs: `Logs/*.log`
5. Run validation scripts: `Scripts/others/check_*.py`

### For Data Issues
1. Query by MSISDN: `python Scripts/others/query_msisdn_from_tx.py <msisdn>`
2. Query by TMUSERID: `python Scripts/others/query_tmuserid_from_tx.py <tmuserid>`
3. Check Parquet data: `python Scripts/others/check_transactions_parquet_data.py`
4. Validate aggregated data: `python Scripts/others/check_aggregated_parquet_data.py`

### For Pipeline Issues
1. Check launchd status: `launchctl list | grep josemanco`
2. View last run: `launchctl print gui/$(id -u)/com.josemanco.<job> | grep "last exit"`
3. Check logs: `tail -f Logs/<stage>.log`
4. Test manually: `bash <stage>.sh`

---

**Last Updated:** 2025-01-19

**Project Path:** `/Users/josemanco/CVAS/CVAS_BEYOND_DATA`

**Maintained By:** Jose Manco

**AI Assistant:** This file was created to help you navigate the CVAS Beyond Data project efficiently. Always start by reading the 3 core documentation files in order: `README.md` → `CONTEXT.md` → `RULES.md`
