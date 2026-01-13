# CVAS Beyond Data - AI Development Rules & Context

> **Purpose:** This document defines critical rules, constraints, and context for AI assistants when modifying code in the CVAS Beyond Data pipeline. Read the `README.md` for general project documentation.

---

## 🚨 CRITICAL ARCHITECTURE CONSTRAINTS

### 1. Sequential Pipeline Dependency (NEVER BREAK)

**RULE:** Scripts MUST execute in strict order. Each depends on the previous completing successfully.

```
1.GET_NBS_BASE.sh (8:05 AM) → 2.FETCH_DAILY_DATA.sh (8:25 AM) → 3.PROCESS_DAILY_AND_BUILD_VIEW.sh (11:30 AM)
```

**Why:** Script 2 needs yesterday's data. Script 3 needs all 6 transaction CSV files from Script 2.

**DO NOT:**
- ❌ Make scripts independent
- ❌ Add parallel execution
- ❌ Remove dependency validation
- ❌ Change execution order

**DO:**
- ✅ Validate previous stage completed before starting
- ✅ Log dependencies clearly
- ✅ Exit with error if prerequisites missing

---

### 2. Six Transaction Types (NEVER CHANGE COUNT)

**RULE:** Exactly 6 transaction types must be processed. Adding/removing types breaks the entire pipeline.

```
ACT, RENO, DCT, CNR, RFND, PPD
```

**Why:** DuckDB aggregation query expects all 6. Missing types cause SQL failures.

**DO NOT:**
- ❌ Remove any transaction type
- ❌ Add new types without updating ALL components
- ❌ Make any type optional

**DO:**
- ✅ If adding type: Update `02_fetch_remote_nova_data.sh`, `03_process_daily.py`, `04_build_subscription_view.py`, `sql/build_subscription_view.sql`
- ✅ Validate all 6 CSV files exist before processing
- ✅ Use consistent case (uppercase) everywhere

---

### 3. Directory Structure (IMMUTABLE)

**RULE:** Never modify the directory structure. Scripts use relative paths from project root.

```
CVAS_BEYOND_DATA/
├── 1.GET_NBS_BASE.sh
├── 2.FETCH_DAILY_DATA.sh  
├── 3.PROCESS_DAILY_AND_BUILD_VIEW.sh
├── Scripts/
│   ├── 01_aggregate_user_base.py
│   ├── 02_fetch_remote_nova_data.sh
│   ├── 03_process_daily.py
│   ├── 04_build_subscription_view.py
│   ├── utils/log_rotation.sh
│   └── others/ (validation scripts)
├── sql/build_subscription_view.sql
├── Daily_Data/ (gitignored)
├── Parquet_Data/ (gitignored)
├── User_Base/ (gitignored)
└── Logs/ (gitignored)
```

**DO NOT:**
- ❌ Move orchestration scripts (1, 2, 3) out of root
- ❌ Rename the `Scripts/` directory
- ❌ Change Parquet storage structure
- ❌ Reorganize folder hierarchy

**DO:**
- ✅ Use relative paths: `Scripts/01_aggregate_user_base.py`
- ✅ Keep new validation scripts in `Scripts/others/`
- ✅ Maintain Hive partitioning structure in Parquet_Data

---

### 4. Strict Schema Enforcement (NON-NEGOTIABLE)

**RULE:** All Parquet files must follow exact schemas. Schema violations cause aggregation failures.

**Key Schemas:**

#### ACT/RENO/PPD (15 columns - with revenue):
```python
{
    'tmuserid': pl.Utf8,
    'msisdn': pl.Utf8,
    'cpc': pl.Int64,
    'trans_type_id': pl.Int64,
    'channel_id': pl.Int64,
    'channel_act': pl.Utf8,
    'trans_date': pl.Datetime,
    'act_date': pl.Datetime,
    'reno_date': pl.Datetime,
    'camp_name': pl.Utf8,
    'tef_prov': pl.Int64,
    'campana_medium': pl.Utf8,
    'campana_id': pl.Utf8,
    'subscription_id': pl.Int64,  # PRIMARY KEY
    'rev': pl.Float64
}
```

#### DCT (13 columns - no revenue):
```python
# Same as above minus 'rev', plus:
{'channel_dct': pl.Utf8}
```

#### CNR (5 columns):
```python
{
    'cancel_date': pl.Datetime,
    'sbn_id': pl.Int64,  # subscription_id
    'tmuserid': pl.Utf8,
    'cpc': pl.Int64,
    'mode': pl.Utf8
}
```

#### RFND (7 columns):
```python
{
    'tmuserid': pl.Utf8,
    'cpc': pl.Int64,
    'refnd_date': pl.Datetime,
    'rfnd_amount': pl.Float64,
    'rfnd_cnt': pl.Int64,
    'sbnid': pl.Int64,  # subscription_id
    'instant_rfnd': pl.Utf8
}
```

**DO NOT:**
- ❌ Change column names (breaks SQL queries)
- ❌ Modify data types
- ❌ Add optional columns
- ❌ Remove existing columns

**DO:**
- ✅ Enforce schemas in `03_process_daily.py` using Polars
- ✅ Validate CSV columns before Parquet conversion
- ✅ Fail loudly if schema mismatch detected

---

### 5. Hive Partitioning (REQUIRED FOR PERFORMANCE)

**RULE:** All transaction Parquet files MUST use Hive partitioning by `year_month=YYYY-MM`.

```
Parquet_Data/transactions/act/year_month=2025-01/*.parquet
Parquet_Data/transactions/act/year_month=2025-02/*.parquet
```

**Why:** DuckDB uses partition pruning for 100x faster queries.

**Implementation:**
```python
df = df.with_columns(pl.lit(year_month).alias('year_month'))
df.write_parquet(
    path,
    use_pyarrow=True,
    pyarrow_options={'partition_cols': ['year_month']}
)
```

**DO NOT:**
- ❌ Remove partition column
- ❌ Use different partitioning scheme
- ❌ Flatten Parquet structure
- ❌ Change date format (must be YYYY-MM)

**DO:**
- ✅ Always add `year_month` column before writing
- ✅ Use format: `YYYY-MM` (e.g., "2025-01")
- ✅ Maintain folder structure: `<type>/year_month=<value>/`

---

### 6. Absolute Python Path in Shell Scripts (LAUNCHD REQUIREMENT)

**RULE:** Shell scripts MUST use absolute path to Python for launchd compatibility.

```bash
# CORRECT (launchd-compatible):
/opt/anaconda3/bin/python Scripts/03_process_daily.py

# INCORRECT (fails in launchd):
python Scripts/03_process_daily.py
python3 Scripts/03_process_daily.py
```

**Why:** launchd runs with minimal PATH. Relative commands fail.

**DO NOT:**
- ❌ Use `python` or `python3` commands
- ❌ Rely on PATH environment variable
- ❌ Use virtualenv activation

**DO:**
- ✅ Use full path: `/opt/anaconda3/bin/python`
- ✅ Test scripts with: `launchctl start com.josemanco.<job>`
- ✅ Use absolute paths for all external commands in plist files

---

## 🛠️ DEVELOPMENT RULES

### Path Management

**RULE:** Use relative paths from project root, except for Python interpreter.

```bash
# CORRECT:
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"
/opt/anaconda3/bin/python Scripts/01_aggregate_user_base.py

# INCORRECT:
/opt/anaconda3/bin/python /Users/josemanco/CVAS/CVAS_BEYOND_DATA/Scripts/01_aggregate_user_base.py
```

**DO NOT:**
- ❌ Hardcode absolute paths (except Python)
- ❌ Assume current working directory
- ❌ Use `~` expansion in scripts

**DO:**
- ✅ Calculate paths relative to `$SCRIPT_DIR`
- ✅ Use `cd` to project root before executing
- ✅ Verify paths exist before using

---

### Cross-Platform Date Handling

**RULE:** Support both macOS and Linux date commands.

```bash
# CORRECT (cross-platform):
if [[ "$OSTYPE" == "darwin"* ]]; then
    yday=$(date -v-1d +%Y-%m-%d)  # macOS
else
    yday=$(date -d "yesterday" +%Y-%m-%d)  # Linux
fi

# INCORRECT (macOS-only):
yday=$(date -v-1d +%Y-%m-%d)
```

**DO NOT:**
- ❌ Use macOS-only date syntax
- ❌ Assume Linux date format
- ❌ Skip OS detection

**DO:**
- ✅ Check `$OSTYPE` before date operations
- ✅ Test on both platforms if modifying date logic
- ✅ Use ISO format: `YYYY-MM-DD`

---

### Log Rotation (MANDATORY)

**RULE:** All orchestration scripts MUST call log rotation at start.

```bash
# At the top of every orchestration script:
source "$(dirname "$0")/Scripts/utils/log_rotation.sh"
rotate_log "$LOGFILE"
```

**Why:** Prevents disk space issues. Logs accumulate quickly (15+ days = several GB).

**DO NOT:**
- ❌ Remove log rotation calls
- ❌ Change retention period without approval
- ❌ Skip rotation for new scripts

**DO:**
- ✅ Source `log_rotation.sh` in all new scripts
- ✅ Call `rotate_log "$LOGFILE"` before logging
- ✅ Keep 15-day retention (default)

---

### Error Handling Pattern

**RULE:** All scripts must exit with non-zero code on failure and log errors.

```bash
# CORRECT:
if [ ! -f "$FILE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ ERROR: File not found: $FILE" >> "$LOGFILE"
    exit 1
fi

# INCORRECT:
if [ ! -f "$FILE" ]; then
    echo "File not found"
fi
```

**DO NOT:**
- ❌ Continue execution after errors
- ❌ Exit with code 0 on failure
- ❌ Suppress error messages

**DO:**
- ✅ Use `set -e` at top of bash scripts
- ✅ Log errors with timestamp: `[YYYY-MM-DD HH:MM:SS] ✗ ERROR: ...`
- ✅ Validate inputs before processing
- ✅ Exit with code 1 on any failure

---

### SQL Query Management

**RULE:** Complex SQL queries (200+ lines) belong in `sql/` directory, NOT embedded in Python.

```python
# CORRECT:
sql_file = project_root / 'sql' / 'build_subscription_view.sql'
query = sql_file.read_text()
query = query.replace('{parquet_path}', str(parquet_path))

# INCORRECT:
query = """
SELECT ... (200 lines)
"""
```

**DO NOT:**
- ❌ Embed long SQL in Python strings
- ❌ Hard-code paths in SQL files
- ❌ Split complex queries across multiple files

**DO:**
- ✅ Store in `sql/` directory
- ✅ Use template variables: `{parquet_path}`
- ✅ Replace variables before execution
- ✅ Keep SQL readable with proper formatting

---

### Python Dependencies (PINNED VERSIONS)

**RULE:** Never change library versions without testing. Polars/PyArrow compatibility is fragile.

```
polars==1.34.0
pyarrow==19.0.0
duckdb==1.2.1
pandas==2.2.3
```

**DO NOT:**
- ❌ Upgrade Polars without testing
- ❌ Use `>=` or `~` version specifiers
- ❌ Add new dependencies without justification

**DO:**
- ✅ Pin exact versions in `requirements.txt`
- ✅ Test thoroughly after version changes
- ✅ Document why each library is needed

---

## 🔒 DATA GOVERNANCE RULES

### User Base Category Mapping (BUSINESS LOGIC)

**RULE:** Category mappings are business-defined. Never change without approval.

```python
# Education + Images → "Edu_Ima"
# News + Sports → "News_Sport"

# EXCLUSIONS (case-insensitive):
# - Services containing "nubico"
# - Services containing "challenge arena"
```

**Location:** `Scripts/01_aggregate_user_base.py:27-53`

**DO NOT:**
- ❌ Modify category names
- ❌ Change exclusion rules
- ❌ Remove business logic

**DO:**
- ✅ Get approval before changing mappings
- ✅ Document reasons for exclusions
- ✅ Test with real data after changes

---

### NBS_BASE Immutability

**RULE:** Historical NBS_BASE CSV files (1123+ files) are immutable snapshots. Never modify.

**Naming:** `YYYYMMDD_NBS_Base.csv` (e.g., `20220818_NBS_Base.csv`)

**DO NOT:**
- ❌ Edit historical CSV files
- ❌ Delete old snapshots
- ❌ Regenerate past dates

**DO:**
- ✅ Only add new daily files
- ✅ Preserve original formatting
- ✅ Validate new files match schema

---

### PII Protection (SECURITY)

**RULE:** Never log or export PII to unsecured locations.

**PII Fields:**
- `tmuserid` (user ID)
- `msisdn` (phone number)

**DO NOT:**
- ❌ Log `tmuserid` or `msisdn` values in production logs
- ❌ Print PII in error messages
- ❌ Export PII to unencrypted files

**DO:**
- ✅ Log counts/aggregates only
- ✅ Use `subscription_id` for debugging
- ✅ Mask PII in logs: `tmuserid: ***1234`

**EXCEPTION:** Query/debugging scripts (`Scripts/others/query_*.py`, `check_users.py`) are allowed to display PII when explicitly queried by the user for troubleshooting purposes. These scripts:
- Are NOT part of the automated pipeline
- Require manual execution with explicit user input
- Display results to terminal only (not logged to files)
- Are used for operational debugging and support

**Examples of Allowed PII Display:**
- `query_msisdn_from_tx.py` - Shows MSISDN and associated TMUSERIDs when queried
- `query_tmuserid_from_tx.py` - Shows TMUSERID and associated MSISDNs when queried
- `check_users.py` - Displays user details when queried by subscription_id

**Examples of Prohibited PII Logging:**
- Pipeline scripts (`01_aggregate_user_base.py`, `03_process_daily.py`, etc.) must NEVER log PII
- Orchestration scripts (`1.GET_NBS_BASE.sh`, etc.) must NEVER log PII
- Error messages in automated processes must use `subscription_id` only

---

### Git Ignore Enforcement

**RULE:** Never commit data files. Only commit code, configs, and documentation.

**NEVER COMMIT:**
- ❌ `Daily_Data/` (temporary CSV files)
- ❌ `Parquet_Data/` (large binary files)
- ❌ `User_Base/NBS_BASE/` (1123+ snapshot files)
- ❌ `User_Base/*.csv` (aggregated outputs)
- ❌ `Logs/` (execution logs)

**DO:**
- ✅ Commit `.sh` scripts
- ✅ Commit `.py` scripts
- ✅ Commit `.sql` files
- ✅ Commit `requirements.txt`
- ✅ Commit `MASTERCPC.csv` (reference data)

---

## 🐛 EDGE CASES & COMMON PITFALLS

### Edge Case 1: Missing Activation Records

**Problem:** Some subscriptions start with RENO (renewal) without prior ACT (activation).

**Solution:** Treat first transaction (ACT or RENO) as activation. Flag with `missing_act_record = true`.

**Location:** `sql/build_subscription_view.sql:69-95`

**DO NOT:**
- ❌ Skip subscriptions without ACT
- ❌ Assume activation always exists

**DO:**
- ✅ Use COALESCE to handle missing ACT
- ✅ Set flag for tracking
- ✅ Use first_renewal_date as fallback

---

### Edge Case 2: CPC Upgrades

**Problem:** Subscriptions can change services (CPC codes) mid-lifecycle.

**Solution:** Track all CPCs chronologically in `cpc_list`. Detect upgrades via `trans_type_id = 1` in ACT table.

**Location:** `sql/build_subscription_view.sql:50-108`

**DO NOT:**
- ❌ Overwrite CPC on upgrade
- ❌ Ignore previous services
- ❌ Assume one CPC per subscription

**DO:**
- ✅ Maintain ordered list: `cpc_list`
- ✅ Track: `first_cpc`, `current_cpc`, `upgraded_to_cpc`
- ✅ Set `has_upgraded = true` flag

---

### Edge Case 3: Subscription Status Hierarchy

**Problem:** A subscription can have both deactivation and cancellation.

**Solution:** Follow strict precedence: DCT > CNR > ACTIVE

```sql
subscription_status = CASE
    WHEN deactivation_date IS NOT NULL THEN 'DEACTIVATED'
    WHEN cancellation_date IS NOT NULL THEN 'CANCELLED'
    ELSE 'ACTIVE'
END
```

**DO NOT:**
- ❌ Use OR logic for status
- ❌ Change precedence order

**DO:**
- ✅ Deactivation overrides cancellation
- ✅ Check DCT first, then CNR
- ✅ Default to ACTIVE if neither

---

### Pitfall 1: RFND Partitioning

**Issue:** Refund dates can be NULL, causing Hive partitioning to fail.

**Solution:** Use `__HIVE_DEFAULT_PARTITION__` for NULL year_month values.

```
Parquet_Data/transactions/rfnd/year_month=__HIVE_DEFAULT_PARTITION__/*.parquet
```

**DO NOT:**
- ❌ Skip NULL refund dates
- ❌ Use "null" or "unknown" as partition value

**DO:**
- ✅ Use exact string: `__HIVE_DEFAULT_PARTITION__`
- ✅ Handle in DuckDB queries with NULL check

---

### Pitfall 2: Launchd Environment Differences

**Issue:** Scripts work manually but fail in launchd.

**Root Cause:** launchd runs with minimal environment (no PATH, HOME, etc.)

**Solution:**
1. Use absolute path to Python: `/opt/anaconda3/bin/python`
2. Use absolute paths in plist `ProgramArguments`
3. Set `WorkingDirectory` in plist

**DO NOT:**
- ❌ Assume PATH is set
- ❌ Use commands without full path
- ❌ Rely on shell aliases

**DO:**
- ✅ Test with: `launchctl start com.josemanco.<job>`
- ✅ Check logs: `Logs/<script>.log`
- ✅ Use absolute paths everywhere in plist

---

### Pitfall 3: Parquet Compression

**Issue:** Wrong compression reduces performance.

**Solution:** Always use SNAPPY (balance of speed and size).

```python
# CORRECT:
df.write_parquet(path, compression='snappy')

# INCORRECT (too slow):
df.write_parquet(path, compression='gzip')
```

**DO NOT:**
- ❌ Use GZIP (10x slower to decompress)
- ❌ Use UNCOMPRESSED (wastes disk space)

**DO:**
- ✅ Always use SNAPPY
- ✅ Specify explicitly in code

---

## ⚡ PERFORMANCE RULES

### DuckDB Query Optimization

**RULE:** Always filter on `year_month` partition column for time-based queries.

```sql
-- GOOD (uses partition pruning):
SELECT * FROM 'Parquet_Data/transactions/act/**/*.parquet'
WHERE year_month = '2025-01'

-- BAD (scans all partitions):
SELECT * FROM 'Parquet_Data/transactions/act/**/*.parquet'
WHERE trans_date >= '2025-01-01'
```

**DO NOT:**
- ❌ Filter on `trans_date` without `year_month`
- ❌ Skip partition filters

**DO:**
- ✅ Filter on `year_month` first
- ✅ Add `trans_date` filter as secondary
- ✅ Use partition pruning for large queries

---

### Polars vs Pandas

**RULE:** Prefer Polars for all new data processing code.

**Use Polars for:**
- CSV reading (10x faster than Pandas)
- Schema enforcement
- Large dataset operations
- Transformations

**Use Pandas for:**
- DuckDB result conversion (`.fetchdf()`)
- Legacy compatibility only

**DO NOT:**
- ❌ Use Pandas for CSV reading
- ❌ Use Pandas for transformations
- ❌ Mix Polars and Pandas unnecessarily

**DO:**
- ✅ Use Polars as primary DataFrame library
- ✅ Convert to Pandas only for DuckDB results
- ✅ Benchmark before changing

---

## 🔧 MODIFICATION CHECKLIST

### Before Modifying Any Script

- [ ] Read entire script to understand context
- [ ] Check what calls this script (grep for filename)
- [ ] Review error handling patterns
- [ ] Understand dependencies (input/output files)
- [ ] Check if changes affect downstream processes

### After Modifying Any Script

- [ ] Test manually with sample data
- [ ] Verify logs show expected output
- [ ] Check downstream dependencies still work
- [ ] Run validation scripts (`Scripts/others/check_*.py`)
- [ ] Test with launchctl if modifying orchestration
- [ ] Update README.md if behavior changes

### When Adding New Transaction Type

- [ ] Add to `file_types` dict in `03_process_daily.py`
- [ ] Add schema to `schemas` dict
- [ ] Add SQL case in `02_fetch_remote_nova_data.sh`
- [ ] Update `sql/build_subscription_view.sql`
- [ ] Update README transaction types section
- [ ] Test with real data end-to-end

---

## 📋 QUICK REFERENCE

### Critical Files & Line Numbers

| File | Purpose | Critical Sections |
|------|---------|-------------------|
| `sql/build_subscription_view.sql` | 241-line DuckDB aggregation | Lines 69-95 (missing ACT), Lines 50-108 (upgrades) |
| `Scripts/01_aggregate_user_base.py` | User base aggregation | Lines 27-53 (category mapping) |
| `Scripts/03_process_daily.py` | CSV→Parquet processor | Schema definitions, Hive partitioning logic |
| `Scripts/04_build_subscription_view.py` | Subscription aggregator | SQL template replacement |

### Remote Server Details

```
Host: 10.26.82.53
User: omadmin
Database: postgres
Table: telefonicaes_sub_mgr_fact
SSH Key: ~/.ssh/id_ed25519
Python: /opt/anaconda3/bin/python
Project: /Users/josemanco/CVAS/CVAS_BEYOND_DATA
```

#### Testing & Validation Scripts (Scripts/others/)
| Script | Purpose |
|--------|---------|
| `check_transactions_parquet_data.py` | Validates transaction Parquet integrity, schema, and partitioning |
| `check_aggregated_parquet_data.py` | Validates aggregated subscription Parquet data |
| `check_users.py` | Validates user data quality and queries by subscription_id/tmuserid/msisdn |
| `extract_music_subscriptions.py` | Extracts music-specific subscriptions for analysis |
| `calculate_lt_ltv.py` | Calculates lifetime and lifetime value metrics |
| `query_msisdn_from_tx.py` | **NEW:** Queries full subscription lifecycle by MSISDN (with MSISDN↔TMUSERID mapping) |
| `query_tmuserid_from_tx.py` | **NEW:** Queries full subscription lifecycle by TMUSERID (with TMUSERID↔MSISDN mapping) |

### Testing Commands

```bash
# Test individual scripts
bash 1.GET_NBS_BASE.sh
bash 2.FETCH_DAILY_DATA.sh 2025-01-15
bash 3.PROCESS_DAILY_AND_BUILD_VIEW.sh 2025-01-15

# Test launchd execution
launchctl start com.josemanco.nbs_base
tail -f Logs/1.GET_NBS_BASE.log

# Validate data
python Scripts/others/check_transactions_parquet_data.py
python Scripts/others/check_aggregated_parquet_data.py

# Query transaction data by MSISDN or TMUSERID
python Scripts/others/query_msisdn_from_tx.py 34686516147
python Scripts/others/query_tmuserid_from_tx.py 12345678

# Check Parquet schema
python3 -c "import pyarrow.parquet as pq; print(pq.read_schema('Parquet_Data/aggregated/subscriptions.parquet'))"
```

### Query Scripts

**`Scripts/others/query_msisdn_from_tx.py`**
- Queries transaction data by MSISDN (automatically adds '34' country code if missing)
- Shows MSISDN → TMUSERID(s) mapping
- Displays full subscription lifecycle grouped by `subscription_id`:
  - ACT, RENO, DCT, CNR, RFND transactions (sorted by trans_date, trans_type_id)
  - Summary: counts per transaction type, total revenue, total refunded
- Separately shows PPD (Pay Per Download) one-time purchases
- Usage: `python Scripts/others/query_msisdn_from_tx.py <msisdn>`

**`Scripts/others/query_tmuserid_from_tx.py`**
- Queries transaction data by TMUSERID
- Shows TMUSERID → MSISDN(s) mapping
- Displays full subscription lifecycle grouped by `subscription_id`:
  - ACT, RENO, DCT, CNR, RFND transactions (sorted by trans_date, trans_type_id)
  - Summary: counts per transaction type, total revenue, total refunded
- Separately shows PPD (Pay Per Download) one-time purchases
- Usage: `python Scripts/others/query_tmuserid_from_tx.py <tmuserid>`

**Query Logic:**
1. Step 1: Find all `subscription_id`s associated with the identifier (from ACT, RENO, DCT)
2. Step 2: Retrieve all transactions (ACT, RENO, DCT, CNR, RFND) for those subscription_ids
3. Step 3: Query PPD transactions directly by the original identifier
4. Note: CNR and RFND don't have `trans_type_id` in source schema; assigned 99 and 100 for sorting

---

## 📊 PROJECT VALIDATION SUMMARY (2025-01-27)

### ✅ Validated Components

#### 1. Pipeline Scripts (Core)
| Script | Status | Validation |
|--------|--------|------------|
| `1.GET_NBS_BASE.sh` | ✅ PASS | Log rotation ✓, Absolute Python path ✓, Error handling ✓, Cross-platform date ✓ |
| `2.FETCH_DAILY_DATA.sh` | ✅ PASS | Log rotation ✓, Sequential execution ✓, Cross-platform date ✓ |
| `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` | ✅ PASS | Log rotation ✓, Absolute Python path ✓, Error handling ✓, Cross-platform date ✓ |
| `Scripts/01_aggregate_user_base.py` | ✅ PASS | Category mapping ✓, Service exclusions ✓, No PII logging ✓ |
| `Scripts/02_fetch_remote_nova_data.sh` | ✅ PASS | Cross-platform date ✓, SSH connection ✓ |
| `Scripts/03_process_daily.py` | ✅ PASS | All 6 transaction types ✓, Hive partitioning ✓, Schema enforcement ✓ |
| `Scripts/04_build_subscription_view.py` | ✅ PASS | DuckDB aggregation ✓, SQL template ✓ |

#### 2. SQL Queries
| File | Status | Validation |
|------|--------|------------|
| `sql/build_subscription_view.sql` | ✅ PASS | 241 lines ✓, All 6 types ✓, Hive partitioning ✓, Edge cases handled ✓ |

#### 3. Utility Scripts
| Script | Status | Validation |
|--------|--------|------------|
| `Scripts/utils/log_rotation.sh` | ✅ PASS | 15-day retention ✓, Cross-platform date ✓ |

#### 4. Query & Validation Scripts (Scripts/others/)
| Script | Status | Validation |
|--------|--------|------------|
| `check_transactions_parquet_data.py` | ✅ PASS | Hive partitioning ✓, Schema validation ✓ |
| `check_aggregated_parquet_data.py` | ✅ PASS | Aggregation validation ✓ |
| `check_users.py` | ✅ PASS | PII display allowed (manual query) ✓ |
| `extract_music_subscriptions.py` | ✅ PASS | PII display allowed (manual query) ✓ |
| `calculate_lt_ltv.py` | ✅ PASS | Metrics calculation ✓ |
| `query_msisdn_from_tx.py` | ✅ PASS | MSISDN↔TMUSERID mapping ✓, Hive partitioning ✓, Country code handling ✓ |
| `query_tmuserid_from_tx.py` | ✅ PASS | TMUSERID↔MSISDN mapping ✓, Hive partitioning ✓ |

#### 5. Configuration Files
| File | Status | Validation |
|------|--------|------------|
| `requirements.txt` | ✅ PASS | Pinned versions ✓ (polars==1.34.0, pyarrow==19.0.0, duckdb==1.2.1, pandas==2.2.3) |
| `.gitignore` | ✅ PASS | Data directories excluded ✓, Logs excluded ✓ |

### 🔍 Key Findings

#### Transaction Type Coverage
- ✅ All 6 transaction types (ACT, RENO, DCT, CNR, RFND, PPD) consistently referenced across:
  - `Scripts/03_process_daily.py` (schema definitions)
  - `Scripts/00_convert_historical.py` (historical conversion)
  - `sql/build_subscription_view.sql` (DuckDB aggregation)
  - Query scripts (lifecycle tracking)

#### Hive Partitioning
- ✅ Implemented in all transaction Parquet writes
- ✅ Used in all DuckDB read operations (`hive_partitioning=true`)
- ✅ Partition format: `year_month=YYYY-MM`
- ✅ Validated in check scripts

#### Python Path Usage
- ✅ All shell scripts use absolute path: `/opt/anaconda3/bin/python`
- ✅ Python scripts use standard shebang: `#!/usr/bin/env python3`
- ✅ Launchd-compatible

#### Cross-Platform Date Handling
- ✅ All shell scripts support both macOS (`date -v`) and Linux (`date -d`)
- ✅ Consistent pattern across all orchestration scripts

#### Log Rotation
- ✅ All 3 orchestration scripts call `rotate_log()` at start
- ✅ 15-day retention enforced
- ✅ Cross-platform compatible

#### Error Handling
- ✅ All orchestration scripts exit with non-zero code on failure
- ✅ Timestamped error logging
- ✅ Validation checks before proceeding

#### PII Protection
- ✅ Pipeline scripts do NOT log PII
- ✅ Query/debugging scripts display PII only when explicitly requested (allowed exception)
- ✅ Clear distinction between automated pipeline and manual debugging tools

#### Schema Enforcement
- ✅ Strict schemas defined in Polars for all 6 transaction types
- ✅ Consistent column names and types
- ✅ Schema validation in check scripts

### 📝 Recent Changes (2025-01-27)

1. **New Query Scripts:**
   - Added `Scripts/others/query_msisdn_from_tx.py` - Query subscription lifecycle by MSISDN
   - Added `Scripts/others/query_tmuserid_from_tx.py` - Query subscription lifecycle by TMUSERID
   - Both scripts show identifier mapping (MSISDN↔TMUSERID)
   - Display full subscription lifecycle grouped by `subscription_id`
   - Separate PPD (Pay Per Download) transactions
   - Automatic country code handling for MSISDN (adds '34' if missing)

2. **Documentation Updates:**
   - Updated `.abacus/rules.md` with query scripts documentation
   - Clarified PII protection exceptions for manual query/debugging scripts
   - Added comprehensive validation summary

### 🎯 Compliance Status

| Rule Category | Status | Notes |
|---------------|--------|-------|
| Sequential Pipeline | ✅ COMPLIANT | 1→2→3 order enforced |
| Six Transaction Types | ✅ COMPLIANT | All 6 types consistently processed |
| Directory Structure | ✅ COMPLIANT | Immutable structure maintained |
| Schema Enforcement | ✅ COMPLIANT | Strict schemas in all processors |
| Hive Partitioning | ✅ COMPLIANT | All transaction Parquet files partitioned |
| Absolute Python Path | ✅ COMPLIANT | All shell scripts use `/opt/anaconda3/bin/python` |
| Path Management | ✅ COMPLIANT | Relative paths from project root |
| Cross-Platform Date | ✅ COMPLIANT | macOS and Linux support |
| Log Rotation | ✅ COMPLIANT | 15-day retention, all scripts |
| Error Handling | ✅ COMPLIANT | Non-zero exit codes, timestamped logs |
| SQL Query Management | ✅ COMPLIANT | Complex SQL in `sql/` directory |
| Python Dependencies | ✅ COMPLIANT | Exact versions pinned |
| Category Mapping | ✅ COMPLIANT | Business logic preserved |
| NBS_BASE Immutability | ✅ COMPLIANT | Historical files untouched |
| PII Protection | ✅ COMPLIANT | No PII in pipeline logs, allowed in manual query scripts |
| Git Ignore | ✅ COMPLIANT | Data directories excluded |

---

## 🎯 TL;DR - MOST IMPORTANT RULES

1. **Sequential Execution:** Never break 1→2→3 script order
2. **Six Transaction Types:** Always process all 6 (ACT, RENO, DCT, CNR, RFND, PPD)
3. **Strict Schemas:** Schema changes break everything. Enforce in Polars.
4. **Hive Partitioning:** Required for DuckDB performance. Never remove.
5. **Absolute Python Path:** Use `/opt/anaconda3/bin/python` in shell scripts
6. **No PII in Pipeline Logs:** Never log `tmuserid` or `msisdn` in automated processes (allowed in manual query scripts)
7. **15-Day Log Retention:** Always call `rotate_log()` at start
8. **Git Ignore Data:** Never commit `Daily_Data/`, `Parquet_Data/`, `Logs/`
9. **Edge Cases:** Handle missing ACT records and CPC upgrades
10. **Cross-Platform:** Support both macOS and Linux date commands

---

**Last Updated:** 2025-01-27
**For General Documentation:** See `README.md`
