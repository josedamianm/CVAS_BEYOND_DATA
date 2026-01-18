# CVAS Beyond Data - Project Context & Session Memory

> **🤖 AI ASSISTANT:** You are reading file 2 of 3. After this file, you MUST also read **`RULES.md`** for architecture constraints. If you haven't read `README.md` yet, read it first.

> **Purpose:** This file maintains project state, recent changes, and critical context to help AI assistants quickly understand the project when starting new sessions.

**Last Updated:** 2025-01-28

---

## 📊 Project Overview

**CVAS Beyond Data** is a data pipeline for processing telecommunications subscription transactions. It aggregates daily transaction data (activations, renewals, deactivations, cancellations, refunds, prepaid) and builds analytical counters by service and CPC.

### Key Components
- **Data Sources:** Remote Nova server (daily CSVs) + Historical Atlas CSVs
- **Processing:** Python (Polars) + DuckDB for aggregation
- **Storage:** Parquet files with Hive partitioning
- **Output:** Service-level and CPC-level transaction counters

---

## 🔧 Recent Fixes & Changes (Jan 2026)

### 1. ✅ Refund Count Logic Fixed (Jan 18, 2026)
**Issue:** `rfnd_count` was counting rows instead of summing the `rfnd_cnt` column, causing undercounting.

**Root Cause:** 
- `rfnd_atlas.csv` contains aggregated refund data where one row can represent multiple refunds
- The `rfnd_cnt` column stores the actual number of refunds per row
- Previous logic: `len(rfnd_df)` → counted rows
- Correct logic: `rfnd_df['rfnd_cnt'].sum()` → sums actual refund counts

**Files Modified:**
- `Scripts/05_build_counters.py:compute_daily_cpc_counts()` - Changed aggregation logic
- `Scripts/utils/counter_utils.py:load_transactions_for_date()` - Ensured `rfnd_cnt` column is loaded

**Validation:**
- Beauty & Health Dec 2025: **7,001 refunds** (was 737) ✅
- Amount: **€15,033.93** (was €8,959.41) ✅

### 2. ✅ Parquet Data Regeneration (Jan 18, 2026)
**Issue:** `Parquet_Data/transactions/rfnd` was outdated and missing 436 rows for Dec 2025.

**Root Cause:**
- Historical conversion script `Scripts/00_convert_historical.py` hadn't been run after recent `rfnd_atlas.csv` updates
- Parquet files were stale compared to source CSVs

**Solution:**
- Re-ran `Scripts/00_convert_historical.py` to regenerate all parquet files from `rfnd_atlas.csv`
- Verified completeness: 1,173 rows, 7,001 refunds, €15,033.93 for Beauty & Health Dec 2025 ✅

**Files Involved:**
- `Scripts/00_convert_historical.py` - Converts historical CSVs to partitioned Parquet
- Source: `/Users/josemanco/Dropbox/BEYOND_DATA_OLD_backup/Historical_Data/rfnd_atlas.csv`
- Output: `Parquet_Data/transactions/rfnd/year_month=*/`

### 3. ✅ Deactivation Count Logic (Previously Fixed)
**Issue:** `dct_count` was including upgrade deactivations, inflating the count.

**Solution:**
- Filter out `channel_dct == 'UPGRADE'` when counting deactivations
- Track upgrade deactivations separately in `upg_dct_count`

**Files Modified:**
- `Scripts/05_build_counters.py:compute_daily_cpc_counts()`

---

## 🗂️ Critical File Locations

### Data Sources
```
/Users/josemanco/Dropbox/BEYOND_DATA_OLD_backup/
├── Historical_Data/
│   ├── act_atlas.csv
│   ├── rfnd_atlas.csv      ← Primary refund source (historical)
│   ├── cnr_atlas.csv
│   ├── reno_atlas.csv
│   ├── dct_atlas.csv
│   └── ppd_atlas.csv
└── Daily_Data/
    ├── act_atlas_day.csv
    ├── rfnd_atlas_day.csv   ← Daily refund updates
    └── ... (other daily CSVs)
```

### Processing Scripts
```
CVAS_BEYOND_DATA/
├── Scripts/
│   ├── 00_convert_historical.py    ← Converts CSVs to Parquet (run when source CSVs update)
│   ├── 05_build_counters.py        ← Builds transaction counters (fixed rfnd_count logic)
│   └── utils/
│       └── counter_utils.py        ← Utility functions (fixed rfnd_cnt loading)
```

### Output Data
```
CVAS_BEYOND_DATA/
├── Parquet_Data/
│   └── transactions/
│       ├── rfnd/year_month=*/      ← Regenerated Jan 18, 2026
│       ├── act/year_month=*/
│       └── ... (other transaction types)
└── Counters/
    ├── Counters_Service.csv        ← Service-level counters (rebuilt Jan 18, 2026)
    └── Counters_CPC.csv            ← CPC-level counters
```

---

## 📋 Known Data Characteristics

### Refund Data (`rfnd_atlas.csv`)
- **Schema:** `sbnid`, `refnd_date`, `rfnd_cnt`, `rfnd_amount`, `tmuserid`, `cpc_id`
- **Key Column:** `rfnd_cnt` - Number of refunds (NOT always 1!)
- **Aggregation:** One row can represent multiple refunds
- **Deduplication Key:** `(sbnid, refnd_date)` - Unique per subscription per day
- **Date Range:** Historical data up to yesterday (updated daily)

### Deactivation Data (`dct_atlas.csv`)
- **Schema:** Includes `channel_dct` column
- **Upgrade Deactivations:** `channel_dct == 'UPGRADE'` - Should be excluded from `dct_count`
- **Regular Deactivations:** All other `channel_dct` values

### Service Categories
```
Beauty & Health → Beauty and Health (in MASTERCPC.csv)
Education & Images → Education
Free Time → Free Time
Games & Ugames → Games
KIDS → Kids
Light → Light
Music → Music
News & Sport → News, Sport
```

---

## 🔍 Data Validation Queries

### Check Refund Counts for Beauty & Health (Dec 2025)
```python
import polars as pl

# Load counters
counters = pl.read_csv('Counters/Counters_Service.csv')

# Filter Beauty & Health Dec 2025
beauty_health = counters.filter(
    (pl.col('tme_category').str.contains('(?i)beauty')) &
    (pl.col('date').str.starts_with('2025-12'))
)

# Aggregate
summary = beauty_health.select([
    pl.col('rfnd_count').sum(),  # Should be 7,001
    pl.col('rfnd_amount').sum()  # Should be €15,033.93
])
```

### Verify Parquet Data Completeness
```python
import polars as pl
from pathlib import Path

# Load parquet data
parquet_path = Path('Parquet_Data/transactions/rfnd')
parquet_files = sorted(parquet_path.glob('**/*.parquet'))
parquet_df = pl.concat([pl.read_parquet(f) for f in parquet_files])

# Load source CSV
rfnd_atlas = pl.read_csv('/Users/josemanco/Dropbox/BEYOND_DATA_OLD_backup/Historical_Data/rfnd_atlas.csv')

# Compare
print(f"Parquet rows: {len(parquet_df):,}")
print(f"CSV rows: {len(rfnd_atlas):,}")
print(f"Difference: {len(rfnd_atlas) - len(parquet_df):,}")
```

---

## 🚨 Common Issues & Solutions

### Issue: Counters don't match manual counts
**Symptoms:** `rfnd_count` or `rfnd_amount` differs from manual CSV aggregation

**Checklist:**
1. ✅ Is `rfnd_cnt` being summed (not row count)?
2. ✅ Are parquet files up to date with source CSVs?
3. ✅ Has `Scripts/00_convert_historical.py` been run recently?
4. ✅ Are deactivations excluding upgrades (`channel_dct != 'UPGRADE'`)?

**Solution:**
```bash
# Regenerate parquet files
cd /Users/josemanco/CVAS/CVAS_BEYOND_DATA
/opt/anaconda3/bin/python Scripts/00_convert_historical.py

# Rebuild counters
./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force
```

### Issue: Missing data in parquet files
**Symptoms:** Parquet row count < CSV row count

**Root Cause:** Historical conversion script not run after CSV updates

**Solution:** Re-run `Scripts/00_convert_historical.py`

---

## 📊 December 2025 Validation Results

### Beauty & Health Counters (Verified Jan 18, 2026)
| Metric | Value | Status |
|--------|-------|--------|
| Activations | 1,390 | ✅ |
| Renewals | 121,973 | ✅ |
| Deactivations | 1,815 | ✅ (excludes upgrades) |
| Refunds | **7,001** | ✅ (fixed from 737) |
| Refund Amount | **€15,033.93** | ✅ (fixed from €8,959.41) |
| Revenue | €218,243.27 | ✅ |

### All Categories (Dec 2025)
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

## 🔄 Workflow for Data Updates

### When Source CSVs Update
1. Run historical conversion: `python Scripts/00_convert_historical.py`
2. Rebuild counters: `./4.BUILD_TRANSACTION_COUNTERS.sh --backfill --force`
3. Validate key metrics (see validation queries above)

### Daily Pipeline Execution
```bash
# Automated via cron (8:05 AM - 9:30 AM)
./1.GET_NBS_BASE.sh          # 8:05 AM
./2.FETCH_DAILY_DATA.sh      # 8:25 AM
./3.PROCESS_DAILY_AND_BUILD_VIEW.sh  # 8:30 AM
./4.BUILD_TRANSACTION_COUNTERS.sh    # 9:30 AM (independent)
```

---

## 🎯 Quick Start for New AI Sessions

### Essential Context
1. **Project Type:** Telecom subscription data pipeline
2. **Tech Stack:** Python (Polars), DuckDB, Parquet, Shell scripts
3. **Key Fix:** Refund counts sum `rfnd_cnt` column (not row count)
4. **Data Location:** Historical CSVs in Dropbox, Parquet in `Parquet_Data/`
5. **Output:** `Counters/Counters_Service.csv` and `Counters/Counters_CPC.csv`

### First Steps
1. Read this file (`CONTEXT.md`)
2. Check `RULES.md` for architecture constraints
3. Review `README.md` for general documentation
4. Validate current state with queries in "Data Validation Queries" section

### Key Files to Know
- `Scripts/05_build_counters.py` - Counter building logic (recently fixed)
- `Scripts/utils/counter_utils.py` - Data loading utilities (recently fixed)
- `Scripts/00_convert_historical.py` - CSV to Parquet conversion
- `MASTERCPC.csv` - Service and CPC metadata

---

## 📝 Session Notes

> **🤖 AI ASSISTANT:** When user says `"End session"` or `"Close session"`, add a new session entry below using this format:
> ```markdown
> ### Session: [DATE] - [Brief Title]
> **Changes Made:**
> - [List each change/fix/addition]
>
> **Files Modified:**
> - [List files with brief description]
>
> **Validation:**
> - [Any tests run or validations performed]
> ```

### Session: Jan 28, 2025 - AI Documentation Instructions Enhancement
**Changes Made:**
- Rewrote AI assistant instructions in `README.md` to be more explicit and forceful
- Added `⛔ STOP - DO NOT PROCEED` command at the top
- Added numbered mandatory steps with table for reading all 3 documentation files
- Added checklist of required understanding before proceeding
- Added consequences warning for skipping documentation files
- Added SESSION MANAGEMENT COMMANDS section with start/end session workflows
- Added cross-reference indicators ("file 2 of 3", "file 3 of 3") to `CONTEXT.md` and `RULES.md`
- Added session notes template with AI instructions in `CONTEXT.md`

**Files Modified:**
- `README.md` - Complete rewrite of AI instructions header, added session management commands
- `CONTEXT.md` - Added file indicator, session notes template, session commands reference
- `RULES.md` - Added file indicator with cross-reference to other docs

**Validation:**
- Instructions tested and confirmed to be more explicit and actionable
- Cross-references between all 3 files ensure models follow complete flow

### Session: Jan 18, 2026 - Refund Count Fix
**Changes Made:**
- `rfnd_count` was undercounting by ~90% for Beauty & Health
- Parquet files were outdated (missing 436 rows for Dec 2025)
- Changed `rfnd_count` logic to sum `rfnd_cnt` column
- Regenerated all parquet files from source CSVs
- Rebuilt all counters with `--backfill --force`

**Files Modified:**
- `Scripts/05_build_counters.py`
- `Scripts/utils/counter_utils.py`

**Validation:**
- Beauty & Health Dec 2025: 7,001 refunds, €15,033.93 ✅
- All categories match manual counts ✅

---

## 🔗 Related Documentation

- **Architecture Rules:** `RULES.md`
- **General Documentation:** `README.md`
- **Git Repository:** (Add GitHub URL when available)

---

**For AI Assistants:** Always read this file first when starting a new session. It contains the most recent project state and critical fixes that may not be in the codebase comments.
