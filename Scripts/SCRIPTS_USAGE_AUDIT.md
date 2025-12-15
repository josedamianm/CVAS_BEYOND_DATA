# Scripts Usage Audit

**Last Updated:** 2025-01-XX  
**Purpose:** Document which scripts are actively used in the CVAS pipeline vs. utility/testing scripts

---

## ✅ ACTIVE PIPELINE SCRIPTS

These scripts are **actively used** in the daily data pipeline orchestration:

### Core Pipeline Scripts

| Script | Used By | Purpose |
|--------|---------|---------|
| **01_aggregate_user_base.py** | `1.GET_NBS_BASE.sh` (line 113) | Aggregates user base data from multiple CSV files |
| **02_fetch_remote_nova_data.sh** | `2.FETCH_DAILY_DATA.sh` (line 57) | Fetches transaction data from remote PostgreSQL server |
| **03_process_daily.py** | `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` (line 83) | Converts daily CSV files to Parquet format |
| **04_build_subscription_view.py** | `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` (line 105) | Builds comprehensive subscription view in DuckDB |

### Utility Scripts

| Script | Used By | Purpose |
|--------|---------|---------|
| **utils/log_rotation.sh** | All 3 orchestration scripts | Log rotation utility (15-day retention) |

---

## ❓ UNUSED/STANDALONE SCRIPTS (Moved to Scripts/others/)

These scripts are **NOT referenced** in the main orchestration pipeline and have been organized into the `Scripts/others/` subfolder:

### Testing/Validation Scripts (in Scripts/others/)

| Script | Type | Likely Purpose |
|--------|------|----------------|
| **check_subscriptions_parquet_data.py** | Validation | Manual testing tool to verify subscription Parquet data integrity |
| **check_transactions_parquet_data.py** | Validation | Manual testing tool to verify transaction Parquet data integrity |
| **check_users.py** | Validation | Manual testing tool to verify user data |

### Analysis/Extract Scripts (in Scripts/others/)

| Script | Type | Likely Purpose |
|--------|------|----------------|
| **extract_music_subscriptions.py** | Extract/Analysis | Specialized script to extract music-specific subscription data |

### Historical/One-Time Scripts

| Script | Type | Likely Purpose |
|--------|------|----------------|
| **00_convert_historical.py** | Historical | One-time conversion script for historical data migration (kept in Scripts/ root) |

---

## 📊 USAGE SUMMARY

| Category | Count | Scripts |
|----------|-------|---------|
| **Active Pipeline** | 4 | 01_aggregate_user_base.py, 02_fetch_remote_nova_data.sh, 03_process_daily.py, 04_build_subscription_view.py |
| **Active Utils** | 1 | utils/log_rotation.sh |
| **Unused (in others/)** | 4 | check_subscriptions_parquet_data.py, check_transactions_parquet_data.py, check_users.py, extract_music_subscriptions.py |
| **Historical (kept)** | 1 | 00_convert_historical.py |
| **TOTAL** | 10 | - |

---

## 🔍 PIPELINE FLOW DIAGRAM

```
┌─────────────────────────────────────────────────────────────────────┐
│ SCRIPT 1: 1.GET_NBS_BASE.sh                                          │
├─────────────────────────────────────────────────────────────────────┤
│ 1. Download NBS_Base.csv from remote server (SCP)                   │
│ 2. Validate file                                                     │
│ 3. Run: 01_aggregate_user_base.py  ← ACTIVE SCRIPT                     │
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

---

## 🎯 STATUS & ORGANIZATION

### ✅ Reorganization Complete!

The Scripts folder has been reorganized for better maintainability:

```
Scripts/
├── 00_convert_historical.py       [Historical - kept in root]
├── 01_aggregate_user_base.py      [Active - Pipeline]
├── 02_fetch_remote_nova_data.sh   [Active - Pipeline]
├── 03_process_daily.py            [Active - Pipeline]
├── 04_build_subscription_view.py  [Active - Pipeline]
├── SCRIPTS_USAGE_AUDIT.md         [Documentation]
├── others/                        [Unused/standalone scripts]
│   ├── check_subscriptions_parquet_data.py
│   ├── check_transactions_parquet_data.py
│   ├── check_users.py
│   └── extract_music_subscriptions.py
└── utils/
    └── log_rotation.sh            [Active - Utility]
```

### Key Changes
- ✅ **Active pipeline scripts** remain in `Scripts/` root for easy access
- ✅ **Unused validation/analysis scripts** moved to `Scripts/others/`
- ✅ **Historical script** (`00_convert_historical.py`) kept in root per requirement
- ✅ **Utils folder** contains shared utilities
- ✅ **No impact on pipeline** - All orchestration scripts continue to work

### Notes
1. **Scripts in `others/` folder:**
   - Available when needed for debugging/analysis
   - Not called by any orchestration scripts
   - Can be used manually as needed

2. **Historical script:**
   - `00_convert_historical.py` kept in Scripts/ root
   - Available for future historical data conversions if needed

3. **Pipeline remains unchanged:**
   - All 3 orchestration scripts work without modification
   - No changes to script locations or paths

---

## 📝 VERIFICATION COMMANDS

To verify this audit, run:

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

---

## 🔄 MAINTENANCE

**Last Updated:** December 15, 2025
**Status:** Reorganization and renaming complete ✅

This audit should be updated when:
- New scripts are added to the pipeline
- Scripts are removed or deprecated
- Orchestration flow changes
- New utilities are created
- Scripts are moved between folders

**Change Log:**
- 2025-12-15: Renamed scripts for better pipeline sequence clarity
  - `aggregate_user_base.py` → `01_aggregate_user_base.py` (now runs before 02_fetch_remote_nova_data.sh)
  - `01_convert_historical.py` → `00_convert_historical.py` (not part of regular pipeline)
- 2025-12-15: Moved 4 unused scripts to `Scripts/others/` folder
  - check_subscriptions_parquet_data.py
  - check_transactions_parquet_data.py
  - check_users.py
  - extract_music_subscriptions.py
