# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a data repository containing daily NBS (Non-Billable Services) base data files spanning from November 2022 to present. The repository contains over 1,000 daily CSV snapshots tracking content provider services and billing information.

## Data Architecture

### Directory Structure
- `NBS_BASE/` - Contains all daily CSV files with standardized naming convention

### File Naming Convention
Files follow the pattern: `YYYYMMDD_NBS_Base.csv`
- Example: `20221114_NBS_Base.csv` represents data from November 14, 2022
- Files are consistently named and can be sorted chronologically

### CSV Schema
Each CSV file contains the following columns:
- `cpc` - Content Provider Code (numeric identifier)
- `content_provider` - Provider name (e.g., "2dayuk")
- `service_name` - Name of the service being tracked
- `last_billed_amount` - Most recent billing amount (decimal)
- `tme_category` - Category classification (e.g., "Light")
- `channel_desc` - Channel description (e.g., "ACTIVE", "CCC-CR")
- `count` - Count/quantity metric

## Data Analysis Commands

### Examining Data Structure
```bash
# View first few rows of a specific date
head -10 NBS_BASE/20221114_NBS_Base.csv

# Count total records in a file
wc -l NBS_BASE/20221114_NBS_Base.csv

# Check unique content providers
cut -d',' -f2 NBS_BASE/20221114_NBS_Base.csv | sort -u

# Check date range of available data
ls -1 NBS_BASE/ | head -1  # earliest date
ls -1 NBS_BASE/ | tail -1  # latest date
```

### File Operations
```bash
# Count total files
ls -1 NBS_BASE/ | wc -l

# Find files in a date range
ls NBS_BASE/ | grep "202211[12]"  # November 2022

# Compare two dates (requires diff)
diff NBS_BASE/20221114_NBS_Base.csv NBS_BASE/20221115_NBS_Base.csv
```

## Working with This Data

### Time Series Analysis
Data is organized chronologically, making it suitable for:
- Daily trend analysis
- Provider activity tracking over time
- Billing amount changes
- Service availability monitoring

### Data Continuity
- Daily files provide consistent snapshots
- Files span approximately 3 years (Nov 2022 - Nov 2025)
- File naming allows for programmatic date parsing and filtering

### Common Analysis Patterns
When analyzing this data:
1. Files can be processed sequentially by date
2. CSV format allows for easy parsing with standard tools (Python pandas, R, Excel)
3. Each file represents a complete daily snapshot
4. The `cpc` field serves as a consistent identifier for providers
