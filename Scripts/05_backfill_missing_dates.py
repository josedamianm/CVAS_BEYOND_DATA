import polars as pl
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
import sys

def get_date_range_from_parquet(parquet_path: Path, file_key: str):
    existing_path = parquet_path / file_key
    
    if not list(existing_path.rglob('*.parquet')):
        return None, None
    
    df = pl.scan_parquet(
        str(existing_path / '**/*.parquet'),
        hive_partitioning=True
    ).collect()
    
    if len(df) == 0:
        return None, None
    
    if 'trans_date' in df.columns:
        date_col = 'trans_date'
    elif 'cancel_date' in df.columns:
        date_col = 'cancel_date'
    elif 'refnd_date' in df.columns:
        date_col = 'refnd_date'
    else:
        return None, None
    
    min_date = df[date_col].dt.date().min()
    max_date = df[date_col].dt.date().max()

    return min_date, max_date

def get_all_dates_in_parquet(parquet_path: Path, file_key: str):
    existing_path = parquet_path / file_key
    
    if not list(existing_path.rglob('*.parquet')):
        return set()
    
    df = pl.scan_parquet(
        str(existing_path / '**/*.parquet'),
        hive_partitioning=True
    ).collect()
    
    if len(df) == 0:
        return set()
    
    if 'trans_date' in df.columns:
        date_col = 'trans_date'
    elif 'cancel_date' in df.columns:
        date_col = 'cancel_date'
    elif 'refnd_date' in df.columns:
        date_col = 'refnd_date'
    else:
        return set()
    
    dates = df.select(pl.col(date_col).dt.date()).unique()[date_col].to_list()
    return set(dates)

def get_date_range_from_csv(historical_path: Path, file_pattern: str, schema: dict):
    csv_files = list(historical_path.glob(f'{file_pattern}*.csv'))

    if not csv_files:
        return None, None

    all_dates = []

    for csv_file in csv_files:
        try:
            df = pl.read_csv(
                csv_file,
                schema=schema,
                null_values=['', 'NULL', 'null'],
                ignore_errors=True
            )

            date_cols = [col for col in df.columns if 'date' in col.lower()]
            if not date_cols:
                continue

            date_col = date_cols[0]

            df = df.with_columns([
                pl.col(date_col).str.strptime(
                    pl.Date,
                    format='%Y-%m-%d',
                    strict=False
                ).alias(date_col)
            ])

            dates = df.select(pl.col(date_col)).unique()[date_col].to_list()
            all_dates.extend(dates)
        except Exception as e:
            print(f"  ⚠️  Error reading {csv_file.name}: {e}")
            continue

    if not all_dates:
        return None, None

    return min(all_dates), max(all_dates)

def find_missing_dates(start_date, end_date, existing_dates):
    missing = []
    current = start_date
    
    while current <= end_date:
        if current not in existing_dates:
            missing.append(current)
        current += timedelta(days=1)
    
    return missing

def backfill_missing_dates(historical_path: str = None, dry_run: bool = False):
    project_root = Path(__file__).resolve().parent.parent
    
    if historical_path:
        historical_path = Path(historical_path)
    else:
        historical_path = project_root / 'Historical_Data'
    
    parquet_path = project_root / 'Parquet_Data' / 'transactions'
    
    file_types = {
        'act': 'act_atlas',
        'reno': 'reno_atlas',
        'dct': 'dct_atlas',
        'cnr': 'cnr_atlas',
        'rfnd': 'rfnd_atlas',
        'ppd': 'ppd_atlas'
    }
    
    schemas = {
        'act': {
            'tmuserid': pl.Utf8,
            'msisdn': pl.Utf8,
            'cpc': pl.Int64,
            'trans_type_id': pl.Int64,
            'channel_id': pl.Int64,
            'channel_act': pl.Utf8,
            'trans_date': pl.Utf8,
            'act_date': pl.Utf8,
            'reno_date': pl.Utf8,
            'camp_name': pl.Utf8,
            'tef_prov': pl.Int64,
            'campana_medium': pl.Utf8,
            'campana_id': pl.Utf8,
            'subscription_id': pl.Int64,
            'rev': pl.Float64
        },
        'reno': {
            'tmuserid': pl.Utf8,
            'msisdn': pl.Utf8,
            'cpc': pl.Int64,
            'trans_type_id': pl.Int64,
            'channel_id': pl.Int64,
            'channel_act': pl.Utf8,
            'trans_date': pl.Utf8,
            'act_date': pl.Utf8,
            'reno_date': pl.Utf8,
            'camp_name': pl.Utf8,
            'tef_prov': pl.Int64,
            'campana_medium': pl.Utf8,
            'campana_id': pl.Utf8,
            'subscription_id': pl.Int64,
            'rev': pl.Float64
        },
        'dct': {
            'tmuserid': pl.Utf8,
            'msisdn': pl.Utf8,
            'cpc': pl.Int64,
            'trans_type_id': pl.Int64,
            'channel_dct': pl.Utf8,
            'trans_date': pl.Utf8,
            'act_date': pl.Utf8,
            'reno_date': pl.Utf8,
            'camp_name': pl.Utf8,
            'tef_prov': pl.Int64,
            'campana_medium': pl.Utf8,
            'campana_id': pl.Utf8,
            'subscription_id': pl.Int64
        },
        'cnr': {
            'cancel_date': pl.Utf8,
            'sbn_id': pl.Int64,
            'tmuserid': pl.Utf8,
            'cpc': pl.Int64,
            'mode': pl.Utf8
        },
        'rfnd': {
            'tmuserid': pl.Utf8,
            'cpc': pl.Int64,
            'refnd_date': pl.Utf8,
            'rfnd_amount': pl.Float64,
            'rfnd_cnt': pl.Int64,
            'sbnid': pl.Int64,
            'instant_rfnd': pl.Utf8
        },
        'ppd': {
            'tmuserid': pl.Utf8,
            'msisdn': pl.Utf8,
            'cpc': pl.Int64,
            'trans_type_id': pl.Int64,
            'channel_id': pl.Int64,
            'trans_date': pl.Utf8,
            'act_date': pl.Utf8,
            'reno_date': pl.Utf8,
            'camp_name': pl.Utf8,
            'tef_prov': pl.Int64,
            'campana_medium': pl.Utf8,
            'campana_id': pl.Utf8,
            'subscription_id': pl.Int64,
            'rev': pl.Float64
        }
    }
    
    print("=" * 80)
    print("BACKFILL MISSING DATES - GAP DETECTION AND REPAIR")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Historical data path: {historical_path}")
    print(f"Parquet data path: {parquet_path}")
    if dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
    print()
    
    total_missing_dates = 0
    
    for file_key, file_pattern in file_types.items():
        print(f"\n{'=' * 80}")
        print(f"Analyzing: {file_key.upper()}")
        print('=' * 80)
        
        print(f"\n1. Checking Parquet data...")
        parquet_min, parquet_max = get_date_range_from_parquet(parquet_path, file_key)
        
        if parquet_min is None:
            print(f"  ⚠️  No existing Parquet data found")
            print(f"  💡 Run Scripts/00_convert_historical.py for initial data load")
            continue
        
        print(f"  ✓ Parquet date range: {parquet_min} to {parquet_max}")

        existing_dates = get_all_dates_in_parquet(parquet_path, file_key)
        print(f"  ✓ Found {len(existing_dates)} unique dates in Parquet")

        print(f"\n2. Checking CSV source data...")
        csv_min, csv_max = get_date_range_from_csv(historical_path, file_pattern, schemas[file_key])

        if csv_min is None:
            print(f"  ⚠️  No CSV files found matching pattern: {file_pattern}*.csv")
            continue

        print(f"  ✓ CSV date range: {csv_min} to {csv_max}")

        print(f"\n3. Detecting gaps...")
        missing_dates = find_missing_dates(parquet_min, parquet_max, existing_dates)

        if not missing_dates:
            print(f"  ✓ No gaps detected - all dates present")
            continue

        print(f"  ⚠️  Found {len(missing_dates)} missing dates:")

        missing_ranges = []
        range_start = missing_dates[0]
        range_end = missing_dates[0]

        for i in range(1, len(missing_dates)):
            if missing_dates[i] == range_end + timedelta(days=1):
                range_end = missing_dates[i]
            else:
                missing_ranges.append((range_start, range_end))
                range_start = missing_dates[i]
                range_end = missing_dates[i]
        missing_ranges.append((range_start, range_end))

        for start, end in missing_ranges:
            if start == end:
                print(f"      • {start}")
            else:
                print(f"      • {start} to {end} ({(end - start).days + 1} days)")

        total_missing_dates += len(missing_dates)

        if dry_run:
            print(f"\n  💡 Would backfill {len(missing_dates)} dates from CSV source")
            continue

        print(f"\n4. Backfilling missing dates...")
        
        print(f"\n4. Backfilling missing dates...")
        
        csv_files = list(historical_path.glob(f'{file_pattern}*.csv'))
        
        if not csv_files:
            print(f"  ✗ No CSV files found")
            continue
        
        all_dfs = []
        for csv_file in csv_files:
            try:
                df = pl.read_csv(
                    csv_file,
                    schema=schemas[file_key],
                    null_values=['', 'NULL', 'null'],
                    ignore_errors=True
                )
                all_dfs.append(df)
            except Exception as e:
                print(f"  ⚠️  Error reading {csv_file.name}: {e}")
                continue
        
        if not all_dfs:
            print(f"  ✗ Could not read any CSV files")
            continue
        
        df_csv = pl.concat(all_dfs)
        print(f"  ✓ Loaded {len(df_csv):,} rows from CSV")
        
        date_cols = [col for col in df_csv.columns if 'date' in col.lower()]
        for date_col in date_cols:
            df_csv = df_csv.with_columns([
                pl.col(date_col).str.strptime(
                    pl.Datetime,
                    format='%Y-%m-%d %H:%M:%S',
                    strict=False
                ).alias(date_col)
            ])
        
        if 'trans_date' in df_csv.columns:
            primary_date_col = 'trans_date'
        elif 'cancel_date' in df_csv.columns:
            primary_date_col = 'cancel_date'
        elif 'refnd_date' in df_csv.columns:
            primary_date_col = 'refnd_date'
        else:
            print(f"  ✗ Could not identify primary date column")
            continue
        
        df_missing = df_csv.filter(
            pl.col(primary_date_col).dt.date().is_in(missing_dates)
        )
        
        print(f"  ✓ Filtered to {len(df_missing):,} rows for missing dates")
        
        if len(df_missing) == 0:
            print(f"  ⚠️  No data found in CSV for missing dates")
            continue
        
        df_missing = df_missing.with_columns([
            pl.col(primary_date_col).dt.strftime('%Y-%m').alias('year_month')
        ])
        
        print(f"  Reading existing Parquet data...")
        existing_path = parquet_path / file_key
        df_existing = pl.scan_parquet(
            str(existing_path / '**/*.parquet'),
            hive_partitioning=True
        ).collect()
        
        print(f"  ✓ Loaded {len(df_existing):,} existing rows")
        
        df_existing = df_existing.select(df_missing.columns)
        
        print(f"  Combining data...")
        df_combined = pl.concat([df_existing, df_missing])
        print(f"  ✓ Combined to {len(df_combined):,} rows")
        
        print(f"  Deduplicating...")
        original_count = len(df_combined)
        
        if file_key in ['act', 'reno', 'dct', 'ppd']:
            unique_cols = ['subscription_id', 'trans_date', 'trans_type_id']
        elif file_key == 'cnr':
            unique_cols = ['sbn_id', 'cancel_date']
        elif file_key == 'rfnd':
            unique_cols = ['sbnid', 'refnd_date']
        
        df_combined = df_combined.unique(subset=unique_cols, keep='last')
        duplicates = original_count - len(df_combined)
        print(f"  ✓ Removed {duplicates:,} duplicates")
        
        print(f"  Removing old Parquet files...")
        for old_file in existing_path.rglob('*.parquet'):
            old_file.unlink()
        for partition_dir in existing_path.glob('year_month=*'):
            if partition_dir.is_dir() and not list(partition_dir.iterdir()):
                partition_dir.rmdir()
        print(f"  ✓ Cleaned up old files")
        
        print(f"  Writing updated Parquet...")
        arrow_table = df_combined.to_arrow()
        
        pq.write_to_dataset(
            arrow_table,
            root_path=str(existing_path),
            partition_cols=['year_month'],
            compression='snappy'
        )
        
        print(f"  ✓ Wrote {len(df_combined):,} rows to Parquet")
        print(f"\n  ✅ Successfully backfilled {len(missing_dates)} dates for {file_key}")
    
    print("\n" + "=" * 80)
    if dry_run:
        print(f"DRY RUN COMPLETE - Found {total_missing_dates} total missing dates")
        print("Run without --dry-run to perform backfill")
    else:
        print(f"BACKFILL COMPLETE - Processed {total_missing_dates} total missing dates")
    print("=" * 80)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Detect and backfill missing dates in Parquet data from CSV sources'
    )
    parser.add_argument(
        '--source-path',
        type=str,
        help='Path to historical CSV files (default: Historical_Data/)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Detect gaps without making changes'
    )
    
    args = parser.parse_args()
    
    try:
        backfill_missing_dates(
            historical_path=args.source_path,
            dry_run=args.dry_run
        )
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
