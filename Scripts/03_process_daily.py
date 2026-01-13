import polars as pl
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import sys

def process_daily_data(date_str: str):
    """
    Process daily CSV files and append to Parquet storage
    
    Args:
        date_str: Date in format 'YYYY-MM-DD' (e.g., '2025-11-10')
    """
    
    # Get project root ensuring it works regardless of CWD
    project_root = Path(__file__).resolve().parent.parent
    daily_path = project_root / 'Daily_Data'
    parquet_path = project_root / 'Parquet_Data' / 'transactions'
    
    # Convert date format for file matching
    file_date = date_str.replace('-', '')  # '2025-11-10' -> '20251110'
    year_month = date_str[:7]  # '2025-11-10' -> '2025-11'
    
    file_types = {
        'act': 'act_atlas',
        'reno': 'reno_atlas',
        'dct': 'dct_atlas',
        'cnr': 'cnr_atlas',
        'rfnd': 'rfnd_atlas',
        'ppd': 'ppd_atlas'
    }
    
    # Schema definitions (same as historical conversion)
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
    
    print("=" * 60)
    print(f"DAILY DATA PROCESSING: {date_str}")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    for file_key, file_pattern in file_types.items():
        print(f"\nProcessing: {file_key.upper()}")
        print("-" * 60)
        
        # Find daily file - try multiple naming patterns
        daily_files = list(daily_path.glob(f'{file_pattern}*day*.csv'))
        
        if not daily_files:
            # Try with date in filename
            daily_files = list(daily_path.glob(f'{file_pattern}*{file_date}*.csv'))
        
        if not daily_files:
            # Try just the pattern
            daily_files = list(daily_path.glob(f'{file_pattern}*.csv'))
            if len(daily_files) > 1:
                print(f"⚠️  Multiple files found, skipping {file_key}")
                continue
        
        if not daily_files:
            print(f"⚠️  No daily file found for {file_key}")
            continue
        
        daily_file = daily_files[0]
        print(f"  File: {daily_file.name}")
        
        try:
            # Read daily CSV
            print(f"  Reading CSV...", end=' ')
            df_daily = pl.read_csv(
                daily_file,
                schema=schemas[file_key],
                null_values=['', 'NULL', 'null'],
                ignore_errors=True
            )
            print(f"✓ {len(df_daily):,} rows")
            
            if len(df_daily) == 0:
                print(f"  ⚠️  Empty file, skipping")
                continue
            
            # Parse date columns
            date_cols = [col for col in df_daily.columns if 'date' in col.lower()]
            for date_col in date_cols:
                df_daily = df_daily.with_columns([
                    pl.col(date_col).str.strptime(
                        pl.Datetime,
                        format='%Y-%m-%d %H:%M:%S',
                        strict=False
                    ).alias(date_col)
                ])
            
            # Add partition column
            if 'trans_date' in df_daily.columns:
                df_daily = df_daily.with_columns([
                    pl.col('trans_date').dt.strftime('%Y-%m').alias('year_month')
                ])
            elif 'cancel_date' in df_daily.columns:
                df_daily = df_daily.with_columns([
                    pl.col('cancel_date').dt.strftime('%Y-%m').alias('year_month')
                ])
            elif 'refnd_date' in df_daily.columns:
                df_daily = df_daily.with_columns([
                    pl.col('refnd_date').dt.strftime('%Y-%m').alias('year_month')
                ])
            
            # Read existing Parquet data (INCLUDING partition columns)
            print(f"  Reading existing Parquet...", end=' ')
            existing_path = parquet_path / file_key
            
            if list(existing_path.rglob('*.parquet')):
                # Use hive_partitioning=True to include partition columns as data columns
                df_existing = pl.scan_parquet(
                    str(existing_path / '**/*.parquet'),
                    hive_partitioning=True
                ).collect()
                print(f"✓ {len(df_existing):,} rows")
                
                # Ensure column order matches
                common_cols = [col for col in df_daily.columns if col in df_existing.columns]
                df_existing = df_existing.select(df_daily.columns)
                
                # Combine
                print(f"  Combining data...", end=' ')
                df_combined = pl.concat([df_existing, df_daily])
                print(f"✓ {len(df_combined):,} rows")
            else:
                print(f"✓ No existing data")
                df_combined = df_daily
            
            # Deduplicate
            print(f"  Deduplicating...", end=' ')
            original_count = len(df_combined)
            
            if file_key in ['act', 'reno', 'dct', 'ppd']:
                unique_cols = ['subscription_id', 'trans_date', 'trans_type_id']
            elif file_key == 'cnr':
                unique_cols = ['sbn_id', 'cancel_date']
            elif file_key == 'rfnd':
                unique_cols = ['sbnid', 'refnd_date']
            
            df_combined = df_combined.unique(subset=unique_cols, keep='last')
            duplicates = original_count - len(df_combined)
            print(f"✓ Removed {duplicates:,} duplicates")
            
            # Delete old parquet files
            print(f"  Removing old Parquet files...", end=' ')
            for old_file in existing_path.rglob('*.parquet'):
                old_file.unlink()
            # Also remove partition directories if empty
            for partition_dir in existing_path.glob('year_month=*'):
                if partition_dir.is_dir() and not list(partition_dir.iterdir()):
                    partition_dir.rmdir()
            print(f"✓")
            
            # Write back to Parquet using PyArrow
            print(f"  Writing updated Parquet...", end=' ')
            arrow_table = df_combined.to_arrow()
            
            if 'year_month' in df_combined.columns:
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(existing_path),
                    partition_cols=['year_month'],
                    compression='snappy'
                )
            else:
                existing_path.mkdir(parents=True, exist_ok=True)
                output_file = existing_path / f'{file_key}.parquet'
                pq.write_table(arrow_table, str(output_file), compression='snappy')
            
            print(f"✓ Complete")
            
        except Exception as e:
            print(f"✗ ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "=" * 60)
    print("DAILY PROCESSING COMPLETE")
    print("=" * 60)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python 02_process_daily.py YYYY-MM-DD")
        print("Example: python 02_process_daily.py 2025-11-10")
        sys.exit(1)
    
    date_str = sys.argv[1]
    
    try:
        process_daily_data(date_str)
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
