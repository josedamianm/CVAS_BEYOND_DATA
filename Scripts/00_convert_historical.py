import polars as pl
from pathlib import Path
from datetime import datetime
import sys
import pyarrow.parquet as pq
import shutil

def convert_historical_csvs():
    """
    Convert all historical CSV files to partitioned Parquet format
    """
    
    project_root = Path(__file__).parent.parent
    
    # Get historical path from args or prompt
    if len(sys.argv) > 1:
        historical_path = Path(sys.argv[1])
    else:
        print(f"\nDefault Historical Data path: {project_root / 'Historical_Data'}")
        user_input = input("Enter path to Historical Data (press Enter for default): ").strip()
        if user_input:
            historical_path = Path(user_input)
        else:
            historical_path = project_root / 'Historical_Data'
    
    if not historical_path.exists():
        print(f"❌ Error: Path does not exist: {historical_path}")
        return

    parquet_path = project_root / 'Parquet_Data' / 'transactions'
    # The instruction provided a duplicate line for parquet_path, keeping the one that uses project_root.
    # parquet_path = Path('/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Parquet_Data/transactions')
    
    # Define file types and their patterns
    file_types = {
        'act': 'act_atlas',
        'reno': 'reno_atlas',
        'dct': 'dct_atlas',
        'cnr': 'cnr_atlas',
        'rfnd': 'rfnd_atlas',
        'ppd': 'ppd_atlas'
    }
    
    # Schema definitions for each file type
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
    print("HISTORICAL DATA CONVERSION TO PARQUET")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Process each file type
    for file_key, file_pattern in file_types.items():
        print(f"\n{'='*60}")
        print(f"Processing: {file_key.upper()} files")
        print(f"{'='*60}")
        
        # Find all CSV files matching pattern
        csv_files = sorted(historical_path.glob(f'{file_pattern}*.csv'))
        
        if not csv_files:
            print(f"⚠️  No files found for pattern: {file_pattern}*.csv")
            continue
        
        print(f"Found {len(csv_files)} file(s)")
        
        all_data = []
        
        for csv_file in csv_files:
            print(f"  Reading: {csv_file.name}...", end=' ')
            
            try:
                # Read CSV with schema
                df = pl.read_csv(
                    csv_file,
                    schema=schemas[file_key],
                    null_values=['', 'NULL', 'null'],
                    ignore_errors=False
                )
                
                # Parse date columns with flexible format handling
                date_cols = [col for col in df.columns if 'date' in col.lower()]
                for date_col in date_cols:
                    # Try parsing with datetime format first, then date-only format
                    df = df.with_columns([
                        pl.when(pl.col(date_col).str.contains(' '))
                        .then(
                            pl.col(date_col).str.strptime(
                                pl.Datetime,
                                format='%Y-%m-%d %H:%M:%S',
                                strict=False
                            )
                        )
                        .otherwise(
                            pl.col(date_col).str.strptime(
                                pl.Datetime,
                                format='%Y-%m-%d',
                                strict=False
                            )
                        ).alias(date_col)
                    ])
                
                # Add partition column (year-month from trans_date or first date column)
                if 'trans_date' in df.columns:
                    df = df.with_columns([
                        pl.col('trans_date').dt.strftime('%Y-%m').alias('year_month')
                    ])
                elif 'cancel_date' in df.columns:
                    df = df.with_columns([
                        pl.col('cancel_date').dt.strftime('%Y-%m').alias('year_month')
                    ])
                elif 'refnd_date' in df.columns:
                    df = df.with_columns([
                        pl.col('refnd_date').dt.strftime('%Y-%m').alias('year_month')
                    ])
                
                all_data.append(df)
                print(f"✓ ({len(df):,} rows)")
                
            except Exception as e:
                print(f"✗ ERROR: {str(e)}")
                continue
        
        if not all_data:
            print(f"⚠️  No data loaded for {file_key}")
            continue
        
        # Concatenate all dataframes
        print(f"\n  Concatenating {len(all_data)} file(s)...", end=' ')
        combined_df = pl.concat(all_data)
        print(f"✓ Total rows: {len(combined_df):,}")
        
        # Remove duplicates
        print(f"  Removing duplicates...", end=' ')
        original_count = len(combined_df)
        
        # Define unique keys for each file type
        if file_key in ['act', 'reno', 'dct', 'ppd']:
            unique_cols = ['subscription_id', 'trans_date', 'trans_type_id']
        elif file_key == 'cnr':
            unique_cols = ['sbn_id', 'cancel_date']
        elif file_key == 'rfnd':
            unique_cols = ['sbnid', 'refnd_date']
        
        combined_df = combined_df.unique(subset=unique_cols, keep='last')
        duplicates_removed = original_count - len(combined_df)
        print(f"✓ Removed {duplicates_removed:,} duplicates")

        # Write to partitioned Parquet using PyArrow
        output_path = parquet_path / file_key
        print(f"  Writing to Parquet: {output_path}...", end=' ')

        try:
            # Remove existing output directory to avoid duplicates
            if output_path.exists():
                shutil.rmtree(output_path)

            # Convert to Arrow table
            arrow_table = combined_df.to_arrow()
            
            if 'year_month' in combined_df.columns:
                # Write partitioned dataset
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(output_path),
                    partition_cols=['year_month'],
                    compression='snappy'
                )
            else:
                # Write single file
                output_path.mkdir(parents=True, exist_ok=True)
                output_file = output_path / f'{file_key}.parquet'
                pq.write_table(arrow_table, str(output_file), compression='snappy')
            
            # Get file size
            total_size = sum(f.stat().st_size for f in output_path.rglob('*.parquet'))
            size_mb = total_size / (1024 * 1024)
            
            print(f"✓ {size_mb:.2f} MB")
            
            # Verify data
            print(f"  Verifying...", end=' ')
            verify_df = pl.scan_parquet(str(output_path / '**/*.parquet')).collect()
            assert len(verify_df) == len(combined_df), "Row count mismatch!"
            print(f"✓ Verified {len(verify_df):,} rows")
            
        except Exception as e:
            print(f"✗ ERROR during write: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nNext step: Run 02_process_daily.py to add 2025-11-10 data")

if __name__ == "__main__":
    try:
        convert_historical_csvs()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
