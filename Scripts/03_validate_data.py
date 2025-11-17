import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime

def validate_parquet_data():
    """
    Validate the Parquet data structure and integrity
    """
    
    #parquet_path = Path('Parquet_Data/transactions')
    parquet_path = Path('/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Parquet_Data/transactions')

    
    print("=" * 60)
    print("DATA VALIDATION REPORT")
    print("=" * 60)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    file_types = ['act', 'reno', 'dct', 'cnr', 'rfnd', 'ppd']
    
    # 1. Row counts and file sizes
    print("\n1. DATA SUMMARY")
    print("-" * 60)
    total_rows = 0
    total_size_mb = 0
    
    for file_type in file_types:
        path = parquet_path / file_type
        if list(path.rglob('*.parquet')):
            df = pl.scan_parquet(str(path / '**/*.parquet'), hive_partitioning=True).collect()
            size = sum(f.stat().st_size for f in path.rglob('*.parquet')) / (1024 * 1024)
            
            total_rows += len(df)
            total_size_mb += size
            
            print(f"{file_type.upper():6} : {len(df):>12,} rows | {size:>8.2f} MB")
    
    print("-" * 60)
    print(f"{'TOTAL':6} : {total_rows:>12,} rows | {total_size_mb:>8.2f} MB")
    
    # 2. Date ranges
    print("\n\n2. DATE RANGES")
    print("-" * 60)
    
    for file_type in file_types:
        path = parquet_path / file_type
        if list(path.rglob('*.parquet')):
            df = pl.scan_parquet(str(path / '**/*.parquet'), hive_partitioning=True).collect()
            
            if 'trans_date' in df.columns:
                date_col = 'trans_date'
            elif 'cancel_date' in df.columns:
                date_col = 'cancel_date'
            elif 'refnd_date' in df.columns:
                date_col = 'refnd_date'
            else:
                continue
            
            min_date = df.select(pl.col(date_col).min()).item()
            max_date = df.select(pl.col(date_col).max()).item()
            
            print(f"{file_type.upper():6} : {min_date} to {max_date}")
    
    # 3. Partition structure
    print("\n\n3. PARTITION STRUCTURE")
    print("-" * 60)
    
    for file_type in file_types:
        path = parquet_path / file_type
        partitions = sorted([p.name for p in path.glob('year_month=*')])
        if partitions:
            print(f"{file_type.upper():6} : {len(partitions)} partitions")
            print(f"         {partitions[0]} to {partitions[-1]}")
    
    # 4. Check for duplicates
    print("\n\n4. DUPLICATE CHECK")
    print("-" * 60)
    
    duplicate_checks = {
        'act': ['subscription_id', 'trans_date', 'trans_type_id'],
        'reno': ['subscription_id', 'trans_date', 'trans_type_id'],
        'dct': ['subscription_id', 'trans_date', 'trans_type_id'],
        'cnr': ['sbn_id', 'cancel_date'],
        'rfnd': ['sbnid', 'refnd_date'],
        'ppd': ['subscription_id', 'trans_date', 'trans_type_id']
    }
    
    for file_type in file_types:
        path = parquet_path / file_type
        if list(path.rglob('*.parquet')):
            df = pl.scan_parquet(str(path / '**/*.parquet'), hive_partitioning=True).collect()
            
            unique_cols = duplicate_checks.get(file_type, [])
            if unique_cols and all(col in df.columns for col in unique_cols):
                original_count = len(df)
                unique_count = df.select(unique_cols).unique().height
                duplicates = original_count - unique_count
                
                if duplicates == 0:
                    print(f"{file_type.upper():6} : ✓ No duplicates")
                else:
                    print(f"{file_type.upper():6} : ⚠️  {duplicates:,} duplicates found!")
    
    # 5. Schema validation
    print("\n\n5. SCHEMA VALIDATION")
    print("-" * 60)
    
    expected_schemas = {
        'act': ['tmuserid', 'msisdn', 'cpc', 'trans_type_id', 'channel_id', 'channel_act', 
                'trans_date', 'act_date', 'reno_date', 'camp_name', 'tef_prov', 
                'campana_medium', 'campana_id', 'subscription_id', 'rev', 'year_month'],
        'reno': ['tmuserid', 'msisdn', 'cpc', 'trans_type_id', 'channel_id', 'channel_act',
                 'trans_date', 'act_date', 'reno_date', 'camp_name', 'tef_prov',
                 'campana_medium', 'campana_id', 'subscription_id', 'rev', 'year_month'],
        'dct': ['tmuserid', 'msisdn', 'cpc', 'trans_type_id', 'channel_dct',
                'trans_date', 'act_date', 'reno_date', 'camp_name', 'tef_prov',
                'campana_medium', 'campana_id', 'subscription_id', 'year_month'],
        'cnr': ['cancel_date', 'sbn_id', 'tmuserid', 'cpc', 'mode', 'year_month'],
        'rfnd': ['tmuserid', 'cpc', 'refnd_date', 'rfnd_amount', 'rfnd_cnt', 
                 'sbnid', 'instant_rfnd', 'year_month'],
        'ppd': ['tmuserid', 'msisdn', 'cpc', 'trans_type_id', 'channel_id',
                'trans_date', 'act_date', 'reno_date', 'camp_name', 'tef_prov',
                'campana_medium', 'campana_id', 'subscription_id', 'rev', 'year_month']
    }
    
    for file_type in file_types:
        path = parquet_path / file_type
        if list(path.rglob('*.parquet')):
            df = pl.scan_parquet(str(path / '**/*.parquet'), hive_partitioning=True).collect()
            
            expected = set(expected_schemas.get(file_type, []))
            actual = set(df.columns)
            
            missing = expected - actual
            extra = actual - expected
            
            if not missing and not extra:
                print(f"{file_type.upper():6} : ✓ Schema correct ({len(actual)} columns)")
            else:
                print(f"{file_type.upper():6} : ⚠️  Schema mismatch")
                if missing:
                    print(f"         Missing: {missing}")
                if extra:
                    print(f"         Extra: {extra}")
    
    print("\n" + "=" * 60)
    print("VALIDATION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    validate_parquet_data()
