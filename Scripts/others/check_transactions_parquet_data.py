import polars as pl
import duckdb
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

def check_transactions_parquet_data():
    """
    Comprehensive validation and performance testing for transaction parquet data.

    Structure:
    1. Daily Data Completeness Check (act, reno, dct)
    2. Data Validation (row counts, date ranges, partitions, duplicates, schema)
    3. Query Performance Test
    """

    parquet_path = Path('/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Parquet_Data/transactions')
    file_types = ['act', 'reno', 'dct', 'cnr', 'rfnd', 'ppd']

    print("=" * 80)
    print("TRANSACTION DATA VALIDATION AND PERFORMANCE REPORT")
    print("=" * 80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # =========================================================================
    # 1. DAILY DATA COMPLETENESS CHECK
    # =========================================================================
    print("\n" + "=" * 80)
    print("1. DAILY DATA COMPLETENESS CHECK (act, reno, dct)")
    print("=" * 80)

    required_types = ["act", "reno", "dct"]
    yesterday = (datetime.now() - timedelta(days=1)).date()

    for trans_type in required_types:
        trans_path = parquet_path / trans_type
        print(f"\n{trans_type.upper()}:")
        print("-" * 40)

        if not trans_path.exists():
            print(f"❌ Directory not found: {trans_path}")
            continue

        try:
            # Read all parquet files under the transaction type directory
            # pyarrow will accept a directory path for partitioned parquet datasets
            table = pq.read_table(str(trans_path))
            df = table.to_pandas()

            if df.empty:
                print("❌ No records found in parquet files")
                continue

            if 'trans_date' not in df.columns:
                print("⚠️  'trans_date' column not found")
                continue

            # Normalize to date objects
            df['trans_date'] = pd.to_datetime(df['trans_date'], errors='coerce').dt.date
            df = df[df['trans_date'].notna()]

            if df.empty:
                print("❌ No valid trans_date values found after parsing")
                continue

            available_dates = set(df['trans_date'].unique())
            min_date = min(available_dates)
            max_date = max(available_dates)

            print(f"First available date: {min_date}")
            print(f"Last available date: {max_date}")
            print(f"Checking until: {yesterday}")
            print()

            # If the dataset starts after yesterday, report that explicitly
            if min_date > yesterday:
                print(f"⚠️  Earliest available date {min_date} is after the target date {yesterday}. Nothing to check.")
                continue

            # Build missing dates list from min_date to yesterday (inclusive)
            current_date = min_date
            missing_dates = []
            while current_date <= yesterday:
                if current_date not in available_dates:
                    missing_dates.append(current_date)
                current_date += timedelta(days=1)

            if missing_dates:
                print(f"❌ Found {len(missing_dates)} missing day(s):")
                # Show up to first 50 missing dates to avoid huge output
                for missing_date in missing_dates[:50]:
                    print(f"   - {missing_date}")
                if len(missing_dates) > 50:
                    print(f"   ... and {len(missing_dates) - 50} more")
            else:
                print(f"✓ All days from {min_date} to {yesterday} have data")

        except Exception as e:
            print(f"❌ Error processing data for {trans_type}: {str(e)}")

    # =========================================================================
    # 2. DATA VALIDATION
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("2. DATA VALIDATION")
    print("=" * 80)

    # 2.1 Data Summary
    print("\n2.1 DATA SUMMARY")
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

    # 2.2 Date Ranges
    print("\n\n2.2 DATE RANGES")
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

    # 2.3 Partition Structure
    print("\n\n2.3 PARTITION STRUCTURE")
    print("-" * 60)

    for file_type in file_types:
        path = parquet_path / file_type
        partitions = sorted([p.name for p in path.glob('year_month=*')])
        if partitions:
            print(f"{file_type.upper():6} : {len(partitions)} partitions")
            print(f"         {partitions[0]} to {partitions[-1]}")

    # 2.4 Duplicate Check
    print("\n\n2.4 DUPLICATE CHECK")
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

    # 2.5 Schema Validation
    print("\n\n2.5 SCHEMA VALIDATION")
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

    # =========================================================================
    # 3. QUERY PERFORMANCE TEST
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("3. QUERY PERFORMANCE TEST")
    print("=" * 80)

    con = duckdb.connect()

    queries = {
        "3.1 Count all ACT transactions": f"""
            SELECT COUNT(*) as total_acts
            FROM read_parquet('{parquet_path}/act/**/*.parquet', hive_partitioning=true)
        """,

        "3.2 Revenue by month (ACT)": f"""
            SELECT
                year_month,
                COUNT(*) as transactions,
                SUM(rev) as total_revenue
            FROM read_parquet('{parquet_path}/act/**/*.parquet', hive_partitioning=true)
            GROUP BY year_month
            ORDER BY year_month DESC
            LIMIT 5
        """,

        "3.3 Latest 10 renewals": f"""
            SELECT
                tmuserid,
                trans_date,
                rev,
                camp_name
            FROM read_parquet('{parquet_path}/reno/**/*.parquet', hive_partitioning=true)
            ORDER BY trans_date DESC
            LIMIT 10
        """,

        "3.4 Cancellations by month": f"""
            SELECT
                year_month,
                COUNT(*) as cancellations
            FROM read_parquet('{parquet_path}/cnr/**/*.parquet', hive_partitioning=true)
            GROUP BY year_month
            ORDER BY year_month DESC
            LIMIT 5
        """,

        "3.5 Total refunds": f"""
            SELECT
                COUNT(*) as refund_count,
                SUM(rfnd_amount) as total_refunded
            FROM read_parquet('{parquet_path}/rfnd/**/*.parquet', hive_partitioning=true)
        """
    }

    for query_name, query in queries.items():
        print(f"\n{query_name}")
        print("-" * 60)

        start = time.time()
        result = con.execute(query).fetchdf()
        elapsed = time.time() - start

        print(result.to_string(index=False))
        print(f"\n⏱️  Query time: {elapsed:.3f} seconds")

    con.close()

    # =========================================================================
    # COMPLETION
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("VALIDATION AND PERFORMANCE TEST COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    check_transactions_parquet_data()
