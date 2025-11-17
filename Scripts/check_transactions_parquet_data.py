import duckdb
from pathlib import Path
import time
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timedelta

def test_query_performance():
    """
    Test query performance on Parquet data using DuckDB
    Also performs a daily completeness validation for act, reno, and dct up to yesterday.
    """

    #parquet_path = Path('Parquet_Data/transactions')
    parquet_path = Path('/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Parquet_Data/transactions')

    print("=" * 60)
    print("QUERY PERFORMANCE TEST")
    print("=" * 60)

    # Connect to DuckDB
    con = duckdb.connect()

    # Test queries
    queries = {
        "1. Count all ACT transactions": f"""
            SELECT COUNT(*) as total_acts
            FROM read_parquet('{parquet_path}/act/**/*.parquet', hive_partitioning=true)
        """,

        "2. Revenue by month (ACT)": f"""
            SELECT
                year_month,
                COUNT(*) as transactions,
                SUM(rev) as total_revenue
            FROM read_parquet('{parquet_path}/act/**/*.parquet', hive_partitioning=true)
            GROUP BY year_month
            ORDER BY year_month DESC
            LIMIT 5
        """,

        "3. Latest 10 renewals": f"""
            SELECT
                tmuserid,
                trans_date,
                rev,
                camp_name
            FROM read_parquet('{parquet_path}/reno/**/*.parquet', hive_partitioning=true)
            ORDER BY trans_date DESC
            LIMIT 10
        """,

        "4. Cancellations by month": f"""
            SELECT
                year_month,
                COUNT(*) as cancellations
            FROM read_parquet('{parquet_path}/cnr/**/*.parquet', hive_partitioning=true)
            GROUP BY year_month
            ORDER BY year_month DESC
            LIMIT 5
        """,

        "5. Total refunds": f"""
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

    print("\n" + "=" * 60)
    print("PERFORMANCE TEST COMPLETE")
    print("=" * 60)


    # -------------------------
    # DAILY DATA COMPLETENESS CHECK
    # -------------------------
    print("\n" + "=" * 80)
    print("DAILY DATA COMPLETENESS CHECK (act, reno, dct)")
    print("=" * 80)
    print()

    base_path = parquet_path
    required_types = ["act", "reno", "dct"]
    yesterday = (datetime.now() - timedelta(days=1)).date()

    for trans_type in required_types:
        trans_path = base_path / trans_type
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

    print()
    print("=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_query_performance()
