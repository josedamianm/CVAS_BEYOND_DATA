import polars as pl
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

def check_subscriptions_parquet_data():
    """
    Comprehensive validation and performance testing for subscriptions parquet data.
    
    Structure:
    1. Daily Data Completeness Check (activation, renewal, deactivation dates)
    2. Monthly Summary (activations, renewals, deactivations, cancellations, refunds)
    3. Data Validation (row counts, date ranges, schema, data quality)
    4. Query Performance Test
    """
    
    parquet_file = Path('/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Parquet_Data/aggregated/subscriptions.parquet')
    
    print("=" * 80)
    print("SUBSCRIPTIONS DATA VALIDATION AND PERFORMANCE REPORT")
    print("=" * 80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    if not parquet_file.exists():
        print(f"❌ File not found: {parquet_file}")
        return
    
    # =========================================================================
    # 1. DAILY DATA COMPLETENESS CHECK
    # =========================================================================
    print("\n" + "=" * 80)
    print("1. DAILY DATA COMPLETENESS CHECK")
    print("=" * 80)
    
    yesterday = (datetime.now() - timedelta(days=1)).date()

    try:
        df = pl.read_parquet(str(parquet_file))

        # First, get the earliest renewal date to use as reference for activation validation
        renewal_df = df.filter(pl.col('last_renewal_date').is_not_null()).select(pl.col('last_renewal_date').cast(pl.Date).alias('date'))
        transactions_start_date = min(renewal_df['date'].to_list()) if renewal_df.height > 0 else None

        date_checks = {
            'ACTIVATION': 'activation_date',
            'LAST RENEWAL': 'last_renewal_date',
            'DEACTIVATION': 'deactivation_date',
            'CANCELLATION': 'cancellation_date'
        }

        for check_name, date_col in date_checks.items():
            print(f"\n{check_name}:")
            print("-" * 40)

            if date_col not in df.columns:
                print(f"⚠️  Column '{date_col}' not found")
                continue

            date_df = df.filter(pl.col(date_col).is_not_null()).select(pl.col(date_col).cast(pl.Date).alias('date'))

            if date_df.height == 0:
                print("❌ No records found")
                continue

            available_dates = set(date_df['date'].to_list())
            min_date = min(available_dates)
            max_date = max(available_dates)

            # For activation, use transactions_start_date as the validation start point
            validation_start_date = min_date
            if check_name == 'ACTIVATION' and transactions_start_date:
                validation_start_date = max(min_date, transactions_start_date)
                print(f"First activation date: {min_date}")
                print(f"Last activation date: {max_date}")
                print(f"Validating from: {validation_start_date} (transactions start date)")
            else:
                print(f"First date: {min_date}")
                print(f"Last date: {max_date}")

            print(f"Checking until: {yesterday}")
            print()

            if validation_start_date > yesterday:
                print(f"⚠️  Earliest date {validation_start_date} is after {yesterday}")
                continue

            current_date = validation_start_date
            missing_dates = []

            while current_date <= yesterday:
                if current_date not in available_dates:
                    missing_dates.append(current_date)
                current_date += timedelta(days=1)

            if missing_dates:
                print(f"❌ Found {len(missing_dates)} missing day(s):")
                for missing_date in missing_dates[:50]:
                    print(f"   - {missing_date}")
                if len(missing_dates) > 50:
                    print(f"   ... and {len(missing_dates) - 50} more")
            else:
                print(f"✓ All days from {validation_start_date} to {yesterday} have data")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    # =========================================================================
    # 2. MONTHLY SUMMARY
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("2. MONTHLY SUMMARY")
    print("=" * 80)
    
    con = duckdb.connect()
    
    print("\n2.1 ACTIVATIONS BY MONTH (Last 12 months)")
    print("-" * 60)
    
    query = f"""
    SELECT
        strftime(activation_date, '%Y-%m') as month,
        COUNT(*) as activations,
        SUM(activation_revenue) as activation_revenue,
        COUNT(DISTINCT tmuserid) as unique_users,
        COUNT(DISTINCT cpc_list[1]) as unique_cpcs
    FROM '{parquet_file}'
    WHERE activation_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY month
    ORDER BY month DESC
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n2.2 RENEWALS BY MONTH (Last 12 months)")
    print("-" * 60)
    
    query = f"""
    SELECT
        strftime(last_renewal_date, '%Y-%m') as month,
        COUNT(*) as subscriptions_with_renewals,
        SUM(renewal_count) as total_renewals,
        SUM(renewal_revenue) as renewal_revenue,
        AVG(renewal_count) as avg_renewals_per_subscription
    FROM '{parquet_file}'
    WHERE last_renewal_date >= CURRENT_DATE - INTERVAL '12 months'
        AND last_renewal_date IS NOT NULL
    GROUP BY month
    ORDER BY month DESC
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n2.3 DEACTIVATIONS BY MONTH (Last 12 months)")
    print("-" * 60)
    
    query = f"""
    SELECT
        strftime(deactivation_date, '%Y-%m') as month,
        COUNT(*) as deactivations,
        COUNT(DISTINCT deactivation_mode) as unique_modes,
        AVG(lifetime_days) as avg_lifetime_days
    FROM '{parquet_file}'
    WHERE deactivation_date >= CURRENT_DATE - INTERVAL '12 months'
        AND deactivation_date IS NOT NULL
    GROUP BY month
    ORDER BY month DESC
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n2.4 CANCELLATIONS BY MONTH (Last 12 months)")
    print("-" * 60)
    
    query = f"""
    SELECT
        strftime(cancellation_date, '%Y-%m') as month,
        COUNT(*) as cancellations,
        COUNT(DISTINCT cancellation_mode) as unique_modes,
        AVG(lifetime_days) as avg_lifetime_days
    FROM '{parquet_file}'
    WHERE cancellation_date >= CURRENT_DATE - INTERVAL '12 months'
        AND cancellation_date IS NOT NULL
    GROUP BY month
    ORDER BY month DESC
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n2.5 REFUNDS BY MONTH (Last 12 months)")
    print("-" * 60)
    
    query = f"""
    SELECT
        strftime(last_refund_date, '%Y-%m') as month,
        COUNT(*) as subscriptions_with_refunds,
        SUM(refund_count) as total_refunds,
        SUM(total_refunded) as total_refunded_amount,
        AVG(total_refunded) as avg_refund_per_subscription
    FROM '{parquet_file}'
    WHERE last_refund_date >= CURRENT_DATE - INTERVAL '12 months'
        AND last_refund_date IS NOT NULL
    GROUP BY month
    ORDER BY month DESC
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    # =========================================================================
    # 3. DATA VALIDATION
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("3. DATA VALIDATION")
    print("=" * 80)
    
    print("\n3.1 DATA SUMMARY")
    print("-" * 60)
    
    try:
        df = pl.read_parquet(str(parquet_file))
        file_size = parquet_file.stat().st_size / (1024 * 1024)
        
        print(f"Total subscriptions: {len(df):,}")
        print(f"File size: {file_size:.2f} MB")
        print(f"Columns: {len(df.columns)}")
        print(f"Memory usage: {df.estimated_size('mb'):.2f} MB")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n3.2 SUBSCRIPTION STATUS BREAKDOWN")
    print("-" * 60)
    
    query = f"""
    SELECT
        subscription_status,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM '{parquet_file}'
    GROUP BY subscription_status
    ORDER BY count DESC
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n3.3 DATE RANGES")
    print("-" * 60)
    
    query = f"""
    SELECT
        'Activation' as date_type,
        MIN(activation_date) as min_date,
        MAX(activation_date) as max_date,
        COUNT(*) as records
    FROM '{parquet_file}'
    WHERE activation_date IS NOT NULL
    UNION ALL
    SELECT
        'Last Renewal' as date_type,
        MIN(last_renewal_date) as min_date,
        MAX(last_renewal_date) as max_date,
        COUNT(*) as records
    FROM '{parquet_file}'
    WHERE last_renewal_date IS NOT NULL
    UNION ALL
    SELECT
        'Deactivation' as date_type,
        MIN(deactivation_date) as min_date,
        MAX(deactivation_date) as max_date,
        COUNT(*) as records
    FROM '{parquet_file}'
    WHERE deactivation_date IS NOT NULL
    UNION ALL
    SELECT
        'Cancellation' as date_type,
        MIN(cancellation_date) as min_date,
        MAX(cancellation_date) as max_date,
        COUNT(*) as records
    FROM '{parquet_file}'
    WHERE cancellation_date IS NOT NULL
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n3.4 DATA QUALITY CHECKS")
    print("-" * 60)
    
    query = f"""
    SELECT
        COUNT(*) as total_subscriptions,
        COUNT(DISTINCT subscription_id) as unique_subscription_ids,
        COUNT(*) - COUNT(DISTINCT subscription_id) as duplicate_subscription_ids,
        SUM(CASE WHEN missing_act_record THEN 1 ELSE 0 END) as missing_act_records,
        SUM(CASE WHEN activation_date IS NULL THEN 1 ELSE 0 END) as null_activation_dates,
        SUM(CASE WHEN has_upgraded THEN 1 ELSE 0 END) as upgraded_subscriptions,
        SUM(CASE WHEN renewal_count > 0 THEN 1 ELSE 0 END) as subscriptions_with_renewals,
        SUM(CASE WHEN refund_count > 0 THEN 1 ELSE 0 END) as subscriptions_with_refunds
    FROM '{parquet_file}'
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n3.5 REVENUE STATISTICS")
    print("-" * 60)
    
    query = f"""
    SELECT
        COUNT(*) as total_subscriptions,
        SUM(activation_revenue) as total_activation_revenue,
        SUM(renewal_revenue) as total_renewal_revenue,
        SUM(total_revenue) as total_revenue,
        SUM(total_refunded) as total_refunded,
        AVG(activation_revenue) as avg_activation_revenue,
        AVG(renewal_revenue) as avg_renewal_revenue,
        AVG(total_revenue) as avg_total_revenue
    FROM '{parquet_file}'
    """
    
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n\n3.6 SCHEMA VALIDATION")
    print("-" * 60)
    
    expected_columns = [
        'subscription_id', 'tmuserid', 'msisdn', 'cpc_list', 'cpc_count',
        'first_cpc', 'current_cpc', 'has_upgraded', 'upgrade_date', 'upgraded_to_cpc',
        'activation_date', 'activation_trans_date', 'missing_act_record',
        'activation_campaign', 'activation_channel', 'activation_revenue', 'activation_month',
        'renewal_count', 'renewal_revenue', 'last_renewal_date', 'first_renewal_date',
        'last_activity_date', 'deactivation_date', 'deactivation_mode',
        'cancellation_date', 'cancellation_mode', 'refund_count', 'total_refunded',
        'last_refund_date', 'total_revenue', 'total_revenue_with_upgrade',
        'subscription_status', 'lifetime_days', 'end_date'
    ]
    
    try:
        df = pl.read_parquet(str(parquet_file))
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)
        
        missing = expected_set - actual_columns
        extra = actual_columns - expected_set
        
        if not missing and not extra:
            print(f"✓ Schema correct ({len(actual_columns)} columns)")
        else:
            print(f"⚠️  Schema mismatch")
            if missing:
                print(f"Missing columns: {missing}")
            if extra:
                print(f"Extra columns: {extra}")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    # =========================================================================
    # 4. QUERY PERFORMANCE TEST
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("4. QUERY PERFORMANCE TEST")
    print("=" * 80)
    
    queries = {
        "4.1 Count active subscriptions": f"""
            SELECT COUNT(*) as active_subscriptions
            FROM '{parquet_file}'
            WHERE subscription_status = 'Active'
        """,
        
        "4.2 Top 10 CPCs by subscription count": f"""
            SELECT
                first_cpc,
                COUNT(*) as subscription_count,
                SUM(total_revenue) as total_revenue
            FROM '{parquet_file}'
            GROUP BY first_cpc
            ORDER BY subscription_count DESC
            LIMIT 10
        """,
        
        "4.3 Average lifetime by status": f"""
            SELECT
                subscription_status,
                COUNT(*) as count,
                AVG(lifetime_days) as avg_lifetime_days,
                AVG(total_revenue) as avg_revenue
            FROM '{parquet_file}'
            GROUP BY subscription_status
            ORDER BY count DESC
        """,
        
        "4.4 Recent activations (last 7 days)": f"""
            SELECT
                CAST(activation_date AS DATE) as date,
                COUNT(*) as activations,
                SUM(activation_revenue) as revenue
            FROM '{parquet_file}'
            WHERE activation_date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY date
            ORDER BY date DESC
        """,

        "4.5 Subscriptions with most renewals": f"""
            SELECT
                subscription_id,
                tmuserid,
                first_cpc,
                renewal_count,
                renewal_revenue,
                lifetime_days,
                subscription_status
            FROM '{parquet_file}'
            WHERE renewal_count > 0
            ORDER BY renewal_count DESC
            LIMIT 10
        """
    }
    
    for query_name, query in queries.items():
        print(f"\n{query_name}")
        print("-" * 60)
        
        start = time.time()
        try:
            result = con.execute(query).fetchdf()
            elapsed = time.time() - start
            
            print(result.to_string(index=False))
            print(f"\n⏱️  Query time: {elapsed:.3f} seconds")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
    
    con.close()
    
    # =========================================================================
    # COMPLETION
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("VALIDATION AND PERFORMANCE TEST COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    check_subscriptions_parquet_data()
