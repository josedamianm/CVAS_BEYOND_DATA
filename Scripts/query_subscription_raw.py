#!/usr/bin/env python3
"""
Query Subscription Data by MSISDN
Provides complete raw output and summarized view
Usage: python query_subscription_raw.py <msisdn>
"""

import duckdb
import polars as pl
import sys
from datetime import datetime

def query_subscription(msisdn):
    """Query subscription data for a given MSISDN"""
    
    con = duckdb.connect()
    
    print('=' * 100)
    print(f'SUBSCRIPTION QUERY FOR MSISDN: {msisdn}')
    print(f'Query Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 100)
    
    # Raw query - SELECT * FROM subscriptions
    query = f"""
    SELECT * 
    FROM 'Parquet_Data/aggregated/subscriptions.parquet'
    WHERE msisdn = '{msisdn}'
    ORDER BY activation_date DESC
    """

    try:
        result = con.execute(query).pl()
    except Exception as e:
        print(f"\n❌ Error executing query: {e}")
        con.close()
        sys.exit(1)

    if len(result) == 0:
        print(f"\n❌ No subscriptions found for MSISDN: {msisdn}")
        con.close()
        sys.exit(0)

    print(f"\n✓ Found {len(result)} subscription(s) for this MSISDN\n")

    # ========================================================================
    # SECTION 1: COMPLETE RAW OUTPUT
    # ========================================================================
    print('=' * 100)
    print('SECTION 1: COMPLETE RAW DATA OUTPUT')
    print('=' * 100)
    print('\nAll columns and values as stored in the parquet file:\n')

    # Display raw data with all columns
    with pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=1000,
        fmt_str_lengths=100
    ):
        print(result)

    # ========================================================================
    # SECTION 2: COLUMN-BY-COLUMN BREAKDOWN
    # ========================================================================
    print('\n\n' + '=' * 100)
    print('SECTION 2: DETAILED COLUMN-BY-COLUMN BREAKDOWN')
    print('=' * 100)

    for idx in range(len(result)):
        row = result.row(idx, named=True)

        print(f'\n{"=" * 100}')
        print(f'SUBSCRIPTION #{idx + 1} - ID: {row["subscription_id"]}')
        print('=' * 100)

        # Group columns by category
        categories = {
            'IDENTIFIERS': [
                'subscription_id', 'tmuserid', 'msisdn'
            ],
            'CPC INFORMATION': [
                'cpc_list', 'cpc_count', 'first_cpc', 'current_cpc',
                'has_upgraded', 'upgrade_date', 'upgraded_to_cpc'
            ],
            'ACTIVATION': [
                'activation_date', 'activation_trans_date', 'activation_month',
                'missing_act_record', 'activation_campaign', 'activation_channel',
                'activation_revenue'
            ],
            'RENEWAL': [
                'renewal_count', 'renewal_revenue', 'first_renewal_date',
                'last_renewal_date', 'last_activity_date'
            ],
            'DEACTIVATION': [
                'deactivation_date', 'deactivation_mode'
            ],
            'CANCELLATION': [
                'cancellation_date', 'cancellation_mode'
            ],
            'REFUND': [
                'refund_count', 'total_refunded', 'last_refund_date'
            ],
            'FINANCIAL': [
                'total_revenue', 'total_revenue_with_upgrade'
            ],
            'STATUS & LIFECYCLE': [
                'subscription_status', 'lifetime_days', 'end_date'
            ]
        }

        for category, columns in categories.items():
            print(f'\n{category}:')
            print('-' * 100)
            for col in columns:
                if col in row:
                    value = row[col]
                    # Format the value
                    if value is None:
                        formatted_value = 'NULL'
                    elif isinstance(value, float):
                        formatted_value = f'{value:.2f}'
                    else:
                        formatted_value = str(value)
                    print(f'  {col:<30} = {formatted_value}')

    # ========================================================================
    # SECTION 3: SUMMARIZED OUTPUT
    # ========================================================================
    print('\n\n' + '=' * 100)
    print('SECTION 3: SUMMARIZED OUTPUT')
    print('=' * 100)

    # Summary statistics for this MSISDN
    summary_query = f"""
    SELECT
        COUNT(*) as total_subscriptions,
        COUNT(DISTINCT subscription_id) as unique_subscriptions,
        COUNT(DISTINCT tmuserid) as unique_users,
        SUM(renewal_count) as total_renewals,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(total_revenue), 2) as avg_revenue_per_sub,
        ROUND(MIN(total_revenue), 2) as min_revenue,
        ROUND(MAX(total_revenue), 2) as max_revenue,
        ROUND(SUM(total_refunded), 2) as total_refunded,
        SUM(refund_count) as total_refunds,
        MIN(activation_date) as first_subscription,
        MAX(activation_date) as last_subscription,
        SUM(CASE WHEN subscription_status = 'Active' THEN 1 ELSE 0 END) as active_subs,
        SUM(CASE WHEN subscription_status = 'Deactivated' THEN 1 ELSE 0 END) as deactivated_subs,
        SUM(CASE WHEN subscription_status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_subs,
        SUM(CASE WHEN has_upgraded = TRUE THEN 1 ELSE 0 END) as upgraded_subs,
        SUM(CASE WHEN missing_act_record = TRUE THEN 1 ELSE 0 END) as missing_act_records,
        ROUND(AVG(lifetime_days), 0) as avg_lifetime_days,
        COUNT(DISTINCT first_cpc) as unique_cpcs
    FROM 'Parquet_Data/aggregated/subscriptions.parquet'
    WHERE msisdn = '{msisdn}'
    """

    summary = con.execute(summary_query).pl()

    # CPC breakdown
    print('\n\n� CPC BREAKDOWN')
    print('-' * 100)
    cpc_query = f"""
    SELECT
        first_cpc,
        COUNT(*) as subscription_count,
        SUM(renewal_count) as total_renewals,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(total_revenue), 2) as avg_revenue,
        MIN(activation_date) as first_activation,
        MAX(activation_date) as last_activation
    FROM 'Parquet_Data/aggregated/subscriptions.parquet'
    WHERE msisdn = '{msisdn}'
    GROUP BY first_cpc
    ORDER BY subscription_count DESC
    """
    cpc_breakdown = con.execute(cpc_query).pl()
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000):
        print(cpc_breakdown)

    # Status breakdown
    print('\n\n📈 STATUS BREAKDOWN')
    print('-' * 100)
    status_query = f"""
    SELECT
        subscription_status,
        COUNT(*) as count,
        ROUND(AVG(lifetime_days), 0) as avg_lifetime_days,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(total_revenue), 2) as avg_revenue
    FROM 'Parquet_Data/aggregated/subscriptions.parquet'
    WHERE msisdn = '{msisdn}'
    GROUP BY subscription_status
    ORDER BY count DESC
    """
    status_breakdown = con.execute(status_query).pl()
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000):
        print(status_breakdown)

    # Timeline view
    print('\n\n📅 SUBSCRIPTION TIMELINE')
    print('-' * 100)
    timeline_query = f"""
    SELECT
        subscription_id,
        first_cpc,
        activation_date,
        last_activity_date,
        end_date,
        subscription_status,
        lifetime_days,
        renewal_count,
        ROUND(total_revenue, 2) as total_revenue
    FROM 'Parquet_Data/aggregated/subscriptions.parquet'
    WHERE msisdn = '{msisdn}'
    ORDER BY activation_date
    """
    timeline = con.execute(timeline_query).pl()
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000):
        print(timeline)

    # Key insights
    print('\n\n' + '=' * 100)
    print('🔍 KEY INSIGHTS')
    print('=' * 100)

    total_subs = len(result)
    active_subs = summary['active_subs'][0]
    total_revenue = summary['total_revenue'][0]
    total_renewals = summary['total_renewals'][0]
    avg_lifetime = summary['avg_lifetime_days'][0]
    missing_act = summary['missing_act_records'][0]
    upgraded = summary['upgraded_subs'][0]

    print(f'\n1. Customer has {total_subs} subscription(s) in total')
    print(f'2. Currently {active_subs} active subscription(s)')
    print(f'3. Generated ${total_revenue:.2f} in total revenue')
    print(f'4. Total of {int(total_renewals)} renewals across all subscriptions')
    print(f'5. Average subscription lifetime: {int(avg_lifetime)} days')

    if missing_act > 0:
        print(f'6. ⚠️  {int(missing_act)} subscription(s) with missing activation records')

    if upgraded > 0:
        print(f'7. 🔄 {int(upgraded)} subscription(s) have been upgraded')

    if active_subs > 0:
        print(f'\n✅ Customer is ACTIVE with ongoing subscription(s)')
    else:
        print(f'\n❌ Customer has NO active subscriptions')

    con.close()

    print('\n' + '=' * 100)
    print('END OF REPORT')
    print('=' * 100)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python query_subscription_raw.py <msisdn>")
        print("Example: python query_subscription_raw.py 34684625552")
        sys.exit(1)

    msisdn = sys.argv[1]
    query_subscription(msisdn)
