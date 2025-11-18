 #!/usr/bin/env python3
"""
Interactive Subscription Query Tool
Query subscriptions by subscription_id, tmuserid, or msisdn
Provides summarized data per subscription followed by complete raw output
"""

import duckdb
import polars as pl
import sys
import os
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent.parent
PARQUET_FILE = SCRIPT_DIR / "Parquet_Data" / "aggregated" / "subscriptions.parquet"


def display_menu():
    print("\n" + "=" * 100)
    print("SUBSCRIPTION QUERY TOOL")
    print("=" * 100)
    print("\nSelect query type:")
    print("  1. Query by Subscription ID (single subscription)")
    print("  2. Query by User ID (tmuserid - may return multiple subscriptions)")
    print("  3. Query by MSISDN (phone number - may return multiple subscriptions)")
    print("  0. Exit")
    print("\n" + "-" * 100)


def get_user_choice():
    while True:
        choice = input("\nEnter your choice (0-3): ").strip()
        if choice in ['0', '1', '2', '3']:
            return choice
        print("❌ Invalid choice. Please enter 0, 1, 2, or 3.")


def get_query_value(query_type):
    prompts = {
        '1': "Enter Subscription ID: ",
        '2': "Enter User ID (tmuserid): ",
        '3': "Enter MSISDN (phone number): "
    }
    return input(prompts[query_type]).strip()


def query_subscriptions(query_type, query_value):
    if not PARQUET_FILE.exists():
        print(f"\n❌ Error: Parquet file not found at: {PARQUET_FILE}")
        print(f"   Please ensure the file exists at the expected location.")
        return

    con = duckdb.connect()

    field_map = {
        '1': 'subscription_id',
        '2': 'tmuserid',
        '3': 'msisdn'
    }

    field_name = field_map[query_type]
    field_display = {
        '1': 'Subscription ID',
        '2': 'User ID (tmuserid)',
        '3': 'MSISDN'
    }

    print('\n' + '=' * 100)
    print(f'SUBSCRIPTION QUERY FOR {field_display[query_type]}: {query_value}')
    print(f'Query Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 100)

    query = f"""
    SELECT *
    FROM '{PARQUET_FILE}'
    WHERE {field_name} = '{query_value}'
    ORDER BY activation_date DESC
    """

    try:
        result = con.execute(query).pl()
    except Exception as e:
        print(f"\n❌ Error executing query: {e}")
        con.close()
        return

    if len(result) == 0:
        print(f"\n❌ No subscriptions found for {field_display[query_type]}: {query_value}")
        con.close()
        return

    print(f"\n✓ Found {len(result)} subscription(s)\n")

    print_summary_per_subscription(result, con, field_name, query_value)
    
    print_aggregated_summary(result, con, field_name, query_value)
    
    print_raw_output(result)
    
    con.close()
    
    print('\n' + '=' * 100)
    print('END OF REPORT')
    print('=' * 100)


def print_summary_per_subscription(result, con, field_name, query_value):
    print('\n' + '=' * 100)
    print('SECTION 1: SUMMARY PER SUBSCRIPTION')
    print('=' * 100)
    
    for idx in range(len(result)):
        row = result.row(idx, named=True)

        print(f'\n{"=" * 100}')
        print(f'SUBSCRIPTION #{idx + 1} - ID: {row["subscription_id"]}')
        print('=' * 100)

        print('\n📋 BASIC INFORMATION')
        print('-' * 100)
        print(f'  Subscription ID:        {row["subscription_id"]}')
        print(f'  User ID:                {row["tmuserid"]}')
        print(f'  MSISDN:                 {row["msisdn"]}')
        print(f'  Status:                 {row["subscription_status"]}')
        print(f'  Lifetime (days):        {row["lifetime_days"]}')

        print('\n📱 CPC INFORMATION')
        print('-' * 100)
        print(f'  CPC List:               {row["cpc_list"]}')
        print(f'  CPC Count:              {row["cpc_count"]}')
        print(f'  First CPC:              {row["first_cpc"]}')
        print(f'  Current CPC:            {row["current_cpc"]}')
        print(f'  Has Upgraded:           {row["has_upgraded"]}')
        if row["has_upgraded"]:
            print(f'  Upgrade Date:           {row["upgrade_date"]}')
            print(f'  Upgraded to CPC:        {row["upgraded_to_cpc"]}')

        print('\n🚀 ACTIVATION DETAILS')
        print('-' * 100)
        print(f'  Activation Date:        {row["activation_date"]}')
        print(f'  Activation Trans Date:  {row["activation_trans_date"]}')
        print(f'  Activation Month:       {row["activation_month"]}')
        print(f'  Missing ACT Record:     {row["missing_act_record"]}')
        print(f'  Campaign:               {row["activation_campaign"]}')
        print(f'  Channel:                {row["activation_channel"]}')
        print(f'  Activation Revenue:     ${row["activation_revenue"]:.2f}')

        print('\n🔄 RENEWAL INFORMATION')
        print('-' * 100)
        print(f'  Renewal Count:          {row["renewal_count"]}')
        print(f'  Renewal Revenue:        ${row["renewal_revenue"]:.2f}')
        if row["first_renewal_date"]:
            print(f'  First Renewal Date:     {row["first_renewal_date"]}')
        if row["last_renewal_date"]:
            print(f'  Last Renewal Date:      {row["last_renewal_date"]}')
        print(f'  Last Activity Date:     {row["last_activity_date"]}')

        print('\n❌ TERMINATION DETAILS')
        print('-' * 100)
        if row["deactivation_date"]:
            print(f'  Deactivation Date:      {row["deactivation_date"]}')
            print(f'  Deactivation Mode:      {row["deactivation_mode"]}')
        else:
            print(f'  Deactivation Date:      None')

        if row["cancellation_date"]:
            print(f'  Cancellation Date:      {row["cancellation_date"]}')
            print(f'  Cancellation Mode:      {row["cancellation_mode"]}')
        else:
            print(f'  Cancellation Date:      None')

        if row["end_date"]:
            print(f'  End Date:               {row["end_date"]}')

        print('\n💰 FINANCIAL SUMMARY')
        print('-' * 100)
        print(f'  Activation Revenue:     ${row["activation_revenue"]:.2f}')
        print(f'  Renewal Revenue:        ${row["renewal_revenue"]:.2f}')
        print(f'  Total Revenue:          ${row["total_revenue"]:.2f}')
        print(f'  Total w/ Upgrade:       ${row["total_revenue_with_upgrade"]:.2f}')

        if row["refund_count"] > 0:
            print(f'\n  ⚠️  REFUNDS:')
            print(f'      Refund Count:       {row["refund_count"]}')
            print(f'      Total Refunded:     ${row["total_refunded"]:.2f}')
            print(f'      Last Refund Date:   {row["last_refund_date"]}')


def print_aggregated_summary(result, con, field_name, query_value):
    print('\n\n' + '=' * 100)
    print('SECTION 2: AGGREGATED SUMMARY')
    print('=' * 100)

    summary_query = f"""
    SELECT
        COUNT(*) as total_subscriptions,
        COUNT(DISTINCT subscription_id) as unique_subscriptions,
        COUNT(DISTINCT tmuserid) as unique_users,
        COUNT(DISTINCT msisdn) as unique_msisdns,
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
    FROM '{PARQUET_FILE}'
    WHERE {field_name} = '{query_value}'
    """

    summary = con.execute(summary_query).pl()

    print('\n📊 OVERALL STATISTICS')
    print('-' * 100)
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000):
        print(summary)

    print('\n\n📱 CPC BREAKDOWN')
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
    FROM '{PARQUET_FILE}'
    WHERE {field_name} = '{query_value}'
    GROUP BY first_cpc
    ORDER BY subscription_count DESC
    """
    cpc_breakdown = con.execute(cpc_query).pl()
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000):
        print(cpc_breakdown)

    print('\n\n📈 STATUS BREAKDOWN')
    print('-' * 100)
    status_query = f"""
    SELECT
        subscription_status,
        COUNT(*) as count,
        ROUND(AVG(lifetime_days), 0) as avg_lifetime_days,
        ROUND(SUM(total_revenue), 2) as total_revenue,
        ROUND(AVG(total_revenue), 2) as avg_revenue
    FROM '{PARQUET_FILE}'
    WHERE {field_name} = '{query_value}'
    GROUP BY subscription_status
    ORDER BY count DESC
    """
    status_breakdown = con.execute(status_query).pl()
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000):
        print(status_breakdown)

    print('\n\n📅 SUBSCRIPTION TIMELINE')
    print('-' * 100)
    timeline_query = f"""
    SELECT
        subscription_id,
        tmuserid,
        msisdn,
        first_cpc,
        activation_date,
        last_activity_date,
        end_date,
        subscription_status,
        lifetime_days,
        renewal_count,
        ROUND(total_revenue, 2) as total_revenue
    FROM '{PARQUET_FILE}'
    WHERE {field_name} = '{query_value}'
    ORDER BY activation_date
    """
    timeline = con.execute(timeline_query).pl()
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=1000):
        print(timeline)

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

    print(f'\n1. Total of {total_subs} subscription(s) found')
    print(f'2. Currently {active_subs} active subscription(s)')
    print(f'3. Generated ${total_revenue:.2f} in total revenue')
    print(f'4. Total of {int(total_renewals)} renewals across all subscriptions')
    print(f'5. Average subscription lifetime: {int(avg_lifetime)} days')

    if missing_act > 0:
        print(f'6. ⚠️  {int(missing_act)} subscription(s) with missing activation records')

    if upgraded > 0:
        print(f'7. 🔄 {int(upgraded)} subscription(s) have been upgraded')

    if active_subs > 0:
        print(f'\n✅ Has ACTIVE subscription(s)')
    else:
        print(f'\n❌ NO active subscriptions')


def print_raw_output(result):
    print('\n\n' + '=' * 100)
    print('SECTION 3: COMPLETE RAW DATA OUTPUT')
    print('=' * 100)
    print('\nAll columns and values as stored in the parquet file (vertical format):\n')

    for idx in range(len(result)):
        row = result.row(idx, named=True)

        print(f'{"*" * 100}')
        print(f'Row {idx + 1} (Subscription ID: {row.get("subscription_id", "N/A")})')
        print(f'{"*" * 100}')

        # Determine the width for the longest column name for alignment
        try:
            keys = list(row.keys())
        except Exception:
            keys = []
        max_col_width = max((len(str(k)) for k in keys), default=0)

        for col_name in keys:
            value = row.get(col_name)
            if value is None:
                formatted_value = 'NULL'
            elif isinstance(value, float):
                formatted_value = f'{value:.2f}'
            elif isinstance(value, list) or isinstance(value, tuple):
                formatted_value = str(value)
            else:
                formatted_value = str(value)

            print(f'{col_name.rjust(max_col_width)}: {formatted_value}')

        if idx < len(result) - 1:
            print()


def main():
    while True:
        display_menu()
        choice = get_user_choice()
        
        if choice == '0':
            print("\n👋 Goodbye!")
            sys.exit(0)
        
        query_value = get_query_value(choice)
        
        if not query_value:
            print("❌ Query value cannot be empty.")
            continue
        
        query_subscriptions(choice, query_value)
        
        print("\n" + "-" * 100)
        continue_choice = input("\nDo you want to perform another query? (y/n): ").strip().lower()
        if continue_choice != 'y':
            print("\n👋 Goodbye!")
            break


if __name__ == "__main__":
    main()
