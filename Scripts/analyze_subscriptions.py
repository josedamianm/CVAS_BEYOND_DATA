import duckdb
import polars as pl
import sys

msisdn = sys.argv[1] if len(sys.argv) > 1 else '34684625552'

con = duckdb.connect()

print('=' * 80)
print(f'SUBSCRIPTION DATA FOR MSISDN: {msisdn}')
print('=' * 80)

query = f"""
SELECT
    subscription_id,
    tmuserid,
    msisdn,
    cpc_list,
    cpc_count,
    first_cpc,
    current_cpc,
    cpc_count,
    first_cpc,
    current_cpc,
    has_upgraded,
    upgrade_date,
    upgraded_to_cpc,
    activation_date,
    activation_trans_date,
    missing_act_record,
    activation_campaign,
    activation_channel,
    activation_revenue,
    activation_month,
    renewal_count,
    renewal_revenue,
    last_renewal_date,
    first_renewal_date,
    last_activity_date,
    deactivation_date,
    deactivation_mode,
    cancellation_date,
    cancellation_mode,
    refund_count,
    total_refunded,
    last_refund_date,
    total_revenue,
    total_revenue_with_upgrade,
    subscription_status,
    lifetime_days,
    end_date
FROM 'Parquet_Data/aggregated/subscriptions.parquet'
WHERE msisdn = '{msisdn}'
ORDER BY activation_date DESC
"""

result = con.execute(query).pl()

if len(result) == 0:
    print(f"\n❌ No subscriptions found for MSISDN: {msisdn}")
    con.close()
    sys.exit(0)

print(f"\n✓ Found {len(result)} subscription(s) for this MSISDN\n")

# Display each subscription in detail
for idx in range(len(result)):
    row = result.row(idx, named=True)

    print('=' * 80)
    print(f'SUBSCRIPTION #{idx + 1}')
    print('=' * 80)

    print('\n📋 BASIC INFORMATION')
    print('-' * 80)
    print(f'  Subscription ID:        {row["subscription_id"]}')
    print(f'  User ID:                {row["tmuserid"]}')
    print(f'  MSISDN:                 {row["msisdn"]}')
    print(f'  Status:                 {row["subscription_status"]}')
    print(f'  Lifetime (days):        {row["lifetime_days"]}')

    print('\n📱 CPC INFORMATION')
    print('-' * 80)
    print(f'  CPC List:               {row["cpc_list"]}')
    print(f'  CPC Count:              {row["cpc_count"]}')
    print(f'  First CPC:              {row["first_cpc"]}')
    print(f'  Current CPC:            {row["current_cpc"]}')
    print(f'  Has Upgraded:           {row["has_upgraded"]}')
    if row["has_upgraded"]:
        print(f'  Upgrade Date:           {row["upgrade_date"]}')
        print(f'  Upgraded to CPC:        {row["upgraded_to_cpc"]}')

    print('\n🚀 ACTIVATION DETAILS')
    print('-' * 80)
    print(f'  Activation Date:        {row["activation_date"]}')
    print(f'  Activation Trans Date:  {row["activation_trans_date"]}')
    print(f'  Activation Month:       {row["activation_month"]}')
    print(f'  Missing ACT Record:     {row["missing_act_record"]}')
    print(f'  Campaign:               {row["activation_campaign"]}')
    print(f'  Channel:                {row["activation_channel"]}')
    print(f'  Activation Revenue:     ${row["activation_revenue"]:.2f}')

    print('\n🔄 RENEWAL INFORMATION')
    print('-' * 80)
    print(f'  Renewal Count:          {row["renewal_count"]}')
    print(f'  Renewal Revenue:        ${row["renewal_revenue"]:.2f}')
    if row["first_renewal_date"]:
        print(f'  First Renewal Date:     {row["first_renewal_date"]}')
    if row["last_renewal_date"]:
        print(f'  Last Renewal Date:      {row["last_renewal_date"]}')
    print(f'  Last Activity Date:     {row["last_activity_date"]}')

    print('\n❌ TERMINATION DETAILS')
    print('-' * 80)
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
    print('-' * 80)
    print(f'  Activation Revenue:     ${row["activation_revenue"]:.2f}')
    print(f'  Renewal Revenue:        ${row["renewal_revenue"]:.2f}')
    print(f'  Total Revenue:          ${row["total_revenue"]:.2f}')
    print(f'  Total w/ Upgrade:       ${row["total_revenue_with_upgrade"]:.2f}')

    if row["refund_count"] > 0:
        print(f'\n  ⚠️  REFUNDS:')
        print(f'      Refund Count:       {row["refund_count"]}')
        print(f'      Total Refunded:     ${row["total_refunded"]:.2f}')
        print(f'      Last Refund Date:   {row["last_refund_date"]}')

    print('\n')

# Summary statistics for this MSISDN
print('=' * 80)
print('SUMMARY STATISTICS FOR THIS MSISDN')
print('=' * 80)

summary_query = f"""
SELECT
    COUNT(*) as total_subscriptions,
    COUNT(DISTINCT subscription_id) as unique_subscriptions,
    SUM(renewal_count) as total_renewals,
    ROUND(SUM(total_revenue), 2) as total_revenue,
    ROUND(AVG(total_revenue), 2) as avg_revenue_per_sub,
    ROUND(SUM(total_refunded), 2) as total_refunded,
    SUM(refund_count) as total_refunds,
    MIN(activation_date) as first_subscription,
    MAX(activation_date) as last_subscription,
    SUM(CASE WHEN subscription_status = 'Active' THEN 1 ELSE 0 END) as active_subs,
    SUM(CASE WHEN subscription_status = 'Deactivated' THEN 1 ELSE 0 END) as deactivated_subs,
    SUM(CASE WHEN subscription_status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_subs,
    SUM(CASE WHEN has_upgraded = TRUE THEN 1 ELSE 0 END) as upgraded_subs
FROM 'Parquet_Data/aggregated/subscriptions.parquet'
WHERE msisdn = '{msisdn}'
"""

summary = con.execute(summary_query).pl()
print(summary)

con.close()
