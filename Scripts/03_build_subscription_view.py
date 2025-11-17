import duckdb
from pathlib import Path
from datetime import datetime

def build_subscription_view():
    """
    Build aggregated subscription view combining all transaction types
    Handles: Upgrades, Missing Activations, CPC changes
    Tracks all CPCs as a list
    """
    
    #parquet_path = Path('Parquet_Data/transactions')
    parquet_path = Path('/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Parquet_Data/transactions')
    #output_path = Path('Parquet_Data/aggregated')
    output_path = Path('/Users/josemanco/CVAS/CVAS_BEYOND_DATA/Parquet_Data/aggregated')
    output_path.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("BUILDING SUBSCRIPTION VIEW")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    con = duckdb.connect()
    
    # Build comprehensive subscription view with special case handling
    print("Creating subscription aggregation...")
    
    query = f"""
    CREATE OR REPLACE TABLE subscriptions AS
    WITH 
    -- Get all transactions (ACT + RENO) to handle missing activations and upgrades
    all_transactions AS (
        SELECT 
            subscription_id,
            tmuserid,
            msisdn,
            cpc,
            trans_type_id,
            trans_date,
            act_date,
            reno_date,
            camp_name,
            channel_act as channel,
            rev,
            year_month,
            'ACT' as transaction_type
        FROM read_parquet('{parquet_path}/act/**/*.parquet', hive_partitioning=true)
        
        UNION ALL
        
        SELECT 
            subscription_id,
            tmuserid,
            msisdn,
            cpc,
            trans_type_id,
            trans_date,
            act_date,
            reno_date,
            camp_name,
            channel_act as channel,
            rev,
            year_month,
            'RENO' as transaction_type
        FROM read_parquet('{parquet_path}/reno/**/*.parquet', hive_partitioning=true)
    ),
    
    -- Get list of all CPCs per subscription (ordered by first appearance)
    cpc_with_order AS (
        SELECT 
            subscription_id,
            cpc,
            MIN(trans_date) as first_seen
        FROM all_transactions
        GROUP BY subscription_id, cpc
    ),
    
    cpc_list AS (
        SELECT 
            subscription_id,
            LIST(cpc ORDER BY first_seen) as cpc_list,
            COUNT(DISTINCT cpc) as cpc_count
        FROM cpc_with_order
        GROUP BY subscription_id
    ),
    
    -- Get first transaction per subscription (handles missing ACT records)
    first_transaction AS (
        SELECT 
            subscription_id,
            tmuserid,
            msisdn,
            cpc as first_cpc,
            act_date as activation_date,
            trans_date as first_trans_date,
            camp_name as activation_campaign,
            channel as activation_channel,
            year_month as activation_month
        FROM all_transactions
        QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date ASC) = 1
    ),
    
    -- Get actual ACT records (to distinguish from inferred activations)
    actual_activations AS (
        SELECT 
            subscription_id,
            trans_date as actual_act_trans_date,
            rev as activation_revenue,
            cpc as act_cpc
        FROM all_transactions
        WHERE transaction_type = 'ACT'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date ASC) = 1
    ),
    
    -- Detect CPC upgrades (when CPC changes after activation)
    cpc_changes AS (
        SELECT 
            subscription_id,
            cpc as new_cpc,
            trans_date as upgrade_date,
            rev as upgrade_revenue
        FROM all_transactions
        WHERE transaction_type = 'ACT' 
        AND trans_type_id = 1  -- Upgrade transaction
        QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date DESC) = 1
    ),
    
    -- Get current CPC (last known CPC for the subscription)
    current_cpc AS (
        SELECT 
            subscription_id,
            cpc as current_cpc
        FROM all_transactions
        QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date DESC) = 1
    ),
    
    -- Aggregate all renewals
    renewals AS (
        SELECT 
            subscription_id,
            COUNT(*) as total_renewals,
            SUM(rev) as total_renewal_revenue,
            MAX(trans_date) as last_renewal_date,
            MIN(trans_date) as first_renewal_date
        FROM all_transactions
        WHERE transaction_type = 'RENO'
        GROUP BY subscription_id
    ),
    
    -- Get deactivations (excluding UPGRADE deactivations)
    deactivations AS (
        SELECT 
            subscription_id,
            trans_date as deactivation_date,
            channel_dct as deactivation_mode
        FROM read_parquet('{parquet_path}/dct/**/*.parquet', hive_partitioning=true)
        WHERE channel_dct != 'UPGRADE'  -- Exclude upgrade-related DCT
        QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date DESC) = 1
    ),
    
    -- Get cancellations
    cancellations AS (
        SELECT 
            sbn_id as subscription_id,
            cancel_date as cancellation_date,
            mode as cancellation_mode
        FROM read_parquet('{parquet_path}/cnr/**/*.parquet', hive_partitioning=true)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY sbn_id ORDER BY cancel_date DESC) = 1
    ),
    
    -- Get refunds
    refunds AS (
        SELECT 
            sbnid as subscription_id,
            COUNT(*) as refund_count,
            SUM(rfnd_amount) as total_refunded,
            MAX(refnd_date) as last_refund_date
        FROM read_parquet('{parquet_path}/rfnd/**/*.parquet', hive_partitioning=true)
        GROUP BY sbnid
    )
    
    -- Final aggregation
    SELECT 
        ft.subscription_id,
        ft.tmuserid,
        ft.msisdn,
        
        -- CPC tracking (as list)
        cpc_l.cpc_list,
        cpc_l.cpc_count,
        ft.first_cpc,
        cc.current_cpc,
        CASE WHEN cpc_l.cpc_count > 1 THEN TRUE ELSE FALSE END as has_upgraded,
        cpc_ch.upgrade_date,
        cpc_ch.new_cpc as upgraded_to_cpc,
        
        -- Activation info
        ft.activation_date,
        COALESCE(aa.actual_act_trans_date, ft.first_trans_date) as activation_trans_date,
        CASE WHEN aa.subscription_id IS NULL THEN TRUE ELSE FALSE END as missing_act_record,
        ft.activation_campaign,
        ft.activation_channel,
        COALESCE(aa.activation_revenue, 0) as activation_revenue,
        ft.activation_month,
        
        -- Renewal info
        COALESCE(r.total_renewals, 0) as renewal_count,
        COALESCE(r.total_renewal_revenue, 0) as renewal_revenue,
        r.last_renewal_date,
        r.first_renewal_date,
        COALESCE(r.last_renewal_date, ft.activation_date) as last_activity_date,
        
        -- Deactivation info
        d.deactivation_date,
        d.deactivation_mode,
        
        -- Cancellation info
        c.cancellation_date,
        c.cancellation_mode,
        
        -- Refund info
        COALESCE(rf.refund_count, 0) as refund_count,
        COALESCE(rf.total_refunded, 0) as total_refunded,
        rf.last_refund_date,
        
        -- Calculated fields
        COALESCE(aa.activation_revenue, 0) + COALESCE(r.total_renewal_revenue, 0) as total_revenue,
        COALESCE(aa.activation_revenue, 0) + COALESCE(r.total_renewal_revenue, 0) + COALESCE(cpc_ch.upgrade_revenue, 0) as total_revenue_with_upgrade,
        
        -- Status determination
        CASE 
            WHEN c.cancellation_date IS NOT NULL THEN 'Cancelled'
            WHEN d.deactivation_date IS NOT NULL THEN 'Deactivated'
            ELSE 'Active'
        END as subscription_status,
        
        -- Lifetime calculation (days)
        CASE 
            WHEN c.cancellation_date IS NOT NULL THEN 
                DATE_DIFF('day', ft.activation_date, c.cancellation_date)
            WHEN d.deactivation_date IS NOT NULL THEN 
                DATE_DIFF('day', ft.activation_date, d.deactivation_date)
            ELSE 
                DATE_DIFF('day', ft.activation_date, CURRENT_DATE)
        END as lifetime_days,
        
        -- End date (for easier filtering)
        COALESCE(c.cancellation_date, d.deactivation_date) as end_date
        
    FROM first_transaction ft
    LEFT JOIN cpc_list cpc_l ON ft.subscription_id = cpc_l.subscription_id
    LEFT JOIN actual_activations aa ON ft.subscription_id = aa.subscription_id
    LEFT JOIN current_cpc cc ON ft.subscription_id = cc.subscription_id
    LEFT JOIN cpc_changes cpc_ch ON ft.subscription_id = cpc_ch.subscription_id
    LEFT JOIN renewals r ON ft.subscription_id = r.subscription_id
    LEFT JOIN deactivations d ON ft.subscription_id = d.subscription_id
    LEFT JOIN cancellations c ON ft.subscription_id = c.subscription_id
    LEFT JOIN refunds rf ON ft.subscription_id = rf.subscription_id
    """
    
    print("  Executing query...", end=' ')
    start = datetime.now()
    con.execute(query)
    elapsed = (datetime.now() - start).total_seconds()
    print(f"✓ ({elapsed:.2f}s)")
    
    # Get row count
    result = con.execute("SELECT COUNT(*) FROM subscriptions").fetchone()
    row_count = result[0]
    print(f"  Total subscriptions: {row_count:,}")
    
    # Export to Parquet
    print("\n  Exporting to Parquet...", end=' ')
    output_file = output_path / 'subscriptions.parquet'
    con.execute(f"""
        COPY subscriptions 
        TO '{output_file}' 
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    
    file_size = output_file.stat().st_size / (1024 * 1024)
    print(f"✓ {file_size:.2f} MB")
    
    # Show sample statistics
    print("\n" + "=" * 60)
    print("SUBSCRIPTION STATISTICS")
    print("=" * 60)
    
    stats_queries = {
        "Status Distribution": """
            SELECT 
                subscription_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM subscriptions
            GROUP BY subscription_status
            ORDER BY count DESC
        """,
        
        "Missing Activation Records": """
            SELECT 
                missing_act_record,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM subscriptions
            GROUP BY missing_act_record
        """,
        
        "CPC Count Distribution": """
            SELECT 
                cpc_count,
                COUNT(*) as subscriptions,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM subscriptions
            GROUP BY cpc_count
            ORDER BY cpc_count
        """,
        
        "Upgrade Statistics": """
            SELECT 
                has_upgraded,
                COUNT(*) as count,
                ROUND(AVG(total_revenue_with_upgrade), 2) as avg_revenue
            FROM subscriptions
            GROUP BY has_upgraded
        """,
        
        "Revenue Summary": """
            SELECT 
                COUNT(*) as total_subscriptions,
                ROUND(SUM(total_revenue), 2) as total_revenue,
                ROUND(AVG(total_revenue), 2) as avg_revenue_per_sub,
                ROUND(AVG(renewal_count), 2) as avg_renewals,
                ROUND(AVG(lifetime_days), 1) as avg_lifetime_days
            FROM subscriptions
        """,
        
        "Top 5 Campaigns by Revenue": """
            SELECT 
                activation_campaign,
                COUNT(*) as subscriptions,
                ROUND(SUM(total_revenue), 2) as revenue,
                ROUND(AVG(lifetime_days), 1) as avg_lifetime
            FROM subscriptions
            WHERE activation_campaign IS NOT NULL
            GROUP BY activation_campaign
            ORDER BY revenue DESC
            LIMIT 5
        """,
        
        "Monthly Activations (Last 6 months)": """
            SELECT 
                activation_month,
                COUNT(*) as new_subscriptions,
                ROUND(SUM(activation_revenue), 2) as revenue,
                SUM(CASE WHEN has_upgraded THEN 1 ELSE 0 END) as upgrades
            FROM subscriptions
            GROUP BY activation_month
            ORDER BY activation_month DESC
            LIMIT 6
        """
    }
    
    for stat_name, stat_query in stats_queries.items():
        print(f"\n{stat_name}:")
        print("-" * 60)
        result = con.execute(stat_query).fetchdf()
        print(result.to_string(index=False))
    
    # Show example of upgrade case with CPC list
    print("\n\nExample: Subscription with Upgrade (Multiple CPCs)")
    print("-" * 60)
    upgrade_example = con.execute("""
        SELECT 
            subscription_id,
            cpc_list,
            cpc_count,
            first_cpc,
            current_cpc,
            upgrade_date,
            activation_date,
            renewal_count,
            total_revenue_with_upgrade,
            subscription_status
        FROM subscriptions
        WHERE has_upgraded = TRUE
        LIMIT 3
    """).fetchdf()
    print(upgrade_example.to_string(index=False))
    
    # Show example of missing activation
    print("\n\nExample: Subscription with Missing ACT Record")
    print("-" * 60)
    missing_act_example = con.execute("""
        SELECT 
            subscription_id,
            cpc_list,
            activation_date,
            activation_trans_date,
            missing_act_record,
            renewal_count,
            total_revenue,
            subscription_status
        FROM subscriptions
        WHERE missing_act_record = TRUE
        LIMIT 3
    """).fetchdf()
    print(missing_act_example.to_string(index=False))
    
    con.close()
    
    print("\n" + "=" * 60)
    print("SUBSCRIPTION VIEW COMPLETE")
    print("=" * 60)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nOutput: {output_file}")

if __name__ == "__main__":
    build_subscription_view()
