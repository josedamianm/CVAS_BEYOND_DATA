import duckdb
from pathlib import Path
from datetime import datetime

def build_subscription_view():
    """
    Build aggregated subscription view combining all transaction types
    Handles: Upgrades, Missing Activations, CPC changes
    Tracks all CPCs as a list
    """
    
    project_root = Path(__file__).parent.parent
    parquet_path = project_root / 'Parquet_Data' / 'transactions'
    output_path = project_root / 'Parquet_Data' / 'aggregated'
    output_path.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("BUILDING SUBSCRIPTION VIEW")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    con = duckdb.connect()
    
    # Load SQL query from external file
    sql_file = project_root / 'sql' / 'build_subscription_view.sql'
    print(f"  Loading SQL from: {sql_file.name}")
    query = sql_file.read_text()
    
    # Replace placeholder with actual parquet path
    query = query.replace('{parquet_path}', str(parquet_path))
    
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
