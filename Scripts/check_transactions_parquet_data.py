import duckdb
from pathlib import Path
import time

def test_query_performance():
    """
    Test query performance on Parquet data using DuckDB
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

if __name__ == "__main__":
    test_query_performance()
