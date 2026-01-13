#!/usr/bin/env python3
import duckdb
from pathlib import Path
import sys
import pandas as pd

def query_tmuserid(tmuserid):
    print(f"Searching for TMUSERID: {tmuserid}")
    
    project_root = Path(__file__).resolve().parent.parent.parent
    parquet_path = project_root / 'Parquet_Data' / 'transactions'
    
    print("=" * 80)
    
    conn = duckdb.connect(':memory:')
    
    # Step 1: Get all subscription_ids for this TMUSERID from ACT, RENO, DCT
    print("\nStep 1: Finding all subscriptions for this TMUSERID...")
    print("-" * 80)

    subscription_ids = set()
    msisdns = set()

    for trans_type in ['act', 'reno', 'dct']:
        trans_dir = parquet_path / trans_type
        if trans_dir.exists():
            parquet_pattern = str(trans_dir / '**' / '*.parquet')
            try:
                query = f"""
                SELECT DISTINCT subscription_id, msisdn
                FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
                WHERE tmuserid = '{tmuserid}'
                """
                result = conn.execute(query).fetchdf()
                subscription_ids.update(result['subscription_id'].tolist())
                msisdns.update(result['msisdn'].tolist())
            except Exception as e:
                print(f"Error reading {trans_type}: {str(e)}")

    if not subscription_ids:
        print("No subscriptions found for this TMUSERID")
        conn.close()
        return

    print(f"Found {len(subscription_ids)} unique subscription(s): {sorted(subscription_ids)}")
    print(f"\nTMUSERID to MSISDN mapping:")
    print(f"  TMUSERID: {tmuserid}")
    print(f"  MSISDN(s): {', '.join(sorted(msisdns))}")
    
    # Step 2: Get all transactions for these subscriptions
    subscription_ids_str = ','.join(map(str, subscription_ids))
    
    all_transactions = []
    
    # ACT transactions
    act_dir = parquet_path / 'act'
    if act_dir.exists():
        parquet_pattern = str(act_dir / '**' / '*.parquet')
        try:
            query = f"""
            SELECT 
                'ACT' as transaction_type,
                trans_type_id,
                subscription_id,
                trans_date,
                act_date,
                reno_date,
                cpc,
                channel_act as channel,
                camp_name,
                rev,
                CAST(NULL AS VARCHAR) as mode,
                CAST(NULL AS DOUBLE) as rfnd_amount,
                CAST(NULL AS BIGINT) as rfnd_cnt,
                CAST(NULL AS VARCHAR) as instant_rfnd
            FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
            WHERE subscription_id IN ({subscription_ids_str})
            """
            result = conn.execute(query).fetchdf()
            all_transactions.append(result)
        except Exception as e:
            print(f"Error reading ACT: {str(e)}")
    
    # RENO transactions
    reno_dir = parquet_path / 'reno'
    if reno_dir.exists():
        parquet_pattern = str(reno_dir / '**' / '*.parquet')
        try:
            query = f"""
            SELECT 
                'RENO' as transaction_type,
                trans_type_id,
                subscription_id,
                trans_date,
                act_date,
                reno_date,
                cpc,
                channel_act as channel,
                camp_name,
                rev,
                CAST(NULL AS VARCHAR) as mode,
                CAST(NULL AS DOUBLE) as rfnd_amount,
                CAST(NULL AS BIGINT) as rfnd_cnt,
                CAST(NULL AS VARCHAR) as instant_rfnd
            FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
            WHERE subscription_id IN ({subscription_ids_str})
            """
            result = conn.execute(query).fetchdf()
            all_transactions.append(result)
        except Exception as e:
            print(f"Error reading RENO: {str(e)}")
    
    # DCT transactions
    dct_dir = parquet_path / 'dct'
    if dct_dir.exists():
        parquet_pattern = str(dct_dir / '**' / '*.parquet')
        try:
            query = f"""
            SELECT 
                'DCT' as transaction_type,
                trans_type_id,
                subscription_id,
                trans_date,
                act_date,
                reno_date,
                cpc,
                channel_dct as channel,
                camp_name,
                CAST(NULL AS DOUBLE) as rev,
                CAST(NULL AS VARCHAR) as mode,
                CAST(NULL AS DOUBLE) as rfnd_amount,
                CAST(NULL AS BIGINT) as rfnd_cnt,
                CAST(NULL AS VARCHAR) as instant_rfnd
            FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
            WHERE subscription_id IN ({subscription_ids_str})
            """
            result = conn.execute(query).fetchdf()
            all_transactions.append(result)
        except Exception as e:
            print(f"Error reading DCT: {str(e)}")
    
    # CNR transactions (no trans_type_id, assign 99 for sorting after DCT)
    cnr_dir = parquet_path / 'cnr'
    if cnr_dir.exists():
        parquet_pattern = str(cnr_dir / '**' / '*.parquet')
        try:
            query = f"""
            SELECT 
                'CNR' as transaction_type,
                99 as trans_type_id,
                sbn_id as subscription_id,
                cancel_date as trans_date,
                CAST(NULL AS TIMESTAMP) as act_date,
                CAST(NULL AS TIMESTAMP) as reno_date,
                cpc,
                mode as channel,
                CAST(NULL AS VARCHAR) as camp_name,
                CAST(NULL AS DOUBLE) as rev,
                mode,
                CAST(NULL AS DOUBLE) as rfnd_amount,
                CAST(NULL AS BIGINT) as rfnd_cnt,
                CAST(NULL AS VARCHAR) as instant_rfnd
            FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
            WHERE sbn_id IN ({subscription_ids_str})
            """
            result = conn.execute(query).fetchdf()
            all_transactions.append(result)
        except Exception as e:
            print(f"Error reading CNR: {str(e)}")
    
    # RFND transactions (no trans_type_id, assign 100 for sorting after CNR)
    rfnd_dir = parquet_path / 'rfnd'
    if rfnd_dir.exists():
        parquet_pattern = str(rfnd_dir / '**' / '*.parquet')
        try:
            query = f"""
            SELECT 
                'RFND' as transaction_type,
                100 as trans_type_id,
                sbnid as subscription_id,
                refnd_date as trans_date,
                CAST(NULL AS TIMESTAMP) as act_date,
                CAST(NULL AS TIMESTAMP) as reno_date,
                cpc,
                CAST(NULL AS VARCHAR) as channel,
                CAST(NULL AS VARCHAR) as camp_name,
                CAST(NULL AS DOUBLE) as rev,
                CAST(NULL AS VARCHAR) as mode,
                rfnd_amount,
                rfnd_cnt,
                instant_rfnd
            FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
            WHERE sbnid IN ({subscription_ids_str})
            """
            result = conn.execute(query).fetchdf()
            all_transactions.append(result)
        except Exception as e:
            print(f"Error reading RFND: {str(e)}")
    
    # Combine all transactions
    if not all_transactions:
        print("No transactions found")
        conn.close()
        return
    
    df_all = pd.concat(all_transactions, ignore_index=True)
    df_all = df_all.sort_values(by=['subscription_id', 'trans_date', 'trans_type_id'])
    
    # Display transactions grouped by subscription_id
    for sub_id in sorted(subscription_ids):
        sub_data = df_all[df_all['subscription_id'] == sub_id].copy()
        
        # Calculate summary
        act_count = len(sub_data[sub_data['transaction_type'] == 'ACT'])
        reno_count = len(sub_data[sub_data['transaction_type'] == 'RENO'])
        dct_count = len(sub_data[sub_data['transaction_type'] == 'DCT'])
        cnr_count = len(sub_data[sub_data['transaction_type'] == 'CNR'])
        rfnd_count = len(sub_data[sub_data['transaction_type'] == 'RFND'])
        
        total_rev = sub_data['rev'].sum() if 'rev' in sub_data.columns else 0
        total_refunded = sub_data['rfnd_amount'].sum() if 'rfnd_amount' in sub_data.columns else 0
        
        print(f"\n{'=' * 80}")
        print(f"SUBSCRIPTION ID: {sub_id}")
        print(f"{'=' * 80}")
        print(f"Summary: ACT={act_count} | RENO={reno_count} | DCT={dct_count} | CNR={cnr_count} | RFND={rfnd_count}")
        print(f"Total Revenue: €{total_rev:.2f} | Total Refunded: €{total_refunded:.2f}")
        print(f"{'-' * 80}")
        
        # Select relevant columns for display
        display_cols = ['transaction_type', 'trans_type_id', 'trans_date', 'cpc', 'channel', 'rev', 'mode', 'rfnd_amount', 'instant_rfnd']
        sub_display = sub_data[display_cols].copy()
        
        print(sub_display.to_string(index=False))
    
    # Step 3: Check for PPD (Pay Per Download) - one-time purchases
    print("\n" + "=" * 80)
    print("ONE-TIME PURCHASES (PPD)")
    print("=" * 80)
    
    ppd_dir = parquet_path / 'ppd'
    if ppd_dir.exists():
        parquet_pattern = str(ppd_dir / '**' / '*.parquet')
        try:
            query = f"""
            SELECT 
                'PPD' as transaction_type,
                trans_type_id,
                subscription_id,
                trans_date,
                cpc,
                channel_id,
                rev
            FROM read_parquet('{parquet_pattern}', hive_partitioning=true)
            WHERE tmuserid = '{tmuserid}'
            ORDER BY trans_date DESC
            """
            result = conn.execute(query).fetchdf()
            
            if len(result) > 0:
                print(f"\nFound {len(result)} one-time purchase(s):")
                print("-" * 80)
                print(result.to_string(index=False))
                total_ppd_rev = result['rev'].sum() if 'rev' in result.columns else 0
                print(f"\nTotal PPD revenue: €{total_ppd_rev:.2f}")
            else:
                print("\nNo one-time purchases found")
        except Exception as e:
            print(f"Error reading PPD: {str(e)}")
    else:
        print("\nPPD directory not found")
    
    print("\n" + "=" * 80)
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python query_tmuserid_from_tx.py <TMUSERID>")
        print("Example: python query_tmuserid_from_tx.py 8343817051345500000")
        sys.exit(1)
    
    tmuserid = sys.argv[1]
    query_tmuserid(tmuserid)
