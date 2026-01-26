#!/usr/bin/env python3
"""
Enrich Users_No_Limits.csv with TMUSERID column.

Reads Users_No_Limits.csv (one MSISDN per line), scans transaction data
to find corresponding TMUSERIDs, and writes back a CSV with both columns.

Usage:
    python Scripts/enrich_users_no_limits.py
"""

import polars as pl
from pathlib import Path
import sys


def find_tmuserids_for_msisdns(parquet_base: Path, msisdns: set[str]) -> dict[str, str]:
    """
    Scan ACT, RENO, DCT, PPD transactions to find tmuserid for each msisdn.
    
    Args:
        parquet_base: Path to Parquet_Data/transactions
        msisdns: Set of MSISDNs to look up
    
    Returns:
        Dict mapping msisdn -> tmuserid (first found)
    """
    mapping = {}
    tx_types = ['act', 'reno', 'dct', 'ppd']
    
    for tx_type in tx_types:
        tx_path = parquet_base / tx_type
        if not tx_path.exists():
            print(f"  ⚠️  {tx_type.upper()} directory not found, skipping")
            continue
        
        print(f"  Scanning {tx_type.upper()}...", end=' ')
        try:
            df = pl.scan_parquet(str(tx_path / "**/*.parquet")).select(['msisdn', 'tmuserid']).collect()
            
            if df.is_empty():
                print("empty")
                continue
            
            matched = df.filter(pl.col('msisdn').cast(pl.Utf8).is_in(msisdns))
            
            for row in matched.iter_rows(named=True):
                msisdn = str(row['msisdn'])
                tmuserid = str(row['tmuserid'])
                
                if msisdn not in mapping and tmuserid:
                    mapping[msisdn] = tmuserid
            
            print(f"✓ found {len([m for m in mapping if m in msisdns])} mappings")
        except Exception as e:
            print(f"✗ error: {e}")
            continue
        
        if len(mapping) == len(msisdns):
            print(f"  All MSISDNs mapped, stopping scan")
            break
    
    return mapping


def main():
    project_root = Path(__file__).resolve().parent.parent
    csv_path = project_root / 'Users_No_Limits.csv'
    parquet_base = project_root / 'Parquet_Data' / 'transactions'
    
    if not csv_path.exists():
        print(f"✗ Error: {csv_path} not found")
        sys.exit(1)
    
    print("=" * 60)
    print("ENRICH Users_No_Limits.csv WITH TMUSERID")
    print("=" * 60)
    
    print(f"\nReading {csv_path.name}...")
    try:
        df = pl.read_csv(csv_path, has_header=False, new_columns=['msisdn'])
        msisdns = df['msisdn'].cast(pl.Utf8).to_list()
        msisdn_set = set(m for m in msisdns if m)
        print(f"  Loaded {len(msisdn_set)} unique MSISDNs")
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        sys.exit(1)
    
    if not msisdn_set:
        print("✗ No MSISDNs found in file")
        sys.exit(1)
    
    print(f"\nScanning transaction data for TMUSERIDs...")
    mapping = find_tmuserids_for_msisdns(parquet_base, msisdn_set)
    
    print(f"\n{'=' * 60}")
    print(f"RESULTS")
    print(f"{'=' * 60}")
    print(f"  Total MSISDNs:        {len(msisdn_set)}")
    print(f"  TMUSERIDs found:      {len(mapping)}")
    print(f"  Missing TMUSERIDs:    {len(msisdn_set) - len(mapping)}")
    
    missing = msisdn_set - set(mapping.keys())
    if missing:
        print(f"\n  ⚠️  MSISDNs without TMUSERID:")
        for msisdn in sorted(missing)[:10]:
            print(f"     {msisdn}")
        if len(missing) > 10:
            print(f"     ... and {len(missing) - 10} more")
    
    result_df = pl.DataFrame({
        'msisdn': list(msisdn_set),
        'tmuserid': [mapping.get(m, '') for m in msisdn_set]
    })
    
    backup_path = csv_path.with_suffix('.csv.bak')
    print(f"\n  Creating backup: {backup_path.name}")
    csv_path.rename(backup_path)
    
    print(f"  Writing enriched CSV: {csv_path.name}")
    result_df.write_csv(csv_path)
    
    print(f"\n✓ Done! {csv_path.name} now has 2 columns: msisdn, tmuserid")
    print(f"  Original file backed up to: {backup_path.name}")


if __name__ == "__main__":
    main()
