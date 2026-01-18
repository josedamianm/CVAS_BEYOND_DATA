#!/usr/bin/env python3
"""
Build Transaction Counters

Generates two outputs:
1. Counters_CPC.parquet - Historical counters by CPC and date
2. Counters_Service.csv - Aggregated counters by Service Name

Usage:
    # Daily run (processes yesterday only)
    python 05_build_counters.py

    # Initial backfill (processes ALL missing dates)
    python 05_build_counters.py --backfill

    # Process specific date
    python 05_build_counters.py YYYY-MM-DD

    # Process date range
    python 05_build_counters.py --start-date 2024-01-01 --end-date 2024-01-31

    # Force recompute existing dates
    python 05_build_counters.py YYYY-MM-DD --force
"""

import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import sys
import argparse
import platform

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.counter_utils import (
    load_transactions_for_date,
    load_mastercpc,
    load_counters_cpc,
    write_atomic_parquet,
    write_atomic_csv,
    discover_all_transaction_dates,
    get_missing_dates,
)

TX_TYPES = ['act', 'reno', 'dct', 'cnr', 'ppd', 'rfnd']


def compute_daily_cpc_counts(parquet_base: Path, target_date: str) -> pl.DataFrame:
    """
    Compute transaction counts, revenue, and refund amounts by CPC for a single date.
    """
    counts_dict = {'date': [], 'cpc': []}
    for tx in TX_TYPES:
        counts_dict[f'{tx}_count'] = []

    all_cpcs = set()
    tx_counts = {}
    tx_revenue = {}
    tx_rfnd_amounts = {}
    tx_act_free = {}
    tx_act_pay = {}
    tx_upg = {}
    tx_upg_dct = {}

    for tx_type in TX_TYPES:
        df = load_transactions_for_date(parquet_base, target_date, tx_type)
        if df.is_empty():
            tx_counts[tx_type] = {}
            tx_revenue[tx_type] = {}
            tx_rfnd_amounts[tx_type] = {}
            tx_act_free[tx_type] = {}
            tx_act_pay[tx_type] = {}
            tx_upg[tx_type] = {}
            tx_upg_dct[tx_type] = {}
            continue

        if tx_type == 'act' and 'channel_act' in df.columns:
            non_upgrade_df = df.filter(pl.col('channel_act') != 'UPGRADE')
            counts = non_upgrade_df.group_by('cpc').agg(pl.len().alias('count'))
            tx_counts[tx_type] = {row['cpc']: row['count'] for row in counts.iter_rows(named=True)}

            upg_counts = df.filter(pl.col('channel_act') == 'UPGRADE').group_by('cpc').agg(pl.len().alias('count'))
            tx_upg[tx_type] = {row['cpc']: row['count'] for row in upg_counts.iter_rows(named=True)}
        elif tx_type == 'dct' and 'channel_dct' in df.columns:
            non_upgrade_df = df.filter(pl.col('channel_dct') != 'UPGRADE')
            counts = non_upgrade_df.group_by('cpc').agg(pl.len().alias('count'))
            tx_counts[tx_type] = {row['cpc']: row['count'] for row in counts.iter_rows(named=True)}

            upg_dct_counts = df.filter(pl.col('channel_dct') == 'UPGRADE').group_by('cpc').agg(pl.len().alias('count'))
            tx_upg_dct[tx_type] = {row['cpc']: row['count'] for row in upg_dct_counts.iter_rows(named=True)}
            tx_upg[tx_type] = {}
        elif tx_type == 'rfnd' and 'rfnd_cnt' in df.columns:
            counts = df.group_by('cpc').agg(pl.col('rfnd_cnt').sum().alias('count'))
            tx_counts[tx_type] = {row['cpc']: row['count'] for row in counts.iter_rows(named=True)}
            tx_upg[tx_type] = {}
        else:
            counts = df.group_by('cpc').agg(pl.len().alias('count'))
            tx_counts[tx_type] = {row['cpc']: row['count'] for row in counts.iter_rows(named=True)}
            tx_upg[tx_type] = {}

        if tx_type == 'act' and 'rev' in df.columns:
            if 'channel_act' in df.columns:
                non_upgrade_df = df.filter(pl.col('channel_act') != 'UPGRADE')
            else:
                non_upgrade_df = df

            free_counts = non_upgrade_df.filter(pl.col('rev') == 0).group_by('cpc').agg(pl.len().alias('count'))
            tx_act_free[tx_type] = {row['cpc']: row['count'] for row in free_counts.iter_rows(named=True)}

            pay_counts = non_upgrade_df.filter(pl.col('rev') > 0).group_by('cpc').agg(pl.len().alias('count'))
            tx_act_pay[tx_type] = {row['cpc']: row['count'] for row in pay_counts.iter_rows(named=True)}
        else:
            tx_act_free[tx_type] = {}
            tx_act_pay[tx_type] = {}

        if tx_type == 'dct' and 'channel_dct' in df.columns and tx_type not in tx_upg_dct:
            upg_dct_counts = df.filter(pl.col('channel_dct') == 'UPGRADE').group_by('cpc').agg(pl.len().alias('count'))
            tx_upg_dct[tx_type] = {row['cpc']: row['count'] for row in upg_dct_counts.iter_rows(named=True)}
        elif tx_type != 'dct':
            tx_upg_dct[tx_type] = {}

        if 'rev' in df.columns:
            revenue = df.group_by('cpc').agg(pl.col('rev').sum().alias('revenue'))
            tx_revenue[tx_type] = {row['cpc']: row['revenue'] for row in revenue.iter_rows(named=True)}
        else:
            tx_revenue[tx_type] = {}

        if 'rfnd_amount' in df.columns:
            rfnd_amt = df.group_by('cpc').agg(pl.col('rfnd_amount').sum().alias('rfnd_amount'))
            tx_rfnd_amounts[tx_type] = {row['cpc']: row['rfnd_amount'] for row in rfnd_amt.iter_rows(named=True)}
        else:
            tx_rfnd_amounts[tx_type] = {}

        all_cpcs.update(tx_counts[tx_type].keys())
        if tx_type == 'act':
            all_cpcs.update(tx_upg[tx_type].keys())

    if not all_cpcs:
        return pl.DataFrame(schema={
            'date': pl.Date,
            'cpc': pl.Int64,
            'act_count': pl.Int64,
            'act_free': pl.Int64,
            'act_pay': pl.Int64,
            'upg_count': pl.Int64,
            'reno_count': pl.Int64,
            'dct_count': pl.Int64,
            'upg_dct_count': pl.Int64,
            'cnr_count': pl.Int64,
            'ppd_count': pl.Int64,
            'rfnd_count': pl.Int64,
            'rfnd_amount': pl.Float64,
            'rev': pl.Float64,
        })

    rows = []
    for cpc in sorted(c for c in all_cpcs if c is not None):
        row = {
            'date': datetime.strptime(target_date, '%Y-%m-%d').date(),
            'cpc': cpc,
        }
        for tx_type in TX_TYPES:
            row[f'{tx_type}_count'] = tx_counts[tx_type].get(cpc, 0)

        row['act_free'] = tx_act_free.get('act', {}).get(cpc, 0)
        row['act_pay'] = tx_act_pay.get('act', {}).get(cpc, 0)
        row['upg_count'] = tx_upg.get('act', {}).get(cpc, 0)
        row['upg_dct_count'] = tx_upg_dct.get('dct', {}).get(cpc, 0)

        total_rfnd = sum(tx_rfnd_amounts[tx_type].get(cpc, 0.0) for tx_type in TX_TYPES)
        row['rfnd_amount'] = round(total_rfnd, 2)

        total_rev = sum(tx_revenue[tx_type].get(cpc, 0.0) for tx_type in TX_TYPES)
        row['rev'] = round(total_rev, 2)

        rows.append(row)

    return pl.DataFrame(rows)


def merge_counters(existing: pl.DataFrame, new: pl.DataFrame, target_date: str) -> pl.DataFrame:
    """
    Merge new daily counts into existing historical counters.
    Replaces data for target_date (idempotent).
    """
    if existing.is_empty():
        return new.with_columns(pl.lit(datetime.now()).alias('last_updated'))

    date_val = datetime.strptime(target_date, '%Y-%m-%d').date()
    filtered = existing.filter(pl.col('date') != date_val)

    if new.is_empty():
        return filtered

    if 'last_updated' not in filtered.columns:
        filtered = filtered.with_columns(pl.lit(datetime.now()).alias('last_updated'))

    if 'rev' not in filtered.columns:
        filtered = filtered.with_columns(pl.lit(0.0).alias('rev'))

    if 'rfnd_amount' not in filtered.columns:
        filtered = filtered.with_columns(pl.lit(0.0).alias('rfnd_amount'))

    if 'act_free' not in filtered.columns:
        filtered = filtered.with_columns(pl.lit(0).cast(pl.Int64).alias('act_free'))

    if 'act_pay' not in filtered.columns:
        filtered = filtered.with_columns(pl.lit(0).cast(pl.Int64).alias('act_pay'))

    if 'upg_count' not in filtered.columns:
        filtered = filtered.with_columns(pl.lit(0).cast(pl.Int64).alias('upg_count'))

    if 'upg_dct_count' not in filtered.columns:
        filtered = filtered.with_columns(pl.lit(0).cast(pl.Int64).alias('upg_dct_count'))

    filtered = filtered.with_columns([
        pl.col('rev').round(2),
        pl.col('rfnd_amount').round(2)
    ])

    new_with_ts = new.with_columns(pl.lit(datetime.now()).alias('last_updated'))

    expected_cols = ['date', 'cpc', 'act_count', 'act_free', 'act_pay', 'upg_count', 'reno_count', 'dct_count', 'upg_dct_count', 'cnr_count', 'ppd_count', 'rfnd_count', 'rfnd_amount', 'rev', 'last_updated']
    filtered = filtered.select(expected_cols)
    new_with_ts = new_with_ts.select(expected_cols)

    return pl.concat([filtered, new_with_ts]).sort(['date', 'cpc'])


def aggregate_by_service(
    counters: pl.DataFrame,
    cpc_map: pl.DataFrame
) -> tuple[pl.DataFrame, list[int]]:
    """
    Aggregate CPC counters by Service Name.
    Excludes services containing 'nubico' (case-insensitive).

    Returns:
        - Aggregated DataFrame
        - List of unmapped CPCs
    """
    if counters.is_empty():
        return pl.DataFrame(schema={
            'date': pl.Date,
            'service_name': pl.Utf8,
            'tme_category': pl.Utf8,
            'cpcs': pl.Utf8,
            'Free_CPC': pl.Int64,
            'Free_Period': pl.Int64,
            'Upgrade_CPC': pl.Int64,
            'CHG_Period': pl.Int64,
            'CHG_Price': pl.Float64,
            'act_count': pl.Int64,
            'act_free': pl.Int64,
            'act_pay': pl.Int64,
            'upg_count': pl.Int64,
            'reno_count': pl.Int64,
            'dct_count': pl.Int64,
            'upg_dct_count': pl.Int64,
            'cnr_count': pl.Int64,
            'ppd_count': pl.Int64,
            'rfnd_count': pl.Int64,
            'rfnd_amount': pl.Float64,
            'rev': pl.Float64,
        }), []

    all_cpcs = set(counters['cpc'].unique().to_list())
    mapped_cpcs = set(cpc_map['cpc'].unique().to_list())
    unmapped_cpcs = sorted(all_cpcs - mapped_cpcs)

    unknown_mapping = pl.DataFrame({
        'cpc': unmapped_cpcs,
        'service_name': ['UNKNOWN'] * len(unmapped_cpcs),
        'tme_category': [''] * len(unmapped_cpcs),
        'Free_CPC': [0] * len(unmapped_cpcs),
        'Free_Period': [0] * len(unmapped_cpcs),
        'Upgrade_CPC': [0] * len(unmapped_cpcs),
        'CHG_Period': [0] * len(unmapped_cpcs),
        'CHG_Price': [0.0] * len(unmapped_cpcs)
    }) if unmapped_cpcs else pl.DataFrame(schema={
        'cpc': pl.Int64,
        'service_name': pl.Utf8,
        'tme_category': pl.Utf8,
        'Free_CPC': pl.Int64,
        'Free_Period': pl.Int64,
        'Upgrade_CPC': pl.Int64,
        'CHG_Period': pl.Int64,
        'CHG_Price': pl.Float64
    })

    full_map = pl.concat([cpc_map, unknown_mapping])

    joined = counters.join(full_map, on='cpc', how='left')

    joined = joined.with_columns([
        pl.col('service_name').fill_null('UNKNOWN'),
        pl.col('tme_category').fill_null(''),
        pl.col('Free_CPC').fill_null(0),
        pl.col('Free_Period').fill_null(0),
        pl.col('Upgrade_CPC').fill_null(0),
        pl.col('CHG_Period').fill_null(0),
        pl.col('CHG_Price').fill_null(0.0)
    ])

    joined = joined.filter(
        ~pl.col('service_name').str.to_lowercase().str.contains('nubico') &
        ~pl.col('service_name').str.to_lowercase().str.contains('movistar apple music')
    )

    aggregated = joined.group_by(['date', 'service_name', 'tme_category']).agg([
        pl.col('cpc').unique().sort().alias('cpcs'),
        pl.col('act_count').sum(),
        pl.col('act_free').sum(),
        pl.col('act_pay').sum(),
        pl.col('upg_count').sum(),
        pl.col('reno_count').sum(),
        pl.col('dct_count').sum(),
        pl.col('upg_dct_count').sum(),
        pl.col('cnr_count').sum(),
        pl.col('ppd_count').sum(),
        pl.col('rfnd_count').sum(),
        pl.col('rfnd_amount').sum().round(2),
        pl.col('rev').sum().round(2),
        pl.col('Free_CPC').first(),
        pl.col('Free_Period').first(),
        pl.col('Upgrade_CPC').first(),
        pl.col('CHG_Period').first(),
        pl.col('CHG_Price').first()
    ]).sort(['date', 'service_name'])

    aggregated = aggregated.with_columns([
        pl.col('cpcs').cast(pl.List(pl.Utf8)).list.join(', ')
    ])

    aggregated = aggregated.select([
        'date',
        'service_name',
        'tme_category',
        'cpcs',
        'Free_CPC',
        'Free_Period',
        'Upgrade_CPC',
        'CHG_Period',
        'CHG_Price',
        'act_count',
        'act_free',
        'act_pay',
        'upg_count',
        'reno_count',
        'dct_count',
        'upg_dct_count',
        'cnr_count',
        'ppd_count',
        'rfnd_count',
        'rfnd_amount',
        'rev'
    ])

    return aggregated, unmapped_cpcs


def process_date(
    target_date: str,
    project_root: Path,
    force: bool = False
) -> dict:
    """
    Process counters for a single date.
    
    Returns dict with processing stats.
    """
    parquet_base = project_root / 'Parquet_Data' / 'transactions'
    counters_dir = project_root / 'Counters'
    counters_cpc_path = counters_dir / 'Counters_CPC.parquet'
    counters_service_path = counters_dir / 'Counters_Service.csv'
    mastercpc_path = project_root / 'MASTERCPC.csv'
    
    stats = {
        'date': target_date,
        'cpcs_processed': 0,
        'unmapped_cpcs': [],
        'tx_counts': {}
    }
    
    print(f"  Loading historical counters...", end=' ')
    existing = load_counters_cpc(counters_cpc_path)
    print(f"✓ {len(existing):,} rows, {existing['date'].n_unique() if not existing.is_empty() else 0} dates")
    
    date_val = datetime.strptime(target_date, '%Y-%m-%d').date()
    if not force and not existing.is_empty():
        if date_val in existing['date'].unique().to_list():
            print(f"  ⚠️  Date {target_date} already processed. Use --force to recompute.")
            return stats
    
    print(f"  Computing daily counts for {target_date}...")
    daily_counts = compute_daily_cpc_counts(parquet_base, target_date)
    
    if daily_counts.is_empty():
        print(f"  ⚠️  No transactions found for {target_date}")
        return stats
    
    stats['cpcs_processed'] = len(daily_counts)
    for tx in TX_TYPES:
        col = f'{tx}_count'
        stats['tx_counts'][tx] = daily_counts[col].sum() if col in daily_counts.columns else 0
    
    tx_summary = ' '.join([f"{tx}={stats['tx_counts'].get(tx, 0):,}" for tx in TX_TYPES])
    print(f"    Transactions: {tx_summary}")
    print(f"    CPCs: {stats['cpcs_processed']:,}")
    
    print(f"  Merging counters...", end=' ')
    merged = merge_counters(existing, daily_counts, target_date)
    print(f"✓ {merged['date'].n_unique()} dates total")
    
    print(f"  Loading MASTERCPC mapping...", end=' ')
    cpc_map = load_mastercpc(mastercpc_path)
    print(f"✓ {len(cpc_map):,} CPC mappings")
    
    print(f"  Aggregating by service...", end=' ')
    service_counters, unmapped = aggregate_by_service(merged, cpc_map)
    stats['unmapped_cpcs'] = unmapped
    print(f"✓ {service_counters['service_name'].n_unique() if not service_counters.is_empty() else 0} services")
    
    if unmapped:
        print(f"\n  ⚠️  WARNING: {len(unmapped)} unmapped CPCs found:")
        print(f"     {unmapped[:20]}{'...' if len(unmapped) > 20 else ''}")
    
    print(f"  Writing Counters_CPC.parquet...", end=' ')
    write_atomic_parquet(merged, counters_cpc_path)
    file_size = counters_cpc_path.stat().st_size / 1024
    print(f"✓ ({file_size:.1f} KB)")
    
    print(f"  Writing Counters_Service.csv...", end=' ')
    write_atomic_csv(service_counters, counters_service_path)
    file_size = counters_service_path.stat().st_size / 1024
    print(f"✓ ({file_size:.1f} KB)")
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Build Transaction Counters',
        epilog='Default behavior: Process yesterday only (daily mode). Use --backfill for initial setup.'
    )
    parser.add_argument('date', nargs='?', help='Target date (YYYY-MM-DD)')
    parser.add_argument('--backfill', action='store_true',
                       help='Backfill mode: process ALL missing dates (use for initial setup)')
    parser.add_argument('--start-date', help='Start date for range processing')
    parser.add_argument('--end-date', help='End date for range processing')
    parser.add_argument('--force', action='store_true', help='Force recompute even if date exists')

    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    parquet_base = project_root / 'Parquet_Data' / 'transactions'
    counters_cpc_path = project_root / 'Counters' / 'Counters_CPC.parquet'

    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, '%Y-%m-%d')
        end = datetime.strptime(args.end_date, '%Y-%m-%d')
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        mode = "date_range"
    elif args.date:
        dates = [args.date]
        mode = "single_date"
    elif args.backfill:
        print("=" * 60)
        print("BACKFILL MODE: PROCESSING ALL MISSING DATES")
        print("=" * 60)
        print(f"Scanning transaction data...")

        all_tx_dates = discover_all_transaction_dates(parquet_base)
        print(f"  Found {len(all_tx_dates)} unique dates in transactions")

        if all_tx_dates:
            print(f"  Date range: {all_tx_dates[0]} to {all_tx_dates[-1]}")

        if args.force:
            print(f"\n⚠️  FORCE MODE: Reprocessing ALL {len(all_tx_dates)} dates")
            dates = all_tx_dates
        else:
            missing_dates = get_missing_dates(parquet_base, counters_cpc_path)

            if not missing_dates:
                print("\n✓ All transaction dates already processed in counters!")
                print("  Use --force to recompute existing dates.")
                return

            print(f"\n  Missing dates in counters: {len(missing_dates)}")
            if len(missing_dates) <= 10:
                print(f"  Dates: {missing_dates}")
            else:
                print(f"  First 5: {missing_dates[:5]}")
                print(f"  Last 5: {missing_dates[-5:]}")

            dates = missing_dates
        mode = "backfill"
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        dates = [yesterday]
        mode = "daily"

        print("=" * 60)
        print("DAILY MODE: PROCESSING YESTERDAY")
        print("=" * 60)
        print(f"Target date: {yesterday}")
        print("(Use --backfill for initial setup to process all missing dates)")

    print("\n" + "=" * 60)
    print("TRANSACTION COUNTERS BUILD")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {mode}")
    print(f"Dates to process: {len(dates)}")
    print(f"Force recompute: {args.force}")
    print()

    all_unmapped = set()
    total_cpcs = 0

    for i, date in enumerate(dates, 1):
        print(f"\n[{i}/{len(dates)}] Processing: {date}")
        print("-" * 60)

        try:
            stats = process_date(date, project_root, args.force)
            total_cpcs += stats['cpcs_processed']
            all_unmapped.update(stats.get('unmapped_cpcs', []))
        except Exception as e:
            print(f"  ✗ ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            continue

    print("\n" + "=" * 60)
    print("BUILD COMPLETE")
    print("=" * 60)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Dates processed: {len(dates)}")
    print(f"Total CPCs: {total_cpcs:,}")

    if all_unmapped:
        print(f"\n⚠️  ALERT: {len(all_unmapped)} unique unmapped CPCs across all dates:")
        print(f"   Consider updating MASTERCPC.csv with: {sorted(all_unmapped)[:10]}...")


if __name__ == "__main__":
    main()

def aggregate_by_service(
    counters: pl.DataFrame,
    cpc_map: pl.DataFrame
) -> tuple[pl.DataFrame, list[int]]:
    """
    Aggregate CPC counters by Service Name.
    Excludes services containing 'nubico' (case-insensitive).

    Returns:
        - Aggregated DataFrame
        - List of unmapped CPCs
    """
    if counters.is_empty():
        return pl.DataFrame(schema={
            'date': pl.Date,
            'service_name': pl.Utf8,
            'tme_category': pl.Utf8,
            'cpcs': pl.Utf8,
            'Free_CPC': pl.Int64,
            'Free_Period': pl.Int64,
            'Upgrade_CPC': pl.Int64,
            'CHG_Period': pl.Int64,
            'CHG_Price': pl.Float64,
            'act_count': pl.Int64,
            'act_free': pl.Int64,
            'act_pay': pl.Int64,
            'upg_count': pl.Int64,
            'reno_count': pl.Int64,
            'dct_count': pl.Int64,
            'upg_dct_count': pl.Int64,
            'cnr_count': pl.Int64,
            'ppd_count': pl.Int64,
            'rfnd_count': pl.Int64,
            'rfnd_amount': pl.Float64,
            'rev': pl.Float64,
        }), []

    all_cpcs = set(counters['cpc'].unique().to_list())
    mapped_cpcs = set(cpc_map['cpc'].unique().to_list())
    unmapped_cpcs = sorted(all_cpcs - mapped_cpcs)

    unknown_mapping = pl.DataFrame({
        'cpc': unmapped_cpcs,
        'service_name': ['UNKNOWN'] * len(unmapped_cpcs),
        'tme_category': [''] * len(unmapped_cpcs),
        'Free_CPC': [0] * len(unmapped_cpcs),
        'Free_Period': [0] * len(unmapped_cpcs),
        'Upgrade_CPC': [0] * len(unmapped_cpcs),
        'CHG_Period': [0] * len(unmapped_cpcs),
        'CHG_Price': [0.0] * len(unmapped_cpcs)
    }) if unmapped_cpcs else pl.DataFrame(schema={
        'cpc': pl.Int64,
        'service_name': pl.Utf8,
        'tme_category': pl.Utf8,
        'Free_CPC': pl.Int64,
        'Free_Period': pl.Int64,
        'Upgrade_CPC': pl.Int64,
        'CHG_Period': pl.Int64,
        'CHG_Price': pl.Float64
    })

    full_map = pl.concat([cpc_map, unknown_mapping])

    joined = counters.join(full_map, on='cpc', how='left')

    joined = joined.with_columns([
        pl.col('service_name').fill_null('UNKNOWN'),
        pl.col('tme_category').fill_null(''),
        pl.col('Free_CPC').fill_null(0),
        pl.col('Free_Period').fill_null(0),
        pl.col('Upgrade_CPC').fill_null(0),
        pl.col('CHG_Period').fill_null(0),
        pl.col('CHG_Price').fill_null(0.0)
    ])

    joined = joined.filter(
        ~pl.col('service_name').str.to_lowercase().str.contains('nubico') &
        ~pl.col('service_name').str.to_lowercase().str.contains('movistar apple music')
    )

    aggregated = joined.group_by(['date', 'service_name', 'tme_category']).agg([
        pl.col('cpc').unique().sort().alias('cpcs'),
        pl.col('act_count').sum(),
        pl.col('act_free').sum(),
        pl.col('act_pay').sum(),
        pl.col('upg_count').sum(),
        pl.col('reno_count').sum(),
        pl.col('dct_count').sum(),
        pl.col('upg_dct_count').sum(),
        pl.col('cnr_count').sum(),
        pl.col('ppd_count').sum(),
        pl.col('rfnd_count').sum(),
        pl.col('rfnd_amount').sum().round(2),
        pl.col('rev').sum().round(2),
        pl.col('Free_CPC').first(),
        pl.col('Free_Period').first(),
        pl.col('Upgrade_CPC').first(),
        pl.col('CHG_Period').first(),
        pl.col('CHG_Price').first()
    ]).sort(['date', 'service_name'])

    aggregated = aggregated.with_columns([
        pl.col('cpcs').cast(pl.List(pl.Utf8)).list.join(', ')
    ])

    aggregated = aggregated.select([
        'date',
        'service_name',
        'tme_category',
        'cpcs',
        'Free_CPC',
        'Free_Period',
        'Upgrade_CPC',
        'CHG_Period',
        'CHG_Price',
        'act_count',
        'act_free',
        'act_pay',
        'upg_count',
        'reno_count',
        'dct_count',
        'upg_dct_count',
        'cnr_count',
        'ppd_count',
        'rfnd_count',
        'rfnd_amount',
        'rev'
    ])

    return aggregated, unmapped_cpcs
