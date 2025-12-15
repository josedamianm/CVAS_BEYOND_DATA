#!/usr/bin/env python3
"""
Extract Music Category Subscriptions
Reads the aggregated subscriptions parquet file and filters for subscriptions
that contain any Music CPC in their cpc_list.
"""

import polars as pl
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent.parent
PARQUET_FILE = SCRIPT_DIR / "Parquet_Data" / "aggregated" / "subscriptions.parquet"
MUSIC_CPCS_FILE = SCRIPT_DIR / "Music_CPCs.txt"
OUTPUT_FILE = SCRIPT_DIR / "music_subscriptions.csv"

def load_music_cpcs():
    """Load Music CPCs from text file"""
    print(f"📖 Loading Music CPCs from: {MUSIC_CPCS_FILE}")
    
    with open(MUSIC_CPCS_FILE, 'r') as f:
        music_cpcs = [int(line.strip()) for line in f if line.strip()]
    
    print(f"✓ Loaded {len(music_cpcs)} Music CPCs")
    return music_cpcs

def extract_music_subscriptions():
    """Extract subscriptions that contain any Music CPC in their cpc_list"""
    
    if not PARQUET_FILE.exists():
        print(f"❌ Error: Parquet file not found at: {PARQUET_FILE}")
        return
    
    if not MUSIC_CPCS_FILE.exists():
        print(f"❌ Error: Music CPCs file not found at: {MUSIC_CPCS_FILE}")
        return
    
    print("\n" + "=" * 100)
    print("MUSIC SUBSCRIPTIONS EXTRACTION")
    print("=" * 100)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    music_cpcs = load_music_cpcs()
    music_cpcs_set = set(music_cpcs)
    
    print(f"\n📂 Reading parquet file: {PARQUET_FILE}")
    df = pl.read_parquet(PARQUET_FILE)
    print(f"✓ Loaded {len(df):,} total subscriptions")
    
    print(f"\n🔍 Filtering for Music subscriptions...")
    music_df = df.filter(
        pl.col("cpc_list").list.eval(pl.element().is_in(music_cpcs_set)).list.any()
    )
    
    print(f"✓ Found {len(music_df):,} Music subscriptions ({len(music_df)/len(df)*100:.2f}% of total)")
    
    print(f"\n💾 Saving to: {OUTPUT_FILE}")

    # Convert list columns to string for CSV compatibility
    music_df_csv = music_df.with_columns([
        pl.col("cpc_list").list.eval(pl.element().cast(pl.Utf8)).list.join(",").alias("cpc_list")
    ])

    music_df_csv.write_csv(OUTPUT_FILE)
    print(f"✓ Music subscriptions saved successfully")
    
    print("\n" + "=" * 100)
    print("SUMMARY STATISTICS")
    print("=" * 100)
    
    print(f"\n📊 Music Subscriptions Overview:")
    print(f"  Total subscriptions:        {len(music_df):,}")
    print(f"  Unique users:               {music_df['tmuserid'].n_unique():,}")
    print(f"  Unique MSISDNs:             {music_df['msisdn'].n_unique():,}")
    print(f"  Active subscriptions:       {music_df.filter(pl.col('subscription_status') == 'Active').height:,}")
    print(f"  Deactivated subscriptions:  {music_df.filter(pl.col('deactivation_date').is_not_null()).height:,}")
    print(f"  Cancelled subscriptions:    {music_df.filter(pl.col('cancellation_date').is_not_null()).height:,}")
    
    print(f"\n💰 Revenue Statistics:")
    total_revenue = music_df['total_revenue'].sum()
    avg_revenue = music_df['total_revenue'].mean()
    print(f"  Total revenue:              ${total_revenue:,.2f}")
    print(f"  Average revenue per sub:    ${avg_revenue:,.2f}")
    
    print(f"\n🔄 Renewal Statistics:")
    total_renewals = music_df['renewal_count'].sum()
    avg_renewals = music_df['renewal_count'].mean()
    print(f"  Total renewals:             {total_renewals:,}")
    print(f"  Average renewals per sub:   {avg_renewals:.2f}")
    
    print(f"\n📅 Date Range:")
    first_activation = music_df['activation_date'].min()
    last_activation = music_df['activation_date'].max()
    print(f"  First activation:           {first_activation}")
    print(f"  Last activation:            {last_activation}")
    
    print(f"\n📱 Top 10 Music CPCs by subscription count:")
    cpc_counts = (
        music_df
        .select(pl.col("cpc_list").explode().alias("cpc"))
        .filter(pl.col("cpc").is_in(music_cpcs_set))
        .group_by("cpc")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
        .head(10)
    )
    print(cpc_counts)
    
    print("\n" + "=" * 100)
    print("✅ EXTRACTION COMPLETE")
    print("=" * 100)

if __name__ == "__main__":
    extract_music_subscriptions()
