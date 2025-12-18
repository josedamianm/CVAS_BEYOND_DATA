#!/usr/bin/env python3

import polars as pl
import sys
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).parent.parent.parent
MASTERCPC_FILE = WORKSPACE_ROOT / "MASTERCPC.csv"
PARQUET_FILE = WORKSPACE_ROOT / "Parquet_Data" / "aggregated" / "subscriptions.parquet"


def load_service_cpcs(service_name):
    if not MASTERCPC_FILE.exists():
        print(f"❌ Error: MASTERCPC.csv not found at: {MASTERCPC_FILE}")
        return None

    df = pl.read_csv(MASTERCPC_FILE)

    service_rows = df.filter(
        pl.col("Service Name").str.to_lowercase() == service_name.lower()
    )

    if service_rows.height == 0:
        print(f"❌ Error: Service '{service_name}' not found in MASTERCPC.csv")
        return None

    all_cpcs = set()

    for cpcs_str in service_rows.select("CPCs").to_series():
        cpcs_str = cpcs_str.strip("{}")
        cpcs = [int(cpc.strip()) for cpc in cpcs_str.split(",") if cpc.strip()]
        all_cpcs.update(cpcs)

    cpc_list = sorted(list(all_cpcs))

    return cpc_list


def calculate_lt_ltv(service_name, activation_month):
    if not PARQUET_FILE.exists():
        print(f"❌ Error: Parquet file not found at: {PARQUET_FILE}")
        return
    
    cpc_list = load_service_cpcs(service_name)
    if cpc_list is None:
        return
    
    print(f"\n📊 Service: {service_name}")
    print(f"   CPC Codes: {cpc_list}")
    print(f"   Activation Month: {activation_month}")
    print("-" * 80)
    
    df = pl.read_parquet(PARQUET_FILE)
    
    filtered_df = df.filter(
        (pl.col("activation_month") == activation_month) &
        (pl.col("cpc_list").list.eval(pl.element().is_in(cpc_list)).list.any())
    )
    
    if filtered_df.height == 0:
        print(f"\n⚠️  No subscriptions found for service '{service_name}' in activation month '{activation_month}'")
        return
    
    avg_lifetime = filtered_df.select(pl.col("lifetime_days").mean()).item()
    avg_ltv = filtered_df.select(pl.col("total_revenue_with_upgrade").mean()).item()
    
    print(f"\n✅ Results:")
    print(f"   Total Subscriptions: {filtered_df.height:,}")
    print(f"   Average Lifetime: {avg_lifetime:.2f} days")
    print(f"   Average LTV: ${avg_ltv:.2f}")
    print()


def main():
    print("\n" + "=" * 80)
    print("LIFETIME & LTV CALCULATOR BY SERVICE AND ACTIVATION MONTH")
    print("=" * 80)
    
    if len(sys.argv) == 3:
        service_name = sys.argv[1]
        activation_month = sys.argv[2]
    else:
        service_name = input("\nEnter Service Name: ").strip()
        activation_month = input("Enter Activation Month (YYYY-MM): ").strip()
    
    if not service_name or not activation_month:
        print("❌ Error: Both service name and activation month are required")
        return
    
    calculate_lt_ltv(service_name, activation_month)


if __name__ == "__main__":
    main()
