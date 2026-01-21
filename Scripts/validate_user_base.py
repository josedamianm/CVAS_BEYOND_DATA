#!/usr/bin/env python3
import pandas as pd
import sys

MASTERCPC_FILE = "MASTERCPC.csv"
SERVICE_FILE = "User_Base/user_base_by_service.csv"
CPC_FILE = "User_Base/user_base_by_cpc.csv"

def validate_service(service_name, sample_date=None):
    print(f"{'='*60}")
    print(f"VALIDATING USER BASE FOR: {service_name}")
    print(f"{'='*60}\n")
    
    master_df = pd.read_csv(MASTERCPC_FILE, skipinitialspace=True)
    service_cpcs = master_df[master_df['service_name'].str.strip() == service_name]['cpc'].tolist()
    
    print(f"Found {len(service_cpcs)} CPCs for '{service_name}':")
    print(f"  CPCs: {service_cpcs}\n")
    
    service_df = pd.read_csv(SERVICE_FILE, sep='|')
    service_df = service_df[service_df['service_name'] == service_name]
    
    cpc_df = pd.read_csv(CPC_FILE, sep='|')
    cpc_df = cpc_df[cpc_df['cpc'].isin(service_cpcs)]
    
    if sample_date:
        service_df = service_df[service_df['date'] == sample_date]
        cpc_df = cpc_df[cpc_df['date'] == sample_date]
        dates_to_check = [sample_date]
    else:
        dates_to_check = service_df['date'].unique()[:10]
    
    print(f"Validating {len(dates_to_check)} dates...\n")
    
    mismatches = []
    matches = 0
    
    for date in dates_to_check:
        service_count = service_df[service_df['date'] == date]['User_Base'].sum()
        cpc_count = cpc_df[cpc_df['date'] == date]['User_Base'].sum()
        
        if service_count != cpc_count:
            mismatches.append({
                'date': date,
                'service_total': service_count,
                'cpc_sum': cpc_count,
                'difference': service_count - cpc_count
            })
        else:
            matches += 1
    
    print(f"{'='*60}")
    print(f"VALIDATION RESULTS")
    print(f"{'='*60}")
    print(f"Total dates checked: {len(dates_to_check)}")
    print(f"Matches: {matches}")
    print(f"Mismatches: {len(mismatches)}\n")
    
    if mismatches:
        print("MISMATCHES FOUND:")
        print(f"{'Date':<12} {'Service Total':>15} {'CPC Sum':>15} {'Difference':>15}")
        print(f"{'-'*60}")
        for m in mismatches[:20]:
            print(f"{m['date']:<12} {m['service_total']:>15} {m['cpc_sum']:>15} {m['difference']:>15}")
        if len(mismatches) > 20:
            print(f"\n... and {len(mismatches) - 20} more mismatches")
        return False
    else:
        print(f"✓ ALL DATES MATCH! Service totals equal sum of CPC user bases.")
        
        sample = service_df.head(3)
        print(f"\nSample validation for first 3 dates:")
        for _, row in sample.iterrows():
            date = row['date']
            service_total = row['User_Base']
            cpc_sum = cpc_df[cpc_df['date'] == date]['User_Base'].sum()
            print(f"  {date}: Service={service_total}, CPC Sum={cpc_sum} ✓")
        
        return True

if __name__ == "__main__":
    service = sys.argv[1] if len(sys.argv) > 1 else "Movistar Musica"
    sample_date = sys.argv[2] if len(sys.argv) > 2 else None
    
    success = validate_service(service, sample_date)
    sys.exit(0 if success else 1)
