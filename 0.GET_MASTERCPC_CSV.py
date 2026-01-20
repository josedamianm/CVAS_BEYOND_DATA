import pandas as pd


def process_cpc_file(input_excel_path, output_csv_path):
    # Reading MASTERCPC.xlsx
    df = pd.read_excel(input_excel_path, sheet_name=0)
    df['CPC to upgrade'] = pd.to_numeric(df['CPC to upgrade'], errors='coerce').astype('Int64')
    df['CPC'] = pd.to_numeric(df['CPC'], errors='coerce').astype('Int64')
    filtered_df = df[['TME Category', 'Service Name', 'CPC', 'CPC to upgrade', 'Subscription Model', 'Price']]

    # CHARGED rows
    charged = filtered_df[filtered_df['Price'] > 0].copy()
    charged['CHG_Period'] = charged['Subscription Model'].str.split().str[0]
    charged = charged.rename(columns={
        'CPC': 'Upgrade_CPC',
        'Price': 'CHG_Price'
    })

    # FREE rows
    free = filtered_df[filtered_df['Price'] == 0].copy()
    free['Free_Period'] = free['Subscription Model'].str.split().str[0]
    free = free.rename(columns={
        'CPC': 'Free_CPC',
        'CPC to upgrade': 'Upgrade_CPC'
    })

    # Merge free with charged
    merged = pd.merge(
        free[['Free_CPC', 'Upgrade_CPC', 'Free_Period', 'Service Name', 'TME Category']],
        charged[['Upgrade_CPC', 'CHG_Price', 'CHG_Period', 'Service Name', 'TME Category']],
        on='Upgrade_CPC',
        suffixes=('_free', '_charged'),
        how='right'  # RIGHT JOIN to keep all charged rows even if no matching free one
    )

    # Use available charged info
    merged['Service Name'] = merged['Service Name_charged'].combine_first(merged['Service Name_free'])
    merged['TME Category'] = merged['TME Category_charged'].combine_first(merged['TME Category_free'])

    # Fill in missing Free_Period with 0 and Free_CPC with None/NaN
    merged['Free_Period'] = merged['Free_Period'].fillna(0)
    merged['Free_CPC'] = merged['Free_CPC'].fillna(pd.NA)

    # Final clean-up
    result = merged[[
        'Service Name', 'TME Category', 'Free_CPC', 'Free_Period',
        'Upgrade_CPC', 'CHG_Period', 'CHG_Price'
    ]]

    result = result.copy()
    result['CPCs'] = result.apply(
        lambda row: set(filter(pd.notna, [row['Free_CPC'], row['Upgrade_CPC']])),
        axis=1
    )

    # Extended period-to-days mapping
    period_to_days = {
        'Monthly': 30,
        'Weekly': 7,
        'Trimonthly': 90,
        'Bimontly': 60,
        'One': 1,
        '3': 3,
        '45': 45,
        0: 0,           # numeric 0
        '0': 0,         # string '0'
        'Other': 0,
        'PPD': 0,
        'Sesion': 0,
        pd.NA: 0,
        None: 0,
    }

    # Apply the mapping and convert to integer
    result = result.copy()
    result['Free_Period'] = result['Free_Period'].map(period_to_days).fillna(0).astype(int)
    result['CHG_Period'] = result['CHG_Period'].map(period_to_days).fillna(0).astype(int)

    # Saving output CSV
    result.to_csv(output_csv_path, index=False, encoding='utf-8')


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python 1_Get_MASTERCPC_CSV.py <input_excel_path> <output_csv_path>")
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        process_cpc_file(input_path, output_path)