import pandas as pd


def process_cpc_file(input_excel_path, output_csv_path):
    df = pd.read_excel(input_excel_path, sheet_name=0)
    df['CPC to upgrade'] = pd.to_numeric(df['CPC to upgrade'], errors='coerce').astype('Int64')
    df['CPC'] = pd.to_numeric(df['CPC'], errors='coerce').astype('Int64')

    filtered_df = df[['TME Category', 'Service Name', 'CPC', 'Subscription Model', 'Price']].copy()

    period_to_days = {
        'Monthly': 30,
        'Weekly': 7,
        'Trimonthly': 90,
        'Bimontly': 60,
        'One': 1,
        '3': 3,
        '45': 45,
        0: 0,
        '0': 0,
        'Other': 0,
        'PPD': 99999,
        'Sesion': 0,
        pd.NA: 0,
        None: 0,
    }

    filtered_df['cpc_period'] = filtered_df['Subscription Model'].str.split().str[0]
    filtered_df['cpc_period'] = filtered_df['cpc_period'].map(period_to_days).fillna(0).astype(int)

    result = filtered_df[['CPC', 'Service Name', 'TME Category', 'cpc_period', 'Price']].copy()
    result = result.rename(columns={
        'CPC': 'cpc',
        'Service Name': 'service_name',
        'TME Category': 'tme_category',
        'Price': 'cpc_price'
    })

    result = result.dropna(subset=['cpc'])
    result = result.drop_duplicates(subset=['cpc'])
    result = result.sort_values('cpc')

    result.to_csv(output_csv_path, index=False, encoding='utf-8')
    print(f"Generated {len(result)} CPC mappings")
    print(f"Output saved to: {output_csv_path}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python 1_Get_MASTERCPC_CSV.py <input_excel_path> <output_csv_path>")
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        process_cpc_file(input_path, output_path)