import polars as pl
import argparse
import os

parser = argparse.ArgumentParser(description='RFND analysis by CPC for given months')
parser.add_argument('--months', nargs='+', default=['2025-10', '2025-11', '2025-12', '2026-01'],
                    help='List of year_month partitions to include (e.g. 2025-10 2025-11)')
parser.add_argument('--output', default='rfnd_analysis.csv',
                    help='Output CSV file path')
args = parser.parse_args()

base = 'Parquet_Data/transactions/rfnd'
dfs = []
for m in args.months:
    path = f'{base}/year_month={m}/*.parquet'
    if not os.path.exists(f'{base}/year_month={m}'):
        print(f'WARNING: partition year_month={m} not found, skipping')
        continue
    dfs.append(pl.scan_parquet(path, hive_partitioning=True))

if not dfs:
    print('ERROR: No valid partitions found.')
    exit(1)

df = pl.concat(dfs).collect()
print(f'Loaded {len(df):,} rows from {len(dfs)} partitions: {args.months}')

mastercpc = pl.read_csv('MASTERCPC.csv').select(['cpc', 'service_name', 'tme_category'])

result = df.group_by(['year_month', 'cpc']).agg([
    pl.col('rfnd_amount').sum().round(2).alias('cpcTotalRfdsAmount'),
    pl.col('rfnd_cnt').sum().alias('cpcTotalRfdsCount'),
    pl.col('tmuserid').n_unique().alias('cpcTotalRfdsCountUU'),
    pl.col('tmuserid').filter(pl.col('instant_rfnd') == 'f').n_unique().alias('RegularRfdsUU'),
    pl.col('tmuserid').filter(pl.col('instant_rfnd') == 't').n_unique().alias('AutomaticRfdsUU'),
]).join(mastercpc, on='cpc', how='left').select([
    'year_month', 'cpc', 'service_name', 'tme_category',
    'cpcTotalRfdsAmount', 'cpcTotalRfdsCount',
    'cpcTotalRfdsCountUU', 'RegularRfdsUU', 'AutomaticRfdsUU'
]).sort(['year_month', 'cpcTotalRfdsCount'], descending=[False, True])

result.write_csv(args.output)
print(f'Saved {len(result)} CPCs to {args.output}')
