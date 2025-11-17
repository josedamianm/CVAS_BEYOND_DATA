import polars as pl
from pathlib import Path

for file_type in ['act', 'reno', 'dct', 'cnr', 'rfnd', 'ppd']:
    path = Path(f'Parquet_Data/transactions/{file_type}')
    if list(path.rglob('*.parquet')):
        df = pl.scan_parquet(str(path / '**/*.parquet')).collect()
        print(f'{file_type.upper()}: {len(df):,} rows')
