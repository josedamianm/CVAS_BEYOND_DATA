import polars as pl
from pathlib import Path
from datetime import datetime
import os
import re
import tempfile


def load_excluded_users(path: Path) -> tuple[set[str], set[str]]:
    """
    Load MSISDNs and TMUSERIDs to exclude from counters.

    Args:
        path: Path to Users_No_Limits.csv
              Format: CSV with columns 'msisdn' and optionally 'tmuserid'
              Legacy format (no header, single column) is also supported

    Returns:
        Tuple of (excluded_msisdns, excluded_tmuserids)
    """
    if not path.exists():
        return set(), set()

    try:
        df = pl.read_csv(path, schema_overrides={'msisdn': pl.Utf8, 'tmuserid': pl.Utf8})

        if 'msisdn' not in df.columns:
            df = pl.read_csv(path, has_header=False, new_columns=['msisdn'])

        msisdns = df['msisdn'].to_list()
        excluded_msisdns = set(m for m in msisdns if m is not None and m != '')

        excluded_tmuserids = set()
        if 'tmuserid' in df.columns:
            tmuserids = df['tmuserid'].to_list()
            excluded_tmuserids = set(t for t in tmuserids if t is not None and t != '')

        return excluded_msisdns, excluded_tmuserids
    except Exception as e:
        print(f"Warning: Error loading {path}: {e}")
        return set(), set()


def load_transactions_for_date(
    parquet_base: Path,
    target_date: str,
    tx_type: str,
    excluded_msisdns: set[str] | None = None,
    excluded_tmuserids: set[str] | None = None
) -> pl.DataFrame:
    """
    Load transactions for a specific date and type.

    Args:
        parquet_base: Base path to Parquet_Data/transactions
        target_date: Date string YYYY-MM-DD
        tx_type: Transaction type (act, reno, dct, cnr, ppd, rfnd)
        excluded_msisdns: Optional set of MSISDNs to exclude from results
        excluded_tmuserids: Optional set of TMUSERIDs to exclude from results

    Returns:
        DataFrame with transactions for that date
    """
    year_month = target_date[:7]
    tx_path = parquet_base / tx_type / f"year_month={year_month}"

    if not tx_path.exists():
        return pl.DataFrame()

    date_col_map = {
        'act': 'trans_date',
        'reno': 'trans_date',
        'dct': 'trans_date',
        'cnr': 'cancel_date',
        'ppd': 'trans_date',
        'rfnd': 'refnd_date'
    }

    date_col = date_col_map[tx_type]

    try:
        df = pl.scan_parquet(str(tx_path / "*.parquet")).collect()

        if df.is_empty() or date_col not in df.columns:
            return pl.DataFrame()

        df = df.filter(
            pl.col(date_col).dt.date().cast(pl.Utf8) == target_date
        )

        if excluded_msisdns and 'msisdn' in df.columns:
            df = df.filter(~pl.col('msisdn').cast(pl.Utf8).is_in(excluded_msisdns))

        if excluded_tmuserids and 'tmuserid' in df.columns:
            df = df.filter(~pl.col('tmuserid').cast(pl.Utf8).is_in(excluded_tmuserids))

        cols_to_select = ['cpc', date_col]
        if 'rev' in df.columns:
            cols_to_select.append('rev')
        if 'rfnd_amount' in df.columns:
            cols_to_select.append('rfnd_amount')
        if 'rfnd_cnt' in df.columns:
            cols_to_select.append('rfnd_cnt')
        if 'channel_act' in df.columns:
            cols_to_select.append('channel_act')
        if 'channel_dct' in df.columns:
            cols_to_select.append('channel_dct')

        return df.select(cols_to_select)
    except Exception:
        return pl.DataFrame()


def discover_all_transaction_dates(parquet_base: Path) -> list[str]:
    """
    Scan all transaction parquet files and extract unique dates.

    Returns:
        Sorted list of date strings (YYYY-MM-DD)
    """
    all_dates = set()
    tx_types = ['act', 'reno', 'dct', 'cnr', 'ppd', 'rfnd']

    date_col_map = {
        'act': 'trans_date',
        'reno': 'trans_date',
        'dct': 'trans_date',
        'cnr': 'cancel_date',
        'ppd': 'trans_date',
        'rfnd': 'refnd_date'
    }

    for tx_type in tx_types:
        tx_path = parquet_base / tx_type
        if not tx_path.exists():
            continue

        date_col = date_col_map[tx_type]

        for partition_dir in tx_path.glob('year_month=*'):
            parquet_files = list(partition_dir.glob('*.parquet'))
            if not parquet_files:
                continue

            try:
                df = pl.scan_parquet(str(partition_dir / "*.parquet")).select(date_col).collect()

                if df.is_empty() or date_col not in df.columns:
                    continue

                dates = df[date_col].dt.date().cast(pl.Utf8).unique().to_list()
                all_dates.update([d for d in dates if d is not None])
            except Exception:
                continue

    return sorted([d for d in all_dates if d is not None])


def get_missing_dates(parquet_base: Path, counters_path: Path) -> list[str]:
    """
    Find dates that exist in transactions but not in counters.

    Returns:
        Sorted list of missing date strings (YYYY-MM-DD)
    """
    tx_dates = set(discover_all_transaction_dates(parquet_base))

    if not counters_path.exists():
        return sorted(tx_dates)

    try:
        counters = pl.read_parquet(counters_path)
        if counters.is_empty():
            return sorted(tx_dates)

        counter_dates = set(counters['date'].dt.date().cast(pl.Utf8).unique().to_list())
    except Exception:
        return sorted(tx_dates)

    missing = tx_dates - counter_dates
    return sorted(missing)


def load_mastercpc(path: Path) -> pl.DataFrame:
    """
    Load MASTERCPC.csv with simplified format.

    Returns DataFrame with columns: [cpc, service_name, tme_category, cpc_period, cpc_price]
    """
    df = pl.read_csv(path, null_values=['', 'NULL'])

    df = df.with_columns([
        pl.col('service_name').fill_null('UNKNOWN'),
        pl.col('tme_category').fill_null(''),
        pl.col('cpc_period').fill_null(0),
        pl.col('cpc_price').fill_null(0.0)
    ])

    return df.unique(subset=['cpc'])


def load_counters_cpc(path: Path) -> pl.DataFrame:
    """
    Load existing Counters_CPC.parquet if it exists.
    """
    if not path.exists():
        return pl.DataFrame(schema={
            'date': pl.Date,
            'cpc': pl.Int64,
            'act_count': pl.Int64,
            'reno_count': pl.Int64,
            'dct_count': pl.Int64,
            'cnr_count': pl.Int64,
            'ppd_count': pl.Int64,
            'rfnd_count': pl.Int64,
            'last_updated': pl.Datetime
        })
    
    return pl.read_parquet(path)


def write_atomic_parquet(df: pl.DataFrame, path: Path) -> None:
    """
    Write DataFrame to Parquet atomically (temp + rename).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    
    fd, tmp_path = tempfile.mkstemp(suffix='.parquet', dir=path.parent)
    os.close(fd)
    
    try:
        df.write_parquet(tmp_path, compression='snappy')
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def write_atomic_csv(df: pl.DataFrame, path: Path) -> None:
    """
    Write DataFrame to CSV atomically (temp + rename).
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(suffix='.csv', dir=path.parent)
    os.close(fd)

    try:
        df.write_csv(tmp_path, quote_style='never')
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def validate_counters_schema(df: pl.DataFrame, expected_cols: list) -> bool:
    """
    Validate that DataFrame has expected columns.
    """
    return all(col in df.columns for col in expected_cols)
