import polars as pl
from pathlib import Path
from datetime import datetime
import os
import re
import tempfile


def load_transactions_for_date(
    parquet_base: Path,
    target_date: str,
    tx_type: str
) -> pl.DataFrame:
    """
    Load transactions for a specific date and type.
    
    Args:
        parquet_base: Base path to Parquet_Data/transactions
        target_date: Date string YYYY-MM-DD
        tx_type: Transaction type (act, reno, dct, cnr, ppd, rfnd)
    
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

        cols_to_select = ['cpc', date_col]
        if 'rev' in df.columns:
            cols_to_select.append('rev')
        if 'rfnd_amount' in df.columns:
            cols_to_select.append('rfnd_amount')
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
    Load and parse MASTERCPC.csv, expanding CPCs set notation.

    Returns DataFrame with columns: [cpc, service_name, tme_category, Free_CPC, Free_Period, Upgrade_CPC, CHG_Period, CHG_Price]
    """
    df = pl.read_csv(path, null_values=['', 'NULL'])

    rows = []
    for row in df.iter_rows(named=True):
        service_name = row.get('Service Name') or 'UNKNOWN'
        tme_category = row.get('TME Category') or ''
        cpcs_str = row.get('CPCs', '')
        upgrade_cpc = row.get('Upgrade_CPC')
        free_cpc = row.get('Free_CPC')
        free_period = row.get('Free_Period', 0)
        chg_period = row.get('CHG_Period', 0)
        chg_price = row.get('CHG_Price', 0.0)

        cpcs = set()

        if cpcs_str and isinstance(cpcs_str, str):
            cleaned = cpcs_str.strip('{}')
            if cleaned:
                for cpc in cleaned.split(','):
                    cpc = cpc.strip()
                    if cpc.isdigit():
                        cpcs.add(int(cpc))

        if upgrade_cpc is not None and not (isinstance(upgrade_cpc, float) and str(upgrade_cpc) == 'nan'):
            try:
                cpcs.add(int(upgrade_cpc))
            except (ValueError, TypeError):
                pass

        for cpc in cpcs:
            rows.append({
                'cpc': cpc,
                'service_name': service_name if service_name else 'UNKNOWN',
                'tme_category': tme_category if tme_category else '',
                'Free_CPC': int(free_cpc) if free_cpc is not None and not (isinstance(free_cpc, float) and str(free_cpc) == 'nan') else 0,
                'Free_Period': int(free_period) if free_period is not None and not (isinstance(free_period, float) and str(free_period) == 'nan') else 0,
                'Upgrade_CPC': int(upgrade_cpc) if upgrade_cpc is not None and not (isinstance(upgrade_cpc, float) and str(upgrade_cpc) == 'nan') else 0,
                'CHG_Period': int(chg_period) if chg_period is not None and not (isinstance(chg_period, float) and str(chg_period) == 'nan') else 0,
                'CHG_Price': float(chg_price) if chg_price is not None and not (isinstance(chg_price, float) and str(chg_price) == 'nan') else 0.0
            })

    if not rows:
        return pl.DataFrame(schema={
            'cpc': pl.Int64,
            'service_name': pl.Utf8,
            'tme_category': pl.Utf8,
            'Free_CPC': pl.Int64,
            'Free_Period': pl.Int64,
            'Upgrade_CPC': pl.Int64,
            'CHG_Period': pl.Int64,
            'CHG_Price': pl.Float64
        })

    return pl.DataFrame(rows).unique(subset=['cpc'])


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
        df.write_csv(tmp_path)
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
