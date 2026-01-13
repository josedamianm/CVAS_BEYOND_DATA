#!/usr/bin/env python3
"""
Unit tests for Transaction Counters
"""

import pytest
import polars as pl
from pathlib import Path
from datetime import datetime, date
import tempfile
import shutil
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'Scripts'))
sys.path.insert(0, str(PROJECT_ROOT / 'Scripts' / 'utils'))

from counter_utils import (
    load_mastercpc,
    load_counters_cpc,
    write_atomic_parquet,
    write_atomic_csv,
)


class TestMasterCPCParsing:
    def test_expand_cpcs_set_notation(self, tmp_path):
        csv_content = """Service Name,TME Category,Free_CPC,Free_Period,Upgrade_CPC,CHG_Period,CHG_Price,CPCs
TestService,Games,,0,100,30,2.0,{100,101,102}
AnotherService,News,,0,200,30,3.0,{200}
"""
        csv_path = tmp_path / "test_mastercpc.csv"
        csv_path.write_text(csv_content)
        
        df = load_mastercpc(csv_path)
        
        assert len(df) == 4
        assert set(df['cpc'].to_list()) == {100, 101, 102, 200}
        
        test_rows = df.filter(pl.col('service_name') == 'TestService')
        assert len(test_rows) == 3

    def test_handles_empty_service_name(self, tmp_path):
        csv_content = """Service Name,TME Category,Free_CPC,Free_Period,Upgrade_CPC,CHG_Period,CHG_Price,CPCs
,News,,0,100,30,3.0,{100}
"""
        csv_path = tmp_path / "test_mastercpc.csv"
        csv_path.write_text(csv_content)
        
        df = load_mastercpc(csv_path)
        
        assert df['service_name'][0] == 'UNKNOWN'

    def test_uses_upgrade_cpc_when_cpcs_empty(self, tmp_path):
        csv_content = """Service Name,TME Category,Free_CPC,Free_Period,Upgrade_CPC,CHG_Period,CHG_Price,CPCs
TestService,Games,,0,999,30,2.0,
"""
        csv_path = tmp_path / "test_mastercpc.csv"
        csv_path.write_text(csv_content)
        
        df = load_mastercpc(csv_path)
        
        assert 999 in df['cpc'].to_list()


class TestCountersMerge:
    def test_merge_replaces_same_date(self):
        sys.path.insert(0, str(PROJECT_ROOT / 'Scripts'))
        from importlib import import_module
        build_counters = import_module('05_build_counters')
        merge_counters = build_counters.merge_counters

        existing = pl.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2)],
            'cpc': [100, 100],
            'act_count': [10, 20],
            'reno_count': [5, 10],
            'dct_count': [1, 2],
            'cnr_count': [0, 1],
            'ppd_count': [0, 0],
            'rfnd_count': [0, 0],
            'last_updated': [datetime.now(), datetime.now()]
        })

        new = pl.DataFrame({
            'date': [date(2024, 1, 1)],
            'cpc': [100],
            'act_count': [15],
            'reno_count': [7],
            'dct_count': [2],
            'cnr_count': [1],
            'ppd_count': [1],
            'rfnd_count': [0],
        })

        merged = merge_counters(existing, new, '2024-01-01')

        jan1_data = merged.filter(pl.col('date') == date(2024, 1, 1))
        assert jan1_data['act_count'][0] == 15

        jan2_data = merged.filter(pl.col('date') == date(2024, 1, 2))
        assert jan2_data['act_count'][0] == 20

    def test_merge_preserves_other_dates(self):
        sys.path.insert(0, str(PROJECT_ROOT / 'Scripts'))
        from importlib import import_module
        build_counters = import_module('05_build_counters')
        merge_counters = build_counters.merge_counters

        existing = pl.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            'cpc': [100, 100, 100],
            'act_count': [10, 20, 30],
            'reno_count': [5, 10, 15],
            'dct_count': [1, 2, 3],
            'cnr_count': [0, 1, 2],
            'ppd_count': [0, 0, 0],
            'rfnd_count': [0, 0, 0],
            'last_updated': [datetime.now()] * 3
        })

        new = pl.DataFrame({
            'date': [date(2024, 1, 2)],
            'cpc': [100],
            'act_count': [99],
            'reno_count': [99],
            'dct_count': [99],
            'cnr_count': [99],
            'ppd_count': [99],
            'rfnd_count': [99],
        })
        
        merged = merge_counters(existing, new, '2024-01-02')
        
        assert merged.filter(pl.col('date') == date(2024, 1, 1))['act_count'][0] == 10
        assert merged.filter(pl.col('date') == date(2024, 1, 3))['act_count'][0] == 30


class TestAtomicWrites:
    def test_atomic_parquet_write(self, tmp_path):
        df = pl.DataFrame({
            'date': [date(2024, 1, 1)],
            'cpc': [100],
            'count': [10]
        })
        
        output_path = tmp_path / "test.parquet"
        write_atomic_parquet(df, output_path)
        
        assert output_path.exists()
        loaded = pl.read_parquet(output_path)
        assert len(loaded) == 1

    def test_atomic_csv_write(self, tmp_path):
        df = pl.DataFrame({
            'service_name': ['Test'],
            'count': [10]
        })

        output_path = tmp_path / "test.csv"
        write_atomic_csv(df, output_path)

        assert output_path.exists()
        loaded = pl.read_csv(output_path)
        assert len(loaded) == 1


class TestAggregateByService:
    def test_unmapped_cpcs_grouped_as_unknown(self):
        sys.path.insert(0, str(PROJECT_ROOT / 'Scripts'))
        from importlib import import_module
        build_counters = import_module('05_build_counters')
        aggregate_by_service = build_counters.aggregate_by_service

        counters = pl.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 1)],
            'cpc': [100, 999],
            'act_count': [10, 5],
            'reno_count': [5, 2],
            'dct_count': [1, 1],
            'cnr_count': [0, 0],
            'ppd_count': [0, 0],
            'rfnd_count': [0, 0],
        })

        cpc_map = pl.DataFrame({
            'cpc': [100],
            'service_name': ['MappedService'],
            'tme_category': ['Games']
        })
        
        result, unmapped = aggregate_by_service(counters, cpc_map)
        
        assert 999 in unmapped
        
        unknown_row = result.filter(pl.col('service_name') == 'UNKNOWN')
        assert len(unknown_row) == 1
        assert unknown_row['act_count'][0] == 5


class TestLoadCountersCPC:
    def test_returns_empty_schema_when_file_missing(self, tmp_path):
        missing_path = tmp_path / "nonexistent.parquet"
        
        df = load_counters_cpc(missing_path)
        
        assert df.is_empty()
        assert 'date' in df.columns
        assert 'cpc' in df.columns
        assert 'act_count' in df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
