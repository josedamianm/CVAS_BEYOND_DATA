# Create a test file: test_install.py
import polars as pl
import duckdb
import pyarrow as pa

print(f"✓ Polars version: {pl.__version__}")
print(f"✓ DuckDB version: {duckdb.__version__}")
print(f"✓ PyArrow version: {pa.__version__}")
