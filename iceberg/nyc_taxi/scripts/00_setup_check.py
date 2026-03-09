"""Phase 0: Verify all imports work correctly."""
import polars as pl
import duckdb
import pyiceberg
import requests
import tqdm

print(f"polars: {pl.__version__}")
print(f"duckdb: {duckdb.__version__}")
print(f"pyiceberg: {pyiceberg.__version__}")
print(f"requests: {requests.__version__}")
print(f"tqdm: {tqdm.__version__}")
print("All imports OK")
