"""Phase 1: Read remote Parquet via Polars."""
import polars as pl

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
print(f"Reading: {url}")
df = pl.read_parquet(url)
print("\n--- Schema ---")
print(df.schema)
print(f"\n--- Shape: {df.shape} ---")
print("\n--- Head(5) ---")
print(df.head(5))
print("\n--- Describe ---")
print(df.describe())
