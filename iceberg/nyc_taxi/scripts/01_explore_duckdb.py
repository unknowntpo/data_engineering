"""Phase 1: Read remote Parquet via DuckDB httpfs."""
import duckdb

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
print(f"Querying: {url}")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

result = con.execute(f"""
    SELECT
        COUNT(*) AS row_count,
        AVG(trip_distance) AS avg_trip_distance,
        AVG(total_amount) AS avg_total_amount
    FROM read_parquet('{url}')
""").df()

print("\n--- Aggregation Result ---")
print(result)
