"""Phase 3: Write processed Parquet files into an Iceberg table."""
import argparse
import warnings
from pathlib import Path

import datetime
import pyarrow as pa
import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, TimestampType, DoubleType, LongType, IntegerType, StringType, FloatType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, MonthTransform

PROJECT_DIR = Path(__file__).parent.parent
DATA_DIR = PROJECT_DIR / "data" / "processed"
CATALOG_NAME = "default"
NAMESPACE = "nyc_taxi"
TABLE_NAME = "yellow_tripdata"
FULL_TABLE = f"{NAMESPACE}.{TABLE_NAME}"

ICEBERG_SCHEMA = Schema(
    NestedField(1,  "VendorID",            LongType(),      required=False),
    NestedField(2,  "tpep_pickup_datetime",  TimestampType(), required=False),
    NestedField(3,  "tpep_dropoff_datetime", TimestampType(), required=False),
    NestedField(4,  "passenger_count",     DoubleType(),    required=False),
    NestedField(5,  "trip_distance",       DoubleType(),    required=False),
    NestedField(6,  "RatecodeID",          DoubleType(),    required=False),
    NestedField(7,  "store_and_fwd_flag",  StringType(),    required=False),
    NestedField(8,  "PULocationID",        LongType(),      required=False),
    NestedField(9,  "DOLocationID",        LongType(),      required=False),
    NestedField(10, "payment_type",        LongType(),      required=False),
    NestedField(11, "fare_amount",         DoubleType(),    required=False),
    NestedField(12, "extra",               DoubleType(),    required=False),
    NestedField(13, "mta_tax",             DoubleType(),    required=False),
    NestedField(14, "tip_amount",          DoubleType(),    required=False),
    NestedField(15, "tolls_amount",        DoubleType(),    required=False),
    NestedField(16, "improvement_surcharge", DoubleType(),  required=False),
    NestedField(17, "total_amount",        DoubleType(),    required=False),
    NestedField(18, "congestion_surcharge", DoubleType(),   required=False),
    NestedField(19, "Airport_fee",         DoubleType(),    required=False),
    NestedField(20, "trip_duration_min",   DoubleType(),    required=False),
)

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="pickup_month"),
)


def get_or_create_table(catalog):
    try:
        ns_tables = catalog.list_namespaces()
        ns_names = [n[0] for n in ns_tables]
        if NAMESPACE not in ns_names:
            catalog.create_namespace(NAMESPACE)
            print(f"Created namespace: {NAMESPACE}")
    except Exception:
        pass

    try:
        table = catalog.load_table(FULL_TABLE)
        print(f"Loaded existing table: {FULL_TABLE}")
    except Exception:
        table = catalog.create_table(
            identifier=FULL_TABLE,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )
        print(f"Created table: {FULL_TABLE}")
    return table


def overwrite_partition(table, path: Path, force: bool = False):
    df = pl.read_parquet(path)

    # Cast to match schema
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").cast(pl.Datetime("us")),
        pl.col("tpep_dropoff_datetime").cast(pl.Datetime("us")),
        pl.col("passenger_count").cast(pl.Float64),
        pl.col("RatecodeID").cast(pl.Float64),
    ])

    # Keep only columns present in schema
    schema_cols = {f.name for f in ICEBERG_SCHEMA.fields}
    existing = [c for c in schema_cols if c in df.columns]
    df = df.select(existing)

    # Derive year/month from path (e.g. data/processed/year=2024/month=01/data.parquet)
    year  = int(path.parts[-3].split("=")[1])
    month = int(path.parts[-2].split("=")[1])

    start = datetime.datetime(year, month, 1)
    end   = datetime.datetime(year + (month // 12), (month % 12) + 1, 1)

    overwrite_filter = And(
        GreaterThanOrEqual("tpep_pickup_datetime", start.isoformat()),
        LessThan("tpep_pickup_datetime", end.isoformat()),
    )

    # Check if partition already has data
    existing_files = list(table.scan(row_filter=overwrite_filter).plan_files())
    if existing_files and not force:
        print(f"  Skipping (partition already exists, use --overwrite to replace): year={year} month={month:02d}")
        return

    # Restrict to rows within the partition month (guards against stray timestamps)
    df = df.filter(
        (pl.col("tpep_pickup_datetime") >= start) &
        (pl.col("tpep_pickup_datetime") < end)
    )

    arrow_table = df.to_arrow()

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        table.overwrite(arrow_table, overwrite_filter=overwrite_filter)
    print(f"  Overwritten {len(df):,} rows from {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=None,
                        help="Single month. Omit for all available.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing partitions. Without this, existing partitions are skipped.")
    args = parser.parse_args()

    warehouse_dir = PROJECT_DIR / "warehouse"
    catalog = load_catalog(
        CATALOG_NAME,
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_dir}/iceberg_catalog.db",
            "warehouse": f"file://{warehouse_dir}",
        },
    )
    table = get_or_create_table(catalog)

    if args.month:
        paths = [DATA_DIR / f"year={args.year}" / f"month={args.month:02d}" / "data.parquet"]
    else:
        paths = sorted(DATA_DIR.glob(f"year={args.year}/month=*/data.parquet"))

    if not paths:
        print("No data files found. Run 02_batch_download.py first.")
        return

    for path in paths:
        if not path.exists():
            print(f"  Skipping (not found): {path}")
            continue
        print(f"\n=== Processing {path} ===")
        overwrite_partition(table, path, force=args.overwrite)

    print("\n--- Verification via DuckDB ---")
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    meta_path = str(table.metadata_location)
    result = con.execute(
        f"SELECT count(*) AS total_rows FROM iceberg_scan('{meta_path}')"
    ).fetchone()
    print(f"Total rows in Iceberg table: {result[0]:,}")


if __name__ == "__main__":
    main()
