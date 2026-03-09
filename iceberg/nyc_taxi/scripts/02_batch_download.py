"""Phase 2: Batch download NYC Taxi Parquet files with Polars processing."""
import argparse
from pathlib import Path

import polars as pl
import requests
from tqdm import tqdm

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


def download_month(year: int, month: int) -> Path:
    url = f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"
    out_dir = DATA_DIR / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    if out_path.exists():
        print(f"  Already exists: {out_path}")
        return out_path

    print(f"  Downloading {url}")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    with open(out_path, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as bar:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
            bar.update(len(chunk))
    return out_path


def process(path: Path) -> Path:
    df = pl.read_parquet(path)

    df = df.with_columns([
        pl.col("tpep_pickup_datetime").cast(pl.Datetime),
        pl.col("tpep_dropoff_datetime").cast(pl.Datetime),
    ])
    df = df.with_columns([
        ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
         .dt.total_seconds() / 60).alias("trip_duration_min")
    ])
    df = df.filter(pl.col("trip_distance") > 0)

    df.write_parquet(path, compression="snappy")
    print(f"  Processed → {path}  shape={df.shape}")
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=None,
                        help="Single month (1-12). Omit for all 12 months.")
    args = parser.parse_args()

    months = [args.month] if args.month else list(range(1, 13))
    for m in months:
        print(f"\n=== {args.year}-{m:02d} ===")
        path = download_month(args.year, m)
        process(path)


if __name__ == "__main__":
    main()
