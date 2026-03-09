
## Project Layout

```
iceberg_parquet/                  ← project root
├── pyproject.toml                ← uv + hatchling build config
├── .pyiceberg.yaml               ← SQLite catalog, local warehouse
├── README.md
├── src/
│   └── nyc_taxi_iceberg/         ← installable package (src layout)
│       └── __init__.py
├── scripts/                      ← runnable entry points (not installed)
│   ├── 00_setup_check.py
│   ├── 01_explore_polars.py
│   ├── 01_explore_duckdb.py
│   ├── 02_batch_download.py      ← Phase 2
│   └── 03_write_iceberg.py       ← Phase 3
├── data/
│   └── raw/                      ← year=YYYY/month=MM/data.parquet
└── warehouse/                    ← Iceberg metadata + data files
```

### Layout conventions
- **src layout**: all library code lives in `src/nyc_taxi_iceberg/`
- **build tool**: `hatchling` (via `[build-system]` in pyproject.toml)
- **uv package = true**: package is installed in editable mode in the venv
- **scripts/**: standalone scripts using `uv run python scripts/…`; may import from `nyc_taxi_iceberg`
- `data/` and `warehouse/` are gitignored (large files)

---

## 階段性任務分解

### Phase 0 – 環境建置
- `uv init --name nyc-taxi-iceberg` (src layout + hatchling)
- `uv add polars[all] duckdb pyiceberg[s3fs,pyarrow,duckdb,sql-sqlite] requests tqdm`
- 建立 `.pyiceberg.yaml`（SQLite catalog + file warehouse）
- `scripts/00_setup_check.py` – verify imports

### Phase 1 – 直接讀取雲端 Parquet（無需下載）
- `scripts/01_explore_polars.py` – Polars 讀 CloudFront URL
- `scripts/01_explore_duckdb.py` – DuckDB + httpfs 直接 SQL 查詢遠端 Parquet

### Phase 2 – 分批下載與初步處理（Polars）
- 來源：`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY}-{MM}.parquet`
- 目標：存成 `data/raw/year=YYYY/month=MM/data.parquet`
- 處理：轉型 datetime、計算 trip_duration、過濾無效距離

### Phase 3 – 建立與寫入 Iceberg Table
- 用 PyIceberg 建立 table（partition by year, month）
- 批次 append Polars DataFrame → Arrow Table → Iceberg
- 驗證：用 DuckDB `iceberg_scan()` 讀取並做簡單聚合

### Phase 4 – Airflow 自動化增量管線
- 使用 TaskFlow API
- 每月觸發任務：
    1. 產生當月（或落後月份）清單
    2. 下載 + Polars 處理
    3. 寫入 Iceberg（append）
- 加入失敗重試、email 通知（選用）

### Phase 5 – 進階與驗證（可選擴展）
- 部署 MinIO（docker-compose）模擬 S3
- 練習 Iceberg 功能：
    - time travel
    - schema evolution
    - row-level delete / merge
    - compaction（手動或用 Spark）
- 商業分析 SQL（用 DuckDB）：
    - 每月總車資 / 小費平均
    - 熱門 Pickup/Dropoff LocationID
    - 尖峰時段分析（weekday / hour）

---

## 快速啟動指令

```bash
# 安裝依賴（含本 package editable install）
uv sync

# 驗證 imports
uv run python scripts/00_setup_check.py

# Phase 1 驗證
uv run python scripts/01_explore_polars.py
uv run python scripts/01_explore_duckdb.py

# Phase 2
uv run python scripts/02_batch_download.py --year 2024 --month 1
```
