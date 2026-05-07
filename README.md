# ☕ Highlands Lakehouse

A production-style **data lakehouse pipeline** built on top of a simulated Highlands Coffee operation — covering real-time CDC ingestion, multi-source weather enrichment, SCD Type 2 dimension modeling, and a Gold analytics layer served to BigQuery.

> **Stack:** Apache Airflow 3 · PySpark 3.5 · Apache Iceberg · Google Cloud Storage · BigQuery · dbt · Docker · SQL Server (Datastream CDC)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline Flow](#pipeline-flow)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Layer Schema](#layer-schema)
- [Setup Guide](#setup-guide)
- [Running the Pipeline](#running-the-pipeline)
- [DAG Reference](#dag-reference)
- [Dashboard](#dashboard)

---

## Overview

Highlands Lakehouse simulates an end-to-end data platform for a Vietnamese coffee chain with multiple stores across cities. The pipeline:

- **Ingests** transactional data (orders, products, stores) via **Google Datastream CDC** from SQL Server
- **Enriches** sales data with **real-time weather** from 3 independent APIs (OpenWeather, Open-Meteo, TomorrowIO)
- **Stores** all data in **Apache Iceberg** tables on GCS — enabling time-travel, schema evolution, and ACID transactions
- **Transforms** data through Bronze → Silver → Gold layers
- **Serves** analytics via **BigQuery External Iceberg Tables** + **dbt-built native tables**
- **Visualizes** through a Looker Studio dashboard showing weather-driven purchase behavior

The core hypothesis being modeled: *rainy weather → more hot drink orders and delivery orders; clear weather → more cold drinks and dine-in.*

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                   │
│                                                                         │
│  SQL Server (HighlandsDB)          Weather APIs                         │
│  ├── Orders (CDC)                  ├── OpenWeatherMap                   │
│  ├── OrderDetails (CDC)            ├── Open-Meteo                       │
│  ├── Products (CDC)                └── TomorrowIO                       │
│  └── Stores (CDC)                                                       │
└──────────────┬──────────────────────────────┬───────────────────────────┘
               │ Google Datastream            │ REST API (hourly)
               ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER — GCS Raw                               │
│  gs://highlands-lakehouse/bronze/cdc/HighlandsDB/dbo/dbo_*/             │
│  gs://highlands-lakehouse/bronze/weather/{source}/{city}/{YYYY/MM/DD}/  │
│                                                                         │
│  Format: Avro (CDC) · Parquet (weather)   No transformation applied     │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ PySpark (Airflow-submitted, local[2])
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER — Iceberg on GCS                        │
│  gs://highlands-lakehouse/iceberg/silver/                               │
│  ├── orders          (MERGE INTO — upsert by order_id)                  │
│  ├── order_details   (MERGE INTO — upsert by detail_id)                 │
│  ├── products        (SCD Type 2 — track price/status history)          │
│  ├── stores          (SCD Type 2 — track city/status history)           │
│  └── weather         (append — multi-source, deduplicated per city/hour)│
│                                                                         │
│  ACID · Time-travel · Schema evolution · Partition pruning              │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ BigQuery External Iceberg Table (auto-synced after write)
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER — BigQuery Native                         │
│  Dataset: gold                                                          │
│  ├── fact_sales_weather      (dbt — hourly grain, weather-enriched)     │
│  └── realtime_order_stats    (BQ Materialized View — 5-min refresh)     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Flow

### Scheduling — Airflow Data-Aware Scheduling (Datasets)

```
@hourly                @hourly               @hourly
    │                      │                     │
dag_openweather    dag_openmeteo         dag_tomorrowio
    │  outlets=            │  outlets=            │  outlets=
    │  BRONZE_OW           │  BRONZE_OM           │  BRONZE_TIO
    └──────────────────────┴─────────────────────┘
                           │
              (ALL 3 datasets updated → AND logic)
                           ▼
                  dag_silver_weather
                  schedule=DatasetOrTimeSchedule(
                      datasets=[OW, OM, TIO],    ← happy path trigger
                      cron="55 * * * *"           ← fallback if any API fails
                  )
                  outlets=SILVER_WEATHER
                           │
cron="25 * * * *"          │
        │                  │
dag_silver_cdc             │
outlets=SILVER_CDC         │
        │                  │
        └──────────────────┘
              (BOTH silver datasets updated)
                           ▼
                   dag_gold_dbt
                   schedule=[SILVER_WEATHER, SILVER_CDC]
                   runs: dbt build → BigQuery gold.*
```

**Timeline per hour (no overlap, no contention):**
```
:00  Bronze APIs run (openweather, openmeteo, tomorrowio)
:05  silver_weather triggered by Datasets → Spark job runs
:15  silver_weather done → SILVER_WEATHER dataset emitted
:25  silver_cdc triggered by cron → Spark job runs
:35  silver_cdc done → SILVER_CDC dataset emitted
:40  gold_dbt triggered (both silver datasets ready) → dbt build
:55  silver_weather cron fallback (safety net if Dataset trigger missed)
```

### Order Simulation

A separate DAG (`dag_order_simulator`) generates synthetic orders hourly, inserting into SQL Server — which Datastream then picks up and streams to GCS Bronze. Order behavior (Hot Drink vs Cold Drink, Delivery vs Dine-in) is probabilistically driven by current weather conditions at each store's city.

```python
# Example: Rain profile
{"delivery_prob": 0.80, "category_weights": {"Hot Drink": 7, "Cold Drink": 1, "Food": 2}}
# → 80% of orders are Delivery, 70% of items are Hot Drinks
```

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 3.1.8 |
| Compute | PySpark | 3.5.5 |
| Table Format | Apache Iceberg (Hadoop catalog) | 1.7.1 |
| Object Storage | Google Cloud Storage | — |
| CDC Replication | Google Datastream | — |
| Data Warehouse | Google BigQuery | — |
| Transformation | dbt-bigquery | 1.11 |
| Source DB | SQL Server (HighlandsDB) | — |
| Containerization | Docker / Docker Compose | — |
| Runtime | Python 3.12 / OpenJDK 17 | — |

---

## Project Structure

```
highlands-lakehouse/
├── docker-compose.yaml         # Airflow + Celery + Redis + Postgres
├── Dockerfile                  # Custom image: Airflow + Java + PySpark + dbt
├── .env                        # API keys, GCP credentials path (not committed)
│
├── dags/
│   ├── pipeline_datasets.py    # Centralized Airflow Dataset declarations
│   ├── dag_openweather.py      # OpenWeatherMap → Bronze GCS (hourly)
│   ├── dag_openmeteo.py        # Open-Meteo → Bronze GCS (hourly)
│   ├── dag_tomorrowio.py       # TomorrowIO → Bronze GCS (hourly)
│   ├── dag_silver_weather.py   # Bronze Parquet → Iceberg silver.weather
│   ├── dag_silver_cdc.py       # Bronze Avro (CDC) → Iceberg silver.orders/products/...
│   ├── dag_gold_dbt.py         # Triggers dbt build → BigQuery gold.*
│   ├── dag_order_simulator.py  # Synthetic order generator → SQL Server
│   ├── bq_sync.py              # BigQuery External Table metadata sync helper
│   │
│   ├── highlands/              # Order simulation domain logic
│   │   ├── simulator.py        # generate_orders() — weather-driven order logic
│   │   ├── profiles.py         # Weather condition → purchase behavior profiles
│   │   └── repository.py       # DB read helpers (stores, customers, products)
│   │
│   ├── spark/                  # PySpark ETL scripts (submitted via spark-submit)
│   │   ├── silver_cdc.py       # CDC: dedup → MERGE INTO + SCD Type 2
│   │   ├── silver_weather.py   # Weather: Bronze Parquet → Iceberg append
│   │   └── common.py           # SparkSession builder (GCS connector, Iceberg config)
│   │
│   └── weather/                # Weather API clients
│       ├── fetcher.py          # OpenWeather / OpenMeteo / TomorrowIO fetchers
│       ├── loader.py           # GCS Bronze Parquet writer
│       └── repository.py       # City metadata reader
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            # BigQuery connection (uses keyfile auth)
│   └── models/gold/
│       ├── sources.yml         # BigQuery External Tables as dbt sources
│       ├── weather_consensus.sql    # Aggregate 3 weather sources → 1 row per city/hour
│       └── fact_sales_weather.sql  # Main fact: orders + weather + SCD2 dims
│
├── data/
│   ├── datasource_highlandsdb_schema.sql   # SQL Server DDL (Cities, Stores, Orders...)
│   ├── datasource_highlandsdb_seed_data.sql # Sample seed data
│   └── gold_bq_mv_realtime_order_stats.sql # BigQuery MV DDL (run once in BQ Console)
│
└── config/
    └── highlands-lakehouse-*.json  # GCP Service Account key (not committed)
```

---

## Layer Schema

### Source — SQL Server (HighlandsDB)

| Table | Description |
|---|---|
| `Cities` | city_id, city_name, lat, lon, is_active |
| `Stores` | store_id, store_name, city_id, is_active |
| `Customers` | customer_id, full_name, phone |
| `Products` | product_id, product_name, category, price, is_active |
| `Orders` | order_id (UUID), store_id, customer_id, order_type, status, total_amount, created_at, updated_at |
| `OrderDetails` | detail_id (UUID), order_id, product_id, quantity, unit_price, subtotal |

### Bronze — GCS Raw

| Path | Format | Source |
|---|---|---|
| `bronze/cdc/HighlandsDB/dbo/dbo_Orders/YYYY/MM/DD/HH/` | Avro | Google Datastream |
| `bronze/cdc/HighlandsDB/dbo/dbo_OrderDetails/...` | Avro | Google Datastream |
| `bronze/cdc/HighlandsDB/dbo/dbo_Products/...` | Avro | Google Datastream |
| `bronze/cdc/HighlandsDB/dbo/dbo_Stores/...` | Avro | Google Datastream |
| `bronze/weather/openweather/{city}/{YYYY/MM/DD/HH}/` | Parquet | OpenWeatherMap API |
| `bronze/weather/openmeteo/{city}/{YYYY/MM/DD/HH}/` | Parquet | Open-Meteo API |
| `bronze/weather/tomorrowio/{city}/{YYYY/MM/DD/HH}/` | Parquet | TomorrowIO API |

### Silver — Iceberg on GCS

| Table | Grain | Strategy | Key Columns |
|---|---|---|---|
| `silver.orders` | 1 row per order | MERGE INTO (upsert) | order_id |
| `silver.order_details` | 1 row per line item | MERGE INTO (upsert) | detail_id |
| `silver.products` | 1 row per version | SCD Type 2 | product_id + valid_from/valid_to/is_current |
| `silver.stores` | 1 row per version | SCD Type 2 | store_id + valid_from/valid_to/is_current |
| `silver.weather` | 1 row per source per city per fetch | Append | source, city_id, fetched_at |

> SCD Type 2 on `products` and `stores` preserves historical price/status — enabling correct revenue attribution even after price changes.

### Gold — BigQuery

| Table | Type | Grain | Description |
|---|---|---|---|
| `gold.weather_consensus` | dbt table | city × hour | Majority-vote condition from 3 weather sources |
| `gold.fact_sales_weather` | dbt table | hour × store × order_type × category | Main analytics fact with weather enrichment |
| `gold.realtime_order_stats` | BQ Materialized View | hour × store × status | Near-real-time order counts (5-min refresh) |

---

## Setup Guide

### Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop ≥ 4.x | Allocate **≥ 10 GB RAM** in Docker settings |
| GCP Project | With BigQuery, GCS, Datastream APIs enabled |
| GCP Service Account | Roles: `BigQuery Admin`, `Storage Admin`, `Datastream Admin` |
| SQL Server | HighlandsDB instance reachable from Docker network |
| API Keys | OpenWeatherMap, TomorrowIO (Open-Meteo is free, no key needed) |

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/highlands-lakehouse.git
cd highlands-lakehouse
```

### 2. Configure environment variables

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

```env
# GCP
GCS_BUCKET=highlands-lakehouse-2026
GCP_SA_KEYFILE=/opt/airflow/config/your-keyfile.json

# Weather APIs
OPENWEATHER_API_KEY=your_key_here
TOMORROWIO_API_KEY=your_key_here

# SQL Server
MSSQL_HOST=host.docker.internal
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PASSWORD=your_password
MSSQL_DB=HighlandsDB
```

### 3. Place your GCP Service Account key

```
config/your-keyfile.json
```

The file is mounted into the container at `/opt/airflow/config/`. Ensure your `.gitignore` excludes `config/*.json`.

### 4. Download Spark JARs

```bash
mkdir -p jars
# Iceberg Spark runtime
curl -L -o jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar

# GCS Connector
curl -L -o jars/gcs-connector-hadoop3-latest.jar \
  https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
```

### 5. Set up SQL Server

Run the schema and seed scripts against your SQL Server instance:

```sql
-- In SQL Server Management Studio or sqlcmd:
-- 1. Run data/datasource_highlandsdb_schema.sql
-- 2. Run data/datasource_highlandsdb_seed_data.sql
```

### 6. Configure Google Datastream

Create a Datastream stream:
- **Source:** SQL Server (HighlandsDB)
- **Tables:** `dbo.Orders`, `dbo.OrderDetails`, `dbo.Products`, `dbo.Stores`
- **Destination:** GCS bucket → path prefix `bronze/cdc/HighlandsDB/dbo`
- **Format:** Avro

### 7. Configure BigQuery

Create the BigQuery dataset and BigLake connection, then configure Airflow Variables:

```
BQ_PROJECT      = your-gcp-project-id
BQ_DATASET      = silver_ext
BQ_LOCATION     = asia-southeast1
BQ_CONNECTION   = projects/.../locations/.../connections/highlands-biglake
GCS_BRONZE_BUCKET = highlands-lakehouse-2026
```

Create the Materialized View (one-time, run in BQ Console):

```sql
-- Paste contents of data/gold_bq_mv_realtime_order_stats.sql
```

### 8. Build and start Airflow

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

Open [http://localhost:8080](http://localhost:8080) — default credentials: `airflow / airflow`

### 9. Configure Airflow Connections

In the Airflow UI → Admin → Connections:

| Conn ID | Type | Details |
|---|---|---|
| `mssql_highlands` | Microsoft SQL Server | host, port 1433, schema=HighlandsDB |
| `google_cloud_default` | Google Cloud | keyfile path or JSON |

### 10. Configure dbt

```bash
# Edit dbt/profiles.yml with your GCP project and dataset
# Then verify connection:
cd dbt && dbt debug
```

---

## Running the Pipeline

### Enable DAGs

In the Airflow UI, unpause DAGs in this order:

1. `weather_openweather_pipeline`
2. `weather_openmeteo_pipeline`
3. `weather_tomorrowio_pipeline`
4. `silver_weather_dag`
5. `silver_cdc_dag`
6. `gold_dbt_dag`
7. `order_simulator_dag`

### Manual trigger (first run)

```bash
# Trigger all weather DAGs to populate Bronze
airflow dags trigger weather_openweather_pipeline
airflow dags trigger weather_openmeteo_pipeline
airflow dags trigger weather_tomorrowio_pipeline

# Then trigger silver and gold
airflow dags trigger silver_weather_dag
airflow dags trigger silver_cdc_dag
# gold_dbt_dag triggers automatically via Dataset events
```

### Check pipeline status

```
Airflow UI → DAGs → Graph view
```

After a complete cycle, verify data in BigQuery:

```sql
SELECT * FROM `your-project.gold.fact_sales_weather` LIMIT 10;
SELECT * FROM `your-project.gold.realtime_order_stats` LIMIT 10;
```

---

## DAG Reference

| DAG | Schedule | Trigger | Description |
|---|---|---|---|
| `weather_openweather_pipeline` | `@hourly` | Cron | Fetches weather from OpenWeatherMap → Bronze GCS Parquet |
| `weather_openmeteo_pipeline` | `@hourly` | Cron | Fetches weather from Open-Meteo → Bronze GCS Parquet |
| `weather_tomorrowio_pipeline` | `@hourly` | Cron | Fetches weather from TomorrowIO → Bronze GCS Parquet |
| `silver_weather_dag` | `55 * * * *` | Dataset (all 3 bronze) OR Cron | Spark: Bronze Parquet → Iceberg silver.weather |
| `silver_cdc_dag` | `25 * * * *` | Cron | Spark: Bronze Avro (CDC) → Iceberg silver.orders/products/stores |
| `gold_dbt_dag` | — | Dataset (silver_weather AND silver_cdc) | dbt build → BigQuery gold.* |
| `order_simulator_dag` | `@hourly` | Cron | Generates synthetic orders → SQL Server (weather-driven behavior) |

---

## Dashboard

The Looker Studio dashboard connects to `gold.fact_sales_weather` and `gold.realtime_order_stats` and visualizes:

- **Revenue by weather condition** — does Rain drive more Hot Drink revenue?
- **Delivery vs Dine-in ratio** by condition and hour
- **Hourly order volume** per city/store
- **Top products** by weather condition
- **Real-time order status** scorecards (from BQ Materialized View, ~5-min lag)

> https://datastudio.google.com/reporting/ec1f1ac1-dea0-4091-96fb-53e6b1d4a8c7

---

## Notes

- **Iceberg Hadoop catalog** is used (no Hive Metastore required) — metadata lives alongside data files in GCS under `iceberg/`
- **BigQuery External Tables** are re-pointed to the latest Iceberg metadata snapshot after every Spark write (handled by `bq_sync.py`)
- **SCD Type 2** on products/stores means historical orders always join to the correct price at time of purchase
- **Weather consensus** uses `APPROX_TOP_COUNT` (majority vote) when 3 sources disagree on condition
- **Celery worker concurrency** is set to `2` — intentional to prevent concurrent Spark JVM memory contention on a single Docker VM

---

## License

MIT
