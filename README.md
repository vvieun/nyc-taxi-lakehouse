[English](README.md) · [Русский](README.ru.md)

# nyc taxi lakehouse

A local batch lakehouse on NYC TLC data: object storage, an open table format, a distributed processing engine, orchestration, modeling and BI, all in Docker and brought up with a single command.

## stack

| Layer | Technology |
| --- | --- |
| Object storage | MinIO (S3-compatible) |
| Table format | Apache Iceberg + REST catalog |
| Processing | Apache Spark 3.5 (PySpark) |
| Query engine | Trino |
| Orchestration | Apache Airflow |
| Transformations / tests | dbt (dbt-trino) |
| Visualization | Apache Superset |

## architecture

The flow is orchestrated by a single Airflow DAG `nyc_taxi_pipeline`:

```
download_tlc -> ingest_bronze -> bronze_to_silver -> silver_to_gold -> data_quality -> dbt_build
```

### medallion layers

- **raw** — source parquet and `taxi_zone_lookup.csv`, uploaded to MinIO as is.
- **bronze** — raw data registered as Iceberg tables.
- **silver** — typing, filtering of invalid trips, deduplication, partitioning by date.
- **gold** — dimensional star schema: `fact_trips`, `dim_zone`, `dim_date`, `dim_payment_type`.
- **analytics** — dbt marts on top of gold through Trino.

## data

A real public dataset — [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) from the New York City Taxi & Limousine Commission. `ingestion/download_tlc.py` pulls the yellow taxi trips (monthly parquet) and the `taxi_zone_lookup.csv` reference straight from the official TLC storage into MinIO. The set of months is configured by `TLC_MONTHS` in `.env` (default `2023-01`, `2023-02`, `2023-03`).

## structure

```
.
├── docker-compose.yml
├── docker/                 spark / airflow / superset images
├── conf/                   spark, trino, superset config
├── ingestion/              upload of TLC parquet into MinIO
├── spark_jobs/             ingest_bronze / bronze_to_silver / silver_to_gold / data_quality
├── airflow/dags/           nyc_taxi_pipeline
├── dbt/                    mart models and tests
├── tests/                  pytest + chispa on transformation functions
└── Makefile
```

## launch

```bash
cp .env.example .env
make up
```

Commands:

| Command | Action |
| --- | --- |
| `make up` | build the spark image and start the whole stack |
| `make ingest` | one-off upload of TLC parquet into MinIO |
| `make trino` | open the Trino SQL console |
| `make test` | run the transformation unit tests |
| `make down` | stop the stack |
| `make clean` | stop the stack and remove volumes |

## interfaces

| Service | URL | Access |
| --- | --- | --- |
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Trino | http://localhost:8082 | admin |
| Superset | http://localhost:8088 | admin / admin |

## running the pipeline

1. Open Airflow, enable the `nyc_taxi_pipeline` DAG and trigger it.
2. After a successful run, check the data in Trino:

```sql
SELECT count(*) FROM iceberg.gold.fact_trips;

SELECT z.borough, count(*) AS trips
FROM iceberg.gold.fact_trips f
JOIN iceberg.gold.dim_zone z ON f.pickup_location_id = z.location_id
GROUP BY z.borough
ORDER BY trips DESC;

SELECT * FROM iceberg.gold.fact_trips.snapshots;
```

3. The dbt marts are available in `iceberg.analytics` (`daily_revenue`, `top_pickup_zones`, `payment_breakdown`).

## superset dashboard

1. In Superset add a Database with the SQLAlchemy URI:

```
trino://admin@trino:8080/iceberg
```

2. Build charts on top of `analytics.daily_revenue`, `analytics.top_pickup_zones`, `analytics.payment_breakdown`.

## what the project shows

- A lakehouse with separated storage and compute: Spark writes and Trino reads the same Iceberg tables.
- Iceberg REST catalog: hidden partitioning, snapshots, time travel, schema evolution.
- Idempotent Spark jobs (`createOrReplace`), safe for reruns and backfill.
- Data quality as a gate in the pipeline: checks for emptiness, nulls, negative values and referential integrity fail the DAG.
- Testable transformations: pure PySpark functions under pytest + chispa.

## tests

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements-dev.txt
make test
```
