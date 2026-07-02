[English](README.md) · [Русский](README.ru.md)

# nyc taxi lakehouse

Локальный batch-lakehouse на NYC TLC данных: объектное хранилище, открытый табличный формат, распределённый движок обработки, оркестрация, моделирование и BI, всё в Docker и поднимается одной командой.

## стек

| Слой | Технология |
| --- | --- |
| Объектное хранилище | MinIO (S3-совместимое) |
| Табличный формат | Apache Iceberg + REST-каталог |
| Обработка | Apache Spark 3.5 (PySpark) |
| Query engine | Trino |
| Оркестрация | Apache Airflow |
| Трансформации / тесты | dbt (dbt-trino) |
| Визуализация | Apache Superset |

## архитектура

Поток оркеструется одним Airflow DAG `nyc_taxi_pipeline`:

```
download_tlc -> ingest_bronze -> bronze_to_silver -> silver_to_gold -> data_quality -> dbt_build
```

### медальон-слои

- **raw** — исходные parquet и `taxi_zone_lookup.csv`, выгруженные в MinIO как есть.
- **bronze** — сырые данные, зарегистрированные как Iceberg-таблицы.
- **silver** — типизация, фильтрация невалидных поездок, дедупликация, партиционирование по дате.
- **gold** — dimensional-модель «звезда»: `fact_trips`, `dim_zone`, `dim_date`, `dim_payment_type`.
- **analytics** — витрины dbt поверх gold через Trino.

## данные

Реальный публичный датасет — [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) от Taxi & Limousine Commission города Нью-Йорк. `ingestion/download_tlc.py` выгружает поездки yellow taxi (помесячные parquet) и справочник `taxi_zone_lookup.csv` напрямую из официального хранилища TLC в MinIO. Набор месяцев задаётся переменной `TLC_MONTHS` в `.env` (по умолчанию `2023-01`, `2023-02`, `2023-03`).

## структура

```
.
├── docker-compose.yml
├── docker/                 образы spark / airflow / superset
├── conf/                   конфиги spark, trino, superset
├── ingestion/              выгрузка TLC parquet в MinIO
├── spark_jobs/             ingest_bronze / bronze_to_silver / silver_to_gold / data_quality
├── airflow/dags/           nyc_taxi_pipeline
├── dbt/                    модели и тесты витрин
├── tests/                  pytest + chispa на функции трансформаций
└── Makefile
```

## запуск

```bash
cp .env.example .env
make up
```

Команды:

| Команда | Действие |
| --- | --- |
| `make up` | собрать spark-образ и поднять весь стек |
| `make ingest` | разово выгрузить TLC parquet в MinIO |
| `make trino` | открыть SQL-консоль Trino |
| `make test` | прогнать юнит-тесты трансформаций |
| `make down` | остановить стек |
| `make clean` | остановить стек и удалить тома |

## интерфейсы

| Сервис | URL | Доступ |
| --- | --- | --- |
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Trino | http://localhost:8082 | admin |
| Superset | http://localhost:8088 | admin / admin |

## запуск пайплайна

1. Открыть Airflow, включить DAG `nyc_taxi_pipeline` и запустить его.
2. После успешного прогона проверить данные в Trino:

```sql
SELECT count(*) FROM iceberg.gold.fact_trips;

SELECT z.borough, count(*) AS trips
FROM iceberg.gold.fact_trips f
JOIN iceberg.gold.dim_zone z ON f.pickup_location_id = z.location_id
GROUP BY z.borough
ORDER BY trips DESC;

SELECT * FROM iceberg.gold.fact_trips.snapshots;
```

3. Витрины dbt доступны в `iceberg.analytics` (`daily_revenue`, `top_pickup_zones`, `payment_breakdown`).

## дашборд Superset

1. В Superset добавить Database с SQLAlchemy URI:

```
trino://admin@trino:8080/iceberg
```

2. Построить чарты поверх `analytics.daily_revenue`, `analytics.top_pickup_zones`, `analytics.payment_breakdown`.

## что демонстрирует проект

- Lakehouse с разделением storage и compute: Spark пишет, Trino читает одни и те же Iceberg-таблицы.
- Iceberg REST-каталог: hidden partitioning, snapshots, time travel, schema evolution.
- Идемпотентные Spark-джобы (`createOrReplace`), безопасные для повторных прогонов и backfill.
- Качество данных как gate в пайплайне: проверки на пустоту, null, отрицательные значения и ссылочную целостность роняют DAG.
- Тестируемые трансформации: чистые PySpark-функции под pytest + chispa.

## тесты

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements-dev.txt
make test
```
