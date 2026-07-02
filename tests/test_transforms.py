from datetime import datetime

from transforms import (
    build_dim_payment_type,
    build_fact_trips,
    clean_trips,
)

RAW_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]


def raw_row(pickup, dropoff, distance, fare, passengers=1, pu=100, do=200):
    return (1, pickup, dropoff, passengers, distance, pu, do, 1, fare, 1.0, fare + 1.0)


def test_clean_trips_filters_invalid_rows(spark):
    rows = [
        raw_row(datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 20), 3.0, 12.0),
        raw_row(datetime(2023, 1, 1, 9, 0), datetime(2023, 1, 1, 9, 10), 0.0, 5.0),
        raw_row(datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 9, 0), 2.0, 5.0),
        raw_row(datetime(2023, 1, 1, 11, 0), datetime(2023, 1, 1, 11, 30), 4.0, -1.0),
    ]
    df = spark.createDataFrame(rows, RAW_COLUMNS)

    result = clean_trips(df)

    assert result.count() == 1
    assert "pickup_date" in result.columns
    assert "trip_duration_minutes" in result.columns


def test_clean_trips_deduplicates(spark):
    pickup = datetime(2023, 1, 1, 8, 0)
    dropoff = datetime(2023, 1, 1, 8, 20)
    rows = [
        raw_row(pickup, dropoff, 3.0, 12.0),
        raw_row(pickup, dropoff, 3.0, 12.0),
    ]
    df = spark.createDataFrame(rows, RAW_COLUMNS)

    assert clean_trips(df).count() == 1


def test_clean_trips_computes_duration(spark):
    rows = [raw_row(datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), 3.0, 12.0)]
    df = spark.createDataFrame(rows, RAW_COLUMNS)

    duration = clean_trips(df).first()["trip_duration_minutes"]

    assert duration == 30.0


def test_dim_payment_type_has_known_codes(spark):
    dim = build_dim_payment_type(spark)
    codes = {row["payment_type"] for row in dim.collect()}

    assert codes == {1, 2, 3, 4, 5, 6}


def test_fact_trips_exposes_measures(spark):
    rows = [raw_row(datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 20), 3.0, 12.0)]
    cleaned = clean_trips(spark.createDataFrame(rows, RAW_COLUMNS))

    fact = build_fact_trips(cleaned)

    assert "date_key" in fact.columns
    assert "total_amount" in fact.columns
    assert fact.first()["date_key"] == 20230101
