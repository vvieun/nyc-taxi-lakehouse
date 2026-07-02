import os
from functools import reduce

from pyspark.sql import functions as F

from session import build_spark

BUCKET = os.environ["LAKEHOUSE_BUCKET"]
DATASET = os.environ.get("TLC_DATASET", "yellow")
MONTHS = [m.strip() for m in os.environ.get("TLC_MONTHS", "").split(",") if m.strip()]

CANONICAL_COLUMNS = [
    ("VendorID", "bigint"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "bigint"),
    ("trip_distance", "double"),
    ("PULocationID", "bigint"),
    ("DOLocationID", "bigint"),
    ("payment_type", "bigint"),
    ("fare_amount", "double"),
    ("tip_amount", "double"),
    ("total_amount", "double"),
]


def raw_path(suffix):
    return f"s3a://{BUCKET}/raw/{suffix}"


def read_month(spark, month):
    df = spark.read.parquet(raw_path(f"{DATASET}/{DATASET}_tripdata_{month}.parquet"))
    projection = [F.col(name).cast(dtype).alias(name) for name, dtype in CANONICAL_COLUMNS]
    return df.select(*projection)


def write_bronze_trips(spark):
    frames = [read_month(spark, month) for month in MONTHS]
    trips = reduce(lambda left, right: left.unionByName(right), frames)
    trips = trips.withColumn("_ingested_at", F.current_timestamp())
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    trips.writeTo(f"lakehouse.bronze.{DATASET}_trips").using("iceberg").createOrReplace()


def write_bronze_zone_lookup(spark):
    df = spark.read.option("header", "true").csv(raw_path("reference/taxi_zone_lookup.csv"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    df.writeTo("lakehouse.bronze.zone_lookup").using("iceberg").createOrReplace()


def main():
    spark = build_spark("ingest_bronze")
    write_bronze_trips(spark)
    write_bronze_zone_lookup(spark)
    spark.stop()


if __name__ == "__main__":
    main()
