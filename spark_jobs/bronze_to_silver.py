import os

from session import build_spark
from transforms import clean_trips

DATASET = os.environ.get("TLC_DATASET", "yellow")


def main():
    spark = build_spark("bronze_to_silver")
    bronze = spark.read.table(f"lakehouse.bronze.{DATASET}_trips")
    silver = clean_trips(bronze)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    (
        silver.writeTo("lakehouse.silver.trips")
        .using("iceberg")
        .partitionedBy("pickup_date")
        .createOrReplace()
    )
    spark.stop()


if __name__ == "__main__":
    main()
