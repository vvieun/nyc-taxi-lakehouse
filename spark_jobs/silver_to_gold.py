from session import build_spark
from transforms import (
    build_dim_date,
    build_dim_payment_type,
    build_dim_zone,
    build_fact_trips,
)


def write_table(df, name, partition_by=None):
    writer = df.writeTo(name).using("iceberg")
    if partition_by:
        writer = writer.partitionedBy(partition_by)
    writer.createOrReplace()


def main():
    spark = build_spark("silver_to_gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    trips = spark.read.table("lakehouse.silver.trips")
    zone_lookup = spark.read.table("lakehouse.bronze.zone_lookup")

    write_table(build_dim_payment_type(spark), "lakehouse.gold.dim_payment_type")
    write_table(build_dim_zone(zone_lookup), "lakehouse.gold.dim_zone")
    write_table(build_dim_date(trips), "lakehouse.gold.dim_date")
    write_table(build_fact_trips(trips), "lakehouse.gold.fact_trips", "pickup_date")

    spark.stop()


if __name__ == "__main__":
    main()
