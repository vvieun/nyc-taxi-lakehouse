from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PAYMENT_TYPES = [
    (1, "Credit card"),
    (2, "Cash"),
    (3, "No charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided trip"),
]


def normalize_trips(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("tpep_pickup_datetime", "pickup_at")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_at")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("VendorID", "vendor_id")
        .select(
            "vendor_id",
            F.col("pickup_at").cast("timestamp").alias("pickup_at"),
            F.col("dropoff_at").cast("timestamp").alias("dropoff_at"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("pickup_location_id").cast("int").alias("pickup_location_id"),
            F.col("dropoff_location_id").cast("int").alias("dropoff_location_id"),
            F.col("payment_type").cast("int").alias("payment_type"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("total_amount").cast("double").alias("total_amount"),
        )
    )


def clean_trips(df: DataFrame) -> DataFrame:
    normalized = normalize_trips(df)
    valid = normalized.filter(
        (F.col("pickup_at").isNotNull())
        & (F.col("dropoff_at").isNotNull())
        & (F.col("pickup_at") < F.col("dropoff_at"))
        & (F.col("trip_distance") > 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("passenger_count") > 0)
    )
    enriched = valid.withColumn("pickup_date", F.to_date("pickup_at")).withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_at").cast("long") - F.col("pickup_at").cast("long")) / 60.0,
    )
    return enriched.dropDuplicates(
        ["vendor_id", "pickup_at", "dropoff_at", "pickup_location_id", "dropoff_location_id"]
    )


def build_dim_payment_type(spark) -> DataFrame:
    return spark.createDataFrame(PAYMENT_TYPES, ["payment_type", "payment_description"])


def build_dim_zone(zone_df: DataFrame) -> DataFrame:
    return zone_df.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )


def build_dim_date(trips: DataFrame) -> DataFrame:
    return (
        trips.select("pickup_date")
        .distinct()
        .filter(F.col("pickup_date").isNotNull())
        .select(
            F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("pickup_date").alias("calendar_date"),
            F.year("pickup_date").alias("year"),
            F.month("pickup_date").alias("month"),
            F.dayofmonth("pickup_date").alias("day"),
            F.dayofweek("pickup_date").alias("day_of_week"),
        )
    )


def build_fact_trips(trips: DataFrame) -> DataFrame:
    return trips.select(
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("date_key"),
        "pickup_date",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )
