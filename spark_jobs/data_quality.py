import sys

from pyspark.sql import functions as F
from session import build_spark


def assert_non_empty(df, name, failures):
    if df.count() == 0:
        failures.append(f"{name} is empty")


def assert_no_nulls(df, name, columns, failures):
    condition = None
    for column in columns:
        check = F.col(column).isNull()
        condition = check if condition is None else (condition | check)
    null_count = df.filter(condition).count()
    if null_count > 0:
        failures.append(f"{name} has {null_count} null values in {columns}")


def assert_non_negative(df, name, columns, failures):
    for column in columns:
        bad = df.filter(F.col(column) < 0).count()
        if bad > 0:
            failures.append(f"{name}.{column} has {bad} negative values")


def assert_referential_integrity(fact, dim, fact_key, dim_key, name, failures):
    orphans = fact.join(dim, fact[fact_key] == dim[dim_key], "left_anti").count()
    if orphans > 0:
        failures.append(f"{name} has {orphans} orphan rows")


def main():
    spark = build_spark("data_quality")
    fact = spark.read.table("lakehouse.gold.fact_trips")
    dim_zone = spark.read.table("lakehouse.gold.dim_zone")
    dim_payment = spark.read.table("lakehouse.gold.dim_payment_type")
    dim_date = spark.read.table("lakehouse.gold.dim_date")

    failures = []
    assert_non_empty(fact, "gold.fact_trips", failures)
    assert_non_empty(dim_zone, "gold.dim_zone", failures)
    assert_no_nulls(fact, "gold.fact_trips", ["date_key", "pickup_location_id"], failures)
    assert_non_negative(fact, "gold.fact_trips", ["fare_amount", "total_amount"], failures)
    assert_referential_integrity(
        fact,
        dim_payment,
        "payment_type",
        "payment_type",
        "fact_trips -> dim_payment_type",
        failures,
    )
    assert_referential_integrity(
        fact, dim_date, "date_key", "date_key", "fact_trips -> dim_date", failures
    )

    spark.stop()

    if failures:
        for failure in failures:
            print(f"FAIL {failure}")
        sys.exit(1)
    print("all data quality checks passed")


if __name__ == "__main__":
    main()
