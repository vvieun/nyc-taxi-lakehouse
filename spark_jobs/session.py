from pyspark.sql import SparkSession


def build_spark(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .getOrCreate()
    )
