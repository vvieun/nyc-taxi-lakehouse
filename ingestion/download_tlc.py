import os
import sys
from io import BytesIO

import boto3
import requests

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def object_exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False


def upload_stream(client, url, bucket, key):
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    client.upload_fileobj(BytesIO(response.content), bucket, key)
    print(f"uploaded s3://{bucket}/{key} ({len(response.content)} bytes)")


def ingest_trips(client, bucket, dataset, months):
    for month in months:
        filename = f"{dataset}_tripdata_{month}.parquet"
        key = f"raw/{dataset}/{filename}"
        if object_exists(client, bucket, key):
            print(f"skip existing s3://{bucket}/{key}")
            continue
        upload_stream(client, f"{TLC_BASE_URL}/{filename}", bucket, key)


def ingest_zone_lookup(client, bucket):
    key = "raw/reference/taxi_zone_lookup.csv"
    if object_exists(client, bucket, key):
        print(f"skip existing s3://{bucket}/{key}")
        return
    upload_stream(client, ZONE_LOOKUP_URL, bucket, key)


def main():
    bucket = os.environ["LAKEHOUSE_BUCKET"]
    dataset = os.environ.get("TLC_DATASET", "yellow")
    months = [m.strip() for m in os.environ.get("TLC_MONTHS", "").split(",") if m.strip()]
    if not months:
        print("no months configured in TLC_MONTHS")
        sys.exit(1)

    client = s3_client()
    ingest_trips(client, bucket, dataset, months)
    ingest_zone_lookup(client, bucket)


if __name__ == "__main__":
    main()
