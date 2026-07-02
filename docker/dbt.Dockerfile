FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-trino==1.8.1

COPY dbt /dbt
WORKDIR /dbt
