.PHONY: build up down clean logs ps images spark-image dbt-image ingest trino lint test

build: images
	docker compose build

images: spark-image dbt-image

spark-image:
	docker build -f docker/spark.Dockerfile -t nyc-taxi-spark:latest .

dbt-image:
	docker build -f docker/dbt.Dockerfile -t nyc-taxi-dbt:latest .

up: images
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

ingest:
	docker run --rm --network lakehouse --env-file .env nyc-taxi-spark:latest \
		spark-submit /opt/ingestion/download_tlc.py

trino:
	docker compose exec trino trino --catalog iceberg

lint:
	ruff check .

test:
	pytest -q
