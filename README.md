# Airflow Weather ETL Pipeline

A simple end-to-end data pipeline built with Apache Airflow, orchestrated in Docker and running in a GitHub Codespace. This is a fundamentals project built to learn core data engineering concepts: extraction, transformation, loading, and scheduling.

## What it does

The pipeline runs automatically every hour and:

1. **Extracts** current weather data (temperature, windspeed, timestamp) for the Chicago area from the free Open-Meteo API
2. **Transforms** the raw API response into a clean, minimal record
3. **Loads** the cleaned record into a local SQLite database, appending a new row on every run

## Stack

- **Apache Airflow 3.3.2** — orchestration, scheduling, and monitoring
- **Docker Compose** — runs Airflow's webserver, scheduler, worker, triggerer, Postgres (metadata store), and Redis (message broker)
- **Python** — pipeline logic (urllib, json, sqlite3)
- **SQLite** — lightweight local data store for pipeline output

## Pipeline structure

extract_weather >> transform_weather >> load_weather

Each task passes data to the next using Airflow XComs. The DAG is defined in `dags/weather_pipeline.py`.

## Running it locally

1. Clone this repo
2. Create the required folders and `.env` file: