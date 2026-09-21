# Data Engineering Portfolio: Airflow Pipelines

Two end-to-end data pipelines built with Apache Airflow, demonstrating orchestration, cloud storage, and cloud data warehousing. Both run in a GitHub Codespace using Docker Compose.

## Project 1: Weather ETL Pipeline

A fundamentals pipeline that runs hourly:

1. **Extracts** current weather data for the Chicago area from the free Open-Meteo API
2. **Transforms** the raw response into a clean, minimal record
3. **Loads** it into a local SQLite database

**Stack:** Apache Airflow, Python, SQLite

**DAG:** `dags/weather_pipeline.py`

## Project 2: Healthcare Data Pipeline

A more advanced pipeline that runs daily, modeling a real cloud data engineering workflow:

1. **Extracts** hospital quality data from the CMS (Centers for Medicare & Medicaid Services) public API
2. **Lands the raw, untouched data** in MinIO, an S3-compatible object store, preserving the original API response before any transformation
3. **Transforms** the data with Python: selects relevant fields, filters out records missing a valid rating
4. **Loads** the cleaned data into Google BigQuery (Sandbox mode) as a queryable table

**Stack:** Apache Airflow, Python, MinIO (S3-compatible storage), Google BigQuery, boto3, google-cloud-bigquery

**DAG:** `dags/healthcare_pipeline.py`

### Why this architecture

This follows a standard data engineering pattern: separating the **raw zone** (untouched source data, preserved for reprocessing or auditing) from the **cleaned/modeled layer** (what analysts and dashboards actually query). If a transformation bug is found later, the pipeline can be corrected and rerun from the raw data without needing to re-call the source API.

## Stack summary

| Component | Tool |
|---|---|
| Orchestration | Apache Airflow 3.3.2 |
| Containerization | Docker Compose |
| Raw object storage | MinIO (S3-compatible) |
| Data warehouse | Google BigQuery (Sandbox) |
| Local structured storage | SQLite |
| Language | Python (requests, boto3, google-cloud-bigquery) |

## Testing & CI

Both pipelines' core transformation logic is covered by unit tests (`tests/test_pipelines.py`), testing the pure data-cleaning functions independently of Airflow itself. A GitHub Actions workflow (`.github/workflows/tests.yml`) runs these tests automatically on every push to `main`.

## Running it locally

1. Clone this repo
2. Create the required folders and `.env` file: