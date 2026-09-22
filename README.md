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

## Project 3: Multi-Source Analytics Pipeline with dbt

A more advanced pipeline that extends Project 2 to answer a real analytical question: **do hospital star ratings actually predict patient outcomes, specifically hospital readmissions, and does that relationship hold across ownership types and states?**

1. **Extracts** a second CMS dataset, the Hospital Readmissions Reduction Program data, alongside the existing hospital ratings data, using two parallel branches in the same DAG
2. **Lands and transforms** both sources independently (raw zone in MinIO, then cleaned in Python), joined on `facility_id` (CCN format)
3. **Loads** both cleaned datasets into BigQuery as separate tables: `hospital_ratings` and `hospital_readmissions`
4. **Models and tests** the data with dbt: staging models for each source, then mart models (`dim_hospital`, `dim_condition`, and `fact_hospital_readmissions`, one row per hospital per condition), backed by 9 dbt tests covering uniqueness, not-null constraints, referential integrity, and accepted values

The full pipeline runs end to end in about 2.5 minutes from a single Airflow trigger.

**Stack:** Apache Airflow, Python, MinIO, Google BigQuery, dbt-bigquery

**DAG:** `dags/healthcare_pipeline.py` (extended to 10 tasks: two parallel extract/land/transform/load branches feeding into `run_dbt_models` and `test_dbt_models`)

**dbt project:** `dbt_project/`

### Finding

Excess readmission ratio drops steadily as star rating increases:

| Rating | Excess Readmission Ratio |
|---|---|
| 1-star | 1.048 |
| 2-star | 1.025 |
| 3-star | 1.006 |
| 4-star | 0.986 |
| 5-star | 0.966 |

The pattern holds without noise: CMS's star rating, a composite of many quality measures, does track with a real outcome metric. This validates that the rating system is measuring something meaningful, not just a marketing number.

### Why dbt

Adding dbt on top of the existing Airflow + BigQuery setup separates orchestration (Airflow: when and in what order things run) from transformation logic (dbt: how raw tables become analysis-ready models), with tests enforcing data quality at the modeling layer rather than trusting the pipeline blindly.

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

## Dashboard

An interactive Tableau dashboard (`US_Hospital_Quality_Dashboard.twbx`) visualizes both pipelines' output:
- A US map showing average hospital rating by state, with a parameter to switch the metric to hospital count instead
- A breakdown of hospitals by ownership type
- Summary KPI tiles: national average rating, and average excess readmission ratio for 1-star vs. 5-star hospitals
- A scatter plot ("Do Higher Star Ratings Predict Lower Readmissions?") plotting overall rating against excess readmission ratio, colored by ownership type, with an overall trend line and filters for readmission measure (via a parameter) and state

To view it, download the file and open it with [Tableau Desktop](https://www.tableau.com/products/desktop) or the free [Tableau Reader](https://www.tableau.com/products/reader).

## Running it locally

1. Clone this repo
2. Create the required folders and `.env` file: