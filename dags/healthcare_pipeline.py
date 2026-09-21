from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import boto3
from google.cloud import bigquery
import os

# --- Configuration ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET = "healthcare-raw"

GCP_PROJECT_ID = "healthcare-pipeline-509120"
GCP_CREDENTIALS_PATH = "/opt/airflow/gcp-credentials.json"
BQ_DATASET = "hospital_data"
BQ_TABLE = "hospital_ratings"

CMS_API_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"

def clean_hospital_record(raw_record):
    """Pure function: shape one raw API record into our clean schema."""
    return {
        'facility_name': raw_record.get('facility_name'),
        'state': raw_record.get('state'),
        'ownership_type': raw_record.get('hospital_ownership'),
        'overall_rating': raw_record.get('hospital_overall_rating'),
    }


def is_valid_record(record):
    """Pure function: does this record have enough info to be useful?"""
    return bool(record.get('state')) and record.get('overall_rating') not in (None, 'Not Available')

def extract_hospital_data(**context):
    """Pull hospital general information from the CMS API, paginating through all records."""
    all_results = []
    offset = 0
    page_size = 500

    while True:
        response = requests.get(CMS_API_URL, params={"limit": page_size, "offset": offset})
        response.raise_for_status()
        data = response.json()
        page_results = data.get('results', [])

        if not page_results:
            break  # no more data, stop paginating

        all_results.extend(page_results)
        offset += page_size

        if len(page_results) < page_size:
            break  # last page was partial, we've reached the end

    combined_data = {'results': all_results}
    context['ti'].xcom_push(key='raw_hospital_data', value=combined_data)
    print(f"Extracted {len(all_results)} hospital records across {offset // page_size} pages")


def land_raw_data_in_minio(**context):
    """Upload the untouched raw API response into MinIO's raw zone."""
    raw_data = context['ti'].xcom_pull(key='raw_hospital_data', task_ids='extract_hospital_data')

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    run_date = context['ds']  # Airflow's built-in "execution date" as YYYY-MM-DD
    object_key = f"raw/hospital_data_{run_date}.json"

    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=json.dumps(raw_data),
    )

    context['ti'].xcom_push(key='minio_object_key', value=object_key)
    print(f"Landed raw data at s3://{MINIO_BUCKET}/{object_key}")

def transform_hospital_data(**context):
    """Clean the raw hospital data and compute state-level aggregates."""
    raw_data = context['ti'].xcom_pull(key='raw_hospital_data', task_ids='extract_hospital_data')
    records = raw_data.get('results', raw_data if isinstance(raw_data, list) else [])

    cleaned_records = [clean_hospital_record(r) for r in records]
    valid_records = [r for r in cleaned_records if is_valid_record(r)]

    context['ti'].xcom_push(key='cleaned_records', value=cleaned_records)
    context['ti'].xcom_push(key='valid_records', value=valid_records)
    print(f"Transformed {len(cleaned_records)} records, {len(valid_records)} have valid ratings")

def load_to_bigquery(**context):
    """Load the cleaned hospital records into a BigQuery table."""
    valid_records = context['ti'].xcom_pull(key='valid_records', task_ids='transform_hospital_data')

    client = bigquery.Client(project=GCP_PROJECT_ID)

    dataset_ref = client.dataset(BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {BQ_DATASET}")

    table_ref = dataset_ref.table(BQ_TABLE)

    schema = [
        bigquery.SchemaField("facility_name", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("ownership_type", "STRING"),
        bigquery.SchemaField("overall_rating", "STRING"),
    ]

    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {BQ_TABLE}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(
        valid_records,
        table_ref,
        job_config=job_config,
    )
    load_job.result()  # waits for the job to finish, raises an error if it fails

    print(f"Loaded {len(valid_records)} records into BigQuery")

with DAG(
    dag_id="healthcare_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["learning", "healthcare", "project2"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_hospital_data",
        python_callable=extract_hospital_data,
    )

    land_task = PythonOperator(
        task_id="land_raw_data_in_minio",
        python_callable=land_raw_data_in_minio,
    )

    transform_task = PythonOperator(
        task_id="transform_hospital_data",
        python_callable=transform_hospital_data,
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    extract_task >> land_task >> transform_task >> load_task
