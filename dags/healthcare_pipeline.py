from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.bash import BashOperator
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
READMISSIONS_API_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/9n3s-kdb3/0"


def clean_hospital_record(raw_record):
    """Pure function: shape one raw API record into our clean schema."""
    return {
        'facility_id': raw_record.get('facility_id'),
        'facility_name': raw_record.get('facility_name'),
        'state': raw_record.get('state'),
        'ownership_type': raw_record.get('hospital_ownership'),
        'overall_rating': raw_record.get('hospital_overall_rating'),
    }

def clean_readmission_record(raw_record):
    """Pure function: shape one raw readmissions record into our clean schema."""
    return {
        'facility_id': raw_record.get('facility_id'),
        'measure_name': raw_record.get('measure_name'),
        'excess_readmission_ratio': raw_record.get('excess_readmission_ratio'),
        'predicted_readmission_rate': raw_record.get('predicted_readmission_rate'),
        'expected_readmission_rate': raw_record.get('expected_readmission_rate'),
        'start_date': raw_record.get('start_date'),
        'end_date': raw_record.get('end_date'),
    }


def _is_number(value):
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def is_valid_readmission_record(record):
    """Pure function: is this readmission record usable for analysis?"""
    ratio = record.get('excess_readmission_ratio')
    return (
        bool(record.get('facility_id'))
        and bool(record.get('measure_name'))
        and ratio not in (None, 'N/A', 'Not Available')
        and _is_number(ratio)
    )


def is_valid_record(record):
    """Pure function: does this record have enough info to be useful?"""
    return (
        bool(record.get('facility_id'))
        and bool(record.get('state'))
        and record.get('overall_rating') not in (None, 'Not Available')
    )

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

def extract_readmissions_data(**context):
    """Pull hospital readmissions data from the CMS API, paginating through all records."""
    all_results = []
    offset = 0
    page_size = 500

    while True:
        response = requests.get(READMISSIONS_API_URL, params={"limit": page_size, "offset": offset})
        response.raise_for_status()
        data = response.json()
        page_results = data.get('results', [])

        if not page_results:
            break

        all_results.extend(page_results)
        offset += page_size

        if len(page_results) < page_size:
            break

    combined_data = {'results': all_results}
    context['ti'].xcom_push(key='raw_readmissions_data', value=combined_data)
    print(f"Extracted {len(all_results)} readmission records across {offset // page_size} pages")

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

def land_readmissions_in_minio(**context):
    """Upload the untouched raw readmissions API response into MinIO's raw zone."""
    raw_data = context['ti'].xcom_pull(key='raw_readmissions_data', task_ids='extract_readmissions_data')

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    run_date = context['ds']
    object_key = f"raw/readmissions_data_{run_date}.json"

    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=json.dumps(raw_data),
    )

    context['ti'].xcom_push(key='minio_readmissions_object_key', value=object_key)
    print(f"Landed raw readmissions data at s3://{MINIO_BUCKET}/{object_key}")

def transform_hospital_data(**context):
    """Clean the raw hospital data and compute state-level aggregates."""
    raw_data = context['ti'].xcom_pull(key='raw_hospital_data', task_ids='extract_hospital_data')
    records = raw_data.get('results', raw_data if isinstance(raw_data, list) else [])

    cleaned_records = [clean_hospital_record(r) for r in records]
    valid_records = [r for r in cleaned_records if is_valid_record(r)]

    context['ti'].xcom_push(key='cleaned_records', value=cleaned_records)
    context['ti'].xcom_push(key='valid_records', value=valid_records)
    print(f"Transformed {len(cleaned_records)} records, {len(valid_records)} have valid ratings")

def transform_readmissions_data(**context):
    """Clean the raw readmissions data."""
    raw_data = context['ti'].xcom_pull(key='raw_readmissions_data', task_ids='extract_readmissions_data')
    records = raw_data.get('results', raw_data if isinstance(raw_data, list) else [])

    cleaned_records = [clean_readmission_record(r) for r in records]
    valid_records = [r for r in cleaned_records if is_valid_readmission_record(r)]

    context['ti'].xcom_push(key='cleaned_readmission_records', value=cleaned_records)
    context['ti'].xcom_push(key='valid_readmission_records', value=valid_records)
    print(f"Transformed {len(cleaned_records)} readmission records, {len(valid_records)} are valid")

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
        bigquery.SchemaField("facility_id", "STRING"),
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

def load_readmissions_to_bigquery(**context):
    """Load the cleaned readmissions records into a BigQuery table."""
    valid_records = context['ti'].xcom_pull(key='valid_readmission_records', task_ids='transform_readmissions_data')

    client = bigquery.Client(project=GCP_PROJECT_ID)
    dataset_ref = client.dataset(BQ_DATASET)

    table_ref = dataset_ref.table('hospital_readmissions')

    schema = [
        bigquery.SchemaField("facility_id", "STRING"),
        bigquery.SchemaField("measure_name", "STRING"),
        bigquery.SchemaField("excess_readmission_ratio", "FLOAT"),
        bigquery.SchemaField("predicted_readmission_rate", "FLOAT"),
        bigquery.SchemaField("expected_readmission_rate", "FLOAT"),
        bigquery.SchemaField("start_date", "STRING"),
        bigquery.SchemaField("end_date", "STRING"),
    ]

    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print("Created table hospital_readmissions")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    load_job = client.load_table_from_json(
        valid_records,
        table_ref,
        job_config=job_config,
    )
    load_job.result()

    print(f"Loaded {len(valid_records)} readmission records into BigQuery")

with DAG(
    dag_id="healthcare_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["learning", "healthcare", "project2", "project3"],
) as dag:

    run_dbt_task = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt_project && dbt run",
    )

    test_dbt_task = BashOperator(
        task_id="test_dbt_models",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )
    # --- Hospital ratings branch ---
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

    # --- Readmissions branch ---
    extract_readmissions_task = PythonOperator(
        task_id="extract_readmissions_data",
        python_callable=extract_readmissions_data,
    )

    land_readmissions_task = PythonOperator(
        task_id="land_readmissions_in_minio",
        python_callable=land_readmissions_in_minio,
    )

    transform_readmissions_task = PythonOperator(
        task_id="transform_readmissions_data",
        python_callable=transform_readmissions_data,
    )

    load_readmissions_task = PythonOperator(
        task_id="load_readmissions_to_bigquery",
        python_callable=load_readmissions_to_bigquery,
    )

       # Both branches run independently in parallel, since they don't depend on each other
    extract_task >> land_task >> transform_task >> load_task
    extract_readmissions_task >> land_readmissions_task >> transform_readmissions_task >> load_readmissions_task

    # dbt models depend on both raw tables existing, so it waits for both branches to finish
    [load_task, load_readmissions_task] >> run_dbt_task >> test_dbt_task