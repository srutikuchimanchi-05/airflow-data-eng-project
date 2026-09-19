from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import json
import sqlite3

# Step 1: EXTRACT — pull current weather for Chicago (near you) from a free public API
def extract_weather(**context):
    url = "https://api.open-meteo.com/v1/forecast?latitude=41.85&longitude=-87.65&current_weather=true"
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read())
    # Push the raw data to XCom so the next task can use it
    context['ti'].xcom_push(key='raw_weather', value=data)
    print("Extracted:", data)

# Step 2: TRANSFORM — pull out just the fields we care about
def transform_weather(**context):
    raw = context['ti'].xcom_pull(key='raw_weather', task_ids='extract_weather')
    current = raw['current_weather']
    cleaned = {
        'temperature_c': current['temperature'],
        'windspeed_kmh': current['windspeed'],
        'observed_at': current['time'],
    }
    context['ti'].xcom_push(key='clean_weather', value=cleaned)
    print("Transformed:", cleaned)

# Step 3: LOAD — write it into a local SQLite database file
def load_weather(**context):
    clean = context['ti'].xcom_pull(key='clean_weather', task_ids='transform_weather')

    conn = sqlite3.connect('/opt/airflow/dags/weather.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            temperature_c REAL,
            windspeed_kmh REAL,
            observed_at TEXT
        )
    ''')
    cursor.execute('''
        INSERT INTO weather_readings (temperature_c, windspeed_kmh, observed_at)
        VALUES (?, ?, ?)
    ''', (clean['temperature_c'], clean['windspeed_kmh'], clean['observed_at']))
    conn.commit()
    conn.close()
    print("Loaded into database:", clean)

# Define the DAG itself
with DAG(
    dag_id="weather_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["learning", "weather", "project1"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    load_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    # This line defines the order: extract, then transform, then load
    extract_task >> transform_task >> load_task