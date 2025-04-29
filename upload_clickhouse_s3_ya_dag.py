#📦 PythonOperator для загрузки из Yandex S3 в ClickHouse
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import zipfile
import boto3
import io
import requests
from botocore import UNSIGNED
from botocore.config import Config

# Настройки ClickHouse
CLICKHOUSE_URL = 'http://172.17.0.2:8123' #'http://lab05-clickhouse-server:8123'#'http://89.208.211.89:8123'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''


def truncate_table():
    for table in ['browser_events','device_events','geo_events','location_events']:
        response = requests.post(
            f'{CLICKHOUSE_URL}/?query=TRUNCATE TABLE raw.{table}',
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
        )
        response.raise_for_status()
        print(f'⚠️ Table {table} truncated')

def upload_s3_to_clickhouse(**context):
    # Настройки S3
    s3 = boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        config=Config(signature_version=UNSIGNED)
        #aws_access_key_id='YOUR_ACCESS_KEY',
        #aws_secret_access_key='YOUR_SECRET_KEY'
    )

    bucket = 'npl-de16-lab8-data'

    # Настройки ClickHouse
    # CLICKHOUSE_URL = '172.17.0.2' #'http://lab05-clickhouse-server:8123'#'http://89.208.211.89:8123'
    # CLICKHOUSE_USER = 'default'
    # CLICKHOUSE_PASSWORD = ''

    def send_to_clickhouse(table, json_lines):
        response = requests.post(
            f'{CLICKHOUSE_URL}/?query=INSERT INTO raw.{table} FORMAT JSONEachRow',
            data=json_lines.encode(),
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()

    def list_all_s3_objects(bucket):
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get('Contents', []):
                yield obj['Key']

    # Основной цикл
    for key in list_all_s3_objects(bucket):
        if not key.endswith('.zip'):
            continue

        print(f'Processing {key}')
        obj = s3.get_object(Bucket=bucket, Key=key)
        with zipfile.ZipFile(io.BytesIO(obj['Body'].read())) as zf:
            for name in zf.namelist():
                with zf.open(name) as f:
                    json_lines = f.read().decode('utf-8')
                    table = key.split('/')[-1].replace('.jsonl.zip', '')
                    try:
                        send_to_clickhouse(table, json_lines)
                        print(f'✓ Uploaded to {table}')
                    except Exception as e:
                        print(f'✗ Error uploading {key} to {table}: {e}')


# DAG и таска
with DAG(
    dag_id='upload_s3_to_clickhouse',
    start_date=datetime(2025, 4, 22),
    schedule_interval=None,
    catchup=False,
    tags=['clickhouse', 's3', 'yandex'],
) as dag:
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    truncate_task = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table,
        provide_context=True,
    )
    upload_task = PythonOperator(
        task_id='upload_all_files',
        python_callable=upload_s3_to_clickhouse,
        provide_context=True,
    )

    start >> truncate_task >> upload_task >> finish