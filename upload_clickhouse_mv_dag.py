#PythonOperator для обновления отчетов в ClickHouse
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import io
import requests

# Настройки ClickHouse
CLICKHOUSE_URL = 'http://172.17.0.2:8123' #'http://lab05-clickhouse-server:8123'#'http://89.208.211.89:8123'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''


def refresh_table():
    for mv in ['mv_rep1_events_per_hour',
'mv_rep2_products_per_hour',
'mv_rep2_products_per_hour_browsers',
'mv_rep3_top10_pages_buy',
'mv_rep3_top10_pages_buy_browser',
'mv_rep4_buys_source',
'mv_rep5_users_segment',
'mv_rep6_1_events_per_our_browsers',
'mv_rep6_4_profile']:
        response = requests.post(
            f'{CLICKHOUSE_URL}/?query=SYSTEM REFRESH VIEW cdm.{mv}',
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
        )
        response.raise_for_status()
        print(f'⚠️ Table {mv} refreshed')

# DAG и таска
with DAG(
    dag_id='upload_clickhouse_mv',
    start_date=datetime(2025, 4, 29),
    schedule_interval=None,
    catchup=False,
    tags=['clickhouse', 'mv'],
) as dag:
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    truncate_task = PythonOperator(
        task_id='refresh_table',
        python_callable=refresh_table,
        provide_context=True,
    )
    

    start >> truncate_task >> finish