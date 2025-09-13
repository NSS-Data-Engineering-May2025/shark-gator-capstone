import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..")) 
# define absolute path
PROJECT_ROOT_IN_AIRFLOW='/opt/airflow'
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)
from shark_gator_tasks import _run_gator_attacks_ingestion_script


with DAG(
    dag_id='gator_attacks_ingestion',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['gator', 'ingestion', 'minio']
) as dag:
    pull_gator_attacks_to_minio_task= PythonOperator(
        task_id= 'pull_gator_attacks_to_minio',
        python_callable= _run_gator_attacks_ingestion_script,
        provide_context=True,
    )
