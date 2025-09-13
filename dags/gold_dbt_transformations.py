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
from shark_gator_tasks import _run_dbt_commands


with DAG(
    dag_id='gold_dbt_transformations',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'dbt', 'transformations']
) as dag:
    run_dbt_gold_models = PythonOperator(
        task_id='run_dbt_gold_models',
        python_callable=_run_dbt_commands,
        op_kwargs={'dbt_command': 'run --profile shark_gator_dbt --select gold'}
    )