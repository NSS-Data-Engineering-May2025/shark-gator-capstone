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
from shark_gator_tasks import _run_attacks_to_snowflake_script


with DAG(
    dag_id='attacks_snowflake_ingestion',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['attacks', 'snowflake']
) as dag:
    attacks_to_snowflake_task = PythonOperator(
        task_id= 'attacks_to_snowflake',
        python_callable= _run_attacks_to_snowflake_script,
        provide_context=True,
    )