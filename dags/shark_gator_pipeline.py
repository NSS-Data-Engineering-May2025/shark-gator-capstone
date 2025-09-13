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
from shark_gator_tasks import (
    _run_gator_attacks_ingestion_script,
    _run_shark_attacks_ingestion_script,
    _run_attacks_to_snowflake_script,
    _run_red_list_to_snowflake_script,
    _run_dbt_commands
)


with DAG(
    dag_id='shark_gator_pipeline',
    start_date=days_ago(1),
    schedule_interval="@monthly",
    catchup=False,
    tags=['shark_gator', 'final', 'monthly', 'analysis']
) as dag:
    pull_gator_attacks_to_minio_task= PythonOperator(
        task_id= 'pull_gator_attacks_to_minio',
        python_callable= _run_gator_attacks_ingestion_script,
        provide_context=True,
    )

    pull_shark_attacks_to_minio_task= PythonOperator(
        task_id= 'pull_shark_attacks_to_minio',
        python_callable= _run_shark_attacks_ingestion_script,
        provide_context=True,
    )
    
    attacks_to_snowflake_task = PythonOperator(
        task_id= 'attacks_to_snowflake',
        python_callable= _run_attacks_to_snowflake_script,
        provide_context=True,
    )

    red_list_to_snowflake_task = PythonOperator(
        task_id= 'red_list_to_snowflake',
        python_callable= _run_red_list_to_snowflake_script,
        provide_context=True,
    )

    run_dbt_silver_models = PythonOperator(
        task_id='run_dbt_silver_models',
        python_callable=_run_dbt_commands,
        op_kwargs={'dbt_command': 'run --profile shark_gator_dbt --select silver'}
    )
    run_dbt_gold_models = PythonOperator(
        task_id='run_dbt_gold_models',
        python_callable=_run_dbt_commands,
        op_kwargs={'dbt_command': 'run --profile shark_gator_dbt --select gold'}
    )


# set task dependencies
pull_gator_attacks_to_minio_task >> pull_shark_attacks_to_minio_task >> attacks_to_snowflake_task >> red_list_to_snowflake_task >> run_dbt_silver_models >> run_dbt_gold_models