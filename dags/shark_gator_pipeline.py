import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from dotenv import load_dotenv
import subprocess


load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), "..")) 

# define absolute path
PROJECT_ROOT_IN_AIRFLOW='/opt/airflow'
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)

# define absolute path of dbt within Airflow container
DBT_PROJECT_DIR_IN_AIRFLOW = '/opt/airflow/dbt/shark_gator_dbt'

# define path for dbt profiles
DBT_PROFILES_DIR_IN_AIRFLOW = '/opt/airflow/dbt/shark_gator_dbt'

try: 
    from ingestion.get_gator_attacks import main as gator_attacks_ingestion_main
    from ingestion.get_shark_attacks import main as shark_attacks_ingestion_main
    from ingestion.attacks_to_snowflake import main as attacks_to_snowflake_main
    from ingestion.red_list_to_snowflake import main as red_list_to_snowflake_main
except ImportError as e:
    print(f"ERROR Could not import external scripts from main. Please check volume mounts and PYTHONPATH in Airflow containers.")
    print(f"Error Importing: {e}")
    raise

# define pyhton callables for PythonOperator that you are importing from main
def _run_gator_attacks_ingestion_script(**kwargs):
    kwargs['ti'].log.info(f"Running job_postings_ingestion_main for DAG run {kwargs['dag_run'].run_id}")
    gator_attacks_ingestion_main()
    kwargs['ti'].log.info(f"Finished running job_postings_ingestion_main")

def _run_shark_attacks_ingestion_script(**kwargs):
    kwargs['ti'].log.info(f"Running payroll_ingestion_main for DAG run {kwargs['dag_run'].run_id}")
    shark_attacks_ingestion_main()
    kwargs['ti'].log.info(f"Finished running payroll_ingestion_main")

def _run_attacks_to_snowflake_script(**kwargs):
    kwargs['ti'].log.info(f"Running lightcast_ingestion for DAG run {kwargs['dag_run'].run_id}")
    attacks_to_snowflake_main()
    kwargs['ti'].log.info(f"Finished running lightcast_ingestion")

def _run_red_list_to_snowflake_script(**kwargs):
    kwargs['ti'].log.info(f"Running load_parquet_snowflake for DAG run {kwargs['dag_run'].run_id}")
    red_list_to_snowflake_main()
    kwargs['ti'].log.info(f"Finished running load_parquet_snowflake")

def _run_dbt_commands(dbt_command, **kwargs):
    ti= kwargs['ti']

    dbt_env = os.environ.copy()
    dbt_env['DBT_PROFILES_DIR'] = DBT_PROFILES_DIR_IN_AIRFLOW

    # dbt_env['SNOWFLAKE_ACCOUNT'] = os.getenv('SNOWFLAKE_ACCOUNT')
    # dbt_env['SNOWFLAKE_USER'] = os.getenv('SNOWFLAKE_USER')
    # dbt_env['SNOWFLAKE_PASSWORD'] = os.getenv('SNOWFLAKE_PASSWORD')
    # dbt_env['SNOWFLAKE_DATABASE'] = os.getenv('SNOWFLAKE_DATABASE')
    # dbt_env['SNOWFLAKE_SCHEMA_BRONZE'] = os.getenv('SNOWFLAKE_SCHEMA_BRONZE')
    # dbt_env['SNOWFLAKE_WAREHOUSE'] = os.getenv('SNOWFLAKE_WAREHOUSE')
    # dbt_env['SNOWFLAKE_ROLE'] = os.getenv('SNOWFLAKE_ROLE')
    # dbt_env['DBT_SCHEMA']= os.getenv('SNOWFLAKE_SCHEMA_SILVER')

    full_terminal_command = f"dbt {dbt_command}"

    process = subprocess.run(
        full_terminal_command,
        cwd= DBT_PROJECT_DIR_IN_AIRFLOW,
        shell=True,
        env=dbt_env,
        capture_output=True,
        text=True
    )

    if process.returncode != 0:
        print(f"Error running dbt command: {dbt_command}")
        print(f"Error message: {process.stderr}, output: {process.stdout}")
        raise ValueError(f"DBT command exited with non-zero status: {process.returncode}")
    else:
        print(f"DBT command '{full_terminal_command}' executed successfully.")
        print(f"Output: {process.stdout}")


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