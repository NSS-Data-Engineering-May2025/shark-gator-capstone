import os
import subprocess
import time
import logging
import configparser
import sys
import datetime 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
AIRFLOW_CFG_PATH = os.path.join(AIRFLOW_HOME, "airflow.cfg")

def get_airflow_executable_path():
    user_bin_path = os.path.join(os.path.expanduser("~airflow"), ".local", "bin")
    airflow_exec_path = os.path.join(user_bin_path, "airflow")
    if os.path.exists(airflow_exec_path):
        return airflow_exec_path
    logger.warning(f"Airflow executable not found at {airflow_exec_path}. Trying default PATH.")
    return "airflow"

AIRFLOW_EXEC = get_airflow_executable_path()


def run_command(command, cwd=None, env=None, check=True):
    logger.info(f"Running command: {' '.join(command)}")
    process = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True)
    if check and process.returncode != 0:
        logger.error(f"Command failed with exit code {process.returncode}")
        logger.error(f"STDOUT: {process.stdout}")
        logger.error(f"STDERR: {process.stderr}")
        raise subprocess.CalledProcessError(process.returncode, command, process.stdout, process.stderr)
    else:
        logger.info(f"STDOUT: {process.stdout}")
        if process.stderr:
            logger.warning(f"STDERR: {process.stderr}")
    return process.stdout

def modify_airflow_cfg(section, key, value):
    logger.info(f"Modifying airflow.cfg: [{section}] {key} = {value}")
    config = configparser.ConfigParser()
    
    if os.path.exists(AIRFLOW_CFG_PATH):
        config.read(AIRFLOW_CFG_PATH)
    else:
        logger.warning(f"airflow.cfg not found at {AIRFLOW_CFG_PATH}. It will be created or read after db migrate.")

    if section not in config:
        config[section] = {}
    
    config[section][key] = value

    with open(AIRFLOW_CFG_PATH, 'w') as configfile:
        config.write(configfile)
    logger.info(f"Successfully updated airflow.cfg: [{section}] {key} = {value}")


def main():
    logger.info("Starting Dev Container Airflow setup (core configuration only)...")

    os.environ['AIRFLOW_HOME'] = AIRFLOW_HOME

    os.makedirs(AIRFLOW_HOME, exist_ok=True)
    run_command(["sudo", "chown", "-R", "airflow:airflow", AIRFLOW_HOME], check=False)
    run_command(["sudo", "chmod", "-R", "775", AIRFLOW_HOME], check=False)
    logger.info(f"Ensured {AIRFLOW_HOME} exists and is writable.")

    # Adjust permissions (ensure airflow user owns mounted volumes)
    logger.info("Adjusting permissions for mounted volumes...")
    try:
        run_command(["sudo", "chown", "-R", "airflow:airflow", os.path.join(AIRFLOW_HOME, 'airflow')], check=False)
        run_command(["sudo", "chmod", "-R", "775", os.path.join(AIRFLOW_HOME, 'airflow')], check=False)
        logger.info("Permissions adjusted.")
    except Exception as e:
        logger.error(f"Failed to adjust permissions: {e}. This might cause issues.")


    # Force SQLite connection string (no Postgres fallback)
    sql_alchemy_conn = f"sqlite:////{AIRFLOW_HOME}/airflow.db"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = sql_alchemy_conn

    # Initialize Airflow DB
    logger.info(f"Initializing Airflow database using SQLite: {sql_alchemy_conn} ...")
    run_command([AIRFLOW_EXEC, "db", "init"], cwd=AIRFLOW_HOME, env=os.environ.copy())
    logger.info("Airflow database initialized.")

    # Force config values
    modify_airflow_cfg("core", "executor", "SequentialExecutor")
    modify_airflow_cfg("database", "sql_alchemy_conn", sql_alchemy_conn)
    modify_airflow_cfg("core", "dags_folder", os.path.join(AIRFLOW_HOME, "dags"))
    modify_airflow_cfg("core", "load_examples", "False")
    modify_airflow_cfg("webserver", "pid", os.path.join(AIRFLOW_HOME, "airflow-webserver.pid"))

    # Create Airflow user
    logger.info("Creating Airflow user...")
    admin_username = os.getenv("_AIRFLOW_WWW_USER_USERNAME", "airflow")
    admin_password = os.getenv("_AIRFLOW_WWW_USER_PASSWORD", "airflow")
    admin_email = "airflow@example.com"
    
    try:
        run_command([
            AIRFLOW_EXEC, "users", "create",
            "--username", admin_username,
            "--firstname", "Airflow",
            "--lastname", "User",
            "--role", "Admin",
            "--email", admin_email,
            "--password", admin_password
        ], cwd=AIRFLOW_HOME, env=os.environ.copy())
        logger.info(f"Airflow user '{admin_username}' created.")
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr:
            logger.warning(f"Airflow user '{admin_username}' already exists. Skipping creation.")
        else:
            raise

    logger.info("Airflow core setup complete. Webserver and Scheduler will be started manually.")

if __name__ == "__main__":
    main()