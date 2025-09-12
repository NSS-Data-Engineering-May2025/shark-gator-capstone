from dotenv import load_dotenv
import os
from minio import Minio, S3Error
import pandas as pd
from io import BytesIO
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timezone

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logs.setup_logger import setup_logger

load_dotenv()

logger = setup_logger('bronze', 'red_list_to_snowflake')

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL_HOST_PORT = os.getenv("MINIO_EXTERNAL_URL")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA_BRONZE = os.getenv("SNOWFLAKE_SCHEMA_BRONZE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

def load_to_snowflake(folder_name, shark_or_gator):
    logger.info(f"Fetching data from MINIO_BUCKET: {MINIO_BUCKET_NAME!r} ({type(MINIO_BUCKET_NAME)})")
    
    minio_client = Minio(
        MINIO_URL_HOST_PORT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    logger.info("Connected to Minio client successfully.")

    objects_from_minio = list(minio_client.list_objects(
        bucket_name=MINIO_BUCKET_NAME,
        prefix=f"shark-gator-capstone/{folder_name}/",
        recursive=True
    ))
    if not objects_from_minio:
        logger.warning(f"No objects found in MinIO bucket: {MINIO_BUCKET_NAME} with prefix: shark-gator-capstone/{folder_name}/")
        raise Exception(f"No objects found in MinIO bucket: {MINIO_BUCKET_NAME} with prefix: shark-gator-capstone/{folder_name}/")
    for obj in objects_from_minio:
        if obj.object_name.endswith('.csv') and not obj.object_name.endswith('_with_html.csv'):
            logger.info(f"Found csv file: {obj.object_name}")
            file_name = os.path.splitext(os.path.basename(obj.object_name).split("-")[0])[0]
            target_table = f"RAW_{shark_or_gator}_RED_LIST_{file_name.upper().replace(' ', '_').replace('.csv','')}"
            try:
                response = minio_client.get_object(
                    bucket_name=MINIO_BUCKET_NAME,
                    object_name=obj.object_name
                )
                csv_bytes_io = BytesIO(response.read())
                response.close()
                response.release_conn()
                logger.info(f"Downloaded {obj.object_name} from MinIO")
                data = pd.read_csv(csv_bytes_io)

                data['SOURCE_FILE'] = obj.object_name
                data['LOAD_TIMESTAMP_UTC'] = datetime.now(timezone.utc)

                data.columns = data.columns.str.upper()
                
                logger.info(f"Added {len(data)} rows from {obj.object_name} to queue")
                try:
                    conn = snowflake.connector.connect(
                        account=SNOWFLAKE_ACCOUNT,
                        user=SNOWFLAKE_USER,
                        password=SNOWFLAKE_PASSWORD,
                        warehouse=SNOWFLAKE_WAREHOUSE,
                        database=SNOWFLAKE_DATABASE,
                        schema=SNOWFLAKE_SCHEMA_BRONZE,
                        role=SNOWFLAKE_ROLE
                    )
                    logger.info("Connected to Snowflake successfully.")
                except Exception as e:
                    logger.error(f"Error connecting to Snowflake: {e}")
                    raise
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=data,
                    table_name=target_table,
                    auto_create_table=True,
                    schema=SNOWFLAKE_SCHEMA_BRONZE,
                    database=SNOWFLAKE_DATABASE,
                    overwrite=True,
                    quote_identifiers=True,  # had a column that included a '/' in the title
                    use_logical_type=True  # Use logical types for better compatibility
                )

                if success:
                    logger.info(f"Data loaded successfully to Snowflake table: {target_table!r} with {nrows} rows and {nchunks} chunks.")
                else:
                    logger.error(f"Failed to load data to Snowflake table: {target_table!r}")
                    raise Exception("Data loading to Snowflake using write_pandas failed.")
            
            except S3Error as s3_err:
                logger.error(f"MinIO error for {obj.object_name}: {s3_err}")
            except Exception as e:
                logger.error(f"An unexpected error occurred processing {obj.object_name}: {e}")
            
        
def main():
    SHARK_RED_LIST_FOLDER = "shark-red-list"
    GATOR_RED_LIST_FOLDER = "gator-red-list"
    SHARK_TABLE= "SHARK"
    GATOR_TABLE= "GATOR"
    try:
        load_to_snowflake(SHARK_RED_LIST_FOLDER, SHARK_TABLE)
        load_to_snowflake(GATOR_RED_LIST_FOLDER, GATOR_TABLE)
        logger.info("Red list data loaded to snowflake successfully.")
    except Exception as e:
        logger.error(f"Error loading red list data to Snowflake: {e}")


if __name__ == "__main__":
    main()