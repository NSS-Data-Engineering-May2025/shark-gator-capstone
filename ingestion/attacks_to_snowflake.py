from dotenv import load_dotenv
import logging
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

logger = setup_logger('bronze', 'attacks_to_snowflake')

MINIO_ACCESS_KEY= os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY= os.getenv("MINIO_SECRET_KEY")
MINIO_URL_HOST_PORT= os.getenv("MINIO_EXTERNAL_URL")
MINIO_BUCKET_NAME=os.getenv("MINIO_BUCKET_NAME")

SNOWFLAKE_ACCOUNT= os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER= os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD= os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE= os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA_BRONZE=os.getenv("SNOWFLAKE_SCHEMA_BRONZE")
SNOWFLAKE_WAREHOUSE= os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE= os.getenv("SNOWFLAKE_ROLE")

def get_data_from_minio(folder_name):
    logger.info(f"Fetching data from MINIO_BUCKET: {MINIO_BUCKET_NAME!r} ({type(MINIO_BUCKET_NAME)})")
    
    minio_client= Minio(
        MINIO_URL_HOST_PORT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    logger.info("Connected to Minio client successfully.")

    objects_from_minio= minio_client.list_objects(
        bucket_name=MINIO_BUCKET_NAME,
        prefix=f"shark-gator-capstone/{folder_name}/",
        recursive=True
        )
    for obj in objects_from_minio:
        if obj.object_name.endswith('.csv'):
            logger.info(f"Found csv file: {obj.object_name}")
            try:
                response= minio_client.get_object(
                    bucket_name=MINIO_BUCKET_NAME,
                    object_name=obj.object_name
                )
                csv_bytes_io = BytesIO(response.read())
                response.close()
                response.release_conn()
                logger.info(f"Downloaded {obj.object_name} from MinIO")
                data= pd.read_csv(csv_bytes_io)

                data['SOURCE_FILE'] = obj.object_name
                data['LOAD_TIMESTAMP_UTC'] = datetime.now(timezone.utc)

                data.columns = data.columns.str.upper()
                
                logger.info(f"Added {len(data)} rows from {obj.object_name} to queue")

            except S3Error as s3_err:
                logger.error(f"MinIO error for {obj.object_name}: {s3_err}")
            except Exception as e:
                logger.error(f"An unexpected error occurred processing {obj.object_name}: {e}")
            return data


def load_to_snowflake(target_table, minio_data):
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
        df=minio_data,
        table_name=target_table,
        auto_create_table=True,
        schema=SNOWFLAKE_SCHEMA_BRONZE,
        database=SNOWFLAKE_DATABASE,
        overwrite=True,
        quote_identifiers=True,  #had a column that included a '/' in the title
        use_logical_type=True  # Use logical types for better compatibility
        )

    if success:
        logger.info(f"Data loaded successfully to Snowflake table: {target_table!r} with {nrows} rows and {nchunks} chunks.")
    else:
        logger.error(f"Failed to load data to Snowflake table: {target_table!r}")
        raise Exception("Data loading to Snowflake using write_pandas failed.")



def main():
    SHARK_ATTACKS_FOLDER = "shark-attacks"
    GATOR_ATTACKS_FOLDER = "gator-attacks"
    SHARK_TARGET_TABLE = "RAW_SHARK_ATTACKS"
    GATOR_TARGET_TABLE = "RAW_GATOR_ATTACKS"
    shark_attack_data= get_data_from_minio(SHARK_ATTACKS_FOLDER)
    gator_attack_data=get_data_from_minio(GATOR_ATTACKS_FOLDER)
    load_to_snowflake(SHARK_TARGET_TABLE, shark_attack_data)
    load_to_snowflake(GATOR_TARGET_TABLE, gator_attack_data)

if __name__ == "__main__":
    main()
    logger.info("All attack data loaded to snowflake successfully.")