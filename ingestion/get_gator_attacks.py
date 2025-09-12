import requests
from dotenv import load_dotenv
import os
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from datetime import datetime, timezone
import pandas as pd

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logs.setup_logger import setup_logger

load_dotenv()

logger = setup_logger('ingestion', 'gator_attacks_to_minio')

COLUMN_NAMES= [
    "INCIDENT_NUMBER",
    "SPECIES",
    "LATITUDE",
    "LONGITUDE",
    "PROVINCE/STATE",
    "COUNTRY",
    "OUTCOME",
    "DATE",
    "CONTRIBUTOR"

]

API_ENDPOINT = os.getenv('GATOR_ATTACKS_API')
if not API_ENDPOINT:
    logger.error("GATOR_ATTACKS_API is not set in the environment variables.")

# Credentials
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_URL_HOST_PORT = os.getenv('MINIO_EXTERNAL_URL')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

def fetch_api_data(endpoint):
    try:
        logger.info(f"Fetching data from {endpoint}")
        response = requests.get(endpoint)
        response.raise_for_status()
        data= response.json()['data']
        gator_attacks_info= pd.DataFrame(data, columns=COLUMN_NAMES)
        gator_attacks_csv= gator_attacks_info.to_csv(index=False)
        return gator_attacks_csv
    except requests.RequestException as e:
        logger.error(f"Error fetching API data: {e}")
        return None
    
def load_to_minio(data):
    time_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"MINIO_BUCKET_NAME:{MINIO_BUCKET_NAME!r} ({type(MINIO_BUCKET_NAME)})")

    try:
        minio_client = Minio(
                endpoint=MINIO_URL_HOST_PORT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
        object_name = f"shark-gator-capstone/gator-attacks/gator-attacks-{time_stamp}.csv"
        logger.info(f"object_name: {object_name!r} ({type(object_name)})")
        logger.info(f"Uploading data to MinIO bucket {MINIO_BUCKET_NAME} with object name {object_name}")
        csv_buffer = BytesIO()
        csv_buffer.write(data.encode('utf-8'))
        csv_buffer.seek(0)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
            )
        logger.info(f"Successfully uploaded {object_name}, {len(data)} records uploaded into MinIO bucket {MINIO_BUCKET_NAME}.")
        logger.info("Finished loading all gator attack data to MinIO successfully.")

    except S3Error as s3_err:
        logger.error(f"MinIO error uploading to MinIO: {s3_err}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during MinIO upload: {e}")


def main():
    gator_attacks_data= fetch_api_data(API_ENDPOINT)
    if gator_attacks_data is not None:
        load_to_minio(gator_attacks_data)

if __name__ == "__main__":
    main()