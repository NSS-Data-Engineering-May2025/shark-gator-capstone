import requests
from dotenv import load_dotenv
import os
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from datetime import datetime, timezone
import pandas as pd
import re
from dateutil import parser

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

def normalize_date(date_value):
    # Try to parse as a full date
    try:
        return parser.parse(str(date_value)).date()
    except Exception:
        pass
    # Try to extract a year
    year_match = re.search(r'\b(20\d{2}|19\d{2})\b', str(date_value))
    if year_match:
        return int(year_match.group(0))
    return None

def fetch_api_data(endpoint):
    try:
        logger.info(f"Fetching data from api: {endpoint}")
        response = requests.get(endpoint)
        response.raise_for_status()
        data= response.json()['data']
        # ! WORK IN PROGRESS !#
        date_column= data[7]
        most_recent_updated_date= '2024-22-11'
        most_recent_updated_year=
        normalized_most_recent_date= normalize_date(most_recent_updated_date)
        for date in date_column:
            normalized_date= normalize_date(date)
            if normalized_date is None or normalized_most_recent_date is None:
                logger.info(f"Skipping unparseable date: {date}")
                continue
            if type(normalized_date) != type(normalized_most_recent_date):
            if normalized_date > normalized_most_recent_date:
                logger.info(f"New data available since last update on {most_recent_updated_date}.")
                most_recent_updated_date= date
                logger.info(f"Updating most recent date to {most_recent_updated_date}.")
                gator_attacks_info= pd.DataFrame(data, columns=COLUMN_NAMES)
                gator_attacks_csv= gator_attacks_info.to_csv(index=False)
                return gator_attacks_csv
            else:
                logger.info(f"No new data available since last update on {most_recent_updated_date}.")
                break
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