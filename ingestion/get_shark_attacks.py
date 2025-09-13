import requests
from dotenv import load_dotenv
import os
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from datetime import datetime, timezone

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logs.setup_logger import setup_logger


load_dotenv()

logger = setup_logger('ingestion', 'shark_attacks_to_minio')

delimiter= ','
preserve_str_with_comma= "true"
HTML_URL = os.getenv('SHARK_ATTACKS_CSV_DOWNLOAD')
if not HTML_URL:
    logger.error("SHARK_ATTACKS_CSV_DOWNLOAD is not set in the environment variables.")

# Credentials
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_URL_HOST_PORT = os.getenv('MINIO_EXTERNAL_URL')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

# Instantiate MinIO client
minio_client = Minio(
    endpoint=MINIO_URL_HOST_PORT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def get_csv_data(url):
    try:
        logger.info(f"Downloading CSV data from {url}")
        response = requests.get(url, params={
            "delimiter": delimiter,
            "quote_all": preserve_str_with_comma
        })
        response.raise_for_status()
        response_bytes= response.content
        return response_bytes
    except requests.RequestException as e:
        logger.error(f"Error downloading CSV data: {e}")
        return None

def load_to_minio(csv_bytes):
    time_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    logger.info(f"MINIO_BUCKET_NAME:{MINIO_BUCKET_NAME!r} ({type(MINIO_BUCKET_NAME)})")
    object_name = f"shark-gator-capstone/shark-attacks/Global-Shark-Attack-Database-{time_stamp}.csv"
    logger.info(f"object_name: {object_name!r} ({type(object_name)})")
    try:
        csv_text = csv_bytes.decode('utf-8')
        record_count = csv_text.count('\n')
        csv_buffer = BytesIO(csv_bytes)
        csv_buffer.seek(0)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
            )
        logger.info(f"Successfully uploaded {object_name}, {record_count} records uploaded into MinIO bucket {MINIO_BUCKET_NAME}.")
        logger.info("Finished loading all shark attack data to MinIO successfully.")

    except S3Error as s3_err:
        logger.error(f"MinIO error uploading to MinIO: {s3_err}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during MinIO upload: {e}")

def main():
    shark_bytes = get_csv_data(HTML_URL)
    if shark_bytes is not None:
        load_to_minio(shark_bytes)

if __name__ == "__main__":
    main()