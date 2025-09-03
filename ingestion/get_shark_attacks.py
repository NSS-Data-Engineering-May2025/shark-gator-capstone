import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import pandas as pd
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from datetime import datetime, timezone
from logs.setup_logger import setup_logger


load_dotenv()

logger = setup_logger('ingestion', 'shark_attacks_to_minio')

HTML_URL = os.getenv('SHARK_ATTACKS_HTML')
if not HTML_URL:
    logger.error("SHARK_ATTACKS_HTML is not set in the environment variables.")

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

def scrape_website(url):
    try:
        logger.info(f"Fetching data from {url}")
        response = requests.get(url)
        response.raise_for_status()  
    except requests.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return None
    
    html = response.text

    tables = pd.read_html(html)
    
    soup = BeautifulSoup(html, 'lxml')
    headers = [h2.get_text(strip=True) for h2 in soup.find_all('h2')]
    
    return headers, tables

def load_to_minio(headers, tables):
    time_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"MINIO_BUCKET_NAME:{MINIO_BUCKET_NAME!r} ({type(MINIO_BUCKET_NAME)})")

    try:
        for header, table in zip(headers, tables):
            object_name = f"shark-gator-capstone/shark-attacks/{header}-{time_stamp}.csv"
            logger.info(f"object_name: {object_name!r} ({type(object_name)})")
            logger.info(f"Writing data from {header} in {header}-{time_stamp}.csv to MinIO bucket {MINIO_BUCKET_NAME}")
            csv_buffer = BytesIO()
            table.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            minio_client.put_object(
                bucket_name=MINIO_BUCKET_NAME,
                object_name=object_name,
                data=csv_buffer,
                length=csv_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            logger.info(f"Successfully uploaded {object_name}, {table.shape[0]} records uploaded into MinIO bucket {MINIO_BUCKET_NAME}.")
        logger.info("Finished loading all shark attack data to MinIO successfully.")

    except S3Error as s3_err:
        logger.error(f"MinIO error uploading to MinIO: {s3_err}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during MinIO upload: {e}")

def main():
    csv_name, csv_info= scrape_website(HTML_URL)
    if csv_info and csv_name is not None:
        load_to_minio(csv_name, csv_info)

if __name__ == "__main__":
    main()