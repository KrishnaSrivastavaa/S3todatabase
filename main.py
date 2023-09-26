import boto3
import pandas as pd
import psycopg2
import logging
import os
import configparser
from datetime import datetime

# Configure logging
log_dir = 'log'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'app.log')
logging.basicConfig(filename=log_file, level=logging.INFO)

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('config/config.ini')

# AWS S3 configuration
s3_bucket_name = config['s3']['bucket_name']
aws_access_key_id = config['s3']['aws_access_key_id']
aws_secret_access_key = config['s3']['aws_secret_access_key']
aws_region_name = config['s3']['aws_region_name']

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key,
                         region_name=aws_region_name)

# PostgreSQL configuration
db_config = {
    'dbname': config['postgresql']['dbname'],
    'user': config['postgresql']['user'],
    'password': config['postgresql']['password'],
    'host': config['postgresql']['host'],
    'port': config['postgresql']['port'],
}

# Function to create table if it doesn't exist
def create_table_if_not_exists(conn, cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS your_table (
                "User ID" INTEGER,
                "Subscription Type" VARCHAR(50),
                "Monthly Revenue" NUMERIC,
                "Join Date" DATE,
                "Last Payment Date" DATE,
                "Country" VARCHAR(50),
                "Age" INTEGER,
                "Gender" VARCHAR(10),
                "Device" VARCHAR(50),
                "Plan Duration" VARCHAR(20)
            )
        """)
        conn.commit()
        logging.info('Table created or already exists in PostgreSQL')
    except Exception as e:
        logging.error(f'Error creating table in PostgreSQL: {str(e)}')

# Function to preprocess CSV data (including date conversion)
def preprocess_csv(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        # Convert date strings to the PostgreSQL-compatible format
        df['Join Date'] = df['Join Date'].apply(lambda x: convert_date_string(x))
        df['Last Payment Date'] = df['Last Payment Date'].apply(lambda x: convert_date_string(x))
        df.to_csv(output_file, index=False)
        logging.info(f'Preprocessed {input_file} and saved as {output_file}')
    except Exception as e:
        logging.error(f'Error preprocessing {input_file}: {str(e)}')

# Function to convert date string to PostgreSQL-compatible format
def convert_date_string(date_str):
    try:
        date_obj = datetime.strptime(date_str, '%d-%m-%y')
        return date_obj.strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(f'Error converting date string: {str(e)}')
        return None

# Function to upload a file to S3
def upload_file_to_s3(local_file_path, s3_key):
    try:
        s3_client.upload_file(local_file_path, s3_bucket_name, s3_key)
        logging.info(f'Uploaded {local_file_path} to S3 as {s3_key}')
    except Exception as e:
        logging.error(f'Error uploading {local_file_path} to S3: {str(e)}')

# Function to fetch CSV from S3
def fetch_csv_from_s3(s3_key, local_path):
    try:
        s3_client.download_file(s3_bucket_name, s3_key, local_path)
        logging.info(f'Fetched {s3_key} from S3 to {local_path}')
    except Exception as e:
        logging.error(f'Error fetching {s3_key} from S3: {str(e)}')

# Main function
def main():
    # Paths
    csv_filename = 'data/input.csv'
    s3_key_uncleaned = 'data/your-input-uncleaned.csv'
    s3_key_cleaned = 'data/your-input-cleaned.csv'
    local_csv_dir = 'data/temp'
    local_csv_path = os.path.join(local_csv_dir, 'your-input.csv')
    preprocessed_csv_path = os.path.join(local_csv_dir, 'preprocessed-data.csv')

    # Create the temp directory if it doesn't exist
    os.makedirs(local_csv_dir, exist_ok=True)

    # Create a PostgreSQL connection
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    create_table_if_not_exists(conn, cursor)

    # Upload uncleaned CSV to S3
    upload_file_to_s3(csv_filename, s3_key_uncleaned)

    # Fetch CSV from S3
    fetch_csv_from_s3(s3_key_uncleaned, local_csv_path)

    # Preprocess CSV (including date conversion)
    preprocess_csv(local_csv_path, preprocessed_csv_path)

    # Upload cleaned CSV to S3
    upload_file_to_s3(preprocessed_csv_path, s3_key_cleaned)

    # Insert preprocessed data into PostgreSQL
    try:
        with open(preprocessed_csv_path, 'r') as f:
            cursor.copy_expert(sql=f"""
                COPY your_table FROM stdin WITH CSV HEADER
            """, file=f)
        conn.commit()
        logging.info('Data inserted into PostgreSQL successfully')
    except Exception as e:
        logging.error(f'Error inserting data into PostgreSQL: {str(e)}')
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
