from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
import pytz
from xml.etree import ElementTree as ET
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import logging
import json
from urllib.parse import urlparse, parse_qs

# .env dosyasını yükle
load_dotenv()

# Logging ayarları
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# UTC zaman damgasını oluştur
utc_now = datetime.now(pytz.UTC)
timestamp_z = utc_now.isoformat() + "Z"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 28, 0, 0, tzinfo=pytz.UTC),
    "retries": 1,
}

# DAG definition
dag = DAG(
    "scrape_and_save_trends",
    default_args=default_args,
    description="Scraping trends data and saving to database",
    schedule_interval="*/10 * * * *",
    catchup=False,
)

# Ülkeler listesi
countries = ["TR", "US", "GB"]

# Ülke kodlarına göre dil kodları sözlüğü
language_codes = {
    "TR": "TR",
    "US": "EN",
    "GB": "EN",
}


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
    )


# Function to create table if it doesn't exist
def create_table_if_not_exists():
    logging.info("Checking if trends table exists.")
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trends (
                id SERIAL PRIMARY KEY,
                source_id INT NOT NULL,
                provider_id INT NOT NULL,
                platform_id INT NOT NULL,
                service_id INT NOT NULL,
                source_type_id INT NOT NULL,
                data_category_id INT NOT NULL,
                schema_type_id INT NOT NULL,
                job_id INT NOT NULL,
                task_id INT NOT NULL,
                time_interval INT NOT NULL,
                fetch_frequency INT NOT NULL,
                language_code VARCHAR(5) NOT NULL,
                country_code VARCHAR(5) NOT NULL,
                region_code VARCHAR(5) NOT NULL,
                data_content JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """
        )
        conn.commit()
        logging.info("Trends table checked/created successfully.")
    except Exception as e:
        logging.error(f"Error creating trends table: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Data scraping and database saving function
def scrape_and_save_trends(**kwargs):
    # Data preparation
    source_id, provider_id, platform_id = 1, 1, 1
    service_id, source_type_id, data_category_id = 1, 1, 1
    schema_type_id, time_interval, fetch_frequency = 1, 10, 10

    job_id = kwargs["dag_run"].run_id
    job_id_hash = hash(job_id) % (10**8)

    records = []

    for country_code in countries:
        url = f"https://trends.google.com/trending/rss?geo={country_code}&sort=recency"
        logging.info(f"Fetching trends data from {url}")

        language_code = language_codes.get(country_code, country_code)

        try:
            response = requests.get(url)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                for item in root.find("channel").findall("item"):
                    title = item.find("title").text
                    approx_traffic = item.find(
                        "{https://trends.google.com/trending/rss}approx_traffic"
                    ).text
                    approx_traffic_value = int(approx_traffic.replace("+", "").strip())

                    link = item.find("link").text
                    parsed_url = urlparse(link)
                    region_code = parse_qs(parsed_url.query).get("geo", [None])[0]

                    data_content = {
                        "query": title,
                        "timestamp": timestamp_z,
                        "popularity_index": approx_traffic_value,
                    }

                    records.append(
                        (
                            source_id,
                            provider_id,
                            platform_id,
                            service_id,
                            source_type_id,
                            data_category_id,
                            schema_type_id,
                            job_id_hash,
                            1,
                            time_interval,
                            fetch_frequency,
                            language_code,
                            country_code,
                            region_code,
                            json.dumps(data_content),
                        )
                    )

                logging.info(f"Trends data fetched successfully for {country_code}.")
            else:
                logging.error(
                    f"Error fetching data for {country_code}: {response.status_code}"
                )

        except requests.exceptions.RequestException as e:
            logging.error(f"Request error for {country_code}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error for {country_code}: {e}")

    # PostgreSQL database connection
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if records:
            insert_query = """
                INSERT INTO trends (
                    source_id, provider_id, platform_id, service_id,
                    source_type_id, data_category_id, schema_type_id,
                    job_id, task_id, time_interval,
                    fetch_frequency, language_code,
                    country_code, region_code, data_content
                ) VALUES %s
            """
            extras.execute_values(cursor, insert_query, records)
            conn.commit()
            logging.info("Trends data saved to the database successfully.")
        else:
            logging.warning("No trends data to save.")

    except Exception as e:
        logging.error(f"Database connection error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Table creation task
create_table_task = PythonOperator(
    task_id="create_trends_table",
    python_callable=create_table_if_not_exists,
    dag=dag,
)

# Data scraping and saving task
scrape_task = PythonOperator(
    task_id="scrape_and_save_trends",
    python_callable=scrape_and_save_trends,
    provide_context=True,
    dag=dag,
)

# Task order
create_table_task >> scrape_task
