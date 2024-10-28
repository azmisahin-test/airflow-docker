from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
import pytz
from xml.etree import ElementTree as ET
import psycopg2  # PostgreSQL için gerekli kütüphane
from dotenv import load_dotenv
import logging
import json

# .env dosyasını yükle
load_dotenv()

# UTC zaman damgasını oluştur
utc_now = datetime.now(pytz.UTC)

# Zulu (Z) formatında timestamp olarak yaz
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
    schedule_interval="*/10 * * * *",  # Every 10 minutes
    catchup=False,  # Geçmiş görevleri işlemeyecek
)


# Function to create table if it doesn't exist
def create_table_if_not_exists():
    logging.info("Checking if trends table exists.")
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT"),
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trends (
                id SERIAL PRIMARY KEY,
                source_type_id INT NOT NULL,
                data_category_id INT NOT NULL,
                schema_type_id INT NOT NULL,
                service_id VARCHAR NOT NULL,  -- service_id türünü VARCHAR olarak güncelledim
                time_interval INT NOT NULL,
                fetch_frequency INT NOT NULL,
                language_code VARCHAR(5) NOT NULL,
                country_code VARCHAR(5) NOT NULL,
                region_code VARCHAR(5) NOT NULL,
                provider_id INT NOT NULL,
                platform_id INT NOT NULL,
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
        cursor.close()
        conn.close()


# Data scraping and database saving function
def scrape_and_save_trends(**kwargs):
    # Airflow run_id'yi al
    run_id = kwargs["dag_run"].run_id
    logging.info(f"Current run_id: {run_id}")

    url = os.getenv("TRENDS_URL")  # .env dosyasından URL al
    logging.info(f"Fetching trends data from {url}")

    try:
        response = requests.get(url)

        if response.status_code == 200:
            # XML verisini parse et
            root = ET.fromstring(response.content)
            records = []  # Kayıtları tutmak için bir liste oluştur

            for item in root.find("channel").findall("item"):
                title = item.find("title").text
                approx_traffic = item.find(
                    "{https://trends.google.com/trending/rss}approx_traffic"
                ).text
                approx_traffic_value = int(approx_traffic.replace("+", "").strip())
                pub_date = item.find("pubDate").text

                # Ülke kodunu linkten çıkar
                link = item.find("link").text
                country_code = link.split("=")[-1]  # linkteki geo parametresini al
                region = country_code  # planlama aşamasında

                # JSON verisi
                data_content = {
                    "data_content": {
                        "query": title,
                        "timestamp": timestamp_z,
                        "popularity_index": approx_traffic_value,
                    }
                }

                # Kayıtları listeye ekle
                records.append(
                    (
                        1,  # source_type_id
                        1,  # data_category_id
                        1,  # schema_type_id
                        run_id,  # service_id (Airflow run_id)
                        10,  # time_interval
                        10,  # fetch_frequency
                        "",  # language_code
                        country_code,  # country_code
                        region,  # region_code
                        1,  # provider_id
                        1,  # platform_id
                        json.dumps(data_content),  # JSON verisi
                    )
                )

            # PostgreSQL database connection
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                port=os.getenv("DB_PORT"),
            )
            cursor = conn.cursor()

            # Database'e tüm verileri ekle
            insert_query = """
                INSERT INTO trends (
                    source_type_id,
                    data_category_id,
                    schema_type_id,
                    service_id,
                    time_interval,
                    fetch_frequency,
                    language_code,
                    country_code,
                    region_code,
                    provider_id,
                    platform_id,
                    data_content
                ) VALUES %s
            """

            psycopg2.extras.execute_values(cursor, insert_query, records)
            conn.commit()
            logging.info("Trends data saved to the database successfully.")
        else:
            logging.error(f"Error fetching data: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        cursor.close()
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
    provide_context=True,  # kwargs'i sağlamak için
    dag=dag,
)

# Task order
create_table_task >> scrape_task
