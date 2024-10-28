from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from xml.etree import ElementTree as ET
import os
import pytz
import psycopg2  # PostgreSQL için gerekli kütüphane
from dotenv import load_dotenv
import logging

# .env dosyasını yükle
load_dotenv()

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
)

# Data scraping and database saving function
def scrape_and_save_trends():
    url = os.getenv("TRENDS_URL")  # .env dosyasından URL al
    logging.info(f"Fetching trends data from {url}")
    
    try:
        response = requests.get(url)

        if response.status_code == 200:
            # Parse the XML content
            root = ET.fromstring(response.content)
            item = root.find(".//item")

            if item is not None:
                most_recent_title = item.find("title").text

                if most_recent_title:
                    try:
                        # PostgreSQL database connection
                        conn = psycopg2.connect(
                            host=os.getenv("DB_HOST"),
                            database=os.getenv("DB_NAME"),
                            user=os.getenv("DB_USER"),
                            password=os.getenv("DB_PASSWORD"),
                            port=os.getenv("DB_PORT")
                        )
                        cursor = conn.cursor()
                        
                        # Create table if it doesn't exist
                        cursor.execute("""\
                            CREATE TABLE IF NOT EXISTS trends (
                                id SERIAL PRIMARY KEY,
                                trend_title TEXT NOT NULL,
                                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        
                        # Insert the trend title into the database
                        cursor.execute("""\
                            INSERT INTO trends (trend_title)
                            VALUES (%s)
                        """, (most_recent_title,))
                        
                        # Commit and close the connection
                        conn.commit()
                        cursor.close()
                        conn.close()
                        
                        logging.info("Most Recent Trends Data Saved to Database")
                    except Exception as e:
                        logging.error(f"Database error: {e}")
                        if conn:
                            conn.rollback()  # Rollback in case of error
                            cursor.close()
                            conn.close()
                else:
                    logging.warning("No trend title found in the response.")
            else:
                logging.warning("No item found in the XML response.")
        else:
            logging.error(f"Error fetching data: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")

# Task definition
scrape_task = PythonOperator(
    task_id="scrape_and_save_trends",
    python_callable=scrape_and_save_trends,
    dag=dag,
)

# Task order
scrape_task
