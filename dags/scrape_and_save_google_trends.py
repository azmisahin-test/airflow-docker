from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from xml.etree import ElementTree as ET
import os
import pytz
import psycopg2  # PostgreSQL için gerekli kütüphane

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime.now(pytz.UTC) - timedelta(minutes=1),
    "retries": 0,
}

# DAG definition
dag = DAG(
    "scrape_and_save_google_trends",
    default_args=default_args,
    description="Scraping Google Trends data and saving to database",
    schedule_interval="*/10 * * * *",
)

# Data scraping and database saving function
def scrape_and_save_google_trends():
    url = "https://trends.google.com/trending/rss?geo=TR&sort=recency"
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
                        host="postgresql",
                        database="postgres_db",
                        user="postgres_user",
                        password="postgres_password",
                        port="5432"
                    )
                    cursor = conn.cursor()
                    
                    # Create table if it doesn't exist
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS google_trends (
                            id SERIAL PRIMARY KEY,
                            trend_title TEXT NOT NULL,
                            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Insert the trend title into the database
                    cursor.execute("""
                        INSERT INTO google_trends (trend_title)
                        VALUES (%s)
                    """, (most_recent_title,))
                    
                    # Commit and close the connection
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                    print("Most Recent Google Trends Data Saved to Database")
                except Exception as e:
                    print(f"Database error: {e}")
    else:
        print(f"Error fetching data: {response.status_code}")

# Task definition
scrape_task = PythonOperator(
    task_id="scrape_and_save_google_trends",
    python_callable=scrape_and_save_google_trends,
    dag=dag,
)

# Task order
scrape_task
