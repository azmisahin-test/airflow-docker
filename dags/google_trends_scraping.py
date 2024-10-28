from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from xml.etree import ElementTree as ET
import os
import pytz

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime.now(pytz.UTC) - timedelta(minutes=1),  # 1 dakika önce
    "retries": 0,  # No retries
}

# DAG definition
dag = DAG(
    "google_trends_scraping",
    default_args=default_args,
    description="Scraping Google Trends data",
    schedule_interval="*/10 * * * *",  # Her 10 dakikada bir çalışır
)

# Data scraping function
def scrape_google_trends():
    url = "https://trends.google.com/trending/rss?geo=TR&sort=recency"
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the XML content
        root = ET.fromstring(response.content)

        # Extract the most recent trend title
        most_recent_title = None
        item = root.find(".//item")
        if item is not None:
            most_recent_title = item.find("title").text

        if most_recent_title:
            # Save processed data
            dags_folder = "/opt/airflow/logs/"
            file_path = os.path.join(dags_folder, "google_trends.txt")
            # Append the new title to the file
            with open(file_path, "a") as f:  # Open in append mode
                f.write(f"{most_recent_title}\n")
            print("Most Recent Google Trends Data Scraped and Saved")
    else:
        print(f"Error fetching data: {response.status_code}")

# Task definition
scrape_task = PythonOperator(
    task_id="scrape_google_trends",
    python_callable=scrape_google_trends,
    dag=dag,
)

# Task order
scrape_task
