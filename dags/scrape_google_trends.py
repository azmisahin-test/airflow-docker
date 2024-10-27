from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from xml.etree import ElementTree as ET
import os
import pytz

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime.now(pytz.UTC),  # Current time
    "retries": 0,  # No retries
}

# DAG definition
dag = DAG(
    "google_trends_scraping",
    default_args=default_args,
    description="Scraping Google Trends data",
    schedule_interval="*/2 * * * *",  # Runs every 2 minutes
    max_active_runs=1,  # Only one active run at a time
    concurrency=1,  # Only one task can run at a time
)

# Data scraping function
def scrape_google_trends():
    url = "https://trends.google.com/trending/rss?geo=TR&sort=recency"
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the XML content
        root = ET.fromstring(response.content)

        # Extract trend titles
        trend_titles = []
        for item in root.findall(".//item"):
            title = item.find("title").text
            trend_titles.append(title)

        # Save processed data
        dags_folder = "/opt/airflow/logs/"
        file_path = os.path.join(dags_folder, "google_trends.txt")
        with open(file_path, "w") as f:
            for title in trend_titles:
                f.write(f"{title}\n")
        print("Google Trends Data Scraped and Saved")
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
