from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import os

# İş akışının başlangıç tarihi
default_args = {
    "owner": "airflow",
    "start_date": datetime.now()
    + timedelta(minutes=1),  # 1 dakika sonra başlaması için ayarlandı
    "retries": 1,
}

# DAG tanımı
dag = DAG(
    "google_trends_scraping",
    default_args=default_args,
    description="Google Trends verilerini çekme",
    schedule_interval="*/10 * * * *",  # Her 10 dakikada bir çalışacak şekilde ayarlandı
)


# Veri çekme fonksiyonu
def scrape_google_trends():
    url = "https://trends.google.com/trending?geo=TR&sort=recency"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        # Trend başlıklarını çekme
        trends = soup.find_all("span", class_="h3")
        trend_titles = [trend.get_text() for trend in trends]

        # İşlenmiş veriyi kaydetme
        dags_folder = "/opt/airflow/dags/"
        file_path = os.path.join(dags_folder, "google_trends.txt")
        with open(file_path, "w") as f:
            for title in trend_titles:
                f.write(f"{title}\n")
        print("Google Trends Data Scraped and Saved")
    else:
        print(f"Error fetching data: {response.status_code}")


# Görev tanımı
scrape_task = PythonOperator(
    task_id="scrape_google_trends",
    python_callable=scrape_google_trends,
    dag=dag,
)

# Görev sırası
scrape_task
