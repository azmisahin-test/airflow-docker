from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import os
import pytz
from dotenv import load_dotenv

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
    "daily_trends_notification",
    default_args=default_args,
    description="Daily notification of trends data",
    schedule_interval="0 8 * * *",  # Her gün saat 08:00'de çalışacak
)

def send_telegram_notification(trend_data):
    bot_token = os.getenv("BOT_TOKEN")  # .env dosyasından bot token al
    chat_id = os.getenv("CHAT_ID")      # .env dosyasından chat ID al
    message = "Today's Trends:\n" + "\n".join(trend_data)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    requests.post(url, json={
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    })

def fetch_and_notify_trends():
    try:
        # PostgreSQL veritabanı bağlantısı
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()
        
        # Sorguyu çalıştır
        cursor.execute("""\
            SELECT trend_title, COUNT(*) AS total_count
            FROM trends
            WHERE DATE(scraped_at) = CURRENT_DATE
            GROUP BY trend_title
            ORDER BY total_count DESC
        """)
        
        trends = cursor.fetchall()
        
        # Trend başlıklarını listeye al
        trend_titles = [f"{title} - {count}" for title, count in trends]
        
        # Bildirimi gönder
        if trend_titles:
            send_telegram_notification(trend_titles)
        else:
            print("No trends found for today.")
        
        # Bağlantıyı kapat
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error fetching trends: {e}")

# Günlük bildirim görevi
notify_task = PythonOperator(
    task_id="notify_daily_trends",
    python_callable=fetch_and_notify_trends,
    dag=dag,
)

notify_task  # Görevi DAG'a ekle
