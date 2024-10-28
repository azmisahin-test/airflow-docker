from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import os
import pytz
from dotenv import load_dotenv
import logging

# .env dosyasını yükle
load_dotenv()

# Logging ayarları
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

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
    schedule_interval="0 0,4,8,12,16,20 * * *",
    catchup=False,
)


def send_telegram_notification(country, trend_data):
    bot_token = os.getenv("BOT_TOKEN")
    chat_id = os.getenv("CHAT_ID")
    message = f"Today's Trends in {country}:\n" + "\n".join(trend_data)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    response = requests.post(
        url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    )

    if response.status_code == 200:
        logging.info(f"Notification sent successfully for {country}")
    else:
        logging.error(f"Failed to send notification for {country}: {response.text}")


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
    )


def fetch_trends(query):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        logging.error(f"Database query error: {e}")
        raise  # Raise the exception for Airflow to catch it
    finally:
        cursor.close()
        conn.close()


def fetch_and_notify_trends(**kwargs):
    countries = ["TR", "US", "GB"]
    for country_code in countries:
        query = f"""
            SELECT 
                data_content->'data_content'->>'query' AS trend_title,
                SUM((data_content->'data_content'->>'popularity_index')::int) AS total_popularity_index,
                COUNT(*) AS trend_count,
                country_code
            FROM 
                trends
            WHERE 
                DATE(created_at) = CURRENT_DATE AND country_code = '{country_code}'
            GROUP BY 
                trend_title, country_code
            ORDER BY 
                total_popularity_index DESC
            LIMIT 10
        """
        trends = fetch_trends(query)

        trend_titles = [
            f"<a href='https://www.google.com/search?q={title}' target='_blank'>[{count} - {popularity}] {title}</a>"
            for title, popularity, count in trends
        ]

        if trend_titles:
            send_telegram_notification(country_code, trend_titles)
        else:
            logging.info(f"No trends found for {country_code} today")

    # Global sorgu ile job_id ve task_id ekleme
    global_query = f"""
        SELECT 
            data_content->'data_content'->>'query' AS trend_title,
            SUM((data_content->'data_content'->>'popularity_index')::int) AS total_popularity_index,
            COUNT(*) AS trend_count
        FROM 
            trends
        WHERE 
            DATE(created_at) = CURRENT_DATE
        GROUP BY 
            trend_title
        ORDER BY 
            total_popularity_index DESC
        LIMIT 10
    """
    global_trends = fetch_trends(global_query)

    global_trend_titles = [
        f"<a href='https://www.google.com/search?q={title}' target='_blank'>[{count} - {popularity}] {title}</a>"
        for title, popularity, count in global_trends
    ]

    if global_trend_titles:
        send_telegram_notification("Worldwide", global_trend_titles)
    else:
        logging.info(f"No global trends found today")


# notify_task tanımı
notify_task = PythonOperator(
    task_id="notify_daily_trends",
    python_callable=fetch_and_notify_trends,
    provide_context=True,
    dag=dag,
)
