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
    "002_daily_trends_notification",
    default_args=default_args,
    description="Daily notification of trends data",
    schedule_interval="0 0,4,8,12,16,20 * * *",
    catchup=False,
)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
    )


# Helper function to format numbers for better readability
def format_number(value):
    """Format the number into a more readable form."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"  # Milyon
    elif value >= 1_000:
        return f"{value / 1_000:.1f}k"  # Bin
    else:
        return str(value)  # Sayıyı olduğu gibi döndür


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


def send_telegram_notification(country, trend_data):
    bot_token = os.getenv("BOT_TOKEN")
    chat_id = os.getenv("CHAT_ID")

    # Notify the user with formatted message
    message = f"📊 *Today's Trends in {country}*:\n\n" + "\n".join(trend_data)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    response = requests.post(
        url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    )

    if response.status_code == 200:
        logging.info(f"Notification sent successfully for {country}")
    else:
        logging.error(f"Failed to send notification for {country}: {response.text}")


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
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def fetch_and_notify_trends(**kwargs):
    # Tabloyu oluştur
    create_table_if_not_exists()

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
            f"🔗 [{format_number(count)} - {format_number(popularity)}] *{title}*"
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
        f"🔗 [{format_number(count)} - {format_number(popularity)}] *{title}*"
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
