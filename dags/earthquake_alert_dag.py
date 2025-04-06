from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, pytz, psycopg2
from math import radians, sin, cos, sqrt, atan2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# === ENV CONFIG ===
import os
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")  # Matches Postgres service name in Docker

DB_CONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,  # Matches Postgres service name in Docker
    "port": 5432
}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def fetch_earthquakes():
    endtime = datetime.utcnow()
    starttime = endtime - timedelta(hours=1)
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    params = {
        'format': 'geojson',
        'starttime': starttime.isoformat(),
        'endtime': endtime.isoformat(),
        'minmagnitude': 4.0
    }
    res = requests.get(url, params=params)
    res.raise_for_status()
    return res.json()['features']



def send_email_alert(subject, message):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(message, "plain"))

        if EMAIL_PORT == 465:
            server = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, timeout=10)
        else:
            server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=10)
            server.ehlo()
            server.starttls()
            server.ehlo()

        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()

        print("✅ Email sent successfully")
        return True

    except Exception as e:
        print(f"❌ SMTP Error: {str(e)}")
        return False

def save_to_db(event_id, place, mag, dist, occurred_at, notified):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO earthquakes (id, place, magnitude, distance_km, occurred_at, notified)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (event_id, place, mag, dist, occurred_at, notified))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print("DB error:", e)

def check_and_alert():
    TH_LAT, TH_LON = 15.87, 100.99
    events = fetch_earthquakes()
    for e in events:
        lon, lat = e['geometry']['coordinates'][:2]
        dist = haversine(lat, lon, TH_LAT, TH_LON)
        if dist <= 500:
            place = e['properties']['place']
            mag = e['properties']['mag']
            event_id = e['id']
            occurred_at = datetime.utcfromtimestamp(e['properties']['time'] / 1000)
            msg = f'🌏 Earthquake Alert!\nLocation: {place}\nMagnitude: {mag}\nDistance from TH: {dist:.1f} km\nTime: {occurred_at.strftime("%Y-%m-%d %H:%M:%S")}'
            print(msg)
            notified = send_email_alert("🌏 Earthquake Alert", msg)
#            save_to_db(event_id, place, mag, dist, occurred_at, notified)

# === DAG DEFINITION ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='earthquake_alert',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    run_alert = PythonOperator(
        task_id='check_earthquake_and_notify',
        python_callable=check_and_alert
    )

    run_alert
