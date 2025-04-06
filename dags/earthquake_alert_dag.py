from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, pytz, psycopg2
from math import radians, sin, cos, sqrt, atan2

# === ENV CONFIG ===
import os

LINE_NOTIFY_TOKEN = os.getenv("LINE_NOTIFY_TOKEN")
DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT", 5432)),
    'dbname': os.getenv("DB_NAME", "your_db"),
    'user': os.getenv("DB_USER", "your_user"),
    'password': os.getenv("DB_PASSWORD", "your_password"),
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

def send_line_notify(msg):
    res = requests.post(
        'https://notify-api.line.me/api/notify',
        headers={'Authorization': f'Bearer {LINE_NOTIFY_TOKEN}'},
        data={'message': msg}
    )
    return res.status_code == 200

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
            notified = send_line_notify(msg)
            save_to_db(event_id, place, mag, dist, occurred_at, notified)

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
