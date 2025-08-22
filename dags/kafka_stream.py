from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid 
default_args = {
    'owner': 'byn227',
    'start_date': datetime(2025, 8, 23, 0, 0, 0)
}

def get_data():
    import requests
    url = "https://api.open-meteo.com/v1/forecast?latitude=48.864716&longitude=2.349014&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,pressure_msl,cloud_cover_low,wind_speed_10m&models=meteofrance_seamless&timezone=auto&forecast_days=1"
    response = requests.get(url)
    res = response.json()
    return res 


def format_data(res):
    """Return a list of JSON records, one per hour, aligning hourly arrays by index.

    Output example item:
    {
      "id": "<uuid>",
      "time": "2025-08-22T00:00",
      "latitude": 48.86,
      "longitude": 2.35,
      "timezone": "Europe/Paris",
      "timezone_abbreviation": "GMT+2",
      "temperature_2m": 18.4,
      "temperature_2m_unit": "°C",
      ...
    }
    """
    hourly = res.get('hourly', {}) or {}
    times = hourly.get('time', []) or []
    units = res.get('hourly_units', {}) or {}

    meta = {
        'latitude': res.get('latitude'),
        'longitude': res.get('longitude'),
        'timezone': res.get('timezone'),
        'timezone_abbreviation': res.get('timezone_abbreviation'),
    }

    # keys we requested from the API
    metric_keys = [
        'temperature_2m',
        'relative_humidity_2m',
        'apparent_temperature',
        'precipitation',
        'pressure_msl',
        'cloud_cover_low',
        'wind_speed_10m',
    ]

    records = []
    for i, t in enumerate(times):
        item = {
            'id': str(uuid.uuid4()),
            'time': t,
            **meta,
        }
        for k in metric_keys:
            arr = hourly.get(k) or []
            item[k] = arr[i] if i < len(arr) else None
            unit = units.get(k)
            if unit is not None:
                item[f"{k}_unit"] = unit
        records.append(item)
    return records

def stream_data():
    import json
    from kafka import KafkaProducer
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    try:
        raw = get_data()
        records = format_data(raw)
        for rec in records:
            producer.send('weather_paris', value=json.dumps(rec).encode('utf-8'))
        producer.flush()
    except Exception as e:
        logging.error('Error in streaming data: %s', e)
    finally:
        producer.close()


with DAG(
    dag_id='weather_daily',
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='streaming_task_from_api',
        python_callable=stream_data
    )
