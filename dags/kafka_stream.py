from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid 
default_args = {
    'owner': 'byn227',
    'start_date': datetime(2025, 8, 28, 0, 0, 0)
}

def get_data():
    import requests
    url = "https://api.open-meteo.com/v1/forecast?latitude=48.8534&longitude=2.3488&daily=sunrise,sunset,sunshine_duration&hourly=temperature_2m,precipitation,relative_humidity_2m,apparent_temperature,surface_pressure,cloud_cover,wind_speed_10m&models=meteofrance_seamless&timezone=auto&forecast_days=1"
    response = requests.get(url)
    res = response.json()
    return res 


def format_data(res):
    hourly = res.get('hourly', {}) or {}
    times = hourly.get('time', []) or []
    units = res.get('hourly_units', {}) or {}

    meta = {
        'latitude': res.get('latitude'),
        'longitude': res.get('longitude'),
        'timezone': res.get('timezone'),
        'timezone_abbreviation': res.get('timezone_abbreviation'),
    }

    # keys we requested from the API (align with URL params)
    metric_keys = [
        'temperature_2m',
        'relative_humidity_2m',
        'apparent_temperature',
        'precipitation',
        'surface_pressure',
        'cloud_cover',
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

def format_daily(res):
    daily = res.get('daily', {}) or {}
    units = res.get('daily_units', {}) or {}

    meta = {
        'latitude': res.get('latitude'),
        'longitude': res.get('longitude'),
        'timezone': res.get('timezone'),
        'timezone_abbreviation': res.get('timezone_abbreviation'),
    }

    times = daily.get('time', []) or []
    sunrises = daily.get('sunrise', []) or []
    sunsets = daily.get('sunset', []) or []
    sunshine = daily.get('sunshine_duration', []) or []

    out = []
    for i, d in enumerate(times):
        item = {
            'id': str(uuid.uuid4()),
            'date': d,
            **meta,
            'sunrise': sunrises[i] if i < len(sunrises) else None,
            'sunset': sunsets[i] if i < len(sunsets) else None,
            'sunshine_duration': sunshine[i] if i < len(sunshine) else None,
        }
        # attach units when available
        if 'sunshine_duration' in units:
            item['sunshine_duration_unit'] = units.get('sunshine_duration')
        out.append(item)
    return out

def stream_data():
    import json
    from kafka import KafkaProducer
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    try:
        raw = get_data()
        # hourly records -> weather_paris
        hourly_records = format_data(raw)
        for rec in hourly_records:
            producer.send('weather_paris', value=json.dumps(rec).encode('utf-8'))
        # daily records -> daily_paris
        daily_records = format_daily(raw)
        for rec in daily_records:
            producer.send('daily_paris', value=json.dumps(rec).encode('utf-8'))
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
