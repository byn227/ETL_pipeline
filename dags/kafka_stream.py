from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid 
default_args = {
    'owner': 'byn227',
    'start_date': datetime(2025, 8, 20, 0, 0, 0)
}

def get_data():
    import requests
    url = "https://api.open-meteo.com/v1/forecast?latitude=48.864716&longitude=2.349014&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,showers,wind_speed_10m&timezone=GMT&forecast_days=1"
    response = requests.get(url)
    res = response.json()
    return res 


def format_data(res):
    data = {}
    lat = res.get('latitude', '')
    lon = res.get('longitude', '')
    tz = res.get('timezone', '')
    tz_abbr = res.get('timezone_abbreviation', '')
    current = res.get('current', {})

    # Weather-centric schema
    data['id'] = str(uuid.uuid4())
    data['latitude'] = str(lat) if lat != '' else ''
    data['longitude'] = str(lon) if lon != '' else ''
    data['timezone'] = tz or ''
    data['timezone_abbreviation'] = tz_abbr or ''
    data['current_time'] = current.get('time', '')

    # current weather metrics as strings for Cassandra TEXT compatibility
    data['temperature_2m'] = str(current.get('temperature_2m', ''))
    data['relative_humidity_2m'] = str(current.get('relative_humidity_2m', ''))
    data['apparent_temperature'] = str(current.get('apparent_temperature', ''))
    data['precipitation'] = str(current.get('precipitation', ''))
    data['rain'] = str(current.get('rain', ''))
    data['snowfall'] = str(current.get('snowfall', ''))
    data['showers'] = str(current.get('showers', ''))
    data['wind_speed_10m'] = str(current.get('wind_speed_10m', ''))

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import logging

    producer=KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    try:
        res=get_data()
        res=format_data(res)
        producer.send('weather_created',value=json.dumps(res).encode('utf-8'))
        producer.flush()
    except Exception as e:
        logging.error('Error in streaming data: %s', e)
    finally:
        producer.close()


with DAG(
    dag_id='weather_automation',
    default_args=default_args,
    schedule="@hourly",
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='streaming_task_from_api',
        python_callable=stream_data
    )
