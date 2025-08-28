from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
default_args = {
    'owner': 'byn227',
    'start_date': datetime(2025, 8, 28, 0, 0, 0)
}
with DAG(
    dag_id="spark_stream_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:
    start_stream = BashOperator(
        task_id="start_stream",
        bash_command="python3 /opt/app/spark_stream.py",
    )