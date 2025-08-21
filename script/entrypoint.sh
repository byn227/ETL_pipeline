#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

# Always initialize/upgrade the metadata database (Postgres)
airflow db upgrade || airflow db init

# Create admin user if missing (idempotent)
if ! airflow users list | grep -q " admin "; then
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

exec airflow webserver