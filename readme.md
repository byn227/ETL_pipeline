# Pipeline ETL : Kafka → Spark Structured Streaming → Postgres → Superset

Flux Open‑Meteo vers Kafka, traitement avec Spark, stockage dans Postgres (tables suffixées par jour) et visualisation avec Superset. Orchestration via Airflow.

## Architecture



## Sujets Kafka
* __Horaire__ : `weather_paris`
* __Quotidien__ : `daily_paris`

Produits par `dags/kafka_stream.py` via l’API Open‑Meteo.

## Tables Postgres (suffixées par jour)
Les tables sont créées avec un suffixe de date courante au démarrage du processus :
* __Horaire__ : `meteo_hourly_YYYYMMDD`
* __Quotidien__ : `meteo_daily_YYYYMMDD`

Colonnes
* __Horaire__ : `id`, `time_text`, `time_ts`, `latitude`, `longitude`, `timezone`, `timezone_abbreviation`, `temperature_2m`, `relative_humidity_2m`, `apparent_temperature`, `precipitation`, `surface_pressure`, `cloud_cover`, `wind_speed_10m`
* __Quotidien__ : `id`, `date_text`, `date_ts`, `latitude`, `longitude`, `timezone`, `timezone_abbreviation`, `sunrise_time`, `sunset_time`, `sunshine_duration`, `sunshine_duration_time`

## Démarrage rapide
1) Démarrer tous les services
```bash
docker compose up -d
```

2) Déclencher le DAG producteur
- UI Airflow → DAGs → `weather_daily` → Trigger

3) Lancer le consommateur de streaming
- UI Airflow → DAGs → `spark_stream_dag` → Trigger (planifié toutes les heures)
- Alternative dev (hôte) : `python3 spark_stream.py`

## Postgres — commandes rapides
* __Ouvrir psql__
```bash
docker exec -it postgres psql -U airflow -d airflow
```

* __Lister les tables du jour__
```bash
docker exec -i postgres psql -U airflow -d airflow -c "\dt meteo_*"
```

* __Décrire les tables du jour__
```bash
docker exec -i postgres psql -U airflow -d airflow -c "\d+ meteo_hourly_$(date +%Y%m%d)"
docker exec -i postgres psql -U airflow -d airflow -c "\d+ meteo_daily_$(date +%Y%m%d)"
```

* __Exemple de requête__
```bash
docker exec -i postgres psql -U airflow -d airflow -c "SELECT * FROM meteo_hourly_$(date +%Y%m%d) ORDER BY time_ts DESC LIMIT 10;"
```

## Superset
* URL : http://localhost:8088
* Ajouter une base : Postgres sur `postgres:5432` (depuis le conteneur), user/pass `airflow/airflow`
* Explorer les tables : `meteo_hourly_YYYYMMDD`, `meteo_daily_YYYYMMDD`


