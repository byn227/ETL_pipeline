# Pipeline ETL : Kafka → Spark Structured Streaming → Postgres → Superset

Flux Open‑Meteo vers Kafka, traitement avec Spark, stockage dans Postgres (tables suffixées par jour) et visualisation avec Superset. Orchestration via Airflow.

## Architecture
* __Kafka__ : Zookeeper, Broker, Schema Registry, Control Center
* __Airflow__ : exécute deux DAGs
  * `weather_daily` — récupère Open‑Meteo et publie dans Kafka
  * `spark_stream_dag` — lance le consommateur de streaming Spark
* __Spark__ (master/worker) pour le dev ; le job de streaming tourne comme un processus Python déclenché par Airflow
* __Postgres__ pour le stockage
* __Superset__ pour les tableaux de bord

## Services et ports
* __Airflow Web__ : http://localhost:8080
* __Kafka Broker__ : localhost:9092 (externe), broker:29092 (interne aux conteneurs)
* __Schema Registry__ : http://localhost:8081
* __Control Center__ : http://localhost:9021
* __Postgres__ : localhost:5432 (user : airflow, pass : airflow, db : airflow)
* __Superset__ : http://localhost:8088

Voir `docker-compose.yml` pour la configuration complète.

## Sujets Kafka
* __Horaire__ : `weather_paris`
* __Quotidien__ : `daily_paris`

Produits par `dags/kafka_stream.py` via l’API Open‑Meteo.

## Consommateur Spark Streaming
* __Entrée__ : `spark_stream.py`
* __Déclenché par__ : `dags/spark_stream_dag.py` (planification : `@hourly`)
* __Bootstrap Kafka__ :
  * Dans les conteneurs : `broker:29092`
  * Depuis l’hôte : `localhost:9092`
* __Checkpoints__ :
  * Horaire : `/tmp/checkpoints/weather_hourly`
  * Quotidien : `/tmp/checkpoints/weather_daily`

## Tables Postgres (suffixées par jour)
Les tables sont créées avec un suffixe de date courante au démarrage du processus :
* __Horaire__ : `meteo_hourly_YYYYMMDD`
* __Quotidien__ : `meteo_daily_YYYYMMDD`

Colonnes
* __Horaire__ : `id`, `time_text`, `time_ts`, `latitude`, `longitude`, `timezone`, `timezone_abbreviation`, `temperature_2m`, `relative_humidity_2m`, `apparent_temperature`, `precipitation`, `surface_pressure`, `cloud_cover`, `wind_speed_10m`
* __Quotidien__ : `id`, `date_text`, `date_ts`, `latitude`, `longitude`, `timezone`, `timezone_abbreviation`, `sunrise_time`, `sunset_time`, `sunshine_duration`, `sunshine_duration_time`

Remarque : le suffixe de date est fixé au démarrage du job. Dites‑moi si vous souhaitez une rotation à minuit ou un routage par temps d’événement.

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

## Kafka — notes rapides
* __Control Center__ : http://localhost:9021
* __Produire depuis l’hôte__ : bootstrap `localhost:9092`
* __Produire dans les conteneurs__ : bootstrap `broker:29092`

## Superset
* URL : http://localhost:8088
* Ajouter une base : Postgres sur `postgres:5432` (depuis le conteneur), user/pass `airflow/airflow`
* Explorer les tables : `meteo_hourly_YYYYMMDD`, `meteo_daily_YYYYMMDD`

## Dépannage
* __Nom de conteneur introuvable avec docker exec__
  - `docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"`
* __Spark n’atteint pas Kafka__
  - Utiliser `broker:29092` quand le consommateur tourne dans un conteneur
* __Tables manquantes__
  - Consulter les logs de `spark_stream_dag` ; `ensure_pg_tables()` s’exécute au démarrage du job

## Organisation du code
* __DAGs__ : `dags/`
* __Job de streaming__ : `spark_stream.py`
* __Entrée Airflow__ : `script/entrypoint.sh`
* __Dépendances Python__ : `requirements.txt`
