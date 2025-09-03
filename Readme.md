# Pipeline ETL Météo Paris

![kafka-python](https://img.shields.io/badge/kafka--python-2.0.2-f39c12?logo=apachekafka&logoColor=black)
![requests](https://img.shields.io/badge/requests-2.31.0-27ae60?logo=python&logoColor=yellow)
![google-cloud-bigquery](https://img.shields.io/badge/google--cloud--bigquery-%3E%3D3.10.0-2980b9?logo=googlebigquery&logoColor=orange)
![pyspark](https://img.shields.io/badge/pyspark-3.5.1-d35400?logo=apachespark&logoColor=black)
![Kafka](https://img.shields.io/badge/Kafka-7.4.0-8e44ad?logo=apachekafka&logoColor=yellow)
![Zookeeper](https://img.shields.io/badge/Zookeeper-7.4.0-16a085?logo=apache&logoColor=black)
![Schema Registry](https://img.shields.io/badge/Schema%20Registry-7.4.0-c0392b?logo=confluent&logoColor=blue)
![Control Center](https://img.shields.io/badge/Control%20Center-7.4.0-2c3e50?logo=confluent&logoColor=lime)
![Airflow](https://img.shields.io/badge/Airflow-2.8.2-1abc9c?logo=apacheairflow&logoColor=red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.0-3498db?logo=postgresql&logoColor=gold)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-e67e22?logo=apachespark&logoColor=cyan)
![Bitnami](https://img.shields.io/badge/Bitnami-Spark%20Image-34495e?logo=bitnami&logoColor=orange)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-9b59b6?logo=googlebigquery&logoColor=yellow)
![NumPy](https://img.shields.io/badge/numpy-1.26.0-2ecc71?logo=numpy&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-2.1.4-1abc9c?logo=pandas&logoColor=black)
![PyArrow](https://img.shields.io/badge/pyarrow-21.0.0-e67e22?logo=apachearrow&logoColor=white)

Flux Open‑Meteo vers Kafka, traitement avec Spark Structured Streaming, chargement dans BigQuery et visualisation via Looker Studio. Orchestration par Airflow.

Vous pouvez changer la ville selon la latitude et la longitude dans l’API. Le ville actuel est Paris.
## Architecture
![System Architecture](assets/system.png)

## Topics Kafka
* __Horaire__: `weather_paris`
* __Quotidien__: `daily_paris`

Produits par `dags/kafka_stream.py` (appel API Open‑Meteo, normalisation et envoi vers Kafka).

## Consommateur Spark Streaming
* __Entrée__: `spark_stream.py`
* __Déclenché par__: `dags/spark_stream_dag.py` (planificatgion: `@daily`)
* __Bootstrap Kafka__:
  * Dans les conteneurs: `broker:29092`
  * Depuis l’hôte: `localhost:9092`

## BigQuery
* __Project ID__: `VOTRE_PROJECT_ID` (modifiable dans `spark_stream.py`)
* __Dataset__: `VOTRE_NOM_DATASET` (modifiable dans `spark_stream.py`)
* __Tables__:
  * Horaire: `meteo_hourly`
  * Quotidien: `meteo_daily`

Schémas selon `spark_stream.py`:
* __meteo_hourly__: `id`(STRING, REQUIRED), `time_text`(STRING), `time_ts`(TIMESTAMP), `latitude`(FLOAT64), `longitude`(FLOAT64), `timezone`(STRING), `timezone_abbreviation`(STRING), `temperature_2m`(FLOAT64), `relative_humidity_2m`(FLOAT64), `apparent_temperature`(FLOAT64), `precipitation`(FLOAT64), `surface_pressure`(FLOAT64), `cloud_cover`(FLOAT64), `wind_speed_10m`(FLOAT64)
* __meteo_daily__: `id`(STRING, REQUIRED), `date_text`(STRING), `date_ts`(DATE), `latitude`(FLOAT64), `longitude`(FLOAT64), `timezone`(STRING), `timezone_abbreviation`(STRING), `sunrise_time`(STRING), `sunset_time`(STRING), `sunshine_duration`(FLOAT64), `sunshine_duration_time`(STRING)


## Démarrage rapide
1) Préparer les credentials BigQuery (placer `config/config.json`).
2) Lancer les services:
```bash
docker compose up -d
```
3) Ouvrir Airflow UI: http://localhost:8080
   * Déclencher le DAG producteur: `weather_daily` (planification `@daily`)
   * Déclencher le DAG consommateur: `spark_stream_dag` (planification `@daily`)

4) Option dev en local (hors Airflow) depuis l’hôte:
```bash
python3 spark_stream.py
```

## Outils & URLs
* __Kafka Control Center__: http://localhost:9021
* __Spark Master UI__: http://localhost:9090
* __Airflow Web UI__: http://localhost:8080
* __BigQuery__: https://console.cloud.google.com/bigquery 

## Organisation du code
* __DAGs__: `dags/`
* __Job Spark Streaming__: `spark_stream.py`
* __Entrypoint Airflow__: `script/entrypoint.sh`
* __Dépendances Python__: `requirements.txt`

## Visualisation par Looker Studio
Le rapport se met automatiquement à jour chaque jour. 

![alt text](assets/report.png)

J’ai filtré les données à la date d’aujourd’hui.

![alt text](asset/filtre.png) 

## Commentaire
C’est un pipeline ETL de type streaming, mais j’ai choisi de le mettre en batch en raison des caractéristiques des données. En effet, l’API Open-Meteo envoie des données une fois par jour. Ainsi, il n’est pas nécessaire d’utiliser Kafka, mais je souhaite l’employer dans un but d’apprentissage et bien comprendre le fonctionnement.