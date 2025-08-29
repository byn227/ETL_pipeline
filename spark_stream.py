import logging
from datetime import datetime, date
import os
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

PROJECT_ID = "leafy-mender-467715-s9"
DATASET_NAME = "meteo_pazi"

def resolve_credentials_path():
    return os.getenv("GCP_CREDENTIALS_FILE")

def get_bigquery_client():
    try:
        return bigquery.Client.from_service_account_json(
            json_credentials_path=resolve_credentials_path(),
            project=PROJECT_ID
        )
    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}")
        return None

SCHEMAS = {
    "hourly": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("time_text", "STRING"),
        bigquery.SchemaField("time_ts", "TIMESTAMP"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("timezone", "STRING"),
        bigquery.SchemaField("timezone_abbreviation", "STRING"),
        bigquery.SchemaField("temperature_2m", "FLOAT64"),
        bigquery.SchemaField("relative_humidity_2m", "FLOAT64"),
        bigquery.SchemaField("apparent_temperature", "FLOAT64"),
        bigquery.SchemaField("precipitation", "FLOAT64"),
        bigquery.SchemaField("surface_pressure", "FLOAT64"),
        bigquery.SchemaField("cloud_cover", "FLOAT64"),
        bigquery.SchemaField("wind_speed_10m", "FLOAT64"),
    ],
    "daily": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_text", "STRING"),
        bigquery.SchemaField("date_ts", "DATE"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("timezone", "STRING"),
        bigquery.SchemaField("timezone_abbreviation", "STRING"),
        bigquery.SchemaField("sunrise_time", "STRING"),
        bigquery.SchemaField("sunset_time", "STRING"),
        bigquery.SchemaField("sunshine_duration", "FLOAT64"),
        bigquery.SchemaField("sunshine_duration_time", "STRING"),
    ]
}

TABLES = {
    "hourly": "meteo_hourly",
    "daily": "meteo_daily"
}

def ensure_bq_tables():
    client = get_bigquery_client()
    if not client:
        return
    for t, schema in SCHEMAS.items():
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLES[t]}"
        client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
        logging.info(f"Ensured table: {table_id}")

def insert_data(table_type, **kwargs):
    """Generic insert into BigQuery (hourly/daily)"""
    client = get_bigquery_client()
    if not client:
        return
    
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLES[table_type]}"
    allowed = {f.name for f in SCHEMAS[table_type]}
    row = {k: kwargs.get(k) for k in allowed}

    errors = client.insert_rows_json(table_id, [row])
    if errors:
        logging.error(f"Insert errors for {table_type}: {errors}")
    else:
        logging.info(f"{table_type.capitalize()} data inserted id={row.get('id')}")

def write_batch_to_bq(batch_df, batch_id, table_type):
    """Write Spark batch to BigQuery"""
    try:
        if (count := batch_df.count()) == 0:
            logging.info(f"Batch {batch_id} empty")
            return
        for row in batch_df.toLocalIterator():
            data = row.asDict(recursive=True)
            normalized = {
                k: (
                    v.strftime('%Y-%m-%dT%H:%M:%SZ') if isinstance(v, datetime) else
                    v.strftime('%Y-%m-%d') if isinstance(v, date) else v
                )
                for k, v in data.items()
            }
            insert_data(table_type, **normalized)
        logging.info(f"Batch {batch_id} written to BigQuery ({table_type})")
    except Exception as e:
        logging.error(f"Batch {batch_id} failed: {e}")

def create_spark_connection():
    try:
        return (SparkSession.builder
            .appName("SparkDataStreaming")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())
    except Exception as e:
        logging.error(f"Spark connection error: {e}")
        return None

def connect_to_kafka_topic(spark, topic):
    try:
        return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load())
    except Exception as e:
        logging.warning(f"Kafka connection error: {e}")
        return None
def create_selection_df_from_kafka_hourly(spark_df):
    """Process hourly weather data from Kafka"""
    # Schema for hourly topic
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("time", StringType(), False),
        StructField("latitude", StringType(), False),
        StructField("longitude", StringType(), False),
        StructField("timezone", StringType(), False),
        StructField("timezone_abbreviation", StringType(), False),
        StructField("temperature_2m", StringType(), True),
        StructField("relative_humidity_2m", StringType(), True),
        StructField("apparent_temperature", StringType(), True),
        StructField("precipitation", StringType(), True),
        StructField("surface_pressure", StringType(), True),
        StructField("cloud_cover", StringType(), True),
        StructField("wind_speed_10m", StringType(), True),
    ])

    df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")

    selection_df = df.select(
        col("id").cast("string").alias("id"),
        col("time").alias("time"),
        col("latitude").cast("double").alias("latitude"),
        col("longitude").cast("double").alias("longitude"),
        col("timezone").alias("timezone"),
        col("timezone_abbreviation").alias("timezone_abbreviation"),
        col("temperature_2m").cast("double").alias("temperature_2m"),
        (col("relative_humidity_2m").cast("double") / lit(100.0)).alias("relative_humidity_2m"),
        col("apparent_temperature").cast("double").alias("apparent_temperature"),
        col("precipitation").cast("double").alias("precipitation"),
        col("surface_pressure").cast("double").alias("surface_pressure"),
        (col("cloud_cover").cast("double") / lit(100.0)).alias("cloud_cover"),
        col("wind_speed_10m").cast("double").alias("wind_speed_10m")
    )

    # Transform time -> "dd/MM/yyyy hAM/PM" with 00:00 -> "0AM"
    ts = to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm")
    day_part = date_format(ts, "dd/MM/yyyy")
    # Set time_text to day only (dd/MM/yyyy)
    time_text = day_part

    selection_df = selection_df \
        .withColumn("time_ts", ts) \
        .withColumn("time_text", time_text)

    # Drop original raw field not present in BQ schema
    selection_df = selection_df.drop("time")

    return selection_df

def create_selection_df_from_kafka_daily(spark_df):
    """Process daily weather data from Kafka"""
    # Schema for daily topic
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("date", StringType(), False),
        StructField("latitude", StringType(), False),
        StructField("longitude", StringType(), False),
        StructField("timezone", StringType(), False),
        StructField("timezone_abbreviation", StringType(), False),
        StructField("sunrise", StringType(), True),
        StructField("sunset", StringType(), True),
        StructField("sunshine_duration", StringType(), True),
    ])

    df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")

    selection_df = df.select(
        col("id").cast("string").alias("id"),
        col("date").alias("date"),
        col("latitude").cast("double").alias("latitude"),
        col("longitude").cast("double").alias("longitude"),
        col("timezone").alias("timezone"),
        col("timezone_abbreviation").alias("timezone_abbreviation"),
        to_timestamp(col("sunrise"), "yyyy-MM-dd'T'HH:mm").alias("sunrise_ts"),
        to_timestamp(col("sunset"), "yyyy-MM-dd'T'HH:mm").alias("sunset_ts"),
        col("sunshine_duration").cast("double").alias("sunshine_duration")
    )

    date_ts = to_date(col("date"), "yyyy-MM-dd")
    selection_df = selection_df \
        .withColumn("date_ts", date_ts) \
        .withColumn("date_text", col("date")) \
        .withColumn("sunrise_time", date_format(col("sunrise_ts"), "HH:mm:ss")) \
        .withColumn("sunset_time", date_format(col("sunset_ts"), "HH:mm:ss"))

    # Convert sunshine_duration (seconds) -> HH:mm:ss
    s = col("sunshine_duration")
    hh = floor(s / lit(3600)).cast("int")
    mm = floor(pmod(s, lit(3600)) / lit(60)).cast("int")
    ss = pmod(s, lit(60)).cast("int")
    selection_df = selection_df.withColumn("sunshine_duration_time", format_string("%02d:%02d:%02d", hh, mm, ss))

    # Drop intermediate/raw fields not present in BQ schema
    selection_df = selection_df.drop("date", "sunrise_ts", "sunset_ts")

    return selection_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = create_spark_connection()

    if spark:
        df_hourly = connect_to_kafka_topic(spark, "weather_paris")
        df_daily = connect_to_kafka_topic(spark, "daily_paris")

        if not (df_hourly or df_daily):
            logging.error("Kafka connections failed")
        else:
            ensure_bq_tables()
            run_tag = datetime.now().strftime("%Y%m%d%H%M%S")

            queries = []
            if df_hourly:
                sel_hourly = create_selection_df_from_kafka_hourly(df_hourly)
                q1 = sel_hourly.writeStream \
                    .option("checkpointLocation", f"/tmp/checkpoints/hourly_{run_tag}") \
                    .foreachBatch(lambda bdf, bid: write_batch_to_bq(bdf, bid, "hourly")) \
                    .start()
                queries.append(q1)

            if df_daily:
                sel_daily = create_selection_df_from_kafka_daily(df_daily)
                q2 = sel_daily.writeStream \
                    .option("checkpointLocation", f"/tmp/checkpoints/daily_{run_tag}") \
                    .foreachBatch(lambda bdf, bid: write_batch_to_bq(bdf, bid, "daily")) \
                    .start()
                queries.append(q2)
            timeout_ms = 5 * 60 * 1000

            try:
                # Wait for any stream to terminate or timeout
                finished = spark.streams.awaitAnyTermination(timeout_ms / 1000)  # Convert to seconds
                if not finished:
                    print("Timeout reached, stopping all queries...")
                    for q in queries:
                        q.stop()
            except KeyboardInterrupt:
                print("Interrupted, stopping all queries...")
                for q in queries:
                    q.stop()
            finally:
                # Give queries a moment to clean up
                import time
                time.sleep(2)
                spark.stop()

