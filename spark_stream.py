import logging
from datetime import datetime
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

# Fixed table names as requested
HOURLY_TABLE = "meteo_hourly"
DAILY_TABLE = "meteo_daily"

def ensure_pg_tables():
    ddl_hourly = f"""
    CREATE TABLE IF NOT EXISTS {HOURLY_TABLE} (
                id text,
                time_text text,
                time_ts timestamp NOT NULL,
                latitude double precision,
                longitude double precision,
                timezone text,
                timezone_abbreviation text,
                temperature_2m double precision,
                relative_humidity_2m double precision,
                apparent_temperature double precision,
                precipitation double precision,
                surface_pressure double precision,
                cloud_cover double precision,
                wind_speed_10m double precision,
                day text,
                UNIQUE (id, time_ts)
            );
    """
    ddl_daily = f"""
    CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
                id text,
                date_text text,
                date_ts date NOT NULL,
                latitude double precision,
                longitude double precision,
                timezone text,
                timezone_abbreviation text,
                sunrise_time text,
                sunset_time text,
                sunshine_duration double precision,
                sunshine_duration_time text,
                UNIQUE (id, date_ts)
            );
    """
    try:
        with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl_hourly)
                cur.execute(ddl_daily)
        logging.info(f"PostgreSQL tables ensured: {HOURLY_TABLE}, {DAILY_TABLE}")
    except Exception as e:
        logging.error(f"Failed to ensure PostgreSQL tables: {e}")


def insert_hourly_data(**kwargs):
    try:
        row_id = kwargs.get('id')
        if row_id is None:
            raise ValueError("id is required")

        with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {HOURLY_TABLE} (
                        id, time_text, time_ts, latitude, longitude, timezone, timezone_abbreviation,
                        temperature_2m, relative_humidity_2m, apparent_temperature,
                        precipitation, surface_pressure, cloud_cover, wind_speed_10m, day
                    ) VALUES (
                        %(id)s, %(time_text)s, %(time_ts)s, %(latitude)s, %(longitude)s, %(timezone)s, %(timezone_abbreviation)s,
                        %(temperature_2m)s, %(relative_humidity_2m)s, %(apparent_temperature)s,
                        %(precipitation)s, %(surface_pressure)s, %(cloud_cover)s, %(wind_speed_10m)s, %(day)s
                    )
                    ON CONFLICT (id, time_ts) DO NOTHING
                    """,
                    kwargs,
                )
                conn.commit()
        logging.info(f"Hourly data inserted into Postgres for id={row_id}")
    except Exception as e:
        logging.error(f'could not insert hourly data due to {e}')

def insert_daily_data(**kwargs):
    try:
        row_id = kwargs.get('id')
        if row_id is None:
            raise ValueError("id is required")

        with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {DAILY_TABLE} (
                        id, date_text, date_ts, latitude, longitude, timezone, timezone_abbreviation,
                        sunrise_time, sunset_time, sunshine_duration, sunshine_duration_time
                    ) VALUES (
                        %(id)s, %(date_text)s, %(date_ts)s, %(latitude)s, %(longitude)s, %(timezone)s, %(timezone_abbreviation)s,
                        %(sunrise_time)s, %(sunset_time)s, %(sunshine_duration)s, %(sunshine_duration_time)s
                    )
                    ON CONFLICT (id, date_ts) DO NOTHING
                    """,
                    kwargs,
                )
                conn.commit()
        logging.info(f"Daily data inserted into Postgres for id={row_id}")
    except Exception as e:
        logging.error(f'could not insert daily data due to {e}')
        
def create_spark_connection():
    spark_c = None
    try:
        spark_c = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages",
            "org.postgresql:postgresql:42.7.4,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.0") \
            .getOrCreate()
        spark_c.sparkContext.setLogLevel("ERROR")
        logging.info('Spark connection created successfully')
    except Exception as e:
        logging.error("Error in creating spark connection: %s", e) 
    return spark_c


def connect_to_kafka_topic(spark, topic):
    spark_df = None
    try:
        spark_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info(f'Kafka connection created successfully for topic={topic}')
    except Exception as e:
        logging.warning("Error in creating kafka connection: %s", e)
    return spark_df

def write_batch_to_pg(batch_df, batch_id, inserter):
    
    try:
        count = batch_df.count()
        logging.info(f"Batch {batch_id} size before write: {count}")
        if count == 0:
            logging.info(f"Batch {batch_id} empty; skipping write")
            return

        # Iterate locally and insert each row
        for row in batch_df.toLocalIterator():
            data = row.asDict(recursive=True)
            try:
                inserter(**data)
            except Exception as row_err:
                logging.error(f"Row insert failed in batch {batch_id}: {row_err}; row={data}")
        logging.info(f"Batch {batch_id} written to Postgres via psycopg2")
    except Exception as e:
        logging.error(f"Failed to process batch {batch_id}: {e}")

def create_selection_df_from_kafka_hourly(spark_df):
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
        (col("relative_humidity_2m").cast("double")/lit(100.0)).alias("relative_humidity_2m"),
        col("apparent_temperature").cast("double").alias("apparent_temperature"),
        col("precipitation").cast("double").alias("precipitation"),
        col("surface_pressure").cast("double").alias("surface_pressure"),
        (col("cloud_cover").cast("double")/lit(100.0)).alias("cloud_cover"),
        col("wind_speed_10m").cast("double").alias("wind_speed_10m")
    )

    # Transform time -> "dd/MM/yyyy hAM/PM" with 00:00 -> "0AM"
    ts = to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm")
    day_part = date_format(ts, "dd/MM/yyyy")
    hour24 = date_format(ts, "H").cast("int")
    hour12 = when(hour24 == 0, lit("0")) \
        .when(hour24 <= 12, date_format(ts, "H")) \
        .otherwise((hour24 - lit(12)).cast("string"))
    ampm = when(hour24 < 12, lit("AM")).otherwise(lit("PM"))
    time_text = concat(day_part, lit(" "), hour12, ampm)

    selection_df = selection_df \
        .withColumn("time_ts", ts) \
        .withColumn("time_text", time_text) \
        .withColumn("day", date_format(ts, "dd/MM/yyyy"))

    print(selection_df)
    return selection_df

def create_selection_df_from_kafka_daily(spark_df):
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

    print(selection_df)
    return selection_df

if __name__ == "__main__":
    spark_con = create_spark_connection()

    if spark_con is not None:
        # Connect to topics
        df_hourly = connect_to_kafka_topic(spark_con, "weather_paris")
        df_daily = connect_to_kafka_topic(spark_con, "daily_paris")

        if df_hourly is None and df_daily is None:
            logging.error("Kafka connections failed for both topics")
        else:
            ensure_pg_tables()

            queries = []
            if df_hourly is not None:
                sel_hourly = create_selection_df_from_kafka_hourly(df_hourly)
                q_hourly = sel_hourly.writeStream \
                    .option("checkpointLocation", "/tmp/checkpoints/weather_hourly") \
                    .foreachBatch(lambda bdf, bid: write_batch_to_pg(bdf, bid, insert_hourly_data)) \
                    .start()
                queries.append(q_hourly)

            if df_daily is not None:
                sel_daily = create_selection_df_from_kafka_daily(df_daily)
                q_daily = sel_daily.writeStream \
                    .option("checkpointLocation", "/tmp/checkpoints/weather_daily") \
                    .foreachBatch(lambda bdf, bid: write_batch_to_pg(bdf, bid, insert_daily_data)) \
                    .start()
                queries.append(q_daily)

            for q in queries:
                q.awaitTermination()