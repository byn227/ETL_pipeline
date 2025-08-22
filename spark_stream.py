import logging
from datetime import datetime
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    date_format,
    when,
    concat,
    lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType



PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

def ensure_pg_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS meteo (
        id text PRIMARY KEY,
        time_text text,
        time_ts timestamp,
        latitude double precision,
        longitude double precision,
        timezone text,
        timezone_abbreviation text,
        temperature_2m double precision,
        relative_humidity_2m double precision,
        apparent_temperature double precision,
        precipitation double precision,
        pressure_msl double precision,
        cloud_cover_low double precision,
        wind_speed_10m double precision
    )
    """
    try:
        with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                conn.commit()
        logging.info("PostgreSQL table ensured: meteo")
    except Exception as e:
        logging.error(f"Failed to ensure PostgreSQL table: {e}")


def insert_data(**kwargs):
    print("inserting data...")

    try:
        row_id = kwargs.get('id')
        if row_id is None:
            raise ValueError("id is required")

        latitude = kwargs.get('latitude')
        longitude = kwargs.get('longitude')
        timezone = kwargs.get('timezone')
        timezone_abbreviation = kwargs.get('timezone_abbreviation')
        time_text = kwargs.get('time_text')
        time_ts = kwargs.get('time_ts')
        temperature_2m = kwargs.get('temperature_2m')
        relative_humidity_2m = kwargs.get('relative_humidity_2m')
        apparent_temperature = kwargs.get('apparent_temperature')
        precipitation = kwargs.get('precipitation')
        pressure_msl = kwargs.get('pressure_msl')
        cloud_cover_low = kwargs.get('cloud_cover_low')
        wind_speed_10m = kwargs.get('wind_speed_10m')

        with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO meteo(
                        id, time_text, time_ts, latitude, longitude, timezone, timezone_abbreviation,
                        temperature_2m, relative_humidity_2m, apparent_temperature,
                        precipitation, pressure_msl, cloud_cover_low, wind_speed_10m)
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        str(row_id), time_text, time_ts, latitude, longitude, timezone, timezone_abbreviation,
                        temperature_2m, relative_humidity_2m, apparent_temperature,
                        precipitation, pressure_msl, cloud_cover_low, wind_speed_10m,
                    ),
                )
                conn.commit()
        logging.info(f"Data inserted into Postgres for id={row_id}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')
        
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


def connect_to_kafka(spark):
    spark_df=None
    try:
        spark_df=spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "weather_paris") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info('Kafka connection created successfully')
    except Exception as e:
        logging.warning("Error in creating kafka connection: %s", e)
    return spark_df

def write_batch_to_pg(batch_df, batch_id):
    # Use psycopg2 insert per-row
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
                insert_data(**data)
            except Exception as row_err:
                logging.error(f"Row insert failed in batch {batch_id}: {row_err}; row={data}")
        logging.info(f"Batch {batch_id} written to Postgres via psycopg2")
    except Exception as e:
        logging.error(f"Failed to process batch {batch_id}: {e}")

def create_selection_df_from_kafka(spark_df):
    # Producer now sends one record per hour with numeric values
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
        StructField("pressure_msl", StringType(), True),
        StructField("cloud_cover_low", StringType(), True),
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
        col("relative_humidity_2m").cast("double").alias("relative_humidity_2m"),
        col("apparent_temperature").cast("double").alias("apparent_temperature"),
        col("precipitation").cast("double").alias("precipitation"),
        col("pressure_msl").cast("double").alias("pressure_msl"),
        col("cloud_cover_low").cast("double").alias("cloud_cover_low"),
        col("wind_speed_10m").cast("double").alias("wind_speed_10m")
    )
    
    # Transform time -> "dd/MM/yyyy hAM/PM" with 00:00 -> "0AM"
    ts = to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm")
    day_part = date_format(ts, "dd/MM/yyyy")
    hour24 = date_format(ts, "H").cast("int")  # 0-23 without leading zero
    hour12 = when(hour24 == 0, lit("0")) \
        .when(hour24 <= 12, date_format(ts, "H")) \
        .otherwise((hour24 - lit(12)).cast("string"))
    ampm = when(hour24 < 12, lit("AM")).otherwise(lit("PM"))
    time_text = concat(day_part, lit(" "), hour12, ampm)

    selection_df = selection_df \
        .withColumn("time_ts", ts) \
        .withColumn("time_text", time_text) \
        .withColumn("relative_humidity_2m", col("relative_humidity_2m") / lit(100.0)) \
        .withColumn("cloud_cover_low", col("cloud_cover_low") / lit(100.0))

    print(selection_df)
    return selection_df

if __name__ == "__main__":
    spark_con=create_spark_connection()

    if spark_con is not None:
        df=connect_to_kafka(spark_con)
        if df is not None:
            selection_df=create_selection_df_from_kafka(df)
            ensure_pg_table()
            streaming_query=selection_df.writeStream \
                        .option("checkpointLocation", "/tmp/checkpoints/weather_created") \
                        .foreachBatch(write_batch_to_pg) \
                        .start()
            streaming_query.awaitTermination()
        else:
            logging.error("Kafka connection failed")
            
            