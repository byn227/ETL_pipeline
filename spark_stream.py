import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
from datetime import datetime

from uuid import UUID
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType



PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

def ensure_pg_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id text  PRIMARY KEY,
        latitude double precision,
        longitude double precision,
        timezone text,
        timezone_abbreviation text,
        current_time_text text,
        temperature_2m DOUBLE PRECISION,
        relative_humidity_2m DOUBLE PRECISION,
        apparent_temperature DOUBLE PRECISION,
        precipitation DOUBLE PRECISION,
        rain DOUBLE PRECISION,
        snowfall DOUBLE PRECISION,
        showers DOUBLE PRECISION,
        wind_speed_10m DOUBLE PRECISION
    )
    """
    try:
        with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                conn.commit()
        logging.info("PostgreSQL table ensured: weather_data")
    except Exception as e:
        logging.error(f"Failed to ensure PostgreSQL table: {e}")


def insert_data(**kwargs):
    print("inserting data...")

    try:
        row_id = kwargs.get('id')
        if row_id is None:
            raise ValueError("id is required")
        try:
            row_id = UUID(str(row_id))
        except Exception:
            raise ValueError("id must be a valid UUID string")

        latitude = kwargs.get('latitude')
        longitude = kwargs.get('longitude')
        timezone = kwargs.get('timezone')
        timezone_abbreviation = kwargs.get('timezone_abbreviation')
        current_time = kwargs.get('current_time')
        temperature_2m = kwargs.get('temperature_2m')
        relative_humidity_2m = kwargs.get('relative_humidity_2m')
        apparent_temperature = kwargs.get('apparent_temperature')
        precipitation = kwargs.get('precipitation')
        rain = kwargs.get('rain')
        snowfall = kwargs.get('snowfall')
        showers = kwargs.get('showers')
        wind_speed_10m = kwargs.get('wind_speed_10m')

        with psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO weather_data(
                        id, latitude, longitude, timezone, timezone_abbreviation, current_time_text,
                        temperature_2m, relative_humidity_2m, apparent_temperature,
                        precipitation, rain, snowfall, showers, wind_speed_10m)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        str(row_id), latitude, longitude, timezone, timezone_abbreviation, current_time,
                        temperature_2m, relative_humidity_2m, apparent_temperature,
                        precipitation, rain, snowfall, showers, wind_speed_10m,
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
            .option("subscribe", "weather_created") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info('Kafka connection created successfully')
    except Exception as e:
        logging.warning("Error in creating kafka connection: %s", e)
    return spark_df

def write_batch_to_pg(batch_df, batch_id):
    # Use psycopg2 insert to avoid JDBC UUID type mismatch
    try:
        count = batch_df.count()
        logging.info(f"Batch {batch_id} size before write: {count}")
        if count == 0:
            logging.info(f"Batch {batch_id} empty; skipping write")
            return

        # Iterate locally and insert each row using validated UUIDs
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
    # Producer sends numbers as strings; parse as strings then cast
    schema=StructType([
        StructField("id", StringType(), False),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("timezone_abbreviation", StringType(), True),
        StructField("current_time", StringType(), True),
        StructField("temperature_2m", StringType(), True),
        StructField("relative_humidity_2m", StringType(), True),
        StructField("apparent_temperature", StringType(), True),
        StructField("precipitation", StringType(), True),
        StructField("rain", StringType(), True),
        StructField("snowfall", StringType(), True),
        StructField("showers", StringType(), True),
        StructField("wind_speed_10m", StringType(), True)
    ])
    df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
    selection_df = df.select(
        col("id").cast("string").alias("id"),
        col("latitude").cast("double").alias("latitude"),
        col("longitude").cast("double").alias("longitude"),
        col("timezone").alias("timezone"),
        col("timezone_abbreviation").alias("timezone_abbreviation"),
        col("current_time").alias("current_time"),
        col("temperature_2m").cast("double").alias("temperature_2m"),
        col("relative_humidity_2m").cast("double").alias("relative_humidity_2m"),
        col("apparent_temperature").cast("double").alias("apparent_temperature"),
        col("precipitation").cast("double").alias("precipitation"),
        col("rain").cast("double").alias("rain"),
        col("snowfall").cast("double").alias("snowfall"),
        col("showers").cast("double").alias("showers"),
        col("wind_speed_10m").cast("double").alias("wind_speed_10m")
    )
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
            
            