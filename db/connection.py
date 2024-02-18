import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder.appName("CityResidenceStreaming")\
                    .config('spark.jars.package', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,'
                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1")\
                    .config('spark.cassandra.connection.host', 'localhost').getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")

    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream.format("kafka") \
            .option("kafka.boostrap.servers", "localhost:9092") \
            .option("subscribe", "user_created") \
            .option("startingOffsets", "earliest").load()

        logging.info("Kafka dataframe created successfully")

    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        # Connecting to the cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()

        return cas_session

    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")

        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("firstname", StringType(), False),
        StructField("lastname", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StringType("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")

    print(sel)

    return sel