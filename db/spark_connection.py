import logging

from connection import create_spark_connection, connect_to_kafka, create_selection_df_from_kafka, create_cassandra_connection
from create_db import create_table, create_keyspace

# Create spark connection
spark_conn = create_spark_connection()

if spark_conn is not None:
    # Connect to kafka with spark connection
    spark_df = connect_to_kafka(spark_conn)
    selection_df = create_selection_df_from_kafka(spark_df)
    session = create_cassandra_connection()

    if session is not None:
        create_keyspace(session)
        create_table(session)

        logging.info("Streaming is being started....")

        streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                           .option('checkpointLocation', '/tmp/checkpoint') \
                           .option('keyspace', 'spark_streams') \
                           .option('table', 'created_users').start())

        streaming_query.awaitTermination()