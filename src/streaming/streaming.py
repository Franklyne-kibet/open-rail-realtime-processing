from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from settings import KAFKA_TOPIC,TRAIN_SCHEMA

def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_kafka_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_kafka_raw

def parse_train_from_kafka_message(df_raw, schema):
    assert df_raw.isStreaming is True, "DataFrame doesn't receive streaming data"
    
    df_raw = df_raw.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    
    # Convert the value column from JSON string to PySpark schema
    df_json = df_raw.select(from_json(col("value"), schema).alias("train"))

    # Flatten the nested columns and select the required columns
    df_flattened = df_json.selectExpr(
        "CAST(train.sequence_number AS LONG) AS sequence_number",
        "CAST(train.schedule_id AS LONG) AS schedule_id",
        "train.unique_id",
        "TO_DATE(train.service_start_date) AS service_start_date",
        "train.location_code",
        "train.scheduled_arrival",
        "train.scheduled_departure",
        "train.actual_arrival",
        "train.actual_departure",
        "train.platforms",
        "train.estimated_time",
        "train.source"
    )

    return df_flattened

def write_cassandra(df):
    if df.isStreaming:
        write_query = df.writeStream \
            .foreachBatch(lambda batchDF, epochId: batchDF.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="service_performance", keyspace="train_service") \
            .save()) \
            .start()
        return write_query
    else:
        print("Data is not streaming")

def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query # pyspark.sql.streaming.StreamingQuery    

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark-Trains") \
        .config("spark.cassandra.connection.host", "172.19.0.2") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.output.consistency.level", "ONE") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel('WARN')

    # Set the configuration option
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    
    # Set the configuration to stop the job gracefully on shutdown
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    
    # read_streaming data
    df_consume_stream=read_from_kafka(consume_topic=KAFKA_TOPIC)
    print(df_consume_stream.printSchema())
    
    # parse streaming data
    df_trains = parse_train_from_kafka_message(df_consume_stream, TRAIN_SCHEMA)
    print(df_trains.printSchema())
    
    # Write the data to cassandra db
    write_cassandra(df_trains)
    
    # Sink the data to console
    sink_console(df_trains, output_mode='append')
    
    spark.streams.awaitAnyTermination()