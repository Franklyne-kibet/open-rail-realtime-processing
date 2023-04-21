# import findspark
# findspark.init()

import pyspark.sql.types as T

BOOTSTRAP_SERVERS = 'localhost:9092'

KAFKA_TOPIC = 'train_data'

# Convert the JSON string to PySpark schema
TRAIN_SCHEMA = T.StructType(
    [T.StructField("sequence_number", T.StringType()),
    T.StructField("schedule_id", T.StringType()),
    T.StructField("unique_id", T.StringType()),
    T.StructField("service_start_date", T.StringType()),
    T.StructField("location_code", T.StringType()),
    T.StructField("scheduled_arrival", T.StringType()),
    T.StructField("scheduled_departure", T.StringType()),
    T.StructField("actual_arrival", T.StringType()),
    T.StructField("actual_departure", T.StringType()),
    T.StructField("platforms", T.StringType()),
    T.StructField("estimated_time", T.StringType()),
    T.StructField("source", T.StringType())
    ])