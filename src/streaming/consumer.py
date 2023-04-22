import json
from kafka import KafkaConsumer
from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    auto_offset_reset='latest', 
    key_deserializer=lambda key: json.loads(key.decode('utf-8')),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000  # stop consuming after 1 second of inactivity
)

for message in consumer:
    print(f"Received message: {message.value}")