import pandas as pd
import json
import time
from kafka import KafkaProducer
from python_callable.kafka.kafka_utils import send_df_to_kafka
import os

def producer_portugal_listings():
    topic_name = 'portugal_listings'
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'portugal_listinigs.csv')
    data = pd.read_csv(file_path)

    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer = lambda x: json.dumps(x).encode('utf-8'),
        acks='all',
        retries=5
    )

    send_df_to_kafka(data, topic_name, producer)