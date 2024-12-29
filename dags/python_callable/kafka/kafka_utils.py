import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import json
import os

FAILED_MESSAGES_FILE = "failed_messages.jsonl"

def send_df_to_kafka(df: pd.DataFrame, 
                     topic_name: str,
                     producer: KafkaProducer, 
                     rows_send_per_sec: int = 10):
    
    resend_failed_messages(producer, topic_name)
    
    for index, row in df.iterrows():
        message = row.to_dict()

        try:
            producer.send(topic_name, value=message).get(timeout=10)
        except KafkaError as e:
            save_failed_message(message)
            print(f"Failed to send msg: \n{e}")
        
        time.sleep(rows_send_per_sec / 10)

def save_failed_message(message):
    with open(FAILED_MESSAGES_FILE, 'a') as f:
        f.write(json.dumps(message) + '\n')

def resend_failed_messages(producer: KafkaProducer, topic_name: str):
    if not os.path.exists(FAILED_MESSAGES_FILE):
        print("File with failed msges does not exists.")
        return
    
    with open(FAILED_MESSAGES_FILE, 'r') as f:
        lines = f.readlines()
    with open(FAILED_MESSAGES_FILE, 'w') as f:
        pass
    for line in lines:
        message = json.loads(line)
        try:
            producer.send(topic_name, value=message).get(timeout=10)
            print(f"Resent message: {message}")
        except KafkaError as e:
            print(f"Failed to resend message: {e}")
            save_failed_message(message)