from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import logging
import socket

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'ecommerce_clickstream'

# File to write messages to
OUTPUT_FILE = 'kafka_messages.jsonl'


def is_port_open(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host, port))
            return True
        except socket.error:
            return False


def main():
    # Check if Kafka port is open
    if not is_port_open('localhost', 9092):
        logging.error("Kafka port 9092 is not open. Is Kafka running?")
        return

    logging.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}")

    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='file_writer_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(0, 10, 1)  # Specify the API version
        )

        logging.info(
            f"Successfully connected to Kafka. Starting to consume messages from topic '{KAFKA_TOPIC}' and write to '{OUTPUT_FILE}'")

        with open(OUTPUT_FILE, 'w') as f:
            for message in consumer:
                json.dump(message.value, f)
                f.write('\n')
                f.flush()
                print('.', end='', flush=True)

    except NoBrokersAvailable:
        logging.error("No brokers available. Check if Kafka is running and the broker address is correct.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()