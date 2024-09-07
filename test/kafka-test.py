from kafka import KafkaProducer
from kafka.errors import KafkaError

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1)
    )
    print("Successfully connected to Kafka")
    producer.send('test-topic', b'test message')
    producer.flush()
    print("Message sent successfully")
except KafkaError as e:
    print(f"Failed to connect to Kafka: {e}")