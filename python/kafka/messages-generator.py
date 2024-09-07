import json
import random
import time
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'ecommerce_clickstream'

# Event generation parameters
EVENT_TYPES = ['page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 'purchase']
PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home & Kitchen', 'Sports', 'Beauty']
SOURCES = ['mobile_app', 'website']

def generate_event():
    timestamp = int(time.time() * 1000)
    return {
        'user_id': f'user_{random.randint(1, 100000)}',
        'session_id': f'session_{random.randint(1, 1000000)}',
        'timestamp': timestamp,
        'event_type': random.choice(EVENT_TYPES),
        'product_id': f'product_{random.randint(1, 10000)}',
        'product_category': random.choice(PRODUCT_CATEGORIES),
        'price': round(random.uniform(1, 1000), 2),
        'quantity': random.randint(1, 10),
        'source': random.choice(SOURCES),
        'page_url': f'/category/{random.choice(PRODUCT_CATEGORIES).lower()}/product_{random.randint(1, 10000)}',
        'user_agent': f'Mozilla/5.0 ({"Mobile" if random.random() < 0.5 else "Desktop"}; rv:{random.randint(70, 100)}.0)',
        'ip_address': f'{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}'
    }

def produce_events(producer, events_per_second):
    while True:
        start_time = time.time()
        events = [generate_event() for _ in range(events_per_second)]
        for event in events:
            producer.send(KAFKA_TOPIC, json.dumps(event).encode('utf-8'))
        producer.flush()
        elapsed_time = time.time() - start_time
        if elapsed_time < 1:
            time.sleep(1 - elapsed_time)

def main():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1)
    )
    events_per_second = 50000
    num_threads = 10
    events_per_thread = events_per_second // num_threads

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for _ in range(num_threads):
            executor.submit(produce_events, producer, events_per_thread)

if __name__ == "__main__":
    main()