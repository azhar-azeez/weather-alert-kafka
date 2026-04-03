from kafka import KafkaProducer
import json
import time
import random

print("Attempting to connect to Kafka at localhost:9094...")

try:
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 5, 0),
        client_id='weather-producer-azhar',
        metadata_max_age_ms=1000,
        request_timeout_ms=5000,
        max_block_ms=5000
    )

    print("✅ Connected! Sending data every 2 seconds...")

    while True:
        data = {'city': 'New York', 'temp': random.randint(15, 35)}
        # We use a callback to see if it actually arrived
        future = producer.send('weather_updates', value=data)
        
        # This .get() will block until the message is sent or fails
        result = future.get(timeout=10)
        print(f"🚀 Sent: {data} (Topic: {result.topic}, Partition: {result.partition})")
        time.sleep(2)

except Exception as e:
    print(f"❌ Failed to connect or send: {e}")