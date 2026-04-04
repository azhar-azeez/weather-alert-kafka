import json
import csv
import os
import time
from kafka import KafkaConsumer

# File setup
csv_file = "weather_history.csv"
fieldnames = ['city', 'temp', 'humidity', 'description', 'heat_index', 'timestamp']

# Create CSV file with headers if it doesn't exist
if not os.path.exists(csv_file):
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

# Get the host dynamically
kafka_host = os.getenv('KAFKA_SERVER', '127.0.0.1:9092')

consumer = None
print(f"🔗 Attempting to connect to Kafka at {kafka_host}...")
while consumer is None:
    try:
        consumer = KafkaConsumer (
            'weather_updates',
            bootstrap_servers=[kafka_host],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            api_version=(3, 5, 0)
        )
    except Exception as e:
        print(f"❌ Could not connect to Kafka at {kafka_host}: {e}. Retrying in 5 seconds...")
        time.sleep(5)

print("📊 Data Warehouse Consumer Connected. Saving live feeds to CSV...")

try:
    for message in consumer:
        data = message.value

        # --- Transformation Step (Data Analyst Skill) ---
        # Simple Heat Index: Temp + (0.55 - 0.55 * Humidity/100) * (Temp - 14.5)
        data['heat_index'] = round(data['temp'] + (0.1 * data['humidity']), 2)

        # --- Persistence Step (Data Engineering Skill) --
        with open(csv_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writerow(data)

        # Output to terminal
        print(f"📥 Logged {data['city']}: {data['temp']}°C | Heat Index: {data['heat_index']}")
        
        if data['temp'] > 30:
            print(f"⚠️  ALERT: Critical heat in {data['city']}")

except KeyboardInterrupt:
    print("Stopping Consumer...")

