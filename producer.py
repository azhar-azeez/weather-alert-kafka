import requests
import json
import time
import sys
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.errors import NoBrokersAvailable

# 1. configuration
API_KEY = "8ac01219aa0b30a52043257975ca8e46"  # API key
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Miami"]
TOPIC = 'weather_updates'

# --- 2. Initialize Producer with Retry Logic ---
kafka_host = os.getenv('KAFKA_SERVER', '127.0.0.1:9092')

producer = None
print(f"🔗 Attempting to connect to Kafka at {kafka_host}...")
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka_host],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(3, 5, 0),
            retries=5  
        )
        print("✅ Producer Connected to Kafka!")
    except Exception as e:
        print(f"❌ Kafka not ready at. Retrying in 5 seconds...")
        time.sleep(5)



def get_live_weather(city):
    """Fetches real data with error handling."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Raises error for 4xx or 5xx responses
        data = response.json()
        return {
            "city": data["name"],
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "description": data["weather"][0]["description"],
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
    except requests.exceptions.RequestException as e:
        print(f"⚠️ API Error for {city}: {e}")
        return None

print(f" Producer Active. Streaming {len(CITIES)} cities to Kafka...")

# --- 3. Main Loop ---
try:
    while True:
        for city in CITIES:
            weather_report = get_live_weather(city)
            
            if weather_report:
                # Use a callback to confirm the message actually hit the broker
                future = producer.send(TOPIC, value=weather_report)
                try:
                    record_metadata = future.get(timeout=10)
                    print(f"✅ Sent: {weather_report['city']} | {weather_report['temp']}°C (Partition: {record_metadata.partition})")
                except KafkaError as e:
                    print(f"❌ Kafka Send Failed: {e}")
        
        print("💤 Sleeping for 5 minutes...")
        time.sleep(300) 

except KeyboardInterrupt:
    print("\n Shutting down Producer. Closing connections...")
    producer.close()