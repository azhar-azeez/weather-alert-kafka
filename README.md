# Real-Time Weather Analytics Pipeline (Kafka + Docker)

A containerized, end-to-end data engineering pipeline that streams live weather data, performs real-time transformations, and persists data for Business Intelligence reporting.



## 🚀 Overview
This project demonstrates a distributed system architecture designed to handle high-velocity data. It fetches live weather metrics for multiple cities via the OpenWeather API, streams them through a **Kafka** broker, and processes them using a Python Consumer to calculate a derived **Heat Index** metric.

## 🛠️ Tech Stack
* **Language:** Python 3.11
* **Streaming:** Apache Kafka & Zookeeper
* **Containerization:** Docker & Docker Compose
* **Data Storage:** CSV (Warehouse Simulation)
* **Visualization:** Power BI

## 🏗️ System Architecture
1. **Weather Producer:** Polls OpenWeather API every 5 minutes and sends JSON payloads to Kafka.
2. **Kafka Broker:** Acts as the messaging backbone, decoupling the data source from the processor.
3. **Analytics Consumer:** Listens to the `weather_updates` topic, performs a transformation (Heat Index calculation), and appends data to a persistent CSV.
4. **BI Layer:** Power BI connects to the CSV for real-time trend analysis.

## 🔧 How to Run
Ensure you have **Docker Desktop** installed, then:

1. Clone the repository:
   ```bash
   git clone [https://github.com/azhar-azeez/weather-alert-kafka.git](https://github.com/azhar-azeez/weather-alert-kafka.git)

2. Start the entire pipeline:
    ```bash
    docker-compose up --build


📊 Business Logic (Transformation)
The consumer performs a real-time transformation to calculate the Heat Index, providing more analytical value than raw temperature:
Heat Index = Temp + (0.1 * Humidity)

🛡️ Key Features
Resiliency: Implemented retry logic for Kafka connection to handle distributed system race conditions.

Environment Aware: Uses environment variables to switch between local and Docker-based Kafka hosts.

Fault Tolerance: Schema-safe CSV writing using DictWriter with extra-action handling.


---
