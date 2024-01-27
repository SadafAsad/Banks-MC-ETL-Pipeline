# Banks MC ETL Pipeline
This project aims to simulate a live data streaming pipeline for collecting, transforming, and storing data. It leverages Python, Airflow, PostgreSQL, Kafka, Spark, and Cassandra to create an end-to-end solution. Docker is employed to containerize everything, ensuring consistency and simplified deployment.

<p align="center">
  <img src="tasks-graph." width='500' />
</p>

## System Architecture
- Data Collection: [randomuser.me](https://randomuser.me/) API serves as the source of raw data
- Data Transformation: Python is employed for transforming raw data into the required format
- Airflow: Orchestrates the entire workflow 
- PostgreSQL: Setup in conjunction with Airflow to be used for metadata storage or other purposes 
- Kafka (Confluent Cloud): Central hub for data streaming
  * Zookeeper:Coordinates and manages Kafka brokers 
  * Control Center: Provides real-time monitoring and management capabilities for Kafka 
  * Schema Registry: Manages schema evolution and compatibility in the Kafka topics
- Spark: Configured with a master and a worker to subscribe to Kafka consumer and process data. 
- Cassandra: Used as the destination storage.

