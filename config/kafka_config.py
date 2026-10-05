import os

# Kafka Connection & Topic Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "ai_telemetry_events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "ai_telemetry_dlq")

# Producer Configuration Parameters
PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",                 # Ensure full replication durability
    "retries": 5,                   # Retry transient network issues
    "max_in_flight_requests_per_connection": 1,  # Maintain strict ordering
}

# Consumer Group Configuration
CONSUMER_GROUP_ID = "aidp_lakehouse_ingestion_group"