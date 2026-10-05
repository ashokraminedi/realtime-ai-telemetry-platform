import sys
import os

# Ensure project root is in Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TELEMETRY_TOPIC, PRODUCER_CONFIG


def create_producer() -> KafkaProducer:
    """
    Initializes and returns a KafkaProducer instance configured with 
    JSON serialization and durability settings.
    """
    return KafkaProducer(
        bootstrap_servers=PRODUCER_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=PRODUCER_CONFIG["acks"],
        retries=PRODUCER_CONFIG["retries"],
        max_in_flight_requests_per_connection=PRODUCER_CONFIG["max_in_flight_requests_per_connection"]
    )


def generate_telemetry_payload() -> dict:
    """
    Generates realistic LLM microservice execution trace telemetry.
    Simulates latency anomalies (~5% rate) and guardrail flags.
    """
    models = [
        ("gpt-4o", "openai"),
        ("claude-3-5-sonnet", "anthropic"),
        ("llama-3-70b", "meta"),
        ("internal-apple-llm-v1", "apple")
    ]
    apps = ["search-agent-v2", "customer-support-bot", "code-assistant", "summarization-engine"]
    orgs = ["retail-analytics", "search-relevance", "core-platform", "siri-intelligence"]

    selected_model, provider = random.choice(models)

    # Simulate latency anomaly (~5% probability)
    is_anomaly = random.random() < 0.05
    latency_ms = random.randint(2500, 8000) if is_anomaly else random.randint(120, 600)

    app_id = random.choice(apps)
    event_id = f"evt_{random.randint(100000, 999999)}"

    return {
        "event_id": event_id,
        "app_id": app_id,
        "environment": "production",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "model_metadata": {
            "model_name": selected_model,
            "provider": provider,
            "prompt_tokens": random.randint(150, 2500),
            "completion_tokens": random.randint(30, 800),
            "latency_ms": latency_ms,
            "is_anomaly_simulated": is_anomaly
        },
        "guardrail_flags": {
            "pii_detected": random.random() < 0.02,
            "content_policy_violated": random.random() < 0.01
        },
        "user_context": {
            "org_unit": random.choice(orgs),
            "region": "us-east-1"
        }
    }


def main():
    print(f"[*] Connecting Kafka Producer to broker: {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = create_producer()
    print(f"[+] Connected! Publishing telemetry events to topic: '{TELEMETRY_TOPIC}'...")
    print("[*] Press Ctrl+C to stop streaming.\n")

    event_count = 0
    try:
        while True:
            payload = generate_telemetry_payload()
            
            # Use app_id as partition key to preserve ordering per application
            producer.send(
                topic=TELEMETRY_TOPIC,
                key=payload["app_id"],
                value=payload
            )
            event_count += 1

            if event_count % 10 == 0:
                print(
                    f"[+] Streamed {event_count} events | "
                    f"Latest: ID={payload['event_id']} | "
                    f"App={payload['app_id']} | "
                    f"Model={payload['model_metadata']['model_name']} | "
                    f"Latency={payload['model_metadata']['latency_ms']}ms"
                )

            time.sleep(0.2)  # ~5 events/sec throughput

    except KeyboardInterrupt:
        print("\n[*] Stopping producer gracefully...")
    except KafkaError as e:
        print(f"[!] Kafka error encountered: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"[+] Producer stopped. Total events emitted: {event_count}")


if __name__ == "__main__":
    main()