import os
import sys

# Ensure project root is in Python import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, year, month, day, hour, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

from config.spark_config import get_spark_session, CHECKPOINT_PATH, WAREHOUSE_PATH
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TELEMETRY_TOPIC

# -------------------------------------------------------------------------
# 1. Telemetry Schema Definition
# -------------------------------------------------------------------------
TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("app_id", StringType(), True),
    StructField("environment", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("model_metadata", StructType([
        StructField("model_name", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("prompt_tokens", LongType(), True),
        StructField("completion_tokens", LongType(), True),
        StructField("latency_ms", LongType(), True),
        StructField("is_anomaly_simulated", BooleanType(), True)
    ]), True),
    StructField("guardrail_flags", StructType([
        StructField("pii_detected", BooleanType(), True),
        StructField("content_policy_violated", BooleanType(), True)
    ]), True),
    StructField("user_context", StructType([
        StructField("org_unit", StringType(), True),
        StructField("region", StringType(), True)
    ]), True)
])

# -------------------------------------------------------------------------
# 2. Table DDL Execution
# -------------------------------------------------------------------------
def create_iceberg_table_if_not_exists(spark: SparkSession):
    """Ensures database and partitioned Apache Iceberg table exist in local catalog."""
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.ai_telemetry_events (
            event_id STRING,
            app_id STRING,
            environment STRING,
            timestamp STRING,
            model_metadata STRUCT<
                model_name: STRING,
                provider: STRING,
                prompt_tokens: BIGINT,
                completion_tokens: BIGINT,
                latency_ms: BIGINT,
                is_anomaly_simulated: BOOLEAN
            >,
            guardrail_flags STRUCT<
                pii_detected: BOOLEAN,
                content_policy_violated: BOOLEAN
            >,
            user_context STRUCT<
                org_unit: STRING,
                region: STRING
            >,
            event_timestamp TIMESTAMP,
            ingested_at TIMESTAMP,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING iceberg
        PARTITIONED BY (year, month, day, hour)
    """)
    print("[+] Apache Iceberg target table verified: 'local.db.ai_telemetry_events'")


# -------------------------------------------------------------------------
# 3. Main Streaming Consumer Execution
# -------------------------------------------------------------------------
def main():
    print("[*] Initializing PySpark Structured Streaming Engine...")
    
    # Required Maven packages for Kafka streaming & Iceberg extensions
    spark = get_spark_session(app_name="AIDP-Telemetry-Lakehouse-Consumer")
    spark.sparkContext.setLogLevel("WARN")

    # Ensure Target Table is initialized
    create_iceberg_table_if_not_exists(spark)

    ingest_checkpoint = os.path.join(CHECKPOINT_PATH, "telemetry_ingest")
    dlq_checkpoint = os.path.join(CHECKPOINT_PATH, "dlq_ingest")
    dlq_storage_path = os.path.join(WAREHOUSE_PATH, "dlq", "malformed_events")

    print(f"[*] Subscribing to Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS} | Topic: '{TELEMETRY_TOPIC}'...")

    # Read Streaming DataFrame from Kafka
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TELEMETRY_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON Payloads according to telemetry schema
    parsed_stream = raw_kafka_stream.select(
        from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_received_at")
    )

    # Route valid events vs malformed payloads
    valid_events = parsed_stream.filter(col("data.event_id").isNotNull()).select("data.*")
    malformed_events = parsed_stream.filter(col("data.event_id").isNull())

    # Add temporal partition columns for downstream partitioning
    processed_stream = valid_events \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", day(col("event_timestamp"))) \
        .withColumn("hour", hour(col("event_timestamp")))

    # Write Micro-Batches into Apache Iceberg Sink
    iceberg_query = (
        processed_stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", ingest_checkpoint)
        .toTable("local.db.ai_telemetry_events")
    )

    # Write Malformed Records to Dead Letter Queue (DLQ)
    dlq_query = (
        malformed_events.writeStream
        .format("json")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", dlq_checkpoint)
        .option("path", dlq_storage_path)
        .start()
    )

    print("[+] Structured Streaming pipeline active. Sinking micro-batches into Iceberg (Press Ctrl+C to exit)...")

    try:
        iceberg_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[*] Gracefully stopping streaming consumers...")
        iceberg_query.stop()
        dlq_query.stop()
        print("[+] Streaming queries terminated cleanly.")


if __name__ == "__main__":
    main()