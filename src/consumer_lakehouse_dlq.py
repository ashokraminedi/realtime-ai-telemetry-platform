import os
import sys

# Add project root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, from_json, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from config.spark_config import get_spark_session

# Strict schema definition for valid AI telemetry events
TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("service_name", StringType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])


def process_micro_batch(df, batch_id):
    """
    ForeachBatch writer executing split-stream routing:
    - Valid records -> Apache Iceberg table
    - Malformed payloads -> Dead-Letter Queue (DLQ)
    """
    if df.isEmpty():
        return

    # Parse raw payload string into structured columns
    parsed_df = df.select(
        col("key").cast("string").alias("raw_key"),
        col("value").cast("string").alias("raw_value"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data")
    )

    # Filter 1: Compliant telemetry data
    valid_events = parsed_df.filter(col("data.event_id").isNotNull()).select(
        col("data.event_id").alias("event_id"),
        col("data.service_name").alias("service_name"),
        col("data.latency_ms").alias("latency_ms"),
        col("data.timestamp").alias("timestamp")
    )

    # Filter 2: Corrupted or unparseable payloads (DLQ stream)
    invalid_events = parsed_df.filter(col("data.event_id").isNull()).select(
        col("raw_key").alias("failed_key"),
        col("raw_value").alias("failed_payload"),
        col("kafka_timestamp").alias("received_at"),
        current_timestamp().alias("dlq_processed_at")
    )

    # Write valid records atomically into Iceberg table
    if not valid_events.isEmpty():
        valid_events.write \
            .format("iceberg") \
            .mode("append") \
            .save("local.db.ai_telemetry_events")
        print(f"[Batch {batch_id}] Successfully appended {valid_events.count()} valid events to Iceberg.")

    # Write malformed records to DLQ storage table for auditing
    if not invalid_events.isEmpty():
        invalid_events.write \
            .format("iceberg") \
            .mode("append") \
            .save("local.db.ai_telemetry_dlq")
        print(f"[!] [Batch {batch_id}] Routed {invalid_events.count()} malformed events to DLQ table.")


def start_streaming_pipeline_with_dlq():
    spark = get_spark_session("Lakehouse-Consumer-DLQ")

    # Ensure Iceberg tables exist
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.ai_telemetry_events (
            event_id STRING,
            service_name STRING,
            latency_ms DOUBLE,
            timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (hours(timestamp))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.ai_telemetry_dlq (
            failed_key STRING,
            failed_payload STRING,
            received_at TIMESTAMP,
            dlq_processed_at TIMESTAMP
        )
        USING iceberg
    """)

    # Read continuous micro-batches from Kafka broker
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ai_telemetry_events") \
        .option("startingOffsets", "latest") \
        .load()

    # Execute split streaming batch writer
    query = kafka_stream.writeStream \
        .foreachBatch(process_micro_batch) \
        #.option("checkpointLocation", "s3a://telemetry-warehouse/checkpoints/ai_telemetry_dlq") \
        # Change checkpointLocation from s3a://... to a local path:
        #.option("checkpointLocation", "/tmp/spark_checkpoints/telemetry_dlq") \
        #.start()

    query.awaitTermination()


if __name__ == "__main__":
    start_streaming_pipeline_with_dlq()