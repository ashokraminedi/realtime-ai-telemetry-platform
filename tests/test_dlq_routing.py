from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import StringType, StructType, TimestampType


def test_dlq_routing_logic(spark):
    """Verifies that valid telemetry records and corrupt payloads are split accurately."""

    raw_data = [
        # Valid record
        (
            '{"event_id": "evt-101", "event_type": "llm_completion", "timestamp": "2026-10-05T12:00:00Z"}'
        ),
        # Corrupt JSON payload
        ('{"event_id": "evt-102", "event_type": "llm_completion", MALFORMED_JSON}'),
        # Missing required field (event_id is null)
        ('{"event_type": "llm_completion", "timestamp": "2026-10-05T12:00:00Z"}'),
    ]

    df_raw = spark.createDataFrame([(x,) for x in raw_data], ["value"])

    # Define validation logic
    schema = StructType()
    schema.add("event_id", StringType(), True)
    schema.add("event_type", StringType(), True)

    parsed_df = df_raw.withColumn("parsed", from_json(col("value"), schema))

    # Filter valid vs invalid
    valid_df = parsed_df.filter(col("parsed.event_id").isNotNull())
    dlq_df = parsed_df.filter(col("parsed.event_id").isNull())

    assert valid_df.count() == 1
    assert dlq_df.count() == 2