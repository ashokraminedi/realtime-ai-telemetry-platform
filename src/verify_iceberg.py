from config.spark_config import get_spark_session

spark = get_spark_session("Iceberg-Verification")

# Query table records
spark.sql("SELECT * FROM local.db.ai_telemetry_events ORDER BY timestamp DESC LIMIT 10").show()

# Inspect Iceberg table metadata & snapshots
spark.sql("SELECT snapshot_id, committed_at, operation FROM local.db.ai_telemetry_events.snapshots").show()