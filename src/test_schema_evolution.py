import os
import sys

# Add project root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.spark_config import get_spark_session


def demonstrate_schema_evolution():
    spark = get_spark_session("Iceberg-Schema-Evolution")
    table_name = "local.db.ai_telemetry_events"

    print("--- 1. Inspecting Current Table Partitioning & Schema ---")
    spark.sql(f"DESCRIBE TABLE {table_name}").show(truncate=False)

    print("--- 2. Applying Schema Evolution (Adding Model Metrics) ---")
    # Additive schema change without full table lock or rewrite
    spark.sql(f"""
        ALTER TABLE {table_name}
        ADD COLUMNS (
            model_version STRING COMMENT 'ML model iteration version',
            gpu_utilization_pct DOUBLE COMMENT 'GPU hardware utilization percentage'
        )
    """)
    print("[✔] Schema evolved: added 'model_version' and 'gpu_utilization_pct'.")

    print("\n--- 3. Verifying Updated Table Schema ---")
    spark.sql(f"DESCRIBE TABLE {table_name}").show(truncate=False)

    print("--- 4. Querying Legacy vs Evolved Snapshots ---")
    # Query historic rows (evolved columns will evaluate to NULL for old records)
    spark.sql(f"""
        SELECT event_id, timestamp, model_version, gpu_utilization_pct 
        FROM {table_name} 
        LIMIT 5
    """).show(truncate=False)


if __name__ == "__main__":
    demonstrate_schema_evolution()