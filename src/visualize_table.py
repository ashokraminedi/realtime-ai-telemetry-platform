import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.spark_config import get_spark_session


def visualize_iceberg_state(table_name: str = "local.db.ai_telemetry_events"):
    spark = get_spark_session("Iceberg-State-Visualizer")

    print("=" * 70)
    print(f"       ICEBERG TABLE METADATA VISUALIZER: {table_name}")
    print("=" * 70)

    print("\n[1] ACTIVE TABLE SCHEMA & PARTITIONING")
    spark.sql(f"DESCRIBE TABLE {table_name}").show(truncate=False)

    print("\n[2] SNAPSHOT COMMIT HISTORY (TIME-TRAVEL TRAIL)")
    spark.sql(
        f"""
        SELECT 
            snapshot_id, 
            committed_at, 
            operation, 
            summary['added-data-files'] AS added_files,
            summary['total-records'] AS total_records
        FROM {table_name}.snapshots
        ORDER BY committed_at DESC
        LIMIT 5
    """
    ).show(truncate=False)

    print("\n[3] DATA FILE STORAGE LAYOUT (PARQUET BLOBS)")
    spark.sql(
        f"""
        SELECT 
            file_path, 
            file_format, 
            record_count, 
            file_size_in_bytes / 1024 / 1024 AS size_mb,
            partition
        FROM {table_name}.files
        LIMIT 10
    """
    ).show(truncate=False)

    print("\n[4] DATA QUALITY - DLQ TABLE AUDIT")
    try:
        spark.sql(
            "SELECT count(*) as total_dlq_records FROM local.db.ai_telemetry_dlq"
        ).show()
    except Exception:
        print("    (DLQ table not initialized yet)")


if __name__ == "__main__":
    visualize_iceberg_state()