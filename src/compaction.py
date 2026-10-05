import sys
import sys

# Add project root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.spark_config import get_spark_session

def run_table_maintenance(table_identifier: str = "local.db.ai_telemetry_events"):
    """
    Executes production maintenance procedures on an Iceberg table:
    1. Small file compaction via rewrite_data_files (binpack).
    2. Historical snapshot expiration (retention cleanup).
    3. Cleanup of orphan files not tracked in metadata.
    """
    spark = get_spark_session("Iceberg-Table-Maintenance")
    print(f"[+] Starting Iceberg maintenance procedures for: {table_identifier}")

    # 1. Compact small data files into target 128MB blobs
    print("\n[1/3] Executing data file compaction (rewrite_data_files)...")
    try:
        compaction_result = spark.sql(f"""
            CALL local.system.rewrite_data_files(
                table => '{table_identifier}',
                strategy => 'binpack',
                options => map(
                    'target-file-size-bytes', '134217728', -- 128 MB
                    'min-input-files', '5'
                )
            )
        """)
        compaction_result.show(truncate=False)
        print("[✔] Data file compaction completed.")
    except Exception as e:
        print(f"[!] File compaction failed or skipped: {e}")

    # 2. Expire snapshots older than 7 days (adjust retention as needed)
    print("\n[2/3] Expiring historical metadata snapshots...")
    try:
        expire_result = spark.sql(f"""
            CALL local.system.expire_snapshots(
                table => '{table_identifier}',
                older_than => DATE_SUB(CURRENT_TIMESTAMP(), 7),
                retain_last => 5
            )
        """)
        expire_result.show(truncate=False)
        print("[✔] Snapshot expiration completed.")
    except Exception as e:
        print(f"[!] Snapshot expiration failed: {e}")

    # 3. Clean up unreferenced orphan files on storage
    print("\n[3/3] Removing untracked orphan storage files...")
    try:
        orphan_result = spark.sql(f"""
            CALL local.system.remove_orphan_files(
                table => '{table_identifier}'
            )
        """)
        orphan_result.show(truncate=False)
        print("[✔] Orphan file removal completed.")
    except Exception as e:
        print(f"[!] Orphan file cleanup failed: {e}")

    print(f"\n[+] Maintenance workflow finalized for {table_identifier}.\n")


if __name__ == "__main__":
    target_table = sys.argv[1] if len(sys.argv) > 1 else "local.db.ai_telemetry_events"
    run_table_maintenance(target_table)