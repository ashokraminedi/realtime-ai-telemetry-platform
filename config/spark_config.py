import os
from pyspark.sql import SparkSession

WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "spark-warehouse/iceberg")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "spark-warehouse/checkpoints")

def get_spark_session(app_name: str = "Realtime-Lakehouse-Engine") -> SparkSession:
    """
    Initializes and returns a PySpark Session configured with Apache Iceberg extensions,
    Kafka connector packages, and catalog definitions.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # ---------------------------------------------------------------------
        # Maven Packages: Apache Iceberg Runtime & Spark Kafka Connector
        # ---------------------------------------------------------------------
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        # Enable Apache Iceberg SQL Extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Define local Hadoop-backed Iceberg Catalog
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        # Streaming & Resource Optimizations
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )
    
    return builder.getOrCreate()