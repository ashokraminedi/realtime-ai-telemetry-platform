import os
import shutil
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Provides a local SparkSession configured with Apache Iceberg for integration tests."""
    warehouse_path = os.path.abspath("./test_warehouse")

    # Clean up previous test warehouse if exists
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    spark_session = (
        SparkSession.builder.appName("Lakehouse-Test-Suite")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.test_cat", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.test_cat.type",
            "hadoop",
        )
        .config("spark.sql.catalog.test_cat.warehouse", warehouse_path)
        .getOrCreate()
    )

    # Setup test database
    spark_session.sql("CREATE DATABASE IF NOT EXISTS test_cat.db")

    yield spark_session

    # Tear down
    spark_session.stop()
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)