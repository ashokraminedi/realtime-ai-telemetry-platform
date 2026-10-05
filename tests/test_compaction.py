def test_compaction_procedure(spark):
    """Tests that Iceberg rewrite_data_files reduces total file count."""
    table_name = "test_cat.db.compaction_test"

    # Create table
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            data STRING
        ) USING iceberg
    """
    )

    # Create multiple small file appends
    for i in range(5):
        df = spark.createDataFrame([(i, f"sample_{i}")], ["id", "data"])
        df.write.format("iceberg").mode("append").save(table_name)

    files_before = spark.sql(f"SELECT * FROM {table_name}.files").count()
    assert files_before >= 5

    # Execute bin-pack compaction SQL call
    spark.sql(
        f"""
        CALL test_cat.system.rewrite_data_files(
            table => '{table_name}'
        )
    """
    )

    files_after = spark.sql(f"SELECT * FROM {table_name}.files").count()
    assert files_after < files_before