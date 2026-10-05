def test_iceberg_schema_evolution(spark):
    """Validates that Iceberg schema evolution allows writing new fields seamlessly."""
    table_name = "test_cat.db.schema_test"

    # Step 1: Create base table (v1 schema)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING,
            event_type STRING
        ) USING iceberg
    """
    )

    # Step 2: Write v1 data
    df_v1 = spark.createDataFrame(
        [("evt-001", "inference")], ["event_id", "event_type"]
    )
    df_v1.write.format("iceberg").mode("append").save(table_name)

    # Step 3: Write v2 data with dynamic new column (prompt_tokens)
    df_v2 = spark.createDataFrame(
        [("evt-002", "inference", 150)],
        ["event_id", "event_type", "prompt_tokens"],
    )

    # Enable mergeSchema option for automatic evolution
    df_v2.write.format("iceberg").option("mergeSchema", "true").mode("append").save(
        table_name
    )

    # Step 4: Validate evolve schema reading
    result_df = spark.read.table(table_name)
    assert len(result_df.columns) == 3
    assert "prompt_tokens" in result_df.columns
    assert result_df.count() == 2