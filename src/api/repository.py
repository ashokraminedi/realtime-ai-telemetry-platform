from datetime import datetime

from pyspark.sql import functions as F


TABLE_NAME = "local.db.ai_telemetry_events"


class TelemetryRepository:
    """
    Controlled repository for telemetry aggregates.

    This repository intentionally exposes only predefined aggregate
    operations. Callers cannot provide SQL, table names, columns,
    GROUP BY expressions, or arbitrary predicates.
    """

    def __init__(self, spark):
        self.spark = spark

    def _base_dataframe(self, since_timestamp: datetime):
        return (
            self.spark.table(TABLE_NAME)
            .filter(
                F.col("event_timestamp")
                >= F.lit(since_timestamp)
            )
        )

    def get_platform_summary(
        self,
        since_timestamp: datetime,
    ):
        df = self._base_dataframe(since_timestamp)

        return df.agg(
            F.count("*").alias("total_events"),

            F.avg(
                "model_metadata.latency_ms"
            ).alias("avg_latency_ms"),

            F.sum(
                F.col(
                    "model_metadata.is_anomaly_simulated"
                ).cast("int")
            ).alias("anomaly_count"),

            F.sum(
                F.col(
                    "guardrail_flags.pii_detected"
                ).cast("int")
            ).alias("pii_detection_count"),

            F.sum(
                F.col(
                    "guardrail_flags.content_policy_violated"
                ).cast("int")
            ).alias("policy_violation_count"),
        )

    def get_model_latency(
        self,
        model_name: str,
        since_timestamp: datetime,
    ):
        df = (
            self._base_dataframe(since_timestamp)
            .filter(
                F.col("model_metadata.model_name")
                == F.lit(model_name)
            )
        )

        return df.agg(
            F.count("*").alias("request_count"),

            F.avg(
                "model_metadata.latency_ms"
            ).alias("avg_latency_ms"),

            F.expr(
                "percentile_approx("
                "model_metadata.latency_ms, 0.50)"
            ).alias("p50_latency_ms"),

            F.expr(
                "percentile_approx("
                "model_metadata.latency_ms, 0.95)"
            ).alias("p95_latency_ms"),

            F.expr(
                "percentile_approx("
                "model_metadata.latency_ms, 0.99)"
            ).alias("p99_latency_ms"),

            F.sum(
                F.col(
                    "model_metadata.is_anomaly_simulated"
                ).cast("int")
            ).alias("anomaly_count"),
        )

    def get_model_usage(
        self,
        model_name: str,
        since_timestamp: datetime,
    ):
        df = (
            self._base_dataframe(since_timestamp)
            .filter(
                F.col("model_metadata.model_name")
                == F.lit(model_name)
            )
        )

        return df.agg(
            F.count("*").alias("request_count"),

            F.sum(
                "model_metadata.prompt_tokens"
            ).alias("prompt_tokens"),

            F.sum(
                "model_metadata.completion_tokens"
            ).alias("completion_tokens"),
        )

    def get_application_health(
        self,
        app_id: str,
        since_timestamp: datetime,
    ):
        df = (
            self._base_dataframe(since_timestamp)
            .filter(F.col("app_id") == F.lit(app_id))
        )

        return df.agg(
            F.count("*").alias("request_count"),

            F.avg(
                "model_metadata.latency_ms"
            ).alias("avg_latency_ms"),

            F.expr(
                "percentile_approx("
                "model_metadata.latency_ms, 0.95)"
            ).alias("p95_latency_ms"),

            F.sum(
                F.col(
                    "model_metadata.is_anomaly_simulated"
                ).cast("int")
            ).alias("anomaly_count"),

            F.sum(
                F.col(
                    "guardrail_flags.pii_detected"
                ).cast("int")
            ).alias("pii_detection_count"),

            F.sum(
                F.col(
                    "guardrail_flags.content_policy_violated"
                ).cast("int")
            ).alias("policy_violation_count"),
        )

    def get_guardrail_summary(
        self,
        since_timestamp: datetime,
    ):
        df = self._base_dataframe(since_timestamp)

        return df.agg(
            F.count("*").alias("total_events"),

            F.sum(
                F.col(
                    "guardrail_flags.pii_detected"
                ).cast("int")
            ).alias("pii_detection_count"),

            F.sum(
                F.col(
                    "guardrail_flags.content_policy_violated"
                ).cast("int")
            ).alias("policy_violation_count"),
        )