import streamlit as st
import pandas as pd

from config.spark_config import get_spark_session


st.set_page_config(
    page_title="AI Telemetry Platform",
    page_icon="📊",
    layout="wide",
)

TABLE_NAME = "local.db.ai_telemetry_events"


@st.cache_resource
def get_spark():
    return get_spark_session("AI-Telemetry-Dashboard")


def query_to_pandas(spark, sql: str) -> pd.DataFrame:
    return spark.sql(sql).toPandas()


spark = get_spark()

st.title("AI Telemetry & Reliability Platform")
st.caption(
    "Real-time operational telemetry powered by "
    "Kafka → Spark Structured Streaming → Apache Iceberg"
)

# -------------------------------------------------------------------
# Refresh control
# -------------------------------------------------------------------

if st.button("Refresh telemetry"):
    st.cache_data.clear()
    st.rerun()


# -------------------------------------------------------------------
# Platform summary
# -------------------------------------------------------------------

summary_df = query_to_pandas(
    spark,
    f"""
    SELECT
        COUNT(*) AS total_events,
        ROUND(AVG(model_metadata.latency_ms), 2) AS avg_latency_ms,
        SUM(CASE
            WHEN model_metadata.is_anomaly_simulated = true
            THEN 1 ELSE 0
        END) AS anomaly_events,
        SUM(CASE
            WHEN guardrail_flags.pii_detected = true
            THEN 1 ELSE 0
        END) AS pii_events,
        SUM(CASE
            WHEN guardrail_flags.content_policy_violated = true
            THEN 1 ELSE 0
        END) AS policy_violations
    FROM {TABLE_NAME}
    """
)

summary = summary_df.iloc[0]

total_events = int(summary["total_events"] or 0)
avg_latency = float(summary["avg_latency_ms"] or 0)
anomaly_events = int(summary["anomaly_events"] or 0)
pii_events = int(summary["pii_events"] or 0)
policy_violations = int(summary["policy_violations"] or 0)

anomaly_rate = (
    anomaly_events / total_events * 100
    if total_events
    else 0
)


# -------------------------------------------------------------------
# KPI cards
# -------------------------------------------------------------------

st.subheader("Platform Overview")

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    "Total Events",
    f"{total_events:,}"
)

col2.metric(
    "Avg Latency",
    f"{avg_latency:,.0f} ms"
)

col3.metric(
    "Anomaly Rate",
    f"{anomaly_rate:.2f}%"
)

col4.metric(
    "PII Detections",
    f"{pii_events:,}"
)

col5.metric(
    "Policy Violations",
    f"{policy_violations:,}"
)


# -------------------------------------------------------------------
# Events by model
# -------------------------------------------------------------------

st.divider()

col1, col2 = st.columns(2)

with col1:

    st.subheader("Events by Model")

    model_df = query_to_pandas(
        spark,
        f"""
        SELECT
            model_metadata.model_name AS model,
            COUNT(*) AS events
        FROM {TABLE_NAME}
        GROUP BY model_metadata.model_name
        ORDER BY events DESC
        """
    )

    if not model_df.empty:
        st.bar_chart(
            model_df.set_index("model")["events"]
        )


# -------------------------------------------------------------------
# Average latency by model
# -------------------------------------------------------------------

with col2:

    st.subheader("Average Model Latency")

    latency_df = query_to_pandas(
        spark,
        f"""
        SELECT
            model_metadata.model_name AS model,
            ROUND(
                AVG(model_metadata.latency_ms),
                2
            ) AS avg_latency_ms
        FROM {TABLE_NAME}
        GROUP BY model_metadata.model_name
        ORDER BY avg_latency_ms DESC
        """
    )

    if not latency_df.empty:
        st.bar_chart(
            latency_df.set_index("model")["avg_latency_ms"]
        )


# -------------------------------------------------------------------
# Provider distribution
# -------------------------------------------------------------------

st.divider()

col1, col2 = st.columns(2)

with col1:

    st.subheader("Events by Provider")

    provider_df = query_to_pandas(
        spark,
        f"""
        SELECT
            model_metadata.provider AS provider,
            COUNT(*) AS events
        FROM {TABLE_NAME}
        GROUP BY model_metadata.provider
        ORDER BY events DESC
        """
    )

    if not provider_df.empty:
        st.bar_chart(
            provider_df.set_index("provider")["events"]
        )


# -------------------------------------------------------------------
# Token usage
# -------------------------------------------------------------------

with col2:

    st.subheader("Token Consumption")

    token_df = query_to_pandas(
        spark,
        f"""
        SELECT
            model_metadata.model_name AS model,
            SUM(model_metadata.prompt_tokens)
                AS prompt_tokens,
            SUM(model_metadata.completion_tokens)
                AS completion_tokens
        FROM {TABLE_NAME}
        GROUP BY model_metadata.model_name
        ORDER BY prompt_tokens DESC
        """
    )

    if not token_df.empty:
        st.bar_chart(
            token_df.set_index("model")[
                ["prompt_tokens", "completion_tokens"]
            ]
        )


# -------------------------------------------------------------------
# Application activity
# -------------------------------------------------------------------

st.divider()

st.subheader("Events by Application")

app_df = query_to_pandas(
    spark,
    f"""
    SELECT
        app_id,
        COUNT(*) AS events
    FROM {TABLE_NAME}
    GROUP BY app_id
    ORDER BY events DESC
    """
)

if not app_df.empty:
    st.bar_chart(
        app_df.set_index("app_id")["events"]
    )


# -------------------------------------------------------------------
# Recent telemetry
# -------------------------------------------------------------------

st.divider()

st.subheader("Recent Telemetry Events")

recent_df = query_to_pandas(
    spark,
    f"""
    SELECT
        event_timestamp,
        app_id,
        model_metadata.model_name AS model,
        model_metadata.provider AS provider,
        model_metadata.latency_ms AS latency_ms,
        model_metadata.prompt_tokens AS prompt_tokens,
        model_metadata.completion_tokens AS completion_tokens,
        model_metadata.is_anomaly_simulated AS anomaly,
        guardrail_flags.pii_detected AS pii_detected,
        guardrail_flags.content_policy_violated
            AS policy_violation
    FROM {TABLE_NAME}
    ORDER BY event_timestamp DESC
    LIMIT 100
    """
)

st.dataframe(
    recent_df,
    use_container_width=True,
    hide_index=True,
)


# -------------------------------------------------------------------
# Iceberg snapshots
# -------------------------------------------------------------------

st.divider()

st.subheader("Iceberg Snapshot History")

snapshots_df = query_to_pandas(
    spark,
    f"""
    SELECT
        snapshot_id,
        committed_at,
        operation
    FROM {TABLE_NAME}.snapshots
    ORDER BY committed_at DESC
    LIMIT 20
    """
)

st.dataframe(
    snapshots_df,
    use_container_width=True,
    hide_index=True,
)