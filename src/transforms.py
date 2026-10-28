"""Pure DataFrame transformations.

They contain no I/O, so the same code runs in Databricks streaming notebooks
and in local unit tests (GitHub Actions).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "purchase"]


def clean_events(df: DataFrame, watermark: str = "1 hour") -> DataFrame:
    """Type, standardise, validate and deduplicate raw (all-string) events.

    Works on both batch and streaming DataFrames.
    try_cast / try_to_timestamp return NULL instead of failing (serverless has ANSI mode on).
    """
    typed = df.select(
        F.trim("event_id").alias("event_id"),
        F.expr("try_cast(user_id AS INT)").alias("user_id"),
        F.lower(F.trim("event_type")).alias("event_type"),
        F.expr("try_cast(product_id AS INT)").alias("product_id"),
        F.expr("try_cast(price AS DOUBLE)").alias("price"),
        F.upper(F.trim("device")).alias("device"),
        F.expr("try_to_timestamp(event_ts, 'yyyy-MM-dd HH:mm:ss')").alias("event_ts"),
    )

    valid = typed.filter(
        F.col("event_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") >= 0)
        & F.col("event_type").isin(VALID_EVENT_TYPES)
    )

    # The watermark lets Spark forget old ids, so streaming state does not grow forever.
    return (valid
            .withWatermark("event_ts", watermark)
            .dropDuplicates(["event_id", "event_ts"]))


def events_per_minute(df: DataFrame) -> DataFrame:
    """How many events of each type happened in each minute."""
    return (df
            .groupBy(F.date_trunc("minute", "event_ts").alias("minute"), "event_type")
            .agg(F.count("*").alias("events"))
            .orderBy("minute", "event_type"))


def revenue_by_product(df: DataFrame) -> DataFrame:
    """Purchases and revenue per product."""
    return (df.filter(F.col("event_type") == "purchase")
            .groupBy("product_id")
            .agg(F.count("*").alias("purchases"),
                 F.round(F.sum("price"), 2).alias("revenue"))
            .orderBy(F.desc("revenue")))


def conversion_funnel(df: DataFrame) -> DataFrame:
    """Unique users at each step (view -> cart -> purchase)."""
    return (df.groupBy("event_type")
            .agg(F.countDistinct("user_id").alias("users"))
            .orderBy(F.desc("users")))


def device_split(df: DataFrame) -> DataFrame:
    """Events and unique users per device."""
    return (df.groupBy("device")
            .agg(F.count("*").alias("events"),
                 F.countDistinct("user_id").alias("users"))
            .orderBy(F.desc("events")))
