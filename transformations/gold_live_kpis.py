from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Create a gold-layer table with daily operational KPIs for monitoring ride activity.
@dp.table(
    comment="Live operational KPIs per day: rides, revenue, cancellation rate, active drivers."
)
def gold_live_kpis():
    return (
        # Read cleaned and enriched ride event data from the silver layer.
        spark.read.table("silver_ride_events")

        # Aggregate KPIs at the daily level.
        .groupBy("event_date")

        # Calculate operational metrics for each event date.
        .agg(
            # Total number of ride records for the day.
            F.count("ride_id").alias("total_rides"),

            # Total revenue from completed rides only.
            F.sum(
                F.when(F.col("status") == "completed", F.col("final_fare"))
                .otherwise(0)
            ).alias("total_revenue_inr"),

            # Percentage of rides that were cancelled.
            F.round(
                F.sum(F.col("is_cancelled").cast("int")) * 100.0 / F.count("ride_id"),
                1
            ).alias("cancellation_rate_pct"),

            # Number of unique drivers active on that day.
            F.countDistinct("driver_id").alias("active_drivers"),

            # Average fare for completed rides only.
            F.round(
                F.avg(
                    F.when(F.col("status") == "completed", F.col("final_fare"))
                ),
                2
            ).alias("avg_fare_inr"),

            # Most recent ingestion timestamp for freshness tracking.
            F.max("_ingest_time").alias("last_updated"),
        )
    )