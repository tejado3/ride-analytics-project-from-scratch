from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Create a Delta Live Tables / Lakeflow table for analyzing ride cancellations.
@dp.table(
    comment="Cancellation breakdown by city, status, and reason for today — actionable root cause view."
)
def gold_cancellation_analysis():
    return (
        # Read cleaned ride event data from the silver layer.
        spark.read.table("silver_ride_events")

        # Keep only rides that were cancelled.
        .filter(F.col("is_cancelled") == True)

        # Group cancellations by date, city, ride status, and cancellation reason.
        .groupBy("event_date", "city", "status", "cancellation_reason")

        # Calculate cancellation metrics for each group.
        .agg(
            # Total number of cancelled rides.
            F.count("ride_id").alias("cancellation_count"),

            # Average surge multiplier at the time of cancellation.
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge_at_cancel"),

            # Average ride distance for cancelled rides.
            F.round(F.avg("distance_km"), 1).alias("avg_distance_km"),
        )

        # Show the most common cancellation groups first.
        .orderBy(F.col("cancellation_count").desc())
    )