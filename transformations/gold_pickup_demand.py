from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Create a gold-layer table showing ride demand by pickup area and city.
@dp.table(
    comment="Ride demand by pickup area and city for today — identifies hot zones needing driver supply."
)
def gold_pickup_demand():
    return (
        # Read cleaned and enriched ride event data from the silver layer.
        spark.read.table("silver_ride_events")

        # Group ride activity by date, city, pickup area, and operational zone.
        .groupBy("event_date", "city", "pickup_area", "zone_id")

        # Calculate demand, completion, cancellation, surge, and revenue metrics.
        .agg(
            # Total number of rides requested in each pickup area.
            F.count("ride_id").alias("total_rides"),

            # Number of rides successfully completed.
            F.sum(
                F.when(F.col("status") == "completed", 1).otherwise(0)
            ).alias("completed_rides"),

            # Number of rides that were cancelled.
            F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_rides"),

            # Percentage of rides cancelled in the pickup area.
            F.round(
                F.sum(F.col("is_cancelled").cast("int")) * 100.0 / F.count("ride_id"),
                1
            ).alias("cancellation_rate_pct"),

            # Average surge multiplier for rides in the pickup area.
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge"),

            # Total revenue from completed rides only.
            F.round(
                F.sum(
                    F.when(F.col("status") == "completed", F.col("final_fare"))
                    .otherwise(0)
                ),
                2
            ).alias("revenue_inr"),
        )

        # Prioritize the busiest pickup areas first.
        .orderBy(F.col("total_rides").desc())
    )