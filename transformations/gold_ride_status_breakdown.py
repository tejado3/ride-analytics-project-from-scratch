from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Create a gold-layer table showing daily ride counts by status.
@dp.table(
    comment="Ride status counts per day. Query pct_of_total as: ROUND(ride_count * 100.0 / SUM(ride_count) OVER (PARTITION BY event_date), 1)."
)
def gold_ride_status_breakdown():
    return (
        # Read cleaned and enriched ride event data from the silver layer.
        spark.read.table("silver_ride_events")

        # Group rides by event date and ride status.
        .groupBy("event_date", "status")

        # Count the number of rides in each status group.
        .agg(
            F.count("ride_id").alias("ride_count")
        )
    )