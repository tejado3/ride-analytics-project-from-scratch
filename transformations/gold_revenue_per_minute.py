from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Create a gold-layer table that tracks completed ride revenue by minute.
@dp.table(comment="Completed ride revenue aggregated per minute over the last 60 minutes.")
def gold_revenue_per_minute():
    return (
        # Read cleaned and enriched ride event data from the silver layer.
        spark.read.table("silver_ride_events")

        # Keep only rides that were successfully completed.
        .filter(F.col("status") == "completed")

        # Limit the analysis to rides from the last 360 minutes.
        .filter(
            F.col("event_time") >= F.expr("current_timestamp() - INTERVAL 360 MINUTES")
        )

        # Round each event timestamp down to the nearest minute.
        .withColumn("minute_bucket", F.date_trunc("minute", "event_time"))

        # Aggregate completed ride revenue and volume per minute.
        .groupBy("minute_bucket")
        .agg(
            # Total fare collected from completed rides in the minute.
            F.round(F.sum("final_fare"), 2).alias("revenue_inr"),

            # Number of completed rides in the minute.
            F.count("ride_id").alias("completed_rides"),
        )

        # Show results in chronological order.
        .orderBy("minute_bucket")
    )