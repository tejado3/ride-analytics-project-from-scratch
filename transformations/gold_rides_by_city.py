from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Create a gold-layer table summarizing ride volume and revenue by city.
@dp.table(comment="Rides and revenue by city for today.")
def gold_rides_by_city():
    return (
        # Read cleaned and enriched ride event data from the silver layer.
        spark.read.table("silver_ride_events")

        # Group ride activity at the city level.
        .groupBy("city")

        # Calculate ride counts and revenue metrics for each city.
        .agg(
            # Total number of ride records in the city.
            F.count("ride_id").alias("total_rides"),

            # Number of rides successfully completed in the city.
            F.sum(
                F.when(F.col("status") == "completed", 1).otherwise(0)
            ).alias("completed_rides"),

            # Total revenue from completed rides only.
            F.round(
                F.sum(
                    F.when(F.col("status") == "completed", F.col("final_fare"))
                    .otherwise(0)
                ),
                2
            ).alias("revenue_inr"),

            # Average fare for completed rides only.
            F.round(
                F.avg(
                    F.when(F.col("status") == "completed", F.col("final_fare"))
                ),
                2
            ).alias("avg_fare_inr"),
        )

        # Show cities with the highest ride volume first.
        .orderBy(F.col("total_rides").desc())
    )