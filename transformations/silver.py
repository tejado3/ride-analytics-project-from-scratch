from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


# Create a silver-layer table with cleaned, validated, and enriched ride event data.
@dp.table(
    comment="Filtered and enriched ride events. Invalid rows dropped; quality metrics tracked.",
    table_properties={"quality": "silver"},
)

# Drop records that fail required data quality checks.
@dp.expect_or_drop("valid_ride_id",  "ride_id IS NOT NULL")
@dp.expect_or_drop("valid_fare",     "final_fare > 0")
@dp.expect_or_drop("valid_distance", "distance_km BETWEEN 0.5 AND 50")
@dp.expect_or_drop("valid_rating",   "driver_rating BETWEEN 1.0 AND 5.0")
@dp.expect_or_drop(
    "valid_status",
    "status IN ('completed','cancelled_by_user','cancelled_by_driver','no_driver_found','in_progress')"
)
@dp.expect_or_drop(
    "valid_vehicle",
    "vehicle_type IN ('Bike','Auto','Mini','Sedan','SUV')"
)
def silver_ride_events():
    return (
        # Read raw ride event data continuously from the bronze streaming table.
        spark.readStream.table("bronze_ride_events")

        # Convert event timestamp into useful time-based fields.
        .withColumn("event_time",    F.to_timestamp("event_time"))
        .withColumn("event_date",    F.to_date("event_time"))
        .withColumn("event_hour",    F.hour("event_time"))
        .withColumn("event_minute",  F.minute("event_time"))
        .withColumn("day_of_week",   F.dayofweek("event_time"))

        # Flag rides that happened on Saturday or Sunday.
        .withColumn(
            "is_weekend",
            F.dayofweek("event_time").isin([1, 7]).cast("boolean")
        )

        # Cast numeric fields to the correct data types for downstream analytics.
        .withColumn("final_fare",    F.col("final_fare").cast(DoubleType()))
        .withColumn("base_fare",     F.col("base_fare").cast(DoubleType()))
        .withColumn("distance_km",   F.col("distance_km").cast(DoubleType()))
        .withColumn("duration_mins", F.col("duration_mins").cast(IntegerType()))

        # Flag rides that ended in any cancellation-related status.
        .withColumn(
            "is_cancelled",
            F.col("status").isin(
                "cancelled_by_user",
                "cancelled_by_driver",
                "no_driver_found"
            ).cast("boolean")
        )

        # Remove rescued malformed fields after validation and enrichment.
        .drop("_rescued_data")
    )