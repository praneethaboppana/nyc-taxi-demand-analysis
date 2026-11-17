# Databricks notebook source
file_path = "/Volumes/workspace/default/taxi_files/yellow_tripdata_2026-01.parquet"
trips = spark.read.parquet(file_path)
display(trips.select("tpep_pickup_datetime", "PULocationID").limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

january_trips = trips.filter(
    (F.col("tpep_pickup_datetime") >= F.lit("2026-01-01").cast("timestamp_ntz"))
    & (F.col("tpep_pickup_datetime") < F.lit("2026-02-01").cast("timestamp_ntz"))
    & F.col("PULocationID").between(1, 263)
)

hourly_pickups = (
    january_trips
    .withColumn("hour_start", F.date_trunc("hour", "tpep_pickup_datetime"))
    .groupBy("PULocationID", "hour_start")
    .count()
    .withColumnRenamed("count", "pickups")
)

display(
    hourly_pickups
    .filter(F.col("PULocationID") == 239)
    .orderBy("hour_start")
    .limit(10)
)

# COMMAND ----------

start = F.lit("2026-01-01 00:00:00").cast("timestamp_ntz")
end = F.lit("2026-01-01 01:00:00").cast("timestamp_ntz")

direct_count = january_trips.filter(
    (F.col("PULocationID") == 239)
    & (F.col("tpep_pickup_datetime") >= start)
    & (F.col("tpep_pickup_datetime") < end)
).count()

print(f"Direct count from trip records: {direct_count}")

# COMMAND ----------

top_zones = (
    hourly_pickups
    .groupBy("PULocationID")
    .agg(F.sum("pickups").alias("january_pickups"))
    .orderBy(F.desc("january_pickups"), F.asc("PULocationID"))
    .limit(10)
)

display(top_zones)

# COMMAND ----------

file_path = "/Volumes/workspace/default/taxi_files/yellow_tripdata_2026-01.parquet"

# COMMAND ----------

zone_path = "/Volumes/workspace/default/taxi_files/taxi_zone_lookup.csv"

zone_lookup = (
    spark.read.option("header", True).csv(zone_path)
    .select(
        F.col("LocationID").cast("int").alias("PULocationID"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("borough")
    )
)

top_zones_named = (
    top_zones.join(zone_lookup, on="PULocationID", how="left")
    .orderBy(F.desc("january_pickups"), F.asc("PULocationID"))
)

display(top_zones_named)

# COMMAND ----------

daily_hourly = (
    january_trips
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
    .groupBy("pickup_date", "hour_of_day")
    .count()
    .withColumnRenamed("count", "pickups")
)

hourly_pattern = (
    daily_hourly
    .withColumn(
        "day_type",
        F.when(F.dayofweek("pickup_date").isin(1, 7), "Weekend")
         .otherwise("Weekday")
    )
    .groupBy("hour_of_day", "day_type")
    .agg(
        F.round(F.avg("pickups"), 1).alias("avg_pickups"),
        F.count("*").alias("days_counted")
    )
    .orderBy("hour_of_day", "day_type")
)

display(hourly_pattern)