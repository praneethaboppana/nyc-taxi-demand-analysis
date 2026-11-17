# NYC Yellow Taxi Pickup Demand Analysis

This project uses Databricks and PySpark to analyze where and when
recorded NYC yellow taxi pickups were busiest in January 2026.

## Data and method

I used the January 2026 Yellow Taxi Trip Records and Taxi Zone Lookup
from the [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

I filtered the trip records to January and grouped pickups by taxi zone
and hour. I verified one hourly total by counting the source records
directly. Both methods gave 316 pickups in zone 239 between midnight
and 1 a.m. on January 1.

## Findings

- Upper East Side South ranked first with 160,343 recorded pickups.
  Nine of the ten busiest pickup zones were in Manhattan.
- From midnight to 1 a.m., weekends averaged 6,395 pickups versus
  2,461.5 on weekdays, about 2.6 times as many.

## Charts

![Taxi analysis chart](Screenshot%202026-11-17%20100224.png)

![Taxi analysis chart](Screenshot%202026-11-17%20100240.png)

## How to run

1. Download the January 2026 Yellow Taxi Parquet file and Taxi Zone Lookup CSV from the TLC page above.
2. Upload both files to a Databricks volume.
3. Import [the Python notebook source](01_load_taxi_data.py) into Databricks.
4. Update `file_path` and `zone_path` to match your uploaded files, then run all cells in order.
