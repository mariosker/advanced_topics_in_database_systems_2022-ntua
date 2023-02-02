# Να βρεθεί, ανά 15 ημέρες, ο μέσος όρος της απόστασης και του κόστους για όλες τις
# διαδρομές με σημείο αναχώρησης διαφορετικό από το σημείο άφιξης.

import time
from pprint import pprint

from pyspark.sql import SparkSession

APP_NAME = "q3_rdd"
files = [
    f"hdfs://master:9000/raw_data/yellow_tripdata_2022-{month_id:02d}.parquet"
    for month_id in range(1, 7)
]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

yellow_tripdata_rdd = spark.read.parquet(*files, mergeSchema=True).rdd

zone_lookup_rdd = spark.read.csv(
    f"hdfs://master:9000/raw_data/taxi+_zone_lookup.csv", header=True
).rdd

# min_date = yellow_tripdata_rdd.min(lambda x: x.tpep_pickup_datetime)

# remove trips where `PUlocationID` == `DOlocationID`
yellow_tripdata_rdd = yellow_tripdata_rdd.filter(
    lambda x: x.PULocationID != x.DOLocationID
)

# get min date from `tpep_pickup_datetime`
min_date = yellow_tripdata_rdd.min(
    lambda x: x.tpep_pickup_datetime
).tpep_pickup_datetime.date()


def group_by_15_day_window(row):
    date = row.tpep_pickup_datetime.date()
    days = date - min_date
    return days // 15


def extract_date_range(group):
    key, rows = group
    start = min(row.tpep_pickup_datetime for row in rows)
    end = max(row.tpep_pickup_datetime for row in rows)
    avg_trip_distance = sum(row.trip_distance for row in rows) / len(rows)
    avg_total_amount = sum(row.total_amount for row in rows) / len(rows)
    return start, end, avg_trip_distance, avg_total_amount


yellow_tripdata_rdd_final = yellow_tripdata_rdd.groupBy(group_by_15_day_window).map(
    extract_date_range
)
print(yellow_tripdata_rdd_final.first())
# 2001-08-23
