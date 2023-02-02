import time
from pprint import pprint

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

APP_NAME = "q1_sql"
HDFS_FOLDER = "hdfs://master:9000/raw_data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()


files = [
    f"{HDFS_FOLDER}/yellow_tripdata_2022-{month:02d}.parquet" for month in range(1, 7)
]

yellow_tripdata_df = spark.read.parquet(*files, mergeSchema=True)
yellow_tripdata_rdd = yellow_tripdata_df.rdd

zone_lookup_df = spark.read.csv(f"{HDFS_FOLDER}/taxi+_zone_lookup.csv", header=True)
zone_lookup_rdd = zone_lookup_df.rdd

start = time.time()

# keep rows where tpep_pickup_datetime is between 2022-01 and 2022-06
yellow_tripdata_df = yellow_tripdata_df.filter(
    (yellow_tripdata_df.tpep_pickup_datetime >= "2022-01-01")
    & (yellow_tripdata_df.tpep_pickup_datetime < "2022-07-01")
)

# Get LocationID for rows that have Zone == "Battery Park"
battery_park_rows = zone_lookup_df.where(zone_lookup_df.Zone == "Battery Park").select(
    ["LocationID"]
)
# Get trips that happend in March
yellow_tripdata_df_march = yellow_tripdata_df.filter(
    f.month(yellow_tripdata_df.tpep_pickup_datetime) == 3
)
# Get trips that started in Battery Park and happened in March
selected_trips = yellow_tripdata_df_march.join(
    battery_park_rows,
    yellow_tripdata_df_march.PULocationID == battery_park_rows.LocationID,
).select(yellow_tripdata_df_march["*"])
# get row with max `tip_amount` from selected_trips
max_tip = selected_trips.agg(f.max(selected_trips.tip_amount))
# join selected_trips with max_tip to get the row with max `tip_amount`
max_tip_row = selected_trips.join(
    max_tip, selected_trips.tip_amount == max_tip["max(tip_amount)"]
)
# show max_tip_row in a vertical format
result = max_tip_row.collect()

end = time.time()
print(f"Execution took {end - start} seconds.")

with open(f'{APP_NAME}.txt', 'w') as f:
    f.write(f"Execution took {end - start} seconds.")

pprint(result)
