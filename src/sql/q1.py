import time

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from tabulate import tabulate

APP_NAME = "q1_sql"
HDFS_FOLDER = "hdfs://master:9000/raw_data"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()


files = [
    f"{HDFS_FOLDER}/yellow_tripdata_2022-{month:02d}.parquet" for month in range(1, 7)
]

yellow_tripdata_df = spark.read.parquet(*files, mergeSchema=True)
zone_lookup_df = spark.read.csv(f"{HDFS_FOLDER}/taxi+_zone_lookup.csv", header=True)

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

# Get the max tip_amount from selected_trips
max_tip = selected_trips.agg({"tip_amount": "max"}).collect()[0][0]

# find selected_trips with max_tip
max_tip_rows = selected_trips.where(selected_trips.tip_amount == max_tip)

# show max_tip_row in a vertical format
result = max_tip_rows.collect()
end = time.time()

print(f"Execution took {end - start} seconds.")
print(result)

headers = result[0].asDict().keys()
md_table = tabulate(result, headers, tablefmt="github")
with open(f"{APP_NAME}.md", "w") as f:
    f.write(f"Execution took {end - start} seconds.\n")
    f.write(f"{md_table}")
