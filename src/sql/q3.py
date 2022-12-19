import time
from pprint import pprint

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import month

APP_NAME = "q3_sql"
files = [
    f"hdfs://master:9000/raw_data/yellow_tripdata_2022-{month_id:02d}.parquet"
    for month_id in range(1, 7)
]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

yellow_tripdata_df = spark.read.parquet(*files, mergeSchema=True)

zone_lookup_df = spark.read.csv(
    f"hdfs://master:9000/raw_data/taxi+_zone_lookup.csv", header=True
)

start = time.time()
# filter yellow_tripdata_df to keep only the rows that have `PUlocationID` != `DOlocationID`
yellow_tripdata_df = yellow_tripdata_df.filter(
    f.col("PUlocationID") != f.col("DOlocationID")
)

# group yellow_tripdata_df by `tpep_pickup_datetime` every 15 days and compute the average `trip_distance` and `total_amount`
yellow_tripdata_df = yellow_tripdata_df.groupBy(
    f.window("tpep_pickup_datetime", "15 days")
).agg(f.avg("trip_distance"), f.avg("total_amount"))

# rename the columns of yellow_tripdata_df
yellow_tripdata_df = yellow_tripdata_df.select(
    f.col("window.start").alias("start"),
    f.col("window.end").alias("end"),
    f.col("avg(trip_distance)").alias("avg_trip_distance"),
    f.col("avg(total_amount)").alias("avg_total_amount"),
)

# collect the data of yellow_tripdata_df
result = yellow_tripdata_df.collect()

end = time.time()
print(f"Execution took {end - start} seconds.")

result = [r.asDict() for r in result]
df = pd.DataFrame(result)
df.to_excel(f"~/results/{APP_NAME}.xlsx")
