import time

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from tabulate import tabulate

APP_NAME = "q5-sql"
files = [
    f"hdfs://master:9000/raw_data/yellow_tripdata_2022-{month_id:02d}.parquet"
    for month_id in range(1, 7)
]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

yellow_tripdata_df = spark.read.parquet(*files, mergeSchema=True)
zone_lookup_df = spark.read.csv(
    "hdfs://master:9000/raw_data/taxi+_zone_lookup.csv", header=True
)

start = time.time()

# keep rows where tpep_pickup_datetime is between 2022-01 and 2022-06
yellow_tripdata_df = yellow_tripdata_df.filter(
    (yellow_tripdata_df.tpep_pickup_datetime >= "2022-01-01")
    & (yellow_tripdata_df.tpep_pickup_datetime < "2022-07-01")
)

# group by month and day of the month and divide the total tip amount
# by the total fare amount to get the average tip percentage
yellow_tripdata_df = yellow_tripdata_df.groupBy(
    f.dayofmonth("tpep_pickup_datetime").alias("day_of_month"),
    f.month("tpep_pickup_datetime").alias("month"),
).agg((f.sum("tip_amount") / f.sum("fare_amount")).alias("average_tip_percentage"))

#  for each month, order by average tip percentage and take the top 5 days
window = Window.partitionBy("month").orderBy(f.desc("average_tip_percentage"))
yellow_tripdata_df = yellow_tripdata_df.withColumn(
    "rank", f.rank().over(window)
).filter(f.col("rank") <= 5)

result = yellow_tripdata_df.collect()

end = time.time()

print(f"Execution took {end - start} seconds.")
print(result)

headers = result[0].asDict().keys()
md_table = tabulate(result, headers, tablefmt="github")
with open(f"{APP_NAME}.md", "w") as f:
    f.write(f"Execution took {end - start} seconds.\n")
    f.write(f"{md_table}")
