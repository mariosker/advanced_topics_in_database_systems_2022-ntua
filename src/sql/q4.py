import time

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from tabulate import tabulate

APP_NAME = "q4-sql"


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

# Turn datetime into week day and hour of the day
yellow_tripdata_df = yellow_tripdata_df.withColumn(
    "week_day", f.date_format(f.col("tpep_pickup_datetime"), "EEEE")
)
yellow_tripdata_df = yellow_tripdata_df.withColumn(
    "hour_of_the_day", f.date_format(f.col("tpep_pickup_datetime"), "k")
)

# group by week day and hour of the day and sum passenger count
yellow_tripdata_df = yellow_tripdata_df.groupBy("hour_of_the_day", "week_day").agg(
    f.max("passenger_count").alias("max_passengers")
)

# for each week_day, sort by max_passengers and take the top 3 rows
window = Window.partitionBy("week_day").orderBy(f.col("max_passengers").desc())
yellow_tripdata_df = yellow_tripdata_df.withColumn(
    "rank", f.row_number().over(window)
).filter(f.col("rank") <= 3)

result = yellow_tripdata_df.collect()

end = time.time()

print(f"Execution took {end - start} seconds.")
print(result)

headers = result[0].asDict().keys()
md_table = tabulate(result, headers, tablefmt="github")
with open(f"{APP_NAME}.md", "w") as f:
    f.write(f"Execution took {end - start} seconds.\n")
    f.write(f"{md_table}")
