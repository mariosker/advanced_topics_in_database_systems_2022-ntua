import time
from pprint import pprint

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import month

APP_NAME = "q4-sql"


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

result = [r.asDict() for r in result]
df = pd.DataFrame(result)
df.to_excel(f"~/results/{APP_NAME}.xlsx")
