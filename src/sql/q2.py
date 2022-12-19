import time
from pprint import pprint

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import month

APP_NAME = "q2-sql"

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

files = [
    f"hdfs://master:9000/raw_data/yellow_tripdata_2022-{month_id:02d}.parquet"
    for month_id in range(1, 7)
]

yellow_tripdata_df = spark.read.parquet(*files, mergeSchema=True)

zone_lookup_df = spark.read.csv(
    f"hdfs://master:9000/raw_data/taxi+_zone_lookup.csv", header=True
)

start = time.time()
# keep the rows with positive tolls_amount
yellow_tripdata_df = yellow_tripdata_df.filter(f.col("tolls_amount") > 0)

# group `tpep_pickup_datetime` of yellow_tripdata_df by month and get the max `tolls_amount` for each month and store it in a new variable `max_tolls_amount`
w = Window.partitionBy(f.month(yellow_tripdata_df.tpep_pickup_datetime))
max_tolls_amount = f.max("tolls_amount").over(w)

# create a new column `max_tolls_amount` in `yellow_tripdata_df` and store the result of `max_tolls_amount` in it
yellow_tripdata_df = yellow_tripdata_df.withColumn("max_tolls_amount", max_tolls_amount)

# get all rows that have `tolls_amount` == `max_tolls_amount`
result = yellow_tripdata_df.where(f.col("tolls_amount") == f.col("max_tolls_amount"))

# drop the column `max_tolls_amount`
result = result.drop("max_tolls_amount")

# collect the result and print it
result = result.collect()

end = time.time()
print(f"Execution took {end - start} seconds.")

result = [r.asDict() for r in result]
df = pd.DataFrame(result)
df.to_excel(f"~/results/{APP_NAME}.xlsx")
