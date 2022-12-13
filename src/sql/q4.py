from pprint import pprint
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import month
import pyspark.sql.functions as f

files = [
    f"hdfs://master:9000/raw_data/yellow_tripdata_2022-{month_id:02d}.parquet"
    for month_id in range(1, 7)
]

spark = SparkSession.builder.appName("q4-sql").getOrCreate()

yellow_tripdata_df = spark.read.parquet(*files, mergeSchema=True)

zone_lookup_df = spark.read.csv(
    f"hdfs://master:9000/raw_data/taxi+_zone_lookup.csv", header=True
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
    f.sum("passenger_count").alias("passenger_sum")
)

# order by passenger sum and take top 3
result = yellow_tripdata_df.orderBy(f.desc("passenger_sum")).take(3)

for row in result:
    pprint(row.asDict())
