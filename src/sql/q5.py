from pprint import pprint
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import month
import pyspark.sql.functions as f

files = [
    f"hdfs://master:9000/raw_data/yellow_tripdata_2022-{month_id:02d}.parquet"
    for month_id in range(1, 7)
]

spark = SparkSession.builder.appName("q5-sql").getOrCreate()

yellow_tripdata_df = spark.read.parquet(*files, mergeSchema=True)

zone_lookup_df = spark.read.csv(
    f"hdfs://master:9000/raw_data/taxi+_zone_lookup.csv", header=True
)

# group by month and day of the month and divide the total tip amount by the total fare amount to get the average tip percentage
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

for row in result:
    pprint(row.asDict())
