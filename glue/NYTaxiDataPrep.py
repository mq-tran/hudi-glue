import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when, unix_timestamp, lit
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
import datetime
import pytz

sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
spark = gc.spark_session

tz_local = pytz.timezone('Australia/Canberra') 
current_local_ts = datetime.datetime.now(tz_local)
ts_format = "%Y-%m-%d %H:%M:%S"

day_minus_ts = (current_local_ts - datetime.timedelta(days=5)).strftime(ts_format)
day_zero_ts = current_local_ts.strftime(ts_format)
day_one_ts = (current_local_ts + datetime.timedelta(days=1)).strftime(ts_format)

day_minus_path = "s3://mqtran/raw/trip_data_2020/day=2/"
day_zero_path = "s3://mqtran/raw/trip_data_2020/day=0/"
day_one_path = "s3://mqtran/raw/trip_data_2020/day=1/"

yellow_tripdata_schema = StructType([ StructField("vendorid",IntegerType(),True), StructField("tpep_pickup_datetime",TimestampType(),True), StructField("tpep_dropoff_datetime",TimestampType(),True), StructField("passenger_count", IntegerType(), True), StructField("trip_distance", DoubleType(), True), StructField("ratecodeid", IntegerType(), True), StructField("store_and_fwd_flag", StringType(), True), StructField("pulocationid", IntegerType(), True), StructField("dolocationid", IntegerType(), True), StructField("payment_type", IntegerType(), True), StructField("fare_amount", DoubleType(), True), StructField("extra", DoubleType(), True), StructField("mta_tax", DoubleType(), True), StructField("tip_amount", DoubleType(), True), StructField("tolls_amount", DoubleType(), True), StructField("improvement_surcharge", DoubleType(), True), StructField("total_amount", DoubleType(), True), StructField("congestion_surcharge", DoubleType(), True), StructField("pk_col", LongType(), True)])

print("starting..")

inputDf = spark.read.schema(yellow_tripdata_schema).option("header", "true").csv("s3://nyc-tlc/trip data/yellow_tripdata_{2020}*.csv").withColumn("pk_col",monotonically_increasing_id() + 1)
inputDf.printSchema()

print("day minus")
outputDf = inputDf.filter(inputDf.vendorid == 1).withColumn("vendorid", when(inputDf.vendorid == 1, 0)).withColumn('update_ts',unix_timestamp(lit(day_minus_ts),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
outputDf.write.parquet(day_minus_path, mode="overwrite")
outputDf.count()

print("day 0")
outputDf = inputDf.withColumn('update_ts',unix_timestamp(lit(day_zero_ts),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
outputDf.write.parquet(day_zero_path, mode="overwrite")
outputDf.count()

print("day 1")
outputDf = inputDf.filter(inputDf.vendorid == 1).withColumn("vendorid", when(inputDf.vendorid == 1, 9)).withColumn('update_ts',unix_timestamp(lit(day_one_ts),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
outputDf.write.parquet(day_one_path, mode="overwrite")
outputDf.count()



print("done")