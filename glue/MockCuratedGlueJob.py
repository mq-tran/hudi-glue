from pyspark import SparkContext 
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when, lit
from pyspark.sql.types import StringType

# sc = SparkContext.getOrCreate()
# gc = GlueContext(sc)
# spark = gc.spark_session

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').getOrCreate()
gc = GlueContext(spark.sparkContext)

print("Start")

initDf = spark.sql("select * from mqtran_db.diamondtaxi")
initDf.printSchema()
initDf.show()

print("----------------------------------------------------------------------------------------------")

initDf = spark.read.format("hudi").load("s3://mqtran/transform/mqtran_db/diamondtaxi/")
initDf.printSchema()
initDf.show()

print("Done")