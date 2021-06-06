import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when, lit, unix_timestamp, concat
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
import time
import datetime
import pytz

import boto3
from botocore.exceptions import ClientError

# don't run in local environment
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_LOCATION', 'OUTPUT_LOCATION', 'DATABASE', 'TABLE', 'PRIMARY_KEY'])
source = args['SOURCE_LOCATION']
destination = args['OUTPUT_LOCATION']
# this is the Glue Catalog database and should come from the event
dbName = args['DATABASE']
tableName = args['TABLE']
primaryKey = args['PRIMARY_KEY']

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

logger.info('Initialization.')
glueClient = boto3.client('glue')

# this should come from dynamoDB i.e. ObjectMetadata or ssm
table_config = {
    'primaryKey': primaryKey
}
dropColumnList = []

logger.info('Processing starts.')
partitionKey = ''
try:
    glueClient.get_table(DatabaseName=dbName,Name=tableName)
    isTableExists = True
    logger.info(dbName + '.' + tableName + ' exists.')
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        isTableExists = False
        logger.info(dbName + '.' + tableName + ' does not exist. Table will be created.')
try:
    primaryKey = table_config['primaryKey']
    isPrimaryKey = True
    logger.info('Primary key:' + primaryKey)
except KeyError as e:
    isPrimaryKey = False
    logger.info('Primary key not found. An append only glueparquet table will be created.')
try:
    partitionKey = table_config['partitionKey']
    isPartitionKey = True
    logger.info('Partition key:' + partitionKey)
except KeyError as e:
    isPartitionKey = False
    logger.info('Partition key not found. Partitions will not be created.')

timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
tz_local = pytz.timezone('Australia/Canberra')
timestamp_local = datetime.datetime.now(tz_local).strftime("%Y-%m-%d %H:%M:%S")
targetPath = destination + dbName + '/' + tableName

# Dataframe
# inputDf = spark.read.load(source).withColumn('update_ts',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

# DynamicFrame
inputDyf = glueContext.create_dynamic_frame_from_options(connection_type = 's3', connection_options = {'paths': [source], 'groupFiles': 'none', 'recurse':True}, format = 'parquet')
inputDf = inputDyf.toDF()
# inputDf = inputDyf.toDF().withColumn('update_ts',unix_timestamp(lit(timestamp_local),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

# Hudi config
morConfig = {'hoodie.datasource.write.storage.type': 'MERGE_ON_READ', 'hoodie.compact.inline': 'false', 'hoodie.compact.inline.max.delta.commits': 20, 'hoodie.parquet.small.file.limit': 0}
commonConfig = {'className' : 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc':'false', 'hoodie.datasource.write.precombine.field': 'update_ts', 'hoodie.datasource.write.recordkey.field': 'pk_col', 'hoodie.table.name': tableName, 'hoodie.consistency.check.enabled': 'true', 'hoodie.datasource.hive_sync.database': dbName, 'hoodie.datasource.hive_sync.table': tableName, 'hoodie.datasource.hive_sync.enable': 'true', 'path': targetPath}
partitionDataConfig = {'hoodie.datasource.write.partitionpath.field': partitionKey, 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor', 'hoodie.datasource.hive_sync.partition_fields': partitionKey}
unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}
incrementalConfig = {'hoodie.upsert.shuffle.parallelism': 10, 'hoodie.datasource.write.operation': 'upsert', 'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}
# For an insert or bulk_insert operation, pre-combining is not performed. Use upsert if data has duplicate or historical data
# initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': 3, 'hoodie.datasource.write.operation': 'bulk_insert'}
initLoadConfig = {'hoodie.upsert.shuffle.parallelism': 10, 'hoodie.datasource.write.operation': 'upsert', 'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}
deleteDataConfig = {'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.EmptyHoodieRecordPayload'}

combinedConf = {}

if(isPrimaryKey):
    logger.info('Going the Hudi way.')
    if(isTableExists):
        logger.info('Incremental load.')
        updatedDf = inputDf.drop(*dropColumnList)
        if updatedDf.count() > 0:
            logger.info('Upserting data.')
            print('Upserting data.')
            if (isPartitionKey):
                logger.info('Writing to partitioned Hudi table.')
                updatedDf = updatedDf.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig}
                glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(updatedDf, glueContext, "updatedDf"), connection_type = "marketplace.spark", connection_options = combinedConf)
                print('Complete: Hudi Upserting data - Partitioned')
            else:
                logger.info('Writing to unpartitioned Hudi table.')
                combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig}
                glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(updatedDf, glueContext, "updatedDf"), connection_type = "marketplace.spark", connection_options = combinedConf)
                print('Complete: Hudi Upserting data - Unpartitioned' )
    else:
        initDf = inputDf.drop(*dropColumnList)
        if initDf.count() > 0:
            logger.info('Inital load.')
            print('Inital load.')
            if (isPartitionKey):
                logger.info('Writing to partitioned Hudi table.')
                initDf = initDf.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                combinedConf = {**commonConfig, **partitionDataConfig, **initLoadConfig}
                glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(initDf, glueContext, "initDf"), connection_type = "marketplace.spark", connection_options = combinedConf)
                print('Complete: Init load - Partitioned')
            else:
                logger.info('Writing to unpartitioned Hudi table.')
                combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
                glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(initDf, glueContext, "initDf"), connection_type = "marketplace.spark", connection_options = combinedConf)
                print('Complete: Hudi Init load - Unpartitioned')
else:
    logger.info('Primary key is required for Hudi.')
    print('Primary key is required for Hudi.')
