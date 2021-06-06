# hudi-glue

Hudi POC for Hudi CoW (Copy on Write) dataset and reference the following AWS blogs:
https://aws.amazon.com/blogs/big-data/creating-a-source-to-lakehouse-data-replication-pipe-using-apache-hudi-aws-glue-aws-dms-and-amazon-redshift/
https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/

Note: The dependent hudi jar file can be found here:
https://aws-bigdata-blog.s3.amazonaws.com/artifacts/hudi-on-glue/Dependencies/Jars/hudi-spark.jar

Note: the spark avro jar is required for Hudi MoR (Merge on Read) dataset. 
https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/2.4.4/spark-avro_2.11-2.4.4.jar

## Order

1. HudiGlueRoleCFn.yml - to create the glue execution role and redshift Glue access role

2. Upload the Glue script to s3. Update the scripts location and run HudiGlueJobCFn.yml

3. Run the NYTaxiDataPrep.py to create day 0, day 1 and day 2 data

4. Run the Hudi job

5. Grant read permission to Hudi tables if Lake formation is enabled.

6. Run spectrum_setup.sql to create external schema for Hudi table in redshift