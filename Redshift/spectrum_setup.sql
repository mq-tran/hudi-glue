-- Create external schmea for the Hudi table
CREATE EXTERNAL SCHEMA mqtran_db FROM DATA CATALOG 
DATABASE 'mqtran_db'
REGION 'ap-southeast-2'
IAM_ROLE 'arn:aws:iam::747843067444:role/LakeHouseRedshiftGlueAccessRole';
-- create table in Glue catalog but need permission
-- create external database if not exists;

SELECT *
FROM mqtran_db.diamondtaxi
limit 10;