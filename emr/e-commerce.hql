-- EMR Scaling test

--Customer, Orders and LineItem
CREATE EXTERNAL TABLE Customer (
c_custkey int,
c_name string,
c_address string,
c_nationkey int,
c_phone string,
c_acctbal double,
c_mktsegment string,
c_comment string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
's3://emr-workshops-us-west-2/athena_federation_workshop/data/s10/customer';

--Set hive properties to make sure we do not face physical/virtual memory issues during our task executions. 
set hive.execution.engine=tez;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=405306368;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled =true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.merge.mapfiles =true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=134217728;
set hive.merge.smallfiles.avgsize=44739242;
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
