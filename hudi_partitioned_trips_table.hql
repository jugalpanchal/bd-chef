CREATE EXTERNAL TABLE `hudi_partitioned_trips_table`(
  `_hoodie_commit_time` string, 
  `_hoodie_commit_seqno` string, 
  `_hoodie_record_key` string, 
  `_hoodie_partition_path` string, 
  `_hoodie_file_name` string, 
  `destination` string, 
  `route_id` string, 
  `trip_id` bigint)
PARTITIONED BY ( 
  `routeid` string, 
  `tstamp` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'hoodie.query.as.ro.table'='false', 
  'path'='s3://jugal-sandbox-us-east-2-etl/hudi/hudi_partitioned_trips_table') 
STORED AS INPUTFORMAT 
  'org.apache.hudi.hadoop.HoodieParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://jugal-sandbox-us-east-2-etl/hudi/hudi_partitioned_trips_table'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'last_commit_time_sync'='20220905211407', 
  'spark.sql.sources.provider'='hudi', 
  'spark.sql.sources.schema.numPartCols'='2', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"destination\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"route_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"trip_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"routeid\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"tstamp\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'spark.sql.sources.schema.partCol.0'='routeid', 
  'spark.sql.sources.schema.partCol.1'='tstamp', 
  'transient_lastDdlTime'='1662412553')
