DROP TABLE IF EXISTS ny_taxi;

CREATE EXTERNAL TABLE ny_taxi (
              vendor_id int,
              lpep_pickup_datetime string,
              lpep_dropoff_datetime string,
              store_and_fwd_flag string,
              rate_code_id smallint,
              pu_location_id int,
              do_location_id int,
              passenger_count int,
              trip_distance double,
              fare_amount double,
              mta_tax double,
              tip_amount double,
              tolls_amount double,
              ehail_fee double,
              improvement_surcharge double,
              total_amount double,
              payment_type smallint,
              trip_type smallint
       )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "${INPUT}";

INSERT OVERWRITE DIRECTORY "${OUTPUT}"
SELECT * FROM ny_taxi WHERE rate_code_id = 1;


-- EMR Scaling test
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
