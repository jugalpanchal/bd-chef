### Copy on write table, partition - read-heavy workloads
### Merge on read table

### Config
- primary key - ex trip_id
- sort key - ex tstamp

### Hoodie Columns
- _hoodie_commit_time string - timestamp
- _hoodie_commit_seqno string - timestamp_n_m where n = 0,1,2,....  m = 1,2,3,....
- _hoodie_record_key string - primary key - ex trip_id
- _hoodie_partition_path string
- _hoodie_file_name string - parquet file name


