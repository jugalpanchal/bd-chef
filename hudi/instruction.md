### CoW - Copy on write table, partition - read-heavy workloads - merge while write so read would be quick.

### MoR - Merge on read table - For near real-time applications that mandate quick upserts - write quick and merge while read.
MOR table stores incoming upserts for each file group, onto a row based delta log (In Avro file format). It means every upsert has its own seperate file(Avro file format, that is .log).
This log is then merged with the existing Parquet file using a compactor during reads. The compaction can be run manually or after some set(we can define the number while setting up the config or write) of commits.


### Config
- primary key - ex trip_id
- sort key - ex tstamp

### Hoodie Columns
- _hoodie_commit_time string - timestamp
- _hoodie_commit_seqno string - timestamp_n_m where n = 0,1,2,....  m = 1,2,3,....
- _hoodie_record_key string - primary key - ex trip_id
- _hoodie_partition_path string
- _hoodie_file_name string - parquet file name



