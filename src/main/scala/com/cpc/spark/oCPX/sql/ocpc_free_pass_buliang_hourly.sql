create table if not exists test.ocpc_free_pass_buliang_hourly(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    ocpc_status             int
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;

