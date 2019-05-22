create table if not exists dl_cpc.ocpc_pid_k_data_hourly(
    unitid                  int,
    conversion_goal         int,
    kvalue                  double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;
