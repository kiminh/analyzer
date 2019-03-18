create table if not exists dl_cpc.ocpc_dark_test_time(
    unitid              int,
    hour                int
)
partitioned by (`date` string, version string)
stored as parquet;