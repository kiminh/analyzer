create table if not exists dl_cpc.ocpc_ab_test_time(
    unitid          int,
    hr              string
)
partitioned by (`date` string, tag string, version string)
stored as parquet;