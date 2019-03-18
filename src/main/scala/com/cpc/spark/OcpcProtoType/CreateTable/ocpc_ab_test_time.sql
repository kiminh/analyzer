CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_ab_test_time(
    unitid          int,
    hr              string
)
PARTITIONED BY (`date` string, tag string, version string)
STORED AS PARQUET;