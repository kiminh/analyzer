CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_k_model_hourly
(
    identifier          string,
    kvalue              double
)
PARTITIONED by (conversion_goal int, `date` string, `hour` string, version string, method string)
STORED as PARQUET;