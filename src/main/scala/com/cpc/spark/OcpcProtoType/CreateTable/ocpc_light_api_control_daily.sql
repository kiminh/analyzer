CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_light_api_control_daily
(
    unit_id                 string,
    ocpc_light              int,
    ocpc_suggest_price      double
)
PARTITIONED by (`date` string, version string)
STORED as PARQUET;