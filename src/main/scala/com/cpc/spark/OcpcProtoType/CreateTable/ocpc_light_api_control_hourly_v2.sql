CREATE TABLE IF NOT EXISTS test.ocpc_light_api_control_hourly_v2
(
    unit_id                 string,
    ocpc_light              int,
    ocpc_suggest_price      double,
    media                   string
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

create table dl_cpc.ocpc_light_api_control_hourly_v2
like test.ocpc_light_api_control_hourly_v2;