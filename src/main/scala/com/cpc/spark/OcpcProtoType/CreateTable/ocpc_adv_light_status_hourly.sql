CREATE TABLE IF NOT EXISTS test.ocpc_adv_light_status_hourly
(
    unitid                  int,
    ocpc_light              int
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;


create table dl_cpc.ocpc_adv_light_status_hourly
like test.ocpc_adv_light_status_hourly;
