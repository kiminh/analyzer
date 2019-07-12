CREATE TABLE IF NOT EXISTS test.ocpc_unit_light_control_hourly
(
    unitid                  int,
    userid                  int,
    adclass                 int,
    media                   string,
    cpa                     double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

create table dl_cpc.ocpc_unit_light_control_hourly
like test.ocpc_unit_light_control_hourly;