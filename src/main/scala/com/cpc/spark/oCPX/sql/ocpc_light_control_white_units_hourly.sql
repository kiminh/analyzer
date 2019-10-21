create table if not exists test.ocpc_light_control_white_units_hourly(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    adclass                 int,
    ocpc_status             int,
    ocpc_light              int,
    ocpc_suggest_price      double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;

alter table dl_cpc.ocpc_light_control_white_units_hourly add columns (media string)