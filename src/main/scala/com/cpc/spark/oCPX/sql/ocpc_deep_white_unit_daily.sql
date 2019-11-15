create table if not exists test.ocpc_deep_white_unit_daily(
    identifier              string,
    media                   string,
    deep_conversion_goal    int,
    cv                      bigint,
    auc                     double,
    flag                    int
)
partitioned by (`date` string, version string)
stored as parquet;


alter table dl_cpc.ocpc_deep_white_unit_hourly add columns (cost double)
alter table dl_cpc.ocpc_deep_white_unit_hourly add columns (cpa double)
