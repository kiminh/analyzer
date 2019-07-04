create table if not exists test.ocpc_unitid_auc_hourly_v2
(
    identifier              string,
    media                   string,
    conversion_goal         int,
    auc                     double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;

create table dl_cpc.ocpc_unitid_auc_hourly_v2
like test.ocpc_unitid_auc_hourly_v2;