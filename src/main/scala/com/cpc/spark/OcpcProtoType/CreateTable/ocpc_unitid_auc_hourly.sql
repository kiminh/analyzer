create table if not exists dl_cpc.ocpc_unitid_auc_hourly(
    unitid                  string,
    auc                     double,
    industry                string
)
partitioned by (conversion_goal int, `date` string, `hour` string, version string)
stored as parquet;