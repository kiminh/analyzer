CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_auto_budget_hourly
(
    unitid          int,
    userid          int,
    planid          int,
    cpa             double,
    kvalue          double,
    conversion_goal int,
    budget          double,
    exp_tag         string
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

alter table dl_cpc.ocpc_auto_budget_hourly add columns (industry string);