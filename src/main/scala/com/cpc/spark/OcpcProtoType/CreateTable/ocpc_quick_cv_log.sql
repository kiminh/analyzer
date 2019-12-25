CREATE TABLE IF NOT EXISTS test.ocpc_quick_cv_log
(
    searchid                string,
    conversion_goal         int
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;

alter table dl_cpc.ocpc_quick_cv_log add columns (conversion_from int)