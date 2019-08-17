CREATE TABLE IF NOT EXISTS test.ocpc_quick_cv_log
(
    searchid                string,
    conversion_goal         int
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;