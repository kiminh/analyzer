CREATE TABLE IF NOT EXISTS test.ocpc_cvr_log_hourly
(
    searchid        string,
    ideaid          int,
    unitid          int,
    userid          int,
    label           int
)
PARTITIONED by (`date` STRING, `hour` STRING, conversion_goal int, conversion_from int)
STORED as PARQUET;
