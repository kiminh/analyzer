CREATE TABLE IF NOT EXISTS test.ocpc_cvlog_hourly
(
    searchid        string,
    ideaid          int,
    unitid          int,
    userid          int,
    label           int
)
PARTITIONED by (`date` STRING, `hour` STRING, conversion_goal int)
STORED as PARQUET;
