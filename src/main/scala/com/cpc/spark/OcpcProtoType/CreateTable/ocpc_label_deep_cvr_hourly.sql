CREATE TABLE IF NOT EXISTS test.ocpc_label_cvr_hourly
(
    searchid        string,
    ideaid          int,
    unitid          int,
    userid          int,
    label           int
)
PARTITIONED by (`date` STRING, `hour` STRING, deep_conversion_goal int)
STORED as PARQUET;
