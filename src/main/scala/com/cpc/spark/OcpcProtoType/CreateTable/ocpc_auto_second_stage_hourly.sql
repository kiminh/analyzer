CREATE TABLE IF NOT EXISTS test.ocpc_auto_second_stage_hourly
(
    unitid              int,
    userid              int,
    conversion_goal     int,
    media               string
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;






