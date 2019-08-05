CREATE TABLE IF NOT EXISTS test.ocpc_cvr_delay_hourly
(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    media                   string,
    click                   bigint,
    cv1                     bigint,
    cv2                     bigint
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;