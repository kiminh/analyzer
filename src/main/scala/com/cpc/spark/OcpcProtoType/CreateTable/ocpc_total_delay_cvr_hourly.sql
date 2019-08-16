CREATE TABLE IF NOT EXISTS test.ocpc_total_delay_cvr_hourly
(
    conversion_goal         int,
    media                   string,
    click                   bigint,
    cv1                     bigint,
    cv2                     bigint,
    lost_cvr                double
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;