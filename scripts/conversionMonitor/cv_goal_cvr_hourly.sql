CREATE TABLE IF NOT EXISTS test.cv_goal_cvr_hourly
(
    conversion_goal         int,
    click                   int,
    cv                      int,
    cvr                     double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;