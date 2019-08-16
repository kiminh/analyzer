CREATE TABLE IF NOT EXISTS test.cv_goal_cvr_cmp_hourly
(
    cvr_yesterday            double,
    cvr_today                double,
    cvr_diff                 double,
    conversion_goal          int,
    version                  string

)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;