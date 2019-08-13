CREATE TABLE IF NOT EXISTS test.model_cvr_cmp_daily
(
    cvr_yesterday            double,
    cvr_today                double,
    cvr_diff                 double,
    model_name               string

)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;