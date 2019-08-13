CREATE TABLE IF NOT EXISTS test.model_cvr_cmp_daily
(
    cvr_yesterday            double,
    cvr_today                double,
    cvr_diff                 double

)
PARTITIONED by (`date` string, `hour` string, model_name string)
STORED as PARQUET;