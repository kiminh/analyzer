CREATE TABLE IF NOT EXISTS test.model_cvr_cmp_daily
(
    cvr_yesterday            double,
    cvr_today                double,
    cvr_diff                 double,
    hour                     string

)
PARTITIONED by (`date` string, model_name string)
STORED as PARQUET;