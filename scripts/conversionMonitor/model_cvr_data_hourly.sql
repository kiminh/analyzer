CREATE TABLE IF NOT EXISTS dl_cpc.model_cvr_data_hourly
(
    click                   int,
    cv                      int
)
PARTITIONED by (`date` string, `hour` string, model_name string)
STORED as PARQUET;