CREATE TABLE IF NOT EXISTS dl_cpc.model_cvr_data_daily
(
    click                   int,
    cv                      int,
    hour                    string
)
PARTITIONED by (`date` string, model_name string)
STORED as PARQUET;