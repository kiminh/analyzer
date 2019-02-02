CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_label_cvr_hourly
(
    searchid        string,
    label           int
)
PARTITIONED by (`date` STRING, `hour` STRING, cvr_goal STRING)
STORED as PARQUET;