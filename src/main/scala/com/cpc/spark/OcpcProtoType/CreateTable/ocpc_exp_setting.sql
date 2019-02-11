CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_exp_setting
(
    map_key                 string,
    map_value               string
)
PARTITIONED by (`date` string, version string, exp_tag string)
STORED as PARQUET;