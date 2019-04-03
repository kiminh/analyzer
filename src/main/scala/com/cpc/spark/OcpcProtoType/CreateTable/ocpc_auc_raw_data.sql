CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_auc_raw_data
(
    searchid                string,
    identifier              string,
    score                   bigint,
    label                   int,
    industry                string
)
PARTITIONED by (conversion_goal int, version string)
STORED as PARQUET;

