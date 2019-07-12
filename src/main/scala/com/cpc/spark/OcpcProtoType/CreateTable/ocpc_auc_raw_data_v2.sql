CREATE TABLE IF NOT EXISTS test.ocpc_auc_raw_data_v2
(
    searchid                string,
    identifier              string,
    media                   string,
    conversion_goal         int,
    score                   bigint,
    label                   int,
    industry                string
)
PARTITIONED by (version string)
STORED as PARQUET;

create table dl_cpc.ocpc_auc_raw_data_v2
like test.ocpc_auc_raw_data_v2;