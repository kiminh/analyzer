CREATE TABLE IF NOT EXISTS test.ocpc_light_qtt_manual_list_version
(
    unitid                  int,
    media                   string,
    cpa                     double,
    `date`                  string,
    `hour`                  string
)
PARTITIONED by (version string)
STORED as PARQUET;


create table dl_cpc.ocpc_light_qtt_manual_list_version
like test.ocpc_light_qtt_manual_list_version;