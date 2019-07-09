CREATE TABLE IF NOT EXISTS test.ocpc_history_suggest_cpa_version
(
    unitid                  int,
    userid                  int,
    adclass                 int,
    media                   string,
    conversion_goal         int,
    cpa_suggest             double,
    `date`                  string,
    `hour`                  string
)
PARTITIONED by (version string)
STORED as PARQUET;

create table dl_cpc.ocpc_history_suggest_cpa_version
like test.ocpc_history_suggest_cpa_version;