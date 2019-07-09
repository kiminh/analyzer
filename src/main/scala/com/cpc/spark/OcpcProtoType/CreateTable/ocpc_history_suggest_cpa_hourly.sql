CREATE TABLE IF NOT EXISTS test.ocpc_history_suggest_cpa_hourly
(
    unitid                  int,
    userid                  int,
    adclass                 int,
    media                   string,
    conversion_goal         int,
    cpa_suggest             double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

create table dl_cpc.ocpc_history_suggest_cpa_hourly
like test.ocpc_history_suggest_cpa_hourly;