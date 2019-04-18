CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_charge_daily
(
    unitid  int,

)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;