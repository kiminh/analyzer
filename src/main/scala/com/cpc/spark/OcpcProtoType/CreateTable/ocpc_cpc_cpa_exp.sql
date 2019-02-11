CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_cpc_cpa_exp
(
    identifier          string,
    cpc_suggest         double,
    conversion_goal     int,
    duration            int
)
PARTITIONED by (`date` string, version string)
STORED as PARQUET;