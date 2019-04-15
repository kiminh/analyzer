CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_suggest_cpa_k_version
(
    identifier          string,
    cpa_suggest         double,
    kvalue              double,
    conversion_goal     int,
    duration            int,
    `date`              string
)
PARTITIONED by (version string)
STORED as PARQUET;