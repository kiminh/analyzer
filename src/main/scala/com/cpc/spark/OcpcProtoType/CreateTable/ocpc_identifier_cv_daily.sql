CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_identifier_cv_daily
(
    identifier          string,
    cv                  bigint
)
PARTITIONED by (`date` string, conversion_goal int, version string)
STORED as PARQUET;