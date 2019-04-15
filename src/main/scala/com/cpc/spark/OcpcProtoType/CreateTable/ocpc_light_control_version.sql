CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_light_control_version
(
    unitid                  string,
    conversion_goal         int,
    cpa                     double,
    `date`                    string
)
PARTITIONED by (version string)
STORED as PARQUET;
