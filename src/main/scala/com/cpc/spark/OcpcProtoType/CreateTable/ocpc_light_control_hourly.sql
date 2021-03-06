CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_light_control_hourly
(
    unitid                  string,
    conversion_goal         int,
    cpa                     double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;

--unitid                  string
--conversion_goal         int
--cpa                     double
--date                    string
--version                 string