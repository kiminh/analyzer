CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_light_qtt_manual_list
(
    unitid                  string,
    conversion_goal         int,
    cpa                     double,
    `date`                  string,
    `hour`                  string
)
PARTITIONED by (version string)
STORED as PARQUET;


--unitid                  bigint
--conversion_goal         bigint
--cpa                     double
--date                    string
--hour                    string