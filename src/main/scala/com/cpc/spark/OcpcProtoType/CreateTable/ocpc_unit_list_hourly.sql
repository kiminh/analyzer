CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_unit_list_hourly
(
    unitid                  bigint,
    userid                  bigint,
    target_medias           string,
    cpa_given               bigint,
    conversion_goal         int,
    update_timestamp        bigint,
    update_time             string,
    update_date             string,
    update_hour             string,
    ocpc_last_open_date     string,
    ocpc_last_open_hour     string,
    status                  int
)
PARTITIONED by (`date` STRING, `hour` STRING)
STORED as PARQUET;