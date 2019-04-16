CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_unit_label_cvr_hourly
(
    searchid                string,
    unitid                  int,
    planid                  int,
    userid                  int
)
PARTITIONED by (conversion_goal int, `date` STRING, `hour` STRING)
STORED as PARQUET;



--searchid                string
--unitid                  int
--planid                  int
--userid                  int
--conversion_goal         int
--date                    string
--hour                    string









