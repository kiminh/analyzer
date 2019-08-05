CREATE TABLE IF NOT EXISTS test.ocpc_cvr_delay_hourly
(
    unitid                  int
    userid                  int
    conversion_goal         int
    media                   string
    date                    string
    hour                    string
    click                   bigint
    click_cv                bigint
    show_cv                 bigint
    hourint                 bigint
)
PARTITIONED by (`date` string, `hour` string)
STORED as PARQUET;
