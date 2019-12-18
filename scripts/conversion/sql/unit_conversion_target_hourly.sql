create table if not exists test.unit_conversion_target_hourly
(
    userid              int,
    unitid              int,
    conversion_target   string
)
partitioned by (`date` string, `hour` string)
stored as parquet;


CREATE TABLE dl_cpc.unit_conversion_target_hourly
LIKE test.unit_conversion_target_hourly;