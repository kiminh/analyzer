create table if not exists test.unit_conversion_target_daily
(
    userid              int,
    unitid              int,
    conversion_target   string
)
partitioned by (`date` string)
stored as parquet;


CREATE TABLE dl_cpc.unit_conversion_target_daily
LIKE test.unit_conversion_target_daily;