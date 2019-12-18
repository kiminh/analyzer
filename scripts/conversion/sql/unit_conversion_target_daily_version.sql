create table if not exists test.unit_conversion_target_daily_version
(
    userid              int,
    unitid              int,
    conversion_target   string
)
partitioned by (`date` string, version string)
stored as parquet;


CREATE TABLE dl_cpc.unit_conversion_target_daily_version
LIKE test.unit_conversion_target_daily_version;