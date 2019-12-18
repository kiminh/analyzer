create table if not exists test.idea_conversion_target_daily_version
(
    userid              int,
    unitid              int,
    ideaid              int,
    conversion_target   string
)
partitioned by (`date` string, version string)
stored as parquet;


CREATE TABLE dl_cpc.idea_conversion_target_daily_version
LIKE test.idea_conversion_target_daily_version;