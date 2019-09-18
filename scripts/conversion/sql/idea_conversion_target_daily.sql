create table if not exists test.idea_conversion_target_daily
(
    userid              int,
    unitid              int,
    ideaid              int,
    conversion_target   string
)
partitioned by (`date` string)
stored as parquet;


CREATE TABLE dl_cpc.idea_conversion_target_daily
LIKE test.idea_conversion_target_daily;