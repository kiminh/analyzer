create table if not exists test.dw_unitid_conversion_target_hourly
(
    userid                  int,
    planid                  int,
    unitid                  int,
    adclass                 int,
    interaction             int,
    is_ocpc                 int,
    src                     int,
    adslot_type             int,
    industry                string,
    ideaids                 array<int>,
    conversion_target       array<string>
)
partitioned by (day string, hour string)
stored as parquet;


CREATE TABLE dl_cpc.dw_unitid_conversion_target_hourly
LIKE test.dw_unitid_conversion_target_hourly;