create table if not exists test.unit_cost_by_media_daily
(
    unitid                  int,
    userid                  int,
    media                   string,
    is_ocpc                 int,
    show                    int,
    click                   int,
    cost                    int
)
partitioned by (`date` string)
stored as parquet;

create table dl_cpc.unit_cost_by_media_daily
like test.unit_cost_by_media_daily;