create table if not exists test.unit_changing_by_media_daily
(
    media                   string,
    is_ocpc                 int,
    unitid_cnt              int,
    userid_cnt              int,
    cost                    double,
    data_type               string
)
partitioned by (`date` string)
stored as parquet;
