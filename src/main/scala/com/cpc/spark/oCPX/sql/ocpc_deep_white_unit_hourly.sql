create table if not exists test.ocpc_deep_white_unit_hourly(
    identifier              string,
    media                   string,
    deep_conversion_goal    int,
    cv                      bigint,
    auc                     double,
    flag                    int,
    cost                    double
)
partitioned by (`date` string, version string)
stored as parquet;