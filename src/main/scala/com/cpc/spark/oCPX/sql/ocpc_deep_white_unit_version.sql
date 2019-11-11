create table if not exists test.ocpc_deep_white_unit_version(
    identifier              string,
    media                   string,
    deep_conversion_goal    int,
    cv                      bigint,
    auc                     double,
    flag                    int,
    `date`                  string,
    cost                    double
)
partitioned by (version string)
stored as parquet;