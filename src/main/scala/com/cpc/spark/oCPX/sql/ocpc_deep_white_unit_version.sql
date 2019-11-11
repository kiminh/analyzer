create table if not exists test.ocpc_deep_white_unit_version(
    identifier              string,
    media                   string,
    deep_conversion_goal    int,
    cv                      bigint,
    auc                     double,
    flag                    int,
    `date`                  string
)
partitioned by (version string)
stored as parquet;


alter table dl_cpc.ocpc_deep_white_unit_version add columns (cost double)
alter table dl_cpc.ocpc_deep_white_unit_version add columns (cpa double)
