create table if not exists test.ocpc_bs_params_pb_hourly(
    key                     string,
    cv                      int,
    cvr                     double,
    ctr                     double
)
partitioned by (`date` string, `hour` string, exp_tag string, version string)
stored as parquet;
