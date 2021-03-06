create table if not exists test.ocpc_bs_params_pb_hourly(
    key                     string,
    cv                      int,
    cvr                     double,
    ctr                     double
)
partitioned by (`date` string, `hour` string, exp_tag string, version string)
stored as parquet;


--alter table test.ocpc_bs_params_pb_hourly add columns (cvr_factor double)
alter table dl_cpc.ocpc_bs_params_pb_hourly add columns (jfb_factor double)

