create table if not exists dl_cpc.ocpc_param_calibration_hourly_v2(
    identifier              string,
    pcoc                    double,
    jfb                     double,
    post_cvr                double,
    high_bid_factor         double,
    low_bid_factor          double,
    cpagiven                double
)
partitioned by (is_hidden int, exp_tag String, conversion_goal int, `date` string, `hour` string, version string)
stored as parquet;