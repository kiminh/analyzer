create table if not exists dl_cpc.ocpc_param_calibration_hourly(
    identifier              string,
    pcoc                    double,
    jfb                     double,
    post_cvr                double,
    high_bid_factor         double,
    low_bid_factor          double
)
partitioned by (is_hidden int, conversion_goal int, `date` string, `hour` string, version string)
stored as parquet;