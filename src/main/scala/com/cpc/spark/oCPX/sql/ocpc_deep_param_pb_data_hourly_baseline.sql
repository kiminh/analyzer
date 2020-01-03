create table if not exists test.ocpc_deep_param_pb_data_hourly_baseline(
    conversion_goal int,
    exp_tag         string,
    cali_value      double,
    jfb_factor      double,
    post_cvr        double,
    high_bid_factor double,
    low_bid_factor  double,
    cpa_suggest     double,
    smooth_factor   double,
    cpagiven        double
)
partitioned by (`date` string, `hour` string)
stored as parquet;
