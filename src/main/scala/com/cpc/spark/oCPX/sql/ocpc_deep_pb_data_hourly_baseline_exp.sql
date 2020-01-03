create table if not exists test.ocpc_deep_pb_data_hourly_baseline_exp(
    conversion_goal int,
    jfb_factor      double,
    post_cvr        double,
    smooth_factor   double,
    cvr_factor      double,
    high_bid_factor double,
    low_bid_factor  double,
    cpagiven        double
)
partitioned by (`date` string, `hour` string, exp_tag string)
stored as parquet;
