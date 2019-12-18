create table if not exists test.ocpc_deep_pb_data_hourly_exp(
    identifier      string,
    conversion_goal int,
    jfb_factor      double,
    post_cvr        double,
    smooth_factor   double,
    cvr_factor      double,
    high_bid_factor double,
    low_bid_factor  double,
    cpagiven        double
)
partitioned by (`date` string, `hour` string, exp_tag string, is_hidden int, version string, deep_conversion_goal int)
stored as parquet;
