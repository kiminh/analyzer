create table if not exists dl_cpc.ocpc_param_pb_data_hourly(
    identifier              string,
    conversion_goal         int,
    is_hidden               int,
    cali_value              double,
    jfb_factor              double,
    post_cvr                double,
    high_bid_factor         double,
    low_bid_factor          double,
    cpa_suggest             double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;