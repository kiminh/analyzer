create table if not exists dl_cpc.ocpc_param_pb_data_hourly_v2(
    identifier              string,
    conversion_goal         int,
    is_hidden               int,
    exp_tag                 string,
    cali_value              double,
    jfb_factor              double,
    post_cvr                double,
    high_bid_factor         double,
    low_bid_factor          double,
    cpa_suggest             double,
    smooth_factor           double,
    cpagiven                double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;

create table test.ocpc_param_pb_data_hourly_v2
like dl_cpc.ocpc_param_pb_data_hourly_v2;