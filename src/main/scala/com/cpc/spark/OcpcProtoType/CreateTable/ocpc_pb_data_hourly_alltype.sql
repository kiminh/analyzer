--"unitid", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "date", "hour", "version"

create table if not exists test.ocpc_pb_data_hourly_alltype(
    identifier              string,
    conversion_goal         int,
    jfb_factor              double,
    post_cvr                double,
    smooth_factor           double,
    cvr_factor              double,
    high_bid_factor         double,
    low_bid_factor          double,
    cpagiven                double
)
partitioned by (`date` string, `hour` string, exp_tag string, is_hidden int, version string)
stored as parquet;

create table dl_cpc.ocpc_pb_data_hourly_alltype
like test.ocpc_pb_data_hourly_alltype;


