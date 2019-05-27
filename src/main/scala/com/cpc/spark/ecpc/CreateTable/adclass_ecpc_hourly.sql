create table if not exists dl_cpc.adclass_ecpc_hourly(
    unitid                  bigint,
    is_api_callback         int,
    post_cvr                double,
    cvr_cal_factor          double,
    high_bid_factor         double,
    low_bid_factor          double
)
partitioned by (`date` string, `hour` string, version string)
stored as parquet;