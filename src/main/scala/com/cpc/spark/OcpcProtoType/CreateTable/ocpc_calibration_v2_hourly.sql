CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_calibration_v2_hourly
(
    exp_tag                 string,
    unitid                  int,
    ideaid                  int,
    slotid                  string,
    slottype                int,
    adtype                  int,
    pcoc                    double,
    jfb                     double,
    post_cvr                double,
    high_bid_factor         double,
    low_bid_factor          double
)
PARTITIONED by (conversion_goal int, `date` string, `hour` string, version string)
STORED as PARQUET;