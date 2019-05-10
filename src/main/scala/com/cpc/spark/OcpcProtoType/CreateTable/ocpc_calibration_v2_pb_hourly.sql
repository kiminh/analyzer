CREATE TABLE IF NOT EXISTS dl_cpc.ocpc_calibration_v2_pb_hourly
(
    exp_tag                 string,
    unitid                  bigint,
    ideaid                  bigint,
    slotid                  string,
    slottype                bigint,
    adtype                  bigint,
    cvr_cal_factor          double,
    jfb_factor              double,
    post_cvr                double,
    high_bid_factor         double,
    low_bid_factor          double
)
PARTITIONED by (`date` string, `hour` string, version string)
STORED as PARQUET;