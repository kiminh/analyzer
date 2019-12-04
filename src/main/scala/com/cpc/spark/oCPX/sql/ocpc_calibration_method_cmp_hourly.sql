create table if not exists test.ocpc_calibration_method_cmp_hourly(
    unitid                  int,
    time                    string,
    click                   bigint,
    real_pcoc               double,
    baseline_pcoc           double,
    pred_pcoc               double,
    baseline_diff           double,
    pred_diff               double
)
partitioned by (`date` string, `hour` string, version string, exp_tag string)
stored as parquet;