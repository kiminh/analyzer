create table if not exists test.ocpc_calibration_method_hourly(
    unitid                  int,
    method                  string,
    baseline_mae            double,
    pred_mae                double,
    click                   bigint,
    cv                      bigint
)
partitioned by (`date` string, `hour` string, version string, exp_tag string)
stored as parquet;
