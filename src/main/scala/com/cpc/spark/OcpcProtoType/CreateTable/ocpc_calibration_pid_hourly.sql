create table if not exists test.ocpc_calibration_pid_hourly
(
    key                     string,
    current_cali            double
)
partitioned by (`date` string, `hour` string, exp_tag string, version string)
stored as parquet;
