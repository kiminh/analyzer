create table if not exists dl_cpc.ocpc_pid_cali_data_hourly(
    unitid                  int,
    current_error           double,
    prev_error              double,
    last_error              double,
    kp                      double,
    ki                      double,
    kd                      double,
    increment_value         double,
    current_calivalue       double
)
partitioned by (conversion_goal int, `date` string, `hour` string, exp_tag string, version string)
stored as parquet;
