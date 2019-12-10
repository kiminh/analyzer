create table if not exists test.ocpc_realtime_pid_parameters_hourly(
    identifier              string,
    conversion_goal         int,
    media                   string,
    current_error           double,
    prev_error              double,
    last_error              double,
    kp                      double,
    ki                      double,
    kd                      double,
    increment_value         double,
    current_cali            double,
    prev_cali               double,
    cvr_factor              double
)
partitioned by (`date` string, `hour` string, version string, exp_tag string)
stored as parquet;