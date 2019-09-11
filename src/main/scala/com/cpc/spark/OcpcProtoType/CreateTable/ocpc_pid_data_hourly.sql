create table if not exists test.ocpc_pid_data_hourly
(
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
    prev_cali               double,
    current_cali            double
)
partitioned by (`date` string, `hour` string, exp_tag string, version string)
stored as parquet;
