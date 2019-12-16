create table if not exists test.ocpc_compensate_schedule_daily(
    unitid                          int,
    calc_dates                      bigint,
    date_diff                       bigint,
    pay_cnt                         bigint,
    current_ocpc_charge_time        string,
    current_deep_ocpc_charge_time   string,
    ocpc_charge_time                string,
    deep_ocpc_charge_time           string,
    is_pay_flag                     int,
    is_deep_pay_flag                int,
    recent_charge_time              string
)
partitioned by (`date` string, version string)
stored as parquet;