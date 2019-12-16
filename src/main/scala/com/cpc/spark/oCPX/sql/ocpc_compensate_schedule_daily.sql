create table if not exists test.ocpc_compensate_schedule_daily(
    unitid                          int,
    calc_dates                      bigint,
    date_diff                       bigint,
    pay_cnt                         bigint,
    last_ocpc_charge_time           string,
    last_deep_ocpc_charge_time      string,
    is_pay_flag                     int,
    is_deep_pay_flag                int,
    first_charge_time               string,
    final_charge_time               string,
    recent_charge_time              string,
    last_ocpc_charge_time_old       string,
    last_deep_ocpc_charge_time_old  string
)
partitioned by (`date` string, version string)
stored as parquet;