create table if not exists test.ocpc_pay_data_daily_v2(
    unitid                  int,
    click                   bigint,
    cv                      bigint,
    cost                    double,
    cpagiven                double,
    ocpc_charge_time        string,
    pay_cnt                 int,
    pay_date                string,
    restart_flag            int
)
partitioned by (`date` string, version string)
stored as parquet;
