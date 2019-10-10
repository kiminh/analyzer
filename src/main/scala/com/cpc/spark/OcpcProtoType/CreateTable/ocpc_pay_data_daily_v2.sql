create table if not exists test.ocpc_pay_data_daily_v2(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    click                   bigint,
    cv                      bigint,
    cost                    double,
    cpagiven                double,
    pay_cnt                 int,
    pay_date                string
)
partitioned by (`date` string, version string)
stored as parquet;
