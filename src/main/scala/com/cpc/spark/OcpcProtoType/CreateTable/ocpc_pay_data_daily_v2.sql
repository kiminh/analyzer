create table if not exists test.ocpc_pay_data_daily_v2(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    click                   bigint,
    cv                      bigint,
    cost                    double,
    cpagiven                double
)
partitioned by (`date` string, version string)
stored as parquet;
