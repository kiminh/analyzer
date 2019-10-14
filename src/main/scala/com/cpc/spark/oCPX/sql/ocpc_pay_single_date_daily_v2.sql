create table if not exists test.ocpc_pay_single_date_daily_v2(
    unitid                  int,
    userid                  int,
    conversion_goal         int,
    click                   bigint,
    cv                      bigint,
    cost                    double,
    cpagiven                double,
    ocpc_charge_time        string
)
partitioned by (`date` string, version string)
stored as parquet;

--unitid                  int
--userid                  int
--conversion_goal         int
--click                   bigint
--cv                      bigint
--cost                    decimal(23,1)
--cpagiven                double
--ocpc_charge_time        string