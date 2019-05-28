create table if not exists dl_cpc.ocpc_pay_data_daily(
    unitid                  int,
    pay                     bigint,
    cost                    bigint,
    cpareal                 double,
    cpagiven                double,
    start_date              string
)
partitioned by (`date` string, version string)
stored as parquet;