create table if not exists dl_cpc.ocpc_pay_cnt_daily(
    unitid                  int,
    pay_cnt                 int
)
partitioned by (`date` string, version string)
stored as parquet;