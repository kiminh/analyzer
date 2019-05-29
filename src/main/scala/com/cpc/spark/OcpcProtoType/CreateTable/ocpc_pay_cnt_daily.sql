create table if not exists dl_cpc.ocpc_pay_cnt_daily(
    unitid                  int,
    pay_cnt                 int,
    pay_date                string
)
partitioned by (`date` string, version string)
stored as parquet;
