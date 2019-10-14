create table if not exists test.ocpc_pay_cnt_daily_v2(
    unitid                  int,
    pay_cnt                 int,
    pay_date                string,
    flag                    int,
    update_flag             int,
    prev_pay_cnt            int,
    prev_pay_date           string
)
partitioned by (`date` string, version string)
stored as parquet;

--"unitid", "pay_cnt", "pay_date", "flag", "update_flag", "prev_pay_cnt", "prev_pay_date"

