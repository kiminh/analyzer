create table if not exists test.conversion_monitor_for_v2(
    label_type      int,
    cv_today        int,
    cv_yesterday    int,
    is_warn         int
)
partitioned by (`date` string, `hour` string)
stored as parquet;


create table dl_cpc.conversion_monitor_for_v2
like test.conversion_monitor_for_v2