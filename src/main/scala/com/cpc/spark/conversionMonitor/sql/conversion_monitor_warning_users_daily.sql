create table if not exists test.conversion_monitor_warning_users_daily(
    userid          int,
    media           string,
    current_click   int,
    prev_click      int,
    current_cv      int,
    prev_cv         int,
    current_charge  double,
    prev_charge     double
)
partitioned by (`date` string, conversion_goal int)
stored as parquet;


create table dl_cpc.conversion_monitor_warning_users_daily
like test.conversion_monitor_warning_users_daily