create table if not exists test.cvr_monitor_daily(
    userid          int,
    media           string,
    click           int,
    cv              int,
    charge          double
)
partitioned by (`date` string, conversion_goal int)
stored as parquet;


create table dl_cpc.cvr_monitor_daily
like test.cvr_monitor_daily