create table if not exists test.cv_delay_distribution_daily(
    unitid          int,
    userid          int,
    conversion_goal int,
    hour_diff       int,
    cv              int
)
partitioned by (`date` string)
stored as parquet;