create table test.ocpc_recall_value_daily (
    id                  string,
    conversion_goal     int,
    recall_value        double
)
partitioned by (`date` string, strat string, hour_diff int)
stored as parquet;