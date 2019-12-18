create table if not exists test.model_sample_cmp_hourly(
    adslot_type         int,
    negative_yesterday  int,
    negative_today      int,
    positive_yesterday  int,
    positive_today      int,
    negative_diff       double,
    positive_diff       double,
    ratio_diff          double
)
partitioned by (`date` string, `hour` string, model_name string)
stored as parquet;

--.select("adslot_type", "negative_yesterday", "negative_today", "positive_yesterday", "positive_today", "negative_diff", "positive_diff", "date", "hour", "model_name")

