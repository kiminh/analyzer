create table if not exists test.model_training_data_daily(
    negative_num        int,
    positive_num        int,
    adslot_type         int
)
partitioned by (`date` string, `hour` string, model_name string)
stored as parquet;
