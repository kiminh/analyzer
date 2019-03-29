create table if not exists test.wt_jianzhan_searchids(
    searchid        string
)
partitioned by(`date` string)
stored as parquet;