create table if not exists dl_cpc.ocpc_pcoc_jfb_hourly(
    identifier      string,
    pcoc            double,
    jfb             double
)
partitioned by (conversion_goal Int, `date` string, `hour` string, version String)
stored as parquet;


alter table dl_cpc.ocpc_pcoc_jfb_hourly add columns (post_cvr double);